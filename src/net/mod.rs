//! The `rubbish::net` module implements network support for all levels of the Rubbish application.

mod error;
mod transport;

use tokio_core::reactor::Handle;
use tokio_core::net::{TcpListener, TcpStream};
use tokio_io::AsyncRead;
use futures::{future, Future, Stream, Sink};
use std::net::SocketAddr;

use net::error::*;

pub struct Node {
    address: SocketAddr,
    handle: Handle,
}

type EmptyFuture = Box<Future<Item = (), Error = Error>>;
type TransportSink = Sink<SinkItem = String, SinkError = Error>;
type TransportStream = Stream<Item = String, Error = Error>;

impl Node {
    /// Create a new node
    pub fn new(address: SocketAddr, handle: Handle) -> Node {
        Node { address, handle }
    }

    /// Begin running this node
    pub fn run(self) -> EmptyFuture {
        let listen_fut = self.listen_for_connections();
        listen_fut
    }

    fn listen_for_connections(&self) -> EmptyFuture {
        let handle = self.handle.clone();

        let listener = match TcpListener::bind(&self.address, &self.handle) {
            Ok(listener) => listener,
            Err(e) => {
                return Box::new(future::err(e.into()));
            }
        };

        // for each connection, create a new neighbor
        let connections = listener.incoming();
        Box::new(
            connections
                .for_each(move |(socket, peer)| {
                    let neighbor = Neighbor::new(peer);
                    handle.spawn(neighbor.run(socket));

                    future::ok(())
                })
                .map_err(|e| e.into()),
        )
    }
}

pub struct Neighbor {
    peer: SocketAddr,
}

impl Neighbor {
    fn new(peer: SocketAddr) -> Neighbor {

        debug!("new neighbor {}", peer);
        let neighbor = Neighbor { peer };

        neighbor
    }

    fn run(&self, socket: TcpStream) -> Box<Future<Item = (), Error = ()>> {
        let (writer, reader) = socket.framed(transport::new()).split();
        let peer = self.peer;

        Box::new(
            writer
                .send_all(reader.and_then(move |req| future::ok(req)))
                .then(move |_| {
                    debug!("connection to neighbor {} closed", peer);
                    Ok(())
                }),
        )
    }
}

/*
use net::error::*;
use mio::{Events, Poll, Token, Ready, PollOpt};
use mio::net::UdpSocket;
use mio_more::channel::{channel, Sender, Receiver};
use mio_more::timer::{Timer, Timeout, Builder};
use std::time::Duration;
use std::cell::RefCell;

enum ControlMessage {
    Stop,
}

// TODO: doc
pub struct Node {
    events: RefCell<Events>,
    poll: RefCell<Poll>,

    // network communication
    socket: UdpSocket,

    // time management
    timer: RefCell<Timer<Box<TimeoutHandler>>>,

    // control of the node
    control_sender: Sender<ControlMessage>,
    control_receiver: Receiver<ControlMessage>,
}

const CONTROL: Token = Token(0);
const TIMER: Token = Token(1);
const PACKET: Token = Token(2);

const PACKET_SIZE: usize = 1024 * 1024;

impl Node {
    /// Generate a new node.
    fn new() -> Result<Node> {
        let addr = "[::]:0".parse()?;
        let socket = UdpSocket::bind(&addr)?;

        let (control_sender, control_receiver) = channel();

        let timer = Builder::default().build();

        let node = Node {
            events: RefCell::new(Events::with_capacity(1024)),
            poll: RefCell::new(Poll::new()?),
            socket: socket,
            timer: RefCell::new(timer),
            control_sender: control_sender,
            control_receiver: control_receiver,
        };
        node.poll.borrow().register(
            &node.socket,
            PACKET,
            // only register for readable; we don't do output buffering
            Ready::readable(),
            PollOpt::level(),
        )?;

        node.poll.borrow().register(
            &*node.timer.borrow(),
            TIMER,
            Ready::readable(),
            PollOpt::level(),
        )?;

        node.poll.borrow().register(
            &node.control_receiver,
            CONTROL,
            Ready::readable(),
            PollOpt::level(),
        )?;

        Ok(node)
    }

    /// Stop this node from serving.
    pub fn stop(&self) -> Result<()> {
        self.control_sender.send(ControlMessage::Stop).chain_err(
            || "could not stop node",
        )
    }

    /// Do a thing after a bit
    pub fn set_timeout<H: 'static + TimeoutHandler>(
        &self,
        delay_from_now: Duration,
        handler: H,
    ) -> Result<Timeout> {
        Ok(self.timer.borrow_mut().set_timeout(
            delay_from_now,
            Box::new(handler),
        )?)
    }

    // TODO cancel_timeout

    /// Begin looping, serving events for this node.
    pub fn serve(&mut self) -> Result<()> {
        let mut buf = [0u8; PACKET_SIZE];

        debug!("node {:p} starting", self);

        loop {
            self.poll.borrow().poll(&mut self.events.borrow_mut(), None)?;

            for event in self.events.borrow_mut().iter() {
                debug!("{:?}", event);
                match event.token() {
                    CONTROL => {
                        let msg = self.control_receiver.try_recv()?;
                        match msg {
                            ControlMessage::Stop => {
                                debug!("node {:p} stopped", self);
                                return Ok(());
                            }
                        }
                    }
                    TIMER => {
                        if let Some(handler) = self.timer.borrow_mut().poll() {
                            debug!("invoking timeout handler");
                            handler.handle(self);
                        }
                    }
                    PACKET => {
                        let bytes = self.socket.recv(&mut buf)?;
                        debug!("{:?}", buf[0..bytes].to_vec());
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
}

pub trait TimeoutHandler {
    /// Handle the timeout firing.
    fn handle(&self, node: &Node);
}

impl<T: Fn(&Node)> TimeoutHandler for T {
    fn handle(&self, node: &Node) {
        self(node)
    }
}
*/

#[cfg(test)]
mod test {
    use super::Node;
    use util::test::init_env_logger;
    use tokio_core::reactor::Core;

    #[test]
    fn test_echo() {
        init_env_logger();

        let mut core = Core::new().unwrap();
        let node = Node::new("0.0.0.0:12345".parse().unwrap(), core.handle());
        core.run(node.run()).unwrap();
    }
}
