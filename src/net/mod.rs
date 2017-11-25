//! The `net` module implements network support for all levels of the Rubbish application.
//!
//! It implements a datagram-like model, layered over IPv6-only UDP.
//!
//! This is a thin layer over the `mio` crate.

mod error;

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

#[cfg(test)]
mod test {
    use super::Node;
    use util::test::init_env_logger;
    use std::time::Duration;

    #[test]
    fn test_stop() {
        init_env_logger();
        let mut node = Node::new().unwrap();

        node.stop().unwrap();
        node.serve().unwrap();
    }

    #[test]
    fn test_timeout() {
        init_env_logger();
        let mut node = Node::new().unwrap();

        node.set_timeout(Duration::from_millis(1), {
            |node: &Node| node.stop().unwrap()
        }).unwrap();
        node.serve().unwrap();
    }
}
