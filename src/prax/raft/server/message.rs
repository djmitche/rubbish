use super::log::LogItem;
use crate::prax::raft::diststate::{self, DistributedState, Request};
use crate::prax::raft::log::LogEntry;
use crate::net::NodeId;
use crate::prax::raft::{Index, Term};
use serde_json::{self, json};

#[derive(Debug, PartialEq, Default)]
pub(super) struct AppendEntriesReq<DS>
where
    DS: DistributedState,
{
    pub(super) term: Term,
    pub(super) leader: NodeId,
    pub(super) prev_log_index: Index,
    pub(super) prev_log_term: Term,
    pub(super) entries: Vec<LogEntry<LogItem<DS::Request>>>,
    pub(super) leader_commit: Index,
}

#[derive(Debug, PartialEq, Default)]
pub(super) struct AppendEntriesRep {
    pub(super) term: Term,
    pub(super) next_index: Index,
    pub(super) success: bool,
}

#[derive(Debug, PartialEq, Default)]
pub(super) struct RequestVoteReq {
    pub(super) term: Term,
    pub(super) candidate_id: NodeId,
    pub(super) last_log_index: Index,
    pub(super) last_log_term: Term,
}

#[derive(Debug, PartialEq, Default)]
pub(super) struct RequestVoteRep {
    pub(super) term: Term,
    pub(super) vote_granted: bool,
}

/// Messages transferred between Raft nodes
#[derive(Debug, PartialEq)]
pub(super) enum Message<DS>
where
    DS: DistributedState,
{
    AppendEntriesReq(AppendEntriesReq<DS>),
    AppendEntriesRep(AppendEntriesRep),
    RequestVoteReq(RequestVoteReq),
    RequestVoteRep(RequestVoteRep),
}

impl<DS> Message<DS>
where
    DS: DistributedState,
{
    pub(super) fn serialize(&self) -> Vec<u8> {
        let value = match *self {
            Message::AppendEntriesReq(ref v) => {
                let entries: Vec<serde_json::Value> = v
                    .entries
                    .iter()
                    .map(|e| {
                        json!({
                            "term": e.term,
                            "item_req":e.item.req.serialize(),
                        })
                    })
                    .collect();

                json!({
                    "type": "AppendEntriesReq",
                    "term": v.term,
                    "leader": v.leader,
                    "prev_log_index": v.prev_log_index,
                    "prev_log_term": v.prev_log_term,
                    "entries": entries,
                    "leader_commit": v.leader_commit,
                })
            }
            Message::AppendEntriesRep(ref v) => json!( {
                "type": "AppendEntriesRep",
                "term": v.term,
                "next_index": v.next_index,
                "success": v.success,
            }),
            Message::RequestVoteReq(ref v) => json!({
                "type": "RequestVoteReq",
                "term": v.term,
                "candidate_id": v.candidate_id,
                "last_log_index": v.last_log_index,
                "last_log_term": v.last_log_term,
            }),
            Message::RequestVoteRep(ref v) => json!({
                "type": "RequestVoteRep",
                "term": v.term,
                "vote_granted": v.vote_granted,
            }),
        };
        // TODO: better way??
        value.to_string().as_bytes().to_vec()
    }

    pub(super) fn deserialize(ser: &[u8]) -> Self {
        let value: serde_json::Value =
            serde_json::from_str(std::str::from_utf8(ser).unwrap()).unwrap();
        let typ = value.get("type").unwrap().as_str().unwrap();
        match typ {
            "AppendEntriesReq" => {
                let entries = value
                    .get("entries")
                    .unwrap()
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|e| {
                        let term = e.get("term").unwrap().as_u64().unwrap();
                        let item_req = e.get("item_req").unwrap();
                        LogEntry {
                            term,
                            item: LogItem {
                                req: <DS as diststate::DistributedState>::Request::deserialize(
                                    item_req,
                                ),
                            },
                        }
                    })
                    .collect();
                Message::AppendEntriesReq(AppendEntriesReq {
                    term: value.get("term").unwrap().as_u64().unwrap(),
                    leader: value.get("leader").unwrap().as_u64().unwrap() as usize,
                    prev_log_index: value.get("prev_log_index").unwrap().as_u64().unwrap(),
                    prev_log_term: value.get("prev_log_term").unwrap().as_u64().unwrap(),
                    entries,
                    leader_commit: value.get("leader_commit").unwrap().as_u64().unwrap(),
                })
            }
            "AppendEntriesRep" => Message::AppendEntriesRep(AppendEntriesRep {
                term: value.get("term").unwrap().as_u64().unwrap(),
                next_index: value.get("next_index").unwrap().as_u64().unwrap(),
                success: value.get("success").unwrap().as_bool().unwrap(),
            }),
            "RequestVoteReq" => Message::RequestVoteReq(RequestVoteReq {
                term: value.get("term").unwrap().as_u64().unwrap(),
                candidate_id: value.get("candidate_id").unwrap().as_u64().unwrap() as usize,
                last_log_index: value.get("last_log_index").unwrap().as_u64().unwrap(),
                last_log_term: value.get("last_log_term").unwrap().as_u64().unwrap(),
            }),
            "RequestVoteRep" => Message::RequestVoteRep(RequestVoteRep {
                term: value.get("term").unwrap().as_u64().unwrap(),
                vote_granted: value.get("vote_granted").unwrap().as_bool().unwrap(),
            }),
            _ => unreachable!(),
        }
    }
}
