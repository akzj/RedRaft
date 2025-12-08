use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

// Type definitions
pub type GroupId = String;
pub type Command = Vec<u8>;
pub type TimerId = u64;
pub type NodeId = String;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct RaftId {
    pub group: GroupId,
    pub node: NodeId,
}

impl Display for RaftId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.group, self.node)
    }
}

impl RaftId {
    pub fn new(group: GroupId, node: NodeId) -> Self {
        Self { group, node }
    }
}

/// Request ID type (used to filter timed-out responses)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub struct RequestId(u64);

impl RequestId {
    pub fn new() -> Self {
        Self(rand::random::<u64>())
    }
}

impl Default for RequestId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<RequestId> for u64 {
    fn from(val: RequestId) -> Self {
        val.0
    }
}

impl From<u64> for RequestId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

