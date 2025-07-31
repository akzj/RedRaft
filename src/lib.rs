use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub type NodeId = String;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// 当前生效的投票节点集合
    pub voters: HashSet<NodeId>,
    /// 如果处于 joint 阶段，则记录旧+新两套配置
    pub joint: Option<JointConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JointConfig {
    pub old: HashSet<NodeId>,
    pub new: HashSet<NodeId>,
}

impl ClusterConfig {
    pub fn empty() -> Self {
        Self {
            voters: HashSet::new(),
            joint: None,
        }
    }
    pub fn simple(voters: HashSet<NodeId>) -> Self {
        Self {
            voters,
            joint: None,
        }
    }
    pub fn enter_joint(&mut self, old: HashSet<NodeId>, new: HashSet<NodeId>) {
        debug_assert!(self.joint.is_none(), "already in joint");
        self.joint = Some(JointConfig {
            old: old.clone(),
            new: new.clone(),
        });
        self.voters = old.union(&new).cloned().collect();
    }
    pub fn leave_joint(&mut self) {
        if let Some(j) = self.joint.take() {
            self.voters = j.new;
        }
    }
    pub fn quorum(&self) -> usize {
        self.voters.len() / 2 + 1
    }
    pub fn joint_quorum(&self) -> Option<(usize, usize)> {
        self.joint
            .as_ref()
            .map(|j| (j.old.len() / 2 + 1, j.new.len() / 2 + 1))
    }
    pub fn contains(&self, id: &NodeId) -> bool {
        self.voters.contains(id)
    }
    pub fn joint_majority(&self, votes: &HashSet<NodeId>) -> bool {
        if let Some(j) = &self.joint {
            votes.intersection(&j.old).count() >= j.old.len() / 2 + 1
                && votes.intersection(&j.new).count() >= j.new.len() / 2 + 1
        } else {
            true
        }
    }
}
use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct InstallSnapshotArgs {
    pub term: u64,
    pub leader_id: NodeId,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub data: Vec<u8>, // 快照数据
}

#[derive(Debug, Clone)]
pub struct InstallSnapshotReply {
    pub term: u64,
    pub success: bool,
}

/// Raft 节点间网络通信接口
use std::future::Future;
use std::pin::Pin;

pub trait Network {
    /// 发送 RequestVote RPC
    fn send_request_vote(
        &self,
        target: NodeId,
        args: RequestVoteArgs,
    ) -> Pin<Box<dyn Future<Output = RequestVoteReply> + Send>>;

    /// 发送 AppendEntries RPC
    fn send_append_entries(
        &self,
        target: NodeId,
        args: AppendEntriesArgs,
    ) -> Pin<Box<dyn Future<Output = AppendEntriesReply> + Send>>;

    /// 发送 InstallSnapshot RPC（可后续扩展）
    fn send_install_snapshot(
        &self,
        target: NodeId,
        args: InstallSnapshotArgs,
    ) -> Pin<Box<dyn Future<Output = InstallSnapshotReply> + Send>>;
}

/// Raft 持久化存储接口
#[derive(Debug)]
pub struct Error(pub String);

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub index: u64,
    pub term: u64,
    pub data: Vec<u8>,
    pub config: ClusterConfig,
}

pub trait Storage {
    /* ========== HardState ========== */
    fn save_hard_state(&mut self, term: u64, voted_for: Option<NodeId>);
    fn load_hard_state(&self) -> (u64, Option<NodeId>);

    /* ========== Log ========== */
    fn append(&mut self, entries: &[LogEntry]) -> Result<(), Error>;
    fn entries(&self, low: u64, high: u64) -> Result<Vec<LogEntry>, Error>;
    fn truncate_suffix(&mut self, idx: u64) -> Result<(), Error>;
    fn truncate_prefix(&mut self, idx: u64) -> Result<(), Error>;
    fn last_index(&self) -> Result<u64, Error>;
    fn term(&self, idx: u64) -> Result<u64, Error>;

    /* ========== Snapshot ========== */
    fn save_snapshot(&mut self, snap: &Snapshot) -> Result<(), Error>;
    fn load_snapshot(&self) -> Result<Snapshot, Error>;

    /* ========== Cluster Config ========== */
    fn save_cluster_config(&mut self, conf: &ClusterConfig) -> Result<(), Error>;
    fn load_cluster_config(&self) -> Result<ClusterConfig, Error>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    Leader,
    Follower,
    Candidate,
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: Vec<u8>, // 可自定义命令类型
}

pub struct RaftNode {
    pub id: NodeId,
    pub peers: Vec<NodeId>, // 当前配置（通常与 config.new 一致）
    pub config: ClusterConfig,
    pub role: Role,
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
    // 日志依赖 Storage trait，不再直接持有 log
    pub commit_index: u64,
    pub last_applied: u64,
    pub next_index: HashMap<NodeId, u64>,
    pub match_index: HashMap<NodeId, u64>,
    pub election_timeout: Duration,
    pub election_timeout_min: u64,
    pub election_timeout_max: u64,
    pub last_heartbeat: Instant,
    pub storage: Box<dyn Storage>,
    pub network: Box<dyn Network + Send + Sync>,
}

#[derive(Debug, Clone)]
pub struct RequestVoteArgs {
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone)]
pub struct RequestVoteReply {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Debug, Clone)]
pub struct AppendEntriesArgs {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone)]
pub struct AppendEntriesReply {
    pub term: u64,
    pub success: bool,
    pub conflict_index: Option<u64>,
}

impl RaftNode {
    pub fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        config: ClusterConfig,
        storage: Box<dyn Storage>,
        network: Box<dyn Network + Send + Sync>,
        election_timeout_min: u64,
        election_timeout_max: u64,
    ) -> Self {
        let (current_term, voted_for) = storage.load_hard_state();
        let loaded_config = match storage.load_cluster_config() {
            Ok(conf) => conf,
            Err(_) => ClusterConfig::empty(),
        };
        let timeout = election_timeout_min
            + rand::random::<u64>() % (election_timeout_max - election_timeout_min + 1);
        RaftNode {
            id,
            peers,
            config: loaded_config,
            role: Role::Follower,
            current_term,
            voted_for,
            // log 由 Storage trait 管理
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            election_timeout: Duration::from_millis(timeout),
            election_timeout_min,
            election_timeout_max,
            last_heartbeat: Instant::now(),
            storage,
            network,
        }
    }

    // 选举相关
    pub async fn start_election(&mut self) {
        // 递增 term，切换 candidate，重置 election timeout，发送 RequestVote
        self.current_term += 1;
        self.role = Role::Candidate;
        self.voted_for = Some(self.id.clone());
        let election_timeout = self.election_timeout_min
            + rand::random::<u64>() % (self.election_timeout_max - self.election_timeout_min + 1);
        self.election_timeout = Duration::from_millis(election_timeout);
        self.last_heartbeat = Instant::now();
        // 持久化 term 和 voted_for
        self.storage
            .save_hard_state(self.current_term, self.voted_for.clone());

        // 统一投票集合（joint时为old+new，否则为voters）
        let effective: std::collections::HashSet<NodeId> = match self.config.joint.as_ref() {
            Some(j) => j.old.union(&j.new).cloned().collect(),
            None => self.config.voters.clone(),
        };
        let last_index = self.storage.last_index().unwrap_or(0);
        let last_term = self.storage.term(last_index).unwrap_or(0);
        let args = RequestVoteArgs {
            term: self.current_term,
            candidate_id: self.id.clone(),
            last_log_index: last_index,
            last_log_term: last_term,
        };

        use tokio::time::{Duration as TokioDuration, timeout};
        let rpc_timeout = TokioDuration::from_millis(300);
        let mut max_term = self.current_term;
        use std::collections::HashMap;
        let mut results: HashMap<NodeId, Result<RequestVoteReply, tokio::time::error::Elapsed>> =
            HashMap::new();
        // 自身投票直接插入
        results.insert(
            self.id.clone(),
            Ok(RequestVoteReply {
                term: self.current_term,
                vote_granted: true,
            }),
        );
        let mut futs = Vec::new();
        for peer in &effective {
            if *peer == self.id {
                continue;
            }
            let net = self.network.as_ref();
            let args = args.clone();
            futs.push((
                peer.clone(),
                timeout(rpc_timeout, net.send_request_vote(peer.clone(), args)),
            ));
        }
        let joined = futures::future::join_all(
            futs.into_iter()
                .map(|(peer, fut)| async move { (peer, fut.await) }),
        )
        .await;
        for (peer, reply) in joined {
            results.insert(peer, reply);
        }
        // 统计票数
        for reply in results.values() {
            if let Ok(r) = reply {
                if r.term > max_term {
                    max_term = r.term;
                }
            }
        }

        // 如果收到更高 term，降级为 follower
        if max_term > self.current_term {
            self.current_term = max_term;
            self.role = Role::Follower;
            self.voted_for = None;
            self.storage
                .save_hard_state(self.current_term, self.voted_for.clone());
            return;
        }

        // 判断是否赢得选举（联合共识需新旧配置均过半）
        let win = if let Some(joint) = self.config.joint.as_ref() {
            let votes_old = results
                .iter()
                .filter(|(id, r)| {
                    r.as_ref().map_or(false, |v| v.vote_granted) && joint.old.contains(&**id)
                })
                .count();
            let votes_new = results
                .iter()
                .filter(|(id, r)| {
                    r.as_ref().map_or(false, |v| v.vote_granted) && joint.new.contains(&**id)
                })
                .count();
            votes_old >= (joint.old.len() / 2 + 1) && votes_new >= (joint.new.len() / 2 + 1)
        } else {
            let votes = results
                .iter()
                .filter(|(_, r)| r.as_ref().map_or(false, |v| v.vote_granted))
                .count();
            votes > effective.len() / 2
        };
        if win {
            self.role = Role::Leader;
            // 初始化 next_index/match_index
            let last_index = self.storage.last_index().unwrap_or(0);
            for peer in &self.peers {
                self.next_index.insert(peer.clone(), last_index + 1);
                self.match_index.insert(peer.clone(), 0);
            }
            // 可立即广播空心跳
            self.broadcast_append_entries(vec![], self.commit_index);
        } else {
            // 分裂投票，重新设置 election_timeout，等待下一轮
            let timeout = self.election_timeout_min
                + rand::random::<u64>()
                    % (self.election_timeout_max - self.election_timeout_min + 1);
            self.election_timeout = Duration::from_millis(timeout);
            // 保持 Candidate 状态，等待下一轮
        }
    }

    /// 发送 AppendEntries RPC 到所有 follower
    pub fn broadcast_append_entries(&self, entries: Vec<LogEntry>, leader_commit: u64) {
        let prev_log_index = self.storage.last_index().unwrap_or(0);
        let prev_log_term = self.storage.term(prev_log_index).unwrap_or(0);
        let args = AppendEntriesArgs {
            term: self.current_term,
            leader_id: self.id.clone(),
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        };
        for peer in &self.peers {
            let _reply = self.network.send_append_entries(peer.clone(), args.clone());
            // 可根据 reply.success 处理日志复制
        }
    }

    /// 发送 InstallSnapshot RPC 到所有 follower
    pub fn broadcast_install_snapshot(&self, snapshot: InstallSnapshotArgs) {
        for peer in &self.peers {
            let _reply = self
                .network
                .send_install_snapshot(peer.clone(), snapshot.clone());
            // 可根据 reply.success 处理快照同步
        }
    }

    pub fn handle_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        // 1. 若 term 更大，无条件降级并更新
        if args.term > self.current_term {
            self.current_term = args.term;
            self.role = Role::Follower;
            self.voted_for = None;
        }

        // 2. 日志最新性检查
        let last_idx = self.storage.last_index().unwrap_or(0);
        let last_term = self.storage.term(last_idx).unwrap_or(0);
        let log_ok = args.last_log_term > last_term
            || (args.last_log_term == last_term && args.last_log_index >= last_idx);

        // 3. 投票规则
        let mut vote_granted = false;
        if args.term == self.current_term
            && (self.voted_for.is_none() || self.voted_for == Some(args.candidate_id.clone()))
            && log_ok
        {
            self.voted_for = Some(args.candidate_id.clone());
            vote_granted = true;
        }

        // 4. 只在投票成功或 term 变化时写盘
        if vote_granted || args.term > self.current_term {
            self.storage
                .save_hard_state(self.current_term, self.voted_for.clone());
        }

        RequestVoteReply {
            term: self.current_term,
            vote_granted,
        }
    }

    // 日志复制相关
    pub fn handle_append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        // 1. term 太小直接拒绝
        if args.term < self.current_term {
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
                conflict_index: Some(self.storage.last_index().unwrap_or(0) + 1),
            };
        }

        // 2. 更新 leader 心跳和状态
        self.last_heartbeat = Instant::now();
        self.current_term = args.term;
        self.role = Role::Follower;

        // 3. 日志连续性检查
        let last_idx = self.storage.last_index().unwrap_or(0);
        if args.prev_log_index > last_idx {
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
                conflict_index: Some(last_idx + 1),
            };
        }

        // 4. prev_term 匹配检查
        let prev_term = self.storage.term(args.prev_log_index).unwrap_or(0);
        if prev_term != args.prev_log_term {
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
                conflict_index: Some(args.prev_log_index),
            };
        }

        // 5. 截断 + 追加
        let _ = self.storage.truncate_suffix(args.prev_log_index + 1);
        let _ = self.storage.append(&args.entries);

        // 6. 更新 commit_index
        let new_last = self.storage.last_index().unwrap_or(0);
        self.commit_index = std::cmp::min(args.leader_commit, new_last);

        AppendEntriesReply {
            term: self.current_term,
            success: true,
            conflict_index: None,
        }
    }

    // 快照、持久化、异常处理等可后续补充
    // ...existing code...
}
