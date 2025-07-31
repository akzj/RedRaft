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
pub trait Storage {
    /// 持久化当前任期
    fn save_current_term(&mut self, term: u64);
    fn load_current_term(&self) -> u64;

    /// 持久化投票对象
    fn save_voted_for(&mut self, voted_for: Option<NodeId>);
    fn load_voted_for(&self) -> Option<NodeId>;

    /// 持久化日志条目
    fn save_log(&mut self, log: &Vec<LogEntry>);
    fn load_log(&self) -> Vec<LogEntry>;

    /// 持久化集群配置
    fn save_cluster_config(&mut self, config: &ClusterConfig);
    fn load_cluster_config(&self) -> ClusterConfig;
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
    pub log: Vec<LogEntry>,
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
        let current_term = storage.load_current_term();
        let voted_for = storage.load_voted_for();
        let log = storage.load_log();
        let loaded_config = storage.load_cluster_config();
        let timeout = election_timeout_min
            + rand::random::<u64>() % (election_timeout_max - election_timeout_min + 1);
        RaftNode {
            id,
            peers,
            config: loaded_config,
            role: Role::Follower,
            current_term,
            voted_for,
            log,
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
        self.storage.save_current_term(self.current_term);
        self.storage.save_voted_for(self.voted_for.clone());

        // 统一投票集合（joint时为old+new，否则为voters）
        let effective: std::collections::HashSet<NodeId> = match self.config.joint.as_ref() {
            Some(j) => j.old.union(&j.new).cloned().collect(),
            None => self.config.voters.clone(),
        };
        let args = RequestVoteArgs {
            term: self.current_term,
            candidate_id: self.id.clone(),
            last_log_index: self.log.len() as u64,
            last_log_term: self.log.last().map_or(0, |entry| entry.term),
        };

        use tokio::time::{Duration as TokioDuration, timeout};
        let rpc_timeout = TokioDuration::from_millis(300);
        let mut max_term = self.current_term;
        use std::collections::HashMap;
        let mut results: HashMap<NodeId, Result<RequestVoteReply, tokio::time::error::Elapsed>> = HashMap::new();
        // 自身投票直接插入
        results.insert(self.id.clone(), Ok(RequestVoteReply {
            term: self.current_term,
            vote_granted: true,
        }));
        let mut futs = Vec::new();
        for peer in &effective {
            if *peer == self.id {
                continue;
            }
            let net = self.network.as_ref();
            let args = args.clone();
            futs.push((peer.clone(), timeout(rpc_timeout, net.send_request_vote(peer.clone(), args))));
        }
        let joined = futures::future::join_all(futs.into_iter().map(|(peer, fut)| async move { (peer, fut.await) })).await;
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
            self.storage.save_current_term(self.current_term);
            self.storage.save_voted_for(self.voted_for.clone());
            return;
        }

        // 判断是否赢得选举（联合共识需新旧配置均过半）
        let win = if let Some(joint) = self.config.joint.as_ref() {
            let votes_old = results.iter()
                            .filter(|(id, r)| r.as_ref().map_or(false, |v| v.vote_granted) && joint.old.contains(&**id))
                            .count();
            let votes_new = results.iter()
                            .filter(|(id, r)| r.as_ref().map_or(false, |v| v.vote_granted) && joint.new.contains(&**id))
                            .count();
            votes_old >= (joint.old.len() / 2 + 1) && votes_new >= (joint.new.len() / 2 + 1)
        } else {
            let votes = results.iter()
                .filter(|(_, r)| r.as_ref().map_or(false, |v| v.vote_granted))
                .count();
            votes > effective.len() / 2
        };
        if win {
            self.role = Role::Leader;
            // 初始化 next_index/match_index
            let last_index = self.log.last().map_or(0, |e| e.index);
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
        let prev_log_index = self.log.last().map_or(0, |e| e.index);
        let prev_log_term = self.log.last().map_or(0, |e| e.term);
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
        // 如果接收到的请求的 term 较大，更新当前节点状态
        if args.term > self.current_term {
            self.current_term = args.term;
            self.role = Role::Follower;
            self.voted_for = None;
            self.storage.save_current_term(self.current_term);
            self.storage.save_voted_for(self.voted_for.clone());
        }

        // 投票规则校验
        let mut vote_granted = false;

        // 1. 检查 term
        if args.term == self.current_term {
            // 2. 检查是否已经投票给其他候选人
            if self.voted_for.is_none() {
                self.voted_for = Some(args.candidate_id.clone());
                vote_granted = true;
                self.storage.save_voted_for(self.voted_for.clone());
            }
        } else if args.term > self.current_term {
            // 3. 如果收到的请求的 term 较大，更新当前节点状态
            self.current_term = args.term;
            self.role = Role::Follower;
            self.voted_for = Some(args.candidate_id.clone());
            vote_granted = true;
            self.storage.save_current_term(self.current_term);
            self.storage.save_voted_for(self.voted_for.clone());
        }

        RequestVoteReply {
            term: self.current_term,
            vote_granted,
        }
    }

    // 日志复制相关
    pub fn handle_append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        // 如果 term 较小，拒绝服务
        if args.term < self.current_term {
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
                conflict_index: None,
            };
        }

        // 更新 leader 信息
        self.last_heartbeat = Instant::now();

        // 如果接收到的日志条目不连续，返回冲突索引
        if args.prev_log_index > 0 && self.log.len() as u64 <= args.prev_log_index {
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
                conflict_index: Some(args.prev_log_index),
            };
        }

        // 日志匹配，追加
        let mut conflict_index = None;
        if let Some(last_entry) = self.log.last_mut() {
            if last_entry.term == args.prev_log_term {
                // 索引连续，直接追加
                self.log.truncate(args.prev_log_index as usize);
                self.log.extend(args.entries);
                conflict_index = Some(args.prev_log_index);
                self.storage.save_log(&self.log);
            } else {
                // 索引不连续，找到冲突点
                conflict_index = Some(args.prev_log_index);
                self.log.truncate(args.prev_log_index as usize);
                self.log.extend(args.entries);
                self.storage.save_log(&self.log);
            }
        } else {
            // 日志为空，直接追加
            self.log.extend(args.entries);
            conflict_index = Some(args.prev_log_index);
            self.storage.save_log(&self.log);
        }

        // 更新提交索引
        if args.leader_commit > self.commit_index {
            self.commit_index = std::cmp::min(args.leader_commit, self.log.len() as u64);
        }

        AppendEntriesReply {
            term: self.current_term,
            success: true,
            conflict_index,
        }
    }

    // 快照、持久化、异常处理等可后续补充
    // ...existing code...
}
