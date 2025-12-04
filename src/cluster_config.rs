use std::collections::HashSet;

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use crate::{RaftId, error::ConfigError};

/// Quorum要求
#[derive(Debug, Clone, PartialEq)]
pub enum QuorumRequirement {
    Simple(usize),
    Joint { old: usize, new: usize },
}

// 为错误类型实现便捷构造函数

// === 集群配置 ===
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Decode, Encode)]
pub struct ClusterConfig {
    pub epoch: u64,     // 配置版本号
    pub log_index: u64, // 最后一次配置变更的日志索引
    pub voters: HashSet<RaftId>,
    pub learners: Option<HashSet<RaftId>>, // 不具有投票权的学习者节点
    pub joint: Option<JointConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Decode, Encode)]
pub struct JointConfig {
    pub log_index: u64, // 最后一次配置变更的日志索引

    pub old_voters: HashSet<RaftId>,
    pub new_voters: HashSet<RaftId>,

    pub old_learners: Option<HashSet<RaftId>>, // 旧配置中的 Learners
    pub new_learners: Option<HashSet<RaftId>>, // 新配置中的 Learners
}

impl ClusterConfig {
    pub fn empty() -> Self {
        Self {
            joint: None,
            epoch: 0,
            log_index: 0,
            learners: None,
            voters: HashSet::new(),
        }
    }

    pub fn log_index(&self) -> u64 {
        self.joint
            .as_ref()
            .map(|j| j.log_index)
            .unwrap_or(self.log_index)
    }

    pub fn simple(voters: HashSet<RaftId>, log_index: u64) -> Self {
        Self {
            learners: None,
            voters,
            epoch: 0,
            log_index,
            joint: None,
        }
    }

    pub fn with_learners(
        voters: HashSet<RaftId>,
        learners: Option<HashSet<RaftId>>,
        log_index: u64,
    ) -> Self {
        Self {
            learners,
            voters,
            epoch: 0,
            log_index,
            joint: None,
        }
    }

    pub fn enter_joint(
        &mut self,
        old_voters: HashSet<RaftId>,
        new_voters: HashSet<RaftId>,
        old_learners: Option<HashSet<RaftId>>,
        new_learners: Option<HashSet<RaftId>>,
        log_index: u64,
    ) -> Result<(), ConfigError> {
        // 验证配置
        if old_voters.is_empty() || new_voters.is_empty() {
            return Err(ConfigError::EmptyConfig);
        }

        if self.joint.is_some() {
            return Err(ConfigError::AlreadyInJoint);
        }

        // 验证 Learners 不能同时是 Voters (在任何配置中)
        let voter_in_learners = |v: &RaftId| {
            old_learners.as_ref().is_some_and(|l| l.contains(v))
                || new_learners.as_ref().is_some_and(|l| l.contains(v))
        };
        if old_voters.iter().any(voter_in_learners) || new_voters.iter().any(voter_in_learners) {
            return Err(ConfigError::InvalidJoint(
                "A node cannot be both a Voter and a Learner in the same configuration".into(),
            ));
        }

        self.epoch += 1;
        self.joint = Some(JointConfig {
            log_index,
            old_voters: old_voters.clone(),
            new_voters: new_voters.clone(),
            new_learners: new_learners.clone(),
            old_learners: old_learners.clone(),
        });
        self.voters = old_voters.union(&new_voters).cloned().collect();
        Ok(())
    }

    pub fn leave_joint(&mut self, log_index: u64) -> Result<Self, ConfigError> {
        match self.joint.take() {
            Some(j) => {
                self.voters = j.new_voters.clone();
                Ok(Self {
                    log_index,
                    joint: None,
                    epoch: self.epoch,
                    voters: j.new_voters,
                    learners: j.new_learners.clone(),
                })
            }
            None => Err(ConfigError::NotInJoint),
        }
    }

    pub fn quorum(&self) -> QuorumRequirement {
        match &self.joint {
            Some(j) => QuorumRequirement::Joint {
                old: j.old_voters.len() / 2 + 1,
                new: j.new_voters.len() / 2 + 1,
            },
            None => QuorumRequirement::Simple(self.voters.len() / 2 + 1),
        }
    }

    pub fn joint_quorum(&self) -> Option<(usize, usize)> {
        self.joint
            .as_ref()
            .map(|j| (j.old_voters.len() / 2 + 1, j.new_voters.len() / 2 + 1))
    }

    pub fn voters_contains(&self, id: &RaftId) -> bool {
        self.voters.contains(id)
    }

    pub fn majority(&self, votes: &HashSet<RaftId>) -> bool {
        if let Some(j) = &self.joint {
            votes.intersection(&j.old_voters).count() > j.old_voters.len() / 2
                && votes.intersection(&j.new_voters).count() > j.new_voters.len() / 2
        } else {
            votes.len() > self.voters.len() / 2
        }
    }

    pub fn is_joint(&self) -> bool {
        self.joint.is_some()
    }

    pub fn get_effective_voters(&self) -> &HashSet<RaftId> {
        &self.voters
    }

    // 验证配置是否合法
    pub fn is_valid(&self) -> bool {
        // 确保配置不会导致无法形成多数派
        if self.voters.is_empty() {
            return false;
        }

        // 对于联合配置，确保新旧配置都能形成多数派
        if let Some(joint) = &self.joint {
            if joint.old_voters.is_empty() || joint.new_voters.is_empty() {
                return false;
            }
        }

        true
    }

    /// 检查是否是单节点集群
    pub fn is_single_voter(&self) -> bool {
        self.voters.len() == 1 && !self.is_joint()
    }

    // === Learner 管理方法 ===
    pub fn add_learner(&mut self, learner: RaftId, log_index: u64) -> Result<(), ConfigError> {
        // 验证 learner 不是 voter
        if self.voters.contains(&learner) {
            return Err(ConfigError::InvalidJoint(
                "Cannot add a voter as learner".into(),
            ));
        }

        // 如果正在 joint 配置中，不允许修改 learners
        if self.is_joint() {
            return Err(ConfigError::AlreadyInJoint);
        }

        let mut learners = self.learners.take().unwrap_or_default();
        learners.insert(learner);
        self.learners = Some(learners);
        self.log_index = log_index;
        self.epoch += 1;
        Ok(())
    }

    pub fn remove_learner(&mut self, learner: &RaftId, log_index: u64) -> Result<(), ConfigError> {
        // 如果正在 joint 配置中，不允许修改 learners
        if self.is_joint() {
            return Err(ConfigError::AlreadyInJoint);
        }

        if let Some(ref mut learners) = self.learners {
            learners.remove(learner);
            if learners.is_empty() {
                self.learners = None;
            }
        }
        self.log_index = log_index;
        self.epoch += 1;
        Ok(())
    }

    pub fn get_learners(&self) -> Option<&HashSet<RaftId>> {
        self.learners.as_ref()
    }

    pub fn learners_contains(&self, id: &RaftId) -> bool {
        self.learners.as_ref().is_some_and(|l| l.contains(id))
    }

    pub fn get_all_nodes(&self) -> HashSet<RaftId> {
        let mut all_nodes = self.voters.clone();
        if let Some(ref learners) = self.learners {
            all_nodes.extend(learners.iter().cloned());
        }
        all_nodes
    }

    pub(crate) fn joint(&self) -> Option<&JointConfig> {
        self.joint.as_ref()
    }

    pub(crate) fn learners(&self) -> Option<&HashSet<RaftId>> {
        self.learners.as_ref()
    }
}
