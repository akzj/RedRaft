use std::collections::HashSet;

use bincode::{Decode, Encode};
use serde::{Deserialize, Serialize};

use crate::{RaftId, error::ConfigError};

/// Quorum requirement
#[derive(Debug, Clone, PartialEq)]
pub enum QuorumRequirement {
    Simple(usize),
    Joint { old: usize, new: usize },
}

// Implement convenient constructors for error types

// === Cluster Configuration ===
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Decode, Encode)]
pub struct ClusterConfig {
    pub epoch: u64,     // Configuration version number
    pub log_index: u64, // Log index of the last configuration change
    pub voters: HashSet<RaftId>,
    pub learners: Option<HashSet<RaftId>>, // Learner nodes without voting rights
    pub joint: Option<JointConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Decode, Encode)]
pub struct JointConfig {
    pub log_index: u64, // Log index of the last configuration change

    pub old_voters: HashSet<RaftId>,
    pub new_voters: HashSet<RaftId>,

    pub old_learners: Option<HashSet<RaftId>>, // Learners in old configuration
    pub new_learners: Option<HashSet<RaftId>>, // Learners in new configuration
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
        // Validate configuration
        if old_voters.is_empty() || new_voters.is_empty() {
            return Err(ConfigError::EmptyConfig);
        }

        if self.joint.is_some() {
            return Err(ConfigError::AlreadyInJoint);
        }

        // Validate that Learners cannot be Voters simultaneously (in any configuration)
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

    // Validate if configuration is legal
    pub fn is_valid(&self) -> bool {
        // Ensure configuration allows forming a majority
        if self.voters.is_empty() {
            return false;
        }

        // For joint configuration, ensure both old and new configurations can form a majority
        if let Some(joint) = &self.joint {
            if joint.old_voters.is_empty() || joint.new_voters.is_empty() {
                return false;
            }
        }

        true
    }

    /// Check if it's a single-node cluster
    pub fn is_single_voter(&self) -> bool {
        self.voters.len() == 1 && !self.is_joint()
    }

    // === Learner Management Methods ===
    pub fn add_learner(&mut self, learner: RaftId, log_index: u64) -> Result<(), ConfigError> {
        // Validate learner is not a voter
        if self.voters.contains(&learner) {
            return Err(ConfigError::InvalidJoint(
                "Cannot add a voter as learner".into(),
            ));
        }

        // Cannot modify learners while in joint configuration
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
        // Cannot modify learners while in joint configuration
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
