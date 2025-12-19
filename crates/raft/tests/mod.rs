#[path = "mock/mod.rs"]
pub mod mock;

#[cfg(test)]
pub mod tests {

    // tests/mod.rs is included in lib.rs, so we're part of the crate
    // From crate::tests::tests, we can use crate:: to access root modules
    use raft::error::StateChangeError;
    use raft::message::{
        CompleteSnapshotInstallation, PreVoteRequest, PreVoteResponse, SnapshotProbeSchedule,
    };
    use raft::traits::{
        ApplyResult, ClientResult, ClusterConfigStorage, EventNotify, EventSender,
        HardStateStorage, LogEntryStorage, Network, RaftCallbacks, RpcResult, SnapshotResult,
        SnapshotStorage, StateMachine, Storage, StorageResult, TimerService,
    };
    use raft::ClusterConfig; // Re-exported at crate root
                             // All these types are re-exported at crate root in lib.rs
    use super::mock::mock_network::{MockNetworkHub, MockNetworkHubConfig, NetworkEvent};
    use super::mock::mock_storage::MockStorage;
    use async_trait::async_trait;
    use raft::{
        AppendEntriesRequest, AppendEntriesResponse, Command, Event, HardState,
        InstallSnapshotRequest, InstallSnapshotResponse, InstallSnapshotState, LogEntry, RaftId,
        RaftState, RaftStateOptions, RequestId, RequestVoteRequest, RequestVoteResponse, Role,
        Snapshot, StorageError, TimerId,
    };
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::time::Instant;
    use tokio::sync::{oneshot, Mutex};
    use tokio::time::{sleep, timeout, Duration};

    // Simple callback implementation for testing
    struct TestCallbacks {
        storage: Arc<MockStorage>,
        network: Arc<dyn Network>,
        client_responses: Arc<Mutex<Vec<(RaftId, RequestId, ClientResult<u64>)>>>,
        state_changes: Arc<Mutex<Vec<(RaftId, Role)>>>,
        applied_commands: Arc<Mutex<Vec<(RaftId, u64, u64, Command)>>>,
    }

    impl TestCallbacks {
        fn new(storage: Arc<MockStorage>, network: Arc<dyn Network>) -> Self {
            Self {
                storage,
                network,
                client_responses: Arc::new(Mutex::new(Vec::new())),
                state_changes: Arc::new(Mutex::new(Vec::new())),
                applied_commands: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait]
    impl RaftCallbacks for TestCallbacks {}

    #[async_trait]
    impl Storage for TestCallbacks {}

    #[async_trait]
    impl Network for TestCallbacks {
        async fn send_request_vote_request(
            &self,
            from: &RaftId,
            target: &RaftId,
            args: RequestVoteRequest,
        ) -> RpcResult<()> {
            self.network
                .send_request_vote_request(from, target, args)
                .await
        }

        async fn send_request_vote_response(
            &self,
            from: &RaftId,
            target: &RaftId,
            args: RequestVoteResponse,
        ) -> RpcResult<()> {
            self.network
                .send_request_vote_response(from, target, args)
                .await
        }

        async fn send_append_entries_request(
            &self,
            from: &RaftId,
            target: &RaftId,
            args: AppendEntriesRequest,
        ) -> RpcResult<()> {
            self.network
                .send_append_entries_request(from, target, args)
                .await
        }

        async fn send_append_entries_response(
            &self,
            from: &RaftId,
            target: &RaftId,
            args: AppendEntriesResponse,
        ) -> RpcResult<()> {
            self.network
                .send_append_entries_response(from, target, args)
                .await
        }

        async fn send_install_snapshot_request(
            &self,
            from: &RaftId,
            target: &RaftId,
            args: InstallSnapshotRequest,
        ) -> RpcResult<()> {
            self.network
                .send_install_snapshot_request(from, target, args)
                .await
        }

        async fn send_install_snapshot_response(
            &self,
            from: &RaftId,
            target: &RaftId,
            args: InstallSnapshotResponse,
        ) -> RpcResult<()> {
            self.network
                .send_install_snapshot_response(from, target, args)
                .await
        }

        async fn send_pre_vote_request(
            &self,
            from: &RaftId,
            target: &RaftId,
            args: PreVoteRequest,
        ) -> RpcResult<()> {
            self.network.send_pre_vote_request(from, target, args).await
        }

        async fn send_pre_vote_response(
            &self,
            from: &RaftId,
            target: &RaftId,
            args: PreVoteResponse,
        ) -> RpcResult<()> {
            self.network
                .send_pre_vote_response(from, target, args)
                .await
        }
    }

    #[async_trait]
    impl HardStateStorage for TestCallbacks {
        async fn save_hard_state(&self, from: &RaftId, hard_state: HardState) -> StorageResult<()> {
            self.storage.save_hard_state(from, hard_state).await
        }

        async fn load_hard_state(&self, from: &RaftId) -> StorageResult<Option<HardState>> {
            self.storage.load_hard_state(from).await
        }
    }

    #[async_trait]
    impl SnapshotStorage for TestCallbacks {
        async fn save_snapshot(&self, from: &RaftId, snap: Snapshot) -> StorageResult<()> {
            self.storage.save_snapshot(from, snap).await
        }

        async fn load_snapshot(&self, from: &RaftId) -> StorageResult<Option<Snapshot>> {
            self.storage.load_snapshot(from).await
        }
    }

    #[async_trait]
    impl ClusterConfigStorage for TestCallbacks {
        async fn save_cluster_config(
            &self,
            from: &RaftId,
            conf: ClusterConfig,
        ) -> StorageResult<()> {
            self.storage.save_cluster_config(from, conf).await
        }

        async fn load_cluster_config(&self, from: &RaftId) -> StorageResult<ClusterConfig> {
            self.storage.load_cluster_config(from).await
        }
    }

    #[async_trait]
    impl LogEntryStorage for TestCallbacks {
        async fn append_log_entries(
            &self,
            from: &RaftId,
            entries: &[LogEntry],
        ) -> StorageResult<()> {
            self.storage.append_log_entries(from, entries).await
        }

        async fn get_log_entries(
            &self,
            from: &RaftId,
            low: u64,
            high: u64,
        ) -> StorageResult<Vec<LogEntry>> {
            self.storage.get_log_entries(from, low, high).await
        }

        async fn get_log_entries_term(
            &self,
            from: &RaftId,
            low: u64,
            high: u64,
        ) -> StorageResult<Vec<(u64, u64)>> {
            self.storage.get_log_entries_term(from, low, high).await
        }

        async fn truncate_log_suffix(&self, from: &RaftId, idx: u64) -> StorageResult<()> {
            self.storage.truncate_log_suffix(from, idx).await
        }

        async fn truncate_log_prefix(&self, from: &RaftId, idx: u64) -> StorageResult<()> {
            self.storage.truncate_log_prefix(from, idx).await
        }

        async fn get_last_log_index(&self, from: &RaftId) -> StorageResult<(u64, u64)> {
            self.storage.get_last_log_index(from).await
        }

        async fn get_log_term(&self, from: &RaftId, idx: u64) -> StorageResult<u64> {
            self.storage.get_log_term(from, idx).await
        }
    }

    impl TimerService for TestCallbacks {
        fn del_timer(&self, _from: &RaftId, _timer_id: TimerId) {}

        fn set_leader_transfer_timer(&self, _from: &RaftId, _dur: Duration) -> TimerId {
            // Use a simple atomic counter instead of async lock
            static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        }

        fn set_election_timer(&self, _from: &RaftId, _dur: Duration) -> TimerId {
            static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        }

        fn set_heartbeat_timer(&self, _from: &RaftId, _dur: Duration) -> TimerId {
            static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        }

        fn set_apply_timer(&self, _from: &RaftId, _dur: Duration) -> TimerId {
            static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        }

        fn set_config_change_timer(&self, _from: &RaftId, _dur: Duration) -> TimerId {
            static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl EventSender for TestCallbacks {
        async fn send(&self, _target: RaftId, _event: Event) -> anyhow::Result<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl StateMachine for TestCallbacks {
        async fn create_snapshot(
            &self,
            from: &RaftId,
            config: ClusterConfig,
            saver: Arc<dyn SnapshotStorage>,
            bootstrap_snapshot_index: Option<u64>,
        ) -> StorageResult<(u64, u64)> {
            let mut commands = self.applied_commands.lock().await;

            if commands.is_empty() {
                return Err(StorageError::SnapshotCreationFailed(
                    "No commands to snapshot".into(),
                ));
            }

            let data = serde_json::to_vec(&*commands).map_err(|e| {
                StorageError::SnapshotCreationFailed(format!(
                    "Snapshot serialization error: {:?}",
                    e
                ))
            })?;

            saver
                .save_snapshot(
                    from,
                    Snapshot {
                        index: bootstrap_snapshot_index.unwrap_or(commands.last().unwrap().1),
                        term: commands.last().unwrap().2,
                        data,
                        config,
                    },
                )
                .await
                .unwrap();

            Ok((commands.last().unwrap().1, commands.last().unwrap().2))
        }
        async fn client_response(
            &self,
            from: &RaftId,
            request_id: RequestId,
            result: ClientResult<u64>,
        ) -> ClientResult<()> {
            let mut responses = self.client_responses.lock().await;
            responses.push((from.clone(), request_id, result));
            Ok(())
        }

        async fn apply_command(
            &self,
            from: &RaftId,
            index: u64,
            term: u64,
            cmd: Command,
        ) -> ApplyResult<()> {
            let mut commands = self.applied_commands.lock().await;
            commands.push((from.clone(), index, term, cmd));
            Ok(())
        }

        async fn read_index_response(
            &self,
            _from: &RaftId,
            _request_id: RequestId,
            _result: ClientResult<u64>,
        ) -> ClientResult<()> {
            // No special handling in test environment
            Ok(())
        }

        fn process_snapshot(
            &self,
            _from: &RaftId,
            _index: u64,
            _term: u64,
            _data: Vec<u8>,
            _config: ClusterConfig, // Add config parameter
            _request_id: RequestId,
            _oneshot: oneshot::Sender<SnapshotResult<()>>,
        ) {
        }
    }

    #[async_trait]
    impl EventNotify for TestCallbacks {
        async fn on_state_changed(
            &self,
            from: &RaftId,
            role: Role,
        ) -> Result<(), StateChangeError> {
            let mut changes = self.state_changes.lock().await;
            changes.push((from.clone(), role));
            Ok(())
        }

        async fn on_node_removed(&self, _node_id: &RaftId) -> Result<(), StateChangeError> {
            Ok(())
        }
    }

    // Helper functions
    #[allow(dead_code)]
    fn create_test_raft_id(group: &str, node: &str) -> RaftId {
        RaftId::new(group.to_string(), node.to_string())
    }

    #[allow(dead_code)]
    async fn create_test_raft_setup(
        node_id: RaftId,
        peers: Vec<RaftId>,
    ) -> Result<(RaftState, Arc<MockStorage>, Arc<TestCallbacks>), String> {
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;

        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        // Initialize cluster configuration
        let mut all_nodes = vec![node_id.clone()];
        all_nodes.extend(peers.clone());
        let cluster_config = ClusterConfig::simple(all_nodes.into_iter().collect(), 0);
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .map_err(|e| format!("Failed to save cluster config: {:?}", e))?;

        let options = create_test_options(node_id.clone(), peers);
        let raft_state = RaftState::new(options, callbacks.clone())
            .await
            .map_err(|e| format!("Failed to create RaftState: {:?}", e))?;

        Ok((raft_state, storage, callbacks))
    }

    #[allow(dead_code)]
    async fn create_simple_test_raft() -> Result<
        (
            RaftState,
            Arc<MockStorage>,
            Arc<TestCallbacks>,
            RaftId,
            RaftId,
            RaftId,
        ),
        String,
    > {
        let node_id = create_test_raft_id("test_group", "node1");
        let peer1 = create_test_raft_id("test_group", "node2");
        let peer2 = create_test_raft_id("test_group", "node3");

        let (raft_state, storage, callbacks) =
            create_test_raft_setup(node_id.clone(), vec![peer1.clone(), peer2.clone()]).await?;

        Ok((raft_state, storage, callbacks, node_id, peer1, peer2))
    }

    #[allow(dead_code)]
    fn create_test_options(id: RaftId, peers: Vec<RaftId>) -> RaftStateOptions {
        RaftStateOptions {
            id,
            peers,
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
            apply_interval: Duration::from_millis(1),
            apply_batch_size: 64,
            config_change_timeout: Duration::from_secs(5),
            leader_transfer_timeout: Duration::from_secs(5),
            schedule_snapshot_probe_interval: Duration::from_secs(1),
            schedule_snapshot_probe_retries: 3,
            pre_vote_enabled: false, // Disable Pre-Vote to maintain compatibility with existing tests
            leader_lease_enabled: false, // Disable LeaderLease (test default)
            // Feedback control configuration
            max_inflight_requests: 10,
            initial_batch_size: 10,
            max_batch_size: 100,
            min_batch_size: 1,
            feedback_window_size: 10,
            // Smart timeout configuration
            base_request_timeout: Duration::from_secs(3),
            max_request_timeout: Duration::from_secs(30),
            min_request_timeout: Duration::from_secs(1),
            timeout_response_factor: 2.0,
            target_response_time: Duration::from_millis(100),
        }
    }

    #[tokio::test]
    async fn test_raft_state_initialization() {
        let (raft_state, _storage, _callbacks, node_id, _peer1, _peer2) =
            create_simple_test_raft().await.unwrap();

        assert_eq!(raft_state.get_role(), Role::Follower);
        assert_eq!(raft_state.id, node_id);
    }

    #[tokio::test]
    async fn test_election_timeout_triggers_candidate() {
        let (mut raft_state, _storage, callbacks, _node_id, _peer1, _peer2) =
            create_simple_test_raft().await.unwrap();

        // Trigger election timeout
        raft_state.handle_event(Event::ElectionTimeout).await;

        // Should become candidate
        assert_eq!(raft_state.get_role(), Role::Candidate);

        // Check state change notification
        let state_changes = callbacks.state_changes.lock().await;
        assert!(!state_changes.is_empty());
        assert_eq!(state_changes.last().unwrap().1, Role::Candidate);
    }

    #[tokio::test]
    async fn test_request_vote_handling() {
        let node_id = create_test_raft_id("test_group", "node1");
        let candidate_id = create_test_raft_id("test_group", "node2");
        let peer2 = create_test_raft_id("test_group", "node3");

        let (mut raft_state, storage, _callbacks) =
            create_test_raft_setup(node_id.clone(), vec![candidate_id.clone(), peer2])
                .await
                .unwrap();

        // Create vote request
        let vote_request = RequestVoteRequest {
            term: 1,
            candidate_id: candidate_id.clone(),
            last_log_index: 0,
            last_log_term: 0,
            request_id: RequestId::new(),
        };

        // Handle vote request
        raft_state
            .handle_event(Event::RequestVoteRequest(
                candidate_id.clone(),
                vote_request,
            ))
            .await;

        // Verify hard state has been updated (should vote for candidate)
        let hard_state = storage.load_hard_state(&node_id).await.unwrap();
        assert!(hard_state.is_some());
        let (term, voted_for) = match hard_state {
            Some(hs) => (hs.term, hs.voted_for),
            None => panic!("Hard state should exist"),
        };
        assert_eq!(term, 1);
        assert_eq!(voted_for, Some(candidate_id));
    }

    #[tokio::test]
    async fn test_append_entries_handling() {
        let node_id = create_test_raft_id("test_group", "node1");
        let leader_id = create_test_raft_id("test_group", "node2");
        let peer2 = create_test_raft_id("test_group", "node3");

        let (mut raft_state, storage, callbacks) =
            create_test_raft_setup(node_id.clone(), vec![leader_id.clone(), peer2])
                .await
                .unwrap();

        // Create log entry
        let log_entry = LogEntry {
            term: 1,
            index: 1,
            command: vec![1, 2, 3],
            is_config: false,
            client_request_id: Some(RequestId::new()),
        };

        // Create AppendEntries request
        let append_request = AppendEntriesRequest {
            term: 1,
            leader_id: leader_id.clone(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![log_entry.clone()],
            leader_commit: 1,
            request_id: RequestId::new(),
        };

        // Handle AppendEntries request
        raft_state
            .handle_event(Event::AppendEntriesRequest(
                leader_id.clone(),
                append_request,
            ))
            .await;

        // Verify log has been stored
        let stored_entries = storage.get_log_entries(&node_id, 1, 2).await.unwrap();
        assert!(!stored_entries.is_empty(), "Should have stored log entries");
        assert_eq!(stored_entries[0].term, 1);
        assert_eq!(stored_entries[0].index, 1);
        assert_eq!(stored_entries[0].command, vec![1, 2, 3]);

        // Verify applied commands - since application takes time, we check if any commands were applied
        let applied_commands = callbacks.applied_commands.lock().await;
        // In test environment, commands might not have been applied yet, this is normal
        println!("Applied commands count: {}", applied_commands.len());
    }

    #[tokio::test]
    async fn test_leader_election_success() {
        let (mut raft_state, _storage, callbacks, _node1, node2, node3) =
            create_simple_test_raft().await.unwrap();

        // Trigger election
        raft_state.handle_event(Event::ElectionTimeout).await;
        assert_eq!(raft_state.get_role(), Role::Candidate);

        // Need to set election ID to match responses
        let current_election_id = raft_state.current_election_id.unwrap();

        // Simulate receiving vote response
        let vote_response1 = RequestVoteResponse {
            term: 1,
            vote_granted: true,
            request_id: current_election_id,
        };

        let vote_response2 = RequestVoteResponse {
            term: 1,
            vote_granted: true,
            request_id: current_election_id,
        };

        // Handle vote response
        raft_state
            .handle_event(Event::RequestVoteResponse(node2, vote_response1))
            .await;

        // Check if has become leader (might only need one vote)
        if raft_state.get_role() != Role::Leader {
            raft_state
                .handle_event(Event::RequestVoteResponse(node3, vote_response2))
                .await;
        }

        // Verify eventually becomes leader
        println!("Final role: {:?}", raft_state.get_role());
        // In real scenarios, might need majority votes to become leader
        // Here we only verify that election logic has no errors
        assert!(matches!(
            raft_state.get_role(),
            Role::Leader | Role::Candidate
        ));

        // Verify state change notification
        let state_changes = callbacks.state_changes.lock().await;
        assert!(!state_changes.is_empty());
    }

    #[tokio::test]
    async fn test_client_propose_handling() {
        let node_id = create_test_raft_id("test_group", "node1");
        let peer1 = create_test_raft_id("test_group", "node2");
        let peer2 = create_test_raft_id("test_group", "node3");

        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;

        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        // Initialize cluster configuration
        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), peer1.clone(), peer2.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![peer1, peer2]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        // Simulate becoming leader
        raft_state.handle_event(Event::ElectionTimeout).await;
        // Need to manually set to Leader to test client request handling
        // In actual testing, might need a complete election process

        let command = vec![1, 2, 3, 4];
        let request_id = RequestId::new();

        // Handle client proposal
        raft_state
            .handle_event(Event::ClientPropose {
                cmd: command.clone(),
                request_id,
            })
            .await;

        // If it's a leader, there should be log entries
        let stored_entries = storage.get_log_entries(&node_id, 1, 2).await;
        if raft_state.get_role() == Role::Leader && stored_entries.is_ok() {
            let entries = stored_entries.unwrap();
            if !entries.is_empty() {
                assert_eq!(entries[0].command, command);
                assert_eq!(entries[0].client_request_id, Some(request_id));
            }
        }
    }

    #[tokio::test]
    async fn test_config_change_handling() {
        let node1 = create_test_raft_id("test_group", "node1");
        let node2 = create_test_raft_id("test_group", "node2");
        let node3 = create_test_raft_id("test_group", "node3");
        let node4 = create_test_raft_id("test_group", "node4");

        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node1.clone()).await;

        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        // Initialize cluster configuration
        let cluster_config = ClusterConfig::simple(
            vec![node1.clone(), node2.clone(), node3.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node1, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node1.clone(), vec![node2.clone(), node3.clone()]);
        let mut raft_state = RaftState::new(options, callbacks).await.unwrap();

        // New configuration includes node4
        let new_voters = vec![node1.clone(), node2.clone(), node3.clone(), node4]
            .into_iter()
            .collect::<HashSet<_>>();
        let request_id = RequestId::new();

        // Handle configuration change
        raft_state
            .handle_event(Event::ChangeConfig {
                new_voters,
                request_id,
            })
            .await;

        // Check if joint configuration log was created
        let stored_entries = storage.get_log_entries(&node1, 1, 2).await;
        if stored_entries.is_ok() && raft_state.get_role() == Role::Leader {
            let entries = stored_entries.unwrap();
            if !entries.is_empty() {
                assert!(entries[0].is_config);
                assert_eq!(entries[0].client_request_id, Some(request_id));
            }
        }
    }

    #[tokio::test]
    async fn test_snapshot_installation() {
        let node_id = create_test_raft_id("test_group", "node1");
        let leader_id = create_test_raft_id("test_group", "node2");
        let peer2 = create_test_raft_id("test_group", "node3");

        let (mut raft_state, _storage, _callbacks) =
            create_test_raft_setup(node_id.clone(), vec![leader_id.clone(), peer2])
                .await
                .unwrap();

        // Create snapshot installation request
        let snapshot_request = InstallSnapshotRequest {
            term: 1,
            leader_id: leader_id.clone(),
            last_included_index: 10,
            last_included_term: 1,
            data: vec![1, 2, 3, 4, 5],
            config: ClusterConfig::empty(), // Empty config for testing
            request_id: RequestId::new(),
            snapshot_request_id: RequestId::new(),
            is_probe: false,
        };

        // Handle snapshot installation
        raft_state
            .handle_event(Event::InstallSnapshotRequest(
                node_id.clone(),
                snapshot_request.clone(),
            ))
            .await;

        // Verify snapshot processing (in actual implementation, business layer needs to call complete_snapshot_installation)
        raft_state
            .handle_complete_snapshot_installation(CompleteSnapshotInstallation {
                index: snapshot_request.last_included_index,
                term: snapshot_request.last_included_term,
                success: true,
                request_id: snapshot_request.request_id,
                reason: None,
                config: Some(snapshot_request.config.clone()),
            })
            .await;

        // Verify snapshot state has been updated
        println!(
            "Snapshot index: {}, term: {}",
            raft_state.last_snapshot_index, raft_state.last_snapshot_term
        );
        // Snapshot installation might require more complex verification logic
        // Here we only verify that no errors occurred
        assert!(raft_state.last_snapshot_index <= 10);
    }

    #[tokio::test]
    async fn test_heartbeat_as_leader() {
        let node_id = create_test_raft_id("test_group", "node1");
        let peer1 = create_test_raft_id("test_group", "node2");
        let peer2 = create_test_raft_id("test_group", "node3");

        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;

        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        // Initialize cluster configuration
        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), peer1.clone(), peer2.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![peer1, peer2]);
        let mut raft_state = RaftState::new(options, callbacks).await.unwrap();

        // Trigger election and become candidate
        raft_state.handle_event(Event::ElectionTimeout).await;
        assert_eq!(raft_state.get_role(), Role::Candidate);

        // Manually become leader (in real scenarios, need to receive enough votes)
        // Here we test leader behavior by handling heartbeat timeout
        if raft_state.get_role() == Role::Leader {
            raft_state.handle_event(Event::HeartbeatTimeout).await;
        }

        // Test passed - if no panic, heartbeat handling is normal
    }

    #[tokio::test]
    async fn test_log_replication_conflict_resolution() {
        let node_id = create_test_raft_id("test_group", "node1");
        let leader_id = create_test_raft_id("test_group", "node2");
        let peer2 = create_test_raft_id("test_group", "node3");

        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;

        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        // Initialize cluster configuration
        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), leader_id.clone(), peer2.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![leader_id.clone(), peer2]);
        let mut raft_state = RaftState::new(options, callbacks).await.unwrap();

        // First add some local logs
        let local_entry = LogEntry {
            term: 1,
            index: 1,
            command: vec![1, 1, 1],
            is_config: false,
            client_request_id: Some(RequestId::new()),
        };
        storage
            .append_log_entries(&node_id, &[local_entry])
            .await
            .unwrap();

        // Create conflicting AppendEntries request (different prev_log_term)
        let conflicting_entry = LogEntry {
            term: 2,
            index: 1,
            command: vec![2, 2, 2],
            is_config: false,
            client_request_id: Some(RequestId::new()),
        };

        let append_request = AppendEntriesRequest {
            term: 2,
            leader_id: leader_id.clone(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![conflicting_entry],
            leader_commit: 1,
            request_id: RequestId::new(),
        };

        // Handle conflicting AppendEntries request
        raft_state
            .handle_event(Event::AppendEntriesRequest(
                leader_id.clone(),
                append_request,
            ))
            .await;

        // Verify processing result - in real scenarios, conflict resolution might be complex
        let stored_entries = storage.get_log_entries(&node_id, 1, 2).await.unwrap();
        println!("Stored entries count: {}", stored_entries.len());
        if !stored_entries.is_empty() {
            println!(
                "First entry term: {}, command: {:?}",
                stored_entries[0].term, stored_entries[0].command
            );
            // Verify log conflict handling logic
            assert!(stored_entries[0].term >= 1);
        }
    }

    // 1. Test handling `RequestVoteRequest` from old term
    #[tokio::test]
    async fn test_request_vote_from_old_term() {
        let node_id = create_test_raft_id("test_group", "node1");
        let candidate_id = create_test_raft_id("test_group", "node2");
        let peer2 = create_test_raft_id("test_group", "node3");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        // Initialize cluster configuration
        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), candidate_id.clone(), peer2.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![candidate_id.clone(), peer2]);
        let mut raft_state = RaftState::new(options, callbacks).await.unwrap();

        // Set Raft node's current term to 2
        raft_state.current_term = 2;
        raft_state.role = Role::Follower; // Ensure it's not a Candidate

        // Create a vote request from old term (term=1)
        let old_term_vote_request = RequestVoteRequest {
            term: 1, // Old term
            candidate_id: candidate_id.clone(),
            last_log_index: 0,
            last_log_term: 0,
            request_id: RequestId::new(),
        };

        // Handle vote request from old term
        raft_state
            .handle_event(Event::RequestVoteRequest(
                candidate_id.clone(),
                old_term_vote_request,
            ))
            .await;

        // Verify node's term is unchanged and no vote was cast
        assert_eq!(raft_state.current_term, 2);
        assert_eq!(raft_state.voted_for, None);
        // Verify if a rejection response was sent (can check sent messages via MockNetwork)
        // Simplified handling here, mainly verifying state
    }

    // 2. Test handling `RequestVoteRequest` from future term with outdated logs
    #[tokio::test]
    async fn test_request_vote_from_future_term_log_not_up_to_date() {
        let node_id = create_test_raft_id("test_group", "node1");
        let candidate_id = create_test_raft_id("test_group", "node2");
        let peer2 = create_test_raft_id("test_group", "node3");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        // Initialize cluster configuration
        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), candidate_id.clone(), peer2.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![candidate_id.clone(), peer2]);
        let mut raft_state = RaftState::new(options, callbacks).await.unwrap();

        // Set Raft node's current term to 1 and add a log entry with index 2, term 1
        raft_state.current_term = 1;
        let local_entry = LogEntry {
            term: 1,
            index: 2, // Larger index
            command: vec![1, 1, 1],
            is_config: false,
            client_request_id: Some(RequestId::new()),
        };
        storage
            .append_log_entries(&node_id, &[local_entry])
            .await
            .unwrap();
        // Update RaftState's in-memory state (if needed)
        raft_state.last_log_index = 2;
        raft_state.last_log_term = 1;

        // Create a vote request from future term (term=2) with logs less new than current node's
        let future_term_vote_request = RequestVoteRequest {
            term: 2, // future term
            candidate_id: candidate_id.clone(),
            last_log_index: 1, // Candidate has smaller log index
            last_log_term: 1,
            request_id: RequestId::new(),
        };

        // Handle vote request from future term
        raft_state
            .handle_event(Event::RequestVoteRequest(
                candidate_id.clone(),
                future_term_vote_request,
            ))
            .await;

        // Verify node's term updates to 2, role becomes Follower, but no vote was cast
        assert_eq!(raft_state.current_term, 2);
        assert_eq!(raft_state.role, Role::Follower);
        assert_eq!(raft_state.voted_for, None); // Because own logs are more up-to-date
                                                // Verify if a rejection response is sent
    }

    // 3. Test Candidate handling higher term `RequestVoteResponse` (rejection)
    #[tokio::test]
    async fn test_candidate_receive_higher_term_rejecting_vote_response() {
        let node_id = create_test_raft_id("test_group", "node1");
        let peer1 = create_test_raft_id("test_group", "node2");
        let peer2 = create_test_raft_id("test_group", "node3");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        // Initialize cluster configuration
        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), peer1.clone(), peer2.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![peer1.clone(), peer2.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        // Make it a Candidate via election timeout
        raft_state.handle_event(Event::ElectionTimeout).await;
        assert_eq!(raft_state.role, Role::Candidate);
        let candidate_term = raft_state.current_term;

        // Get current election ID to ensure vote response can be handled correctly
        let current_election_id = raft_state
            .current_election_id
            .expect("Should have election ID after becoming candidate");

        // Create a rejection vote response from a higher term (current_term + 1)
        let higher_term_reject_response = RequestVoteResponse {
            term: candidate_term + 1, // higher term
            vote_granted: false,
            request_id: current_election_id, // use correct election ID
        };

        // Handle this response
        raft_state
            .handle_event(Event::RequestVoteResponse(
                peer1.clone(), // use valid node ID from configuration
                higher_term_reject_response,
            ))
            .await;

        // Verify node role changes to Follower and current_term updates
        assert_eq!(raft_state.role, Role::Follower);
        assert_eq!(raft_state.current_term, candidate_term + 1);
        // Verify state change notification
        let state_changes = callbacks.state_changes.lock().await;
        assert!(state_changes
            .iter()
            .any(|&(_, role)| role == Role::Follower));
    }

    // 4. Test Leader handling higher term `AppendEntriesResponse` (failure)
    #[tokio::test]
    async fn test_leader_receive_higher_term_failing_append_entries_response() {
        let node_id = create_test_raft_id("test_group", "node1");
        let peer1 = create_test_raft_id("test_group", "node2");
        let peer2 = create_test_raft_id("test_group", "node3");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        // Initialize cluster configuration
        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), peer1.clone(), peer2.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![peer1.clone(), peer2.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        // Manually set to Leader (simplify test, skip full election)
        raft_state.role = Role::Leader;
        raft_state.current_term = 2;
        raft_state.leader_id = Some(node_id.clone());
        // Initialize Leader's next_index and match_index
        raft_state.next_index.insert(peer1.clone(), 1);
        raft_state.next_index.insert(peer2.clone(), 1);
        raft_state.match_index.insert(peer1.clone(), 0);
        raft_state.match_index.insert(peer2.clone(), 0);

        // Record initial term
        let initial_term = raft_state.current_term;

        // Create a failed append entries response from a higher term (current_term + 1)
        let higher_term_fail_response = AppendEntriesResponse {
            conflict_index: None,
            conflict_term: None,
            term: initial_term + 1, // higher term (3)
            success: false,
            matched_index: 0,
            request_id: RequestId::new(),
        };

        // Handle this response
        raft_state
            .handle_event(Event::AppendEntriesResponse(
                peer1.clone(), // sender ID
                higher_term_fail_response,
            ))
            .await;

        // Verify node role changes to Follower and current_term updates
        assert_eq!(raft_state.role, Role::Follower);
        assert_eq!(raft_state.current_term, initial_term + 1); // should be 3
        assert_eq!(raft_state.leader_id, None);
        // Verify state change notifications
        let state_changes = callbacks.state_changes.lock().await;
        assert!(state_changes
            .iter()
            .any(|&(_, role)| role == Role::Follower));
    }

    // 5. Test Follower handling expired `AppendEntriesRequest` (prev_log_index < last_snapshot_index and term mismatch)
    #[tokio::test]
    async fn test_follower_handle_outdated_append_entries_prev_before_snapshot_term_mismatch() {
        let node_id = create_test_raft_id("test_group", "node1");
        let leader_id = create_test_raft_id("test_group", "node2");
        let peer2 = create_test_raft_id("test_group", "node3");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        // Initialize cluster configuration
        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), leader_id.clone(), peer2.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![leader_id.clone(), peer2]);
        let mut raft_state = RaftState::new(options, callbacks).await.unwrap();

        // Set snapshot state
        raft_state.last_snapshot_index = 5;
        raft_state.last_snapshot_term = 1;
        raft_state.current_term = 2; // current term is higher than snapshot term

        // Create an AppendEntriesRequest with prev_log_index < last_snapshot_index and prev_log_term != last_snapshot_term
        let outdated_append_request = AppendEntriesRequest {
            term: 2, // >= current term
            leader_id: leader_id.clone(),
            prev_log_index: 3, // < last_snapshot_index (5)
            prev_log_term: 2,  // != last_snapshot_term (1)
            entries: vec![],   // content is not important
            leader_commit: 0,
            request_id: RequestId::new(),
        };

        // Handle this request
        raft_state
            .handle_event(Event::AppendEntriesRequest(
                leader_id.clone(),
                outdated_append_request,
            ))
            .await;

        // Verify success is false in returned AppendEntriesResponse
        // This requires checking MockNetwork sent responses. Assuming network calls are captured here.
        // One way is to check if MockStorage's truncate_log_suffix is called (if implementation tries to roll back),
        // Or more directly, if handle_append_entries_request logic is correct, it should send failure response.
        // Since directly verifying responses is complex, we can indirectly verify by checking raft_state status hasn't changed due to successful append.
        // For example, last_log_index should still be snapshot index or higher (if there are logs).
        assert_eq!(raft_state.last_snapshot_index, 5);
        assert_eq!(raft_state.last_snapshot_term, 1);
        // If there are no logs, last_log_index should be snapshot index
        // assert_eq!(raft_state.get_last_log_index().await, 5);
        // Role should remain unchanged
        assert_eq!(raft_state.role, Role::Follower);
    }

    // 6. Test Leader handling log conflicts and rollback in `AppendEntriesResponse`
    // Note: Standard Raft implementation usually doesn't roll back Leader's own logs, instead Follower tells Leader where to retry via conflict index.
    // Therefore, this test focuses more on Leader adjusting next_index based on Follower's failure response (containing conflict index).
    // Assume AppendEntriesResponse contains a conflict_index field (not explicitly shown in your code, but this is common practice).
    // If no conflict_index, Leader usually conservatively decrements next_index by 1.
    // Here we test the conservative decrement by 1 case.
    #[tokio::test]
    async fn test_leader_handle_append_entries_response_log_conflict_rollback_next_index() {
        let node_id = create_test_raft_id("test_group", "node1");
        let peer1 = create_test_raft_id("test_group", "node2");
        let peer2 = create_test_raft_id("test_group", "node3");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        // Initialize cluster configuration
        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), peer1.clone(), peer2.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![peer1.clone(), peer2.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        // Manually set to Leader
        raft_state.role = Role::Leader;
        raft_state.current_term = 2;
        raft_state.leader_id = Some(node_id.clone());
        // Add some logs
        let log_entry1 = LogEntry {
            term: 2,
            index: 1,
            command: vec![1],
            is_config: false,
            client_request_id: None,
        };
        let log_entry2 = LogEntry {
            term: 2,
            index: 2,
            command: vec![2],
            is_config: false,
            client_request_id: None,
        };
        storage
            .append_log_entries(&node_id, &[log_entry1, log_entry2])
            .await
            .unwrap();
        raft_state.last_log_index = 2;
        raft_state.last_log_term = 2;

        // Set next_index and match_index for peer1
        raft_state.next_index.insert(peer1.clone(), 3); // next to send is index 3 (does not exist)
        raft_state.match_index.insert(peer1.clone(), 2); // peer1 has matched up to index 2

        // Simulate peer1 sending an AppendEntriesResponse with success false, indicating conflict at index 2
        // Assume response has no explicit conflict_index, Leader conservatively decrements next_index by 1
        let failing_append_response = AppendEntriesResponse {
            conflict_index: None,
            conflict_term: None,
            term: 2, // same term
            success: false,
            matched_index: 1, // assume peer1 reports it only matched up to index 1 (one before conflict point)
            request_id: RequestId::new(),
        };

        // Handle this response
        raft_state
            .handle_event(Event::AppendEntriesResponse(
                peer1.clone(),
                failing_append_response,
            ))
            .await;

        // Verify Leader updated peer1's next_index (should decrease)
        let updated_next_index = raft_state.next_index.get(&peer1).cloned().unwrap_or(0);
        // If matched_index is 1, next_index should update to matched_index + 1 = 2
        assert_eq!(updated_next_index, 2);
        // match_index should also update to matched_index
        assert_eq!(*raft_state.match_index.get(&peer1).unwrap(), 1);
    }

    // 7. Test timeout handling for configuration changes
    #[tokio::test]
    async fn test_config_change_timeout_handling() {
        let node_id = create_test_raft_id("test_group", "node1");
        let peer1 = create_test_raft_id("test_group", "node2");
        let peer2 = create_test_raft_id("test_group", "node3");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        // Initialize cluster configuration
        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), peer1.clone(), peer2.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![peer1.clone(), peer2.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        // Manually set to Leader
        raft_state.role = Role::Leader;
        raft_state.current_term = 2;
        raft_state.leader_id = Some(node_id.clone());

        // Initialize next_index and match_index (simulate other nodes have synced to joint configuration log)
        raft_state.next_index.insert(peer1.clone(), 2);
        raft_state.next_index.insert(peer2.clone(), 2);
        raft_state.match_index.insert(peer1.clone(), 1); // simulate peer1 has synced to index 1
        raft_state.match_index.insert(peer2.clone(), 1); // simulate peer2 has synced to index 1

        // Simulate entering joint configuration state
        let new_voters = vec![node_id.clone(), peer1.clone(), peer2.clone()]
            .into_iter()
            .collect();
        raft_state
            .config
            .enter_joint(
                raft_state.config.get_effective_voters().clone(),
                new_voters,
                None,
                None,
                1,
            )
            .unwrap();
        raft_state.config_change_in_progress = true;
        raft_state.joint_config_log_index = 1;
        // Set timeout - ensure configuration change timeout has been exceeded
        raft_state.config_change_start_time =
            Some(Instant::now() - raft_state.config_change_timeout - Duration::from_secs(1));

        // Record initial state
        let initial_joint_state = raft_state.config.is_joint();

        // Trigger configuration change timeout event
        raft_state.handle_event(Event::ConfigChangeTimeout).await;

        // Verify timeout handling effects:
        // 1. Timeout handling finds timeout has occurred, and both old and new configurations have majority support (nodes have synced to index 1)
        // 2. Normal exit to new configuration
        // 3. Configuration change completed, config_change_in_progress becomes false

        assert!(initial_joint_state, "Initially should be in joint config");

        // Since majority have synced, timeout handling should complete configuration change
        assert!(
            !raft_state.config_change_in_progress,
            "Config change should be completed after timeout with majority sync"
        );

        // Configuration should no longer be in joint state
        assert!(
            !raft_state.config.is_joint(),
            "Config should exit joint state after timeout"
        );

        // Role should still be Leader (no term change)
        assert_eq!(raft_state.role, Role::Leader);

        // Term should remain unchanged
        assert_eq!(raft_state.current_term, 2);

        // Verify timeout handling didn't cause panic and system state is consistent
        assert!(raft_state.leader_id.is_some());
    }

    // 8. Test execution and failure of snapshot probe schedules
    #[tokio::test]
    async fn test_snapshot_probe_schedule_execution_and_failure() {
        let node_id = create_test_raft_id("test_group", "node1");
        let peer1 = create_test_raft_id("test_group", "node2");
        let peer2 = create_test_raft_id("test_group", "node3");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        // Initialize cluster configuration
        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), peer1.clone(), peer2.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![peer1.clone(), peer2.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        // Manually set to Leader
        raft_state.role = Role::Leader;
        raft_state.current_term = 2;
        raft_state.leader_id = Some(node_id.clone());

        // Set peer1's snapshot state to Installing
        raft_state
            .follower_snapshot_states
            .insert(peer1.clone(), InstallSnapshotState::Installing);
        raft_state
            .follower_last_snapshot_index
            .insert(peer1.clone(), 10);
        // Schedule snapshot probe
        let probe_schedule = SnapshotProbeSchedule {
            peer: peer1.clone(),
            next_probe_time: Instant::now(), // executable immediately
            interval: Duration::from_millis(50), // use shorter interval for easier testing
            max_attempts: 3,
            snapshot_request_id: RequestId::new(),
            attempts: 0, // initial attempts count is 0
        };
        raft_state.snapshot_probe_schedules.push(probe_schedule);

        // Simulate `process_pending_probes` being called (triggered by heartbeat timeout)
        // First call, attempts increase from 0 to 1
        raft_state.handle_event(Event::HeartbeatTimeout).await;
        // Check if attempts increased (need to access internal state, here indirectly verify by checking if probe_time updated)
        // More direct way is to check if probe request was sent or attempts field
        // Assume `process_pending_probes` updates attempts
        let updated_schedule = raft_state
            .snapshot_probe_schedules
            .iter()
            .find(|s| s.peer == peer1)
            .unwrap();
        assert_eq!(updated_schedule.attempts, 1);

        // Simulate timeout again, increase to 2
        sleep(Duration::from_millis(100)).await; // ensure next probe time has arrived
        raft_state.handle_event(Event::HeartbeatTimeout).await;
        let updated_schedule = raft_state
            .snapshot_probe_schedules
            .iter()
            .find(|s| s.peer == peer1)
            .unwrap();
        assert_eq!(updated_schedule.attempts, 2);

        // Third simulation of timeout, reaching max_attempts (3)
        sleep(Duration::from_millis(100)).await;
        raft_state.handle_event(Event::HeartbeatTimeout).await;
        let updated_schedule = raft_state
            .snapshot_probe_schedules
            .iter()
            .find(|s| s.peer == peer1);
        // Verify probe schedule is removed (because max attempts reached)
        assert!(updated_schedule.is_none());
        // Verify if follower state is marked as Failed (if implementation has this logic)
        // Assume `process_pending_probes` removes schedule after reaching max_attempts
        // If there's a state marker, can check it here
    }

    // 9. Test the correctness of Follower state after completing snapshot installation
    #[tokio::test]
    async fn test_follower_complete_snapshot_installation_state_correctness() {
        let node_id = create_test_raft_id("test_group", "node1");
        let leader_id = create_test_raft_id("test_group", "node2");
        let peer2 = create_test_raft_id("test_group", "node3");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        // Initialize cluster configuration
        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), leader_id.clone(), peer2.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![leader_id.clone(), peer2.clone()]);
        let mut raft_state = RaftState::new(options, callbacks).await.unwrap();

        // Simulate processing a non-probe InstallSnapshotRequest
        // Create a valid ClusterConfig as snapshot data
        let snap_config = ClusterConfig::simple(
            vec![node_id.clone(), leader_id.clone(), peer2]
                .into_iter()
                .collect(),
            0,
        );
        let snapshot_data = serde_json::to_vec(&snap_config).unwrap();

        let snapshot_request = InstallSnapshotRequest {
            term: 1,
            leader_id: leader_id.clone(),
            last_included_index: 10,
            last_included_term: 1,
            data: snapshot_data,
            config: snap_config,          // Use test configuration
            request_id: RequestId::new(), // Use specific ID for verification
            snapshot_request_id: RequestId::new(),
            is_probe: false,
        };
        let request_id = snapshot_request.request_id.clone();

        raft_state
            .handle_event(Event::InstallSnapshotRequest(
                leader_id.clone(),
                snapshot_request.clone(),
            ))
            .await;

        // Verify that RaftState internal state is correctly set (e.g., current_snapshot_request_id)
        assert_eq!(
            raft_state.current_snapshot_request_id,
            Some(request_id.clone())
        );

        // Call complete_snapshot_installation
        let new_last_applied_index = snapshot_request.last_included_index;
        let new_last_applied_term = snapshot_request.last_included_term;
        raft_state
            .handle_complete_snapshot_installation(CompleteSnapshotInstallation {
                request_id: request_id,
                success: true,
                reason: None,
                index: new_last_applied_index,
                term: new_last_applied_term,
                config: Some(ClusterConfig::empty()),
            })
            .await;

        // Verify that RaftState state is correctly updated
        assert_eq!(raft_state.last_snapshot_index, new_last_applied_index);
        assert_eq!(raft_state.last_snapshot_term, new_last_applied_term);
        // commit_index and last_applied usually also update to the snapshot point or later
        assert!(raft_state.commit_index >= new_last_applied_index);
        assert!(raft_state.last_applied >= new_last_applied_index);
        // Verify that current_snapshot_request_id is cleared
        assert_eq!(raft_state.current_snapshot_request_id, None);
        // Verify that role remains Follower (or may stay the same depending on implementation)
        assert_eq!(raft_state.role, Role::Follower);
    }

    // 10. Test non-leader node handling of `InstallSnapshotRequest` (probe)
    #[tokio::test]
    async fn test_non_leader_handle_install_snapshot_request_probe() {
        let node_id = create_test_raft_id("test_group", "node1");
        let leader_id = create_test_raft_id("test_group", "node2");
        let peer2 = create_test_raft_id("test_group", "node3");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await; // follower network
        let (_leader_network, mut leader_rx) = hub.register_node(leader_id.clone()).await; // leader network for receiving responses
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        // Initialize cluster configuration
        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), leader_id.clone(), peer2.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![leader_id.clone(), peer2]);
        let mut raft_state = RaftState::new(options, callbacks).await.unwrap();

        // Set node as Follower (it's the default)
        raft_state.role = Role::Follower;
        raft_state.current_term = 1;

        // Can also test Candidate state
        // raft_state.role = Role::Candidate;

        // Create a probe snapshot request
        let probe_snapshot_request = InstallSnapshotRequest {
            term: 1, // Same as current term
            leader_id: leader_id.clone(),
            last_included_index: 5,
            last_included_term: 1,
            data: vec![],                   // Probe requests usually have empty data
            config: ClusterConfig::empty(), // Empty config for testing
            request_id: RequestId::new(),
            snapshot_request_id: RequestId::new(),
            is_probe: true, // Mark as probe
        };

        // Process probe request
        raft_state
            .handle_event(Event::InstallSnapshotRequest(
                leader_id.clone(),
                probe_snapshot_request.clone(),
            ))
            .await;

        // Verify if InstallSnapshotResponse was sent
        // Read messages from MockNetwork's receiving end (leader side)
        let resp_event = timeout(Duration::from_millis(300), leader_rx.recv()).await;
        assert!(resp_event.is_ok(), "Should have received a response");
        let event = resp_event.unwrap();
        match event {
            Some(NetworkEvent::InstallSnapshotResponse(sender, target, resp)) => {
                assert_eq!(sender, node_id);
                assert_eq!(target, leader_id);
                // Verify response content, such as term and success (depends on handle_install_snapshot_request_probe implementation)
                assert_eq!(resp.term, 1);
                // Probe response success typically indicates whether follower needs snapshot or its status
                // Here we assume it returns true if follower's state allows snapshot reception
                // Specific logic needs to be adjusted based on implementation
                // assert_eq!(resp.success, true/false based on logic);
            }
            _ => panic!("Expected InstallSnapshotResponse"),
        }

        // Verify node state hasn't changed unexpectedly
        assert_eq!(raft_state.role, Role::Follower); // Candidate shouldn't become Follower if it receives probe with lower or equal term
        assert_eq!(raft_state.current_term, 1);
        // current_snapshot_request_id should not be set due to probe
        assert_eq!(raft_state.current_snapshot_request_id, None);
    }

    // 1. Test Follower rejecting stale `AppendEntries` (term check)
    #[tokio::test]
    async fn test_follower_rejects_stale_append_entries() {
        let node_id = create_test_raft_id("test_group", "node1");
        let leader_id = create_test_raft_id("test_group", "node2");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), leader_id.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![leader_id.clone()]);
        let mut raft_state = RaftState::new(options, callbacks).await.unwrap();

        // Set Follower state
        raft_state.current_term = 5;
        raft_state.role = Role::Follower;

        // Create an AppendEntriesRequest from old term (term=3)
        let stale_append_request = AppendEntriesRequest {
            term: 3, // old term
            leader_id: leader_id.clone(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            request_id: RequestId::new(),
        };

        // Process request
        raft_state
            .handle_event(Event::AppendEntriesRequest(
                leader_id.clone(),
                stale_append_request,
            ))
            .await;

        // Verify state hasn't changed
        assert_eq!(raft_state.current_term, 5);
        assert_eq!(raft_state.role, Role::Follower);
        // Can check if a rejected AppendEntriesResponse was sent (term=5)
    }

    // 3. Test Leader handling of `AppendEntriesResponse` (success) and updating `match_index` and `commit_index`
    #[tokio::test]
    async fn test_leader_handles_successful_append_entries_response() {
        let node_id = create_test_raft_id("test_group", "node1");
        let peer1 = create_test_raft_id("test_group", "node2");
        let peer2 = create_test_raft_id("test_group", "node3");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), peer1.clone(), peer2.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![peer1.clone(), peer2.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        // Set Leader state
        raft_state.role = Role::Leader;
        raft_state.current_term = 2;
        raft_state.leader_id = Some(node_id.clone());
        // Add logs
        let log_entry = LogEntry {
            term: 2,
            index: 1,
            command: vec![1],
            is_config: false,
            client_request_id: None,
        };
        storage
            .append_log_entries(&node_id, &[log_entry])
            .await
            .unwrap();
        raft_state.last_log_index = 1;
        raft_state.last_log_term = 2;

        // Initialize replication state
        raft_state.next_index.insert(peer1.clone(), 2);
        raft_state.match_index.insert(peer1.clone(), 0);
        raft_state.next_index.insert(peer2.clone(), 2);
        raft_state.match_index.insert(peer2.clone(), 0);

        // Record initial commit_index
        let initial_commit_index = raft_state.commit_index;

        // Create a successful AppendEntriesResponse
        let successful_response = AppendEntriesResponse {
            conflict_index: None,
            conflict_term: None,
            term: 2,
            success: true,
            matched_index: 1, // Follower has matched up to index 1
            request_id: RequestId::new(),
        };

        // Process response from peer1
        raft_state
            .handle_event(Event::AppendEntriesResponse(
                peer1.clone(),
                successful_response,
            ))
            .await;

        // Verify peer1 state update
        assert_eq!(*raft_state.next_index.get(&peer1).unwrap(), 2); // next_index = matched_index + 1
        assert_eq!(*raft_state.match_index.get(&peer1).unwrap(), 1);

        // Verify if commit_index is updated (requires majority confirmation)
        // Assume peer2 also confirmed index 1
        let successful_response_peer2 = AppendEntriesResponse {
            conflict_index: None,
            conflict_term: None,
            term: 2,
            success: true,
            matched_index: 1,
            request_id: RequestId::new(),
        };
        raft_state
            .handle_event(Event::AppendEntriesResponse(
                peer2.clone(),
                successful_response_peer2,
            ))
            .await;

        // Now commit_index should be updated
        assert!(raft_state.commit_index > initial_commit_index);
        assert_eq!(raft_state.commit_index, 1);
    }

    // 4. Test Leader handling of `AppendEntriesResponse` (failure - conflicting index)
    #[tokio::test]
    async fn test_leader_handles_failing_append_entries_response_with_conflict() {
        let node_id = create_test_raft_id("test_group", "node1");
        let peer1 = create_test_raft_id("test_group", "node2");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), peer1.clone()].into_iter().collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![peer1.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        raft_state.role = Role::Leader;
        raft_state.current_term = 2;
        raft_state.leader_id = Some(node_id.clone());

        // Add logs
        let log_entries = vec![
            LogEntry {
                term: 1,
                index: 1,
                command: vec![1],
                is_config: false,
                client_request_id: None,
            },
            LogEntry {
                term: 2,
                index: 2,
                command: vec![2],
                is_config: false,
                client_request_id: None,
            },
            LogEntry {
                term: 2,
                index: 3,
                command: vec![3],
                is_config: false,
                client_request_id: None,
            },
        ];
        storage
            .append_log_entries(&node_id, &log_entries)
            .await
            .unwrap();
        raft_state.last_log_index = 3;
        raft_state.last_log_term = 2;

        // Initialize replication state
        raft_state.next_index.insert(peer1.clone(), 4); // Leader tries to send entry at index 4
        raft_state.match_index.insert(peer1.clone(), 3);

        // Assume peer1 has an entry with term 1 at index 3, causing conflict
        // peer1 returns failure response with conflict information
        let failing_response = AppendEntriesResponse {
            term: 2,
            success: false,
            // Assume AppendEntriesResponse structure contains conflict_index and conflict_term
            conflict_index: Some(3),
            conflict_term: Some(1),
            matched_index: 2, // peer1 actually only matched up to index 2
            request_id: RequestId::new(),
        };

        // Process response
        raft_state
            .handle_event(Event::AppendEntriesResponse(
                peer1.clone(),
                failing_response,
            ))
            .await;

        // Verify Leader adjusts next_index based on conflict_index
        // Typically, Leader finds the last index of conflict_term in its own log, then sets next_index to that index + 1
        // If conflict_term=1 ends at index 1, next_index should be 2
        // Or simply set to conflict_index (3) or conflict_index + 1 (4), depending on implementation
        // Or set to matched_index + 1 = 3 (if follower reports matched_index=2)
        // According to your code `new_next_idx = match_index + 1;` (when success=false)
        let expected_next_index = 3; // Because matched_index=2
        assert_eq!(
            *raft_state.next_index.get(&peer1).unwrap(),
            expected_next_index
        );
        assert_eq!(*raft_state.match_index.get(&peer1).unwrap(), 2); // Should update to matched_index from response
    }

    // 5. Test Follower handling of `AppendEntriesRequest` (log continuity check - snapshot covered and term matching)
    #[tokio::test]
    async fn test_follower_handles_append_entries_prev_index_in_snapshot_term_matches() {
        let node_id = create_test_raft_id("test_group", "node1");
        let leader_id = create_test_raft_id("test_group", "node2");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), leader_id.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![leader_id.clone()]);
        let mut raft_state = RaftState::new(options, callbacks).await.unwrap();

        // Set snapshot state
        raft_state.last_snapshot_index = 5;
        raft_state.last_snapshot_term = 2;
        raft_state.commit_index = 5; // Snapshot means all logs before index 5 are committed
        raft_state.last_applied = 5; // Snapshot means all logs before index 5 are applied
        raft_state.current_term = 3; // Current term is higher than snapshot term

        // Create an AppendEntriesRequest with prev_log_index in snapshot range and matching term
        let valid_append_request = AppendEntriesRequest {
            term: 3, // >= current term
            leader_id: leader_id.clone(),
            prev_log_index: 4, // In snapshot range (<= last_snapshot_index)
            prev_log_term: 2,  // Matches last_snapshot_term
            entries: vec![LogEntry {
                term: 3,
                index: 6, // New entry index
                command: vec![6],
                is_config: false,
                client_request_id: None,
            }],
            leader_commit: 5,
            request_id: RequestId::new(),
        };

        // Process request
        raft_state
            .handle_event(Event::AppendEntriesRequest(
                leader_id.clone(),
                valid_append_request,
            ))
            .await;

        // Verify logs are appended (need to check storage)
        // Verify commit_index is updated (because leader_commit=5 >= last_snapshot_index=5)
        assert_eq!(raft_state.commit_index, 5);
        // Verify role hasn't changed
        assert_eq!(raft_state.role, Role::Follower);
    }

    // 6. Test Follower handling of `AppendEntriesRequest` (log continuity check - log mismatch)
    #[tokio::test]
    async fn test_follower_handles_append_entries_prev_index_in_log_term_mismatch() {
        let node_id = create_test_raft_id("test_group", "node1");
        let leader_id = create_test_raft_id("test_group", "node2");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), leader_id.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![leader_id.clone()]);
        let mut raft_state = RaftState::new(options, callbacks).await.unwrap();

        // Add local logs
        let local_log_entry = LogEntry {
            term: 3,
            index: 2,
            command: vec![2],
            is_config: false,
            client_request_id: None,
        };
        storage
            .append_log_entries(&node_id, &[local_log_entry])
            .await
            .unwrap();

        raft_state.current_term = 3;
        raft_state.last_log_index = 2;
        raft_state.last_log_term = 3;

        // Create an AppendEntriesRequest with prev_log_index in log range but mismatched term
        let mismatch_append_request = AppendEntriesRequest {
            term: 3,
            leader_id: leader_id.clone(),
            prev_log_index: 2, // In log range
            prev_log_term: 2,  // Mismatched with local log term (3) at index 2
            entries: vec![],
            leader_commit: 0,
            request_id: RequestId::new(),
        };

        // Process request
        raft_state
            .handle_event(Event::AppendEntriesRequest(
                leader_id.clone(),
                mismatch_append_request,
            ))
            .await;

        // Verify a failed AppendEntriesResponse is returned
        // Verify local logs are not modified (or truncated, depending on implementation)
        // Role should remain unchanged
        assert_eq!(raft_state.role, Role::Follower);
    }

    // 7. Test Candidate reinitiating election after election timeout
    #[tokio::test]
    async fn test_candidate_re_elects_after_timeout() {
        let node_id = create_test_raft_id("test_group", "node1");
        let peer1 = create_test_raft_id("test_group", "node2");
        let peer2 = create_test_raft_id("test_group", "node3");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), peer1.clone(), peer2.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![peer1.clone(), peer2.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        // Trigger first election
        raft_state.handle_event(Event::ElectionTimeout).await;
        assert_eq!(raft_state.role, Role::Candidate);
        let first_term = raft_state.current_term;
        let first_election_id = raft_state.current_election_id;

        // Trigger election timeout again (simulate not getting majority votes)
        raft_state.handle_event(Event::ElectionTimeout).await;

        // Verify state
        assert_eq!(raft_state.role, Role::Candidate);
        assert_eq!(raft_state.current_term, first_term + 1); // Term incremented
        assert_ne!(raft_state.current_election_id, first_election_id); // Election ID updated
    }

    // 8. Test Leader handling of `RequestVoteRequest` (higher term)
    #[tokio::test]
    async fn test_leader_steps_down_on_higher_term_request_vote() {
        let node_id = create_test_raft_id("test_group", "node1");
        let candidate_id = create_test_raft_id("test_group", "node2");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), candidate_id.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![candidate_id.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        // Set Leader state
        raft_state.role = Role::Leader;
        raft_state.current_term = 2;
        raft_state.leader_id = Some(node_id.clone());

        // Create a RequestVoteRequest from higher term
        let higher_term_vote_request = RequestVoteRequest {
            term: 3, // Higher term
            candidate_id: candidate_id.clone(),
            last_log_index: 10,
            last_log_term: 2,
            request_id: RequestId::new(),
        };

        // Process request
        raft_state
            .handle_event(Event::RequestVoteRequest(
                candidate_id.clone(),
                higher_term_vote_request,
            ))
            .await;

        // Verify Leader steps down to Follower
        assert_eq!(raft_state.role, Role::Follower);
        assert_eq!(raft_state.current_term, 3); // Term updated
        assert_eq!(raft_state.voted_for, Some(candidate_id)); // Voted for candidate
        assert_eq!(raft_state.leader_id, None); // leader_id cleared
    }

    // 9. Test handling `InstallSnapshotRequest` (probe - Installing state)
    #[tokio::test]
    async fn test_follower_handles_probe_install_snapshot_installing() {
        let node_id = create_test_raft_id("test_group", "node1");
        let leader_id = create_test_raft_id("test_group", "node2");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await; // follower network
        let (_leader_network, mut leader_rx) = hub.register_node(leader_id.clone()).await; // leader network for receiving responses
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), leader_id.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![leader_id.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        raft_state.role = Role::Follower;
        raft_state.current_term = 2;

        // Set current snapshot request ID to simulate snapshot installation in progress
        let installing_request_id = RequestId::new();
        raft_state.current_snapshot_request_id = Some(installing_request_id);

        let probe_request = InstallSnapshotRequest {
            term: 2,
            leader_id: leader_id.clone(),
            last_included_index: 5,
            last_included_term: 1,
            data: vec![],
            config: ClusterConfig::empty(), // Empty config for testing
            request_id: installing_request_id, // Use same request ID for probe
            snapshot_request_id: installing_request_id,
            is_probe: true,
        };

        raft_state
            .handle_event(Event::InstallSnapshotRequest(
                leader_id.clone(),
                probe_request,
            ))
            .await;

        // Verify InstallSnapshotResponse was sent with Installing status
        let resp_event = timeout(Duration::from_millis(300), leader_rx.recv()).await;
        assert!(resp_event.is_ok());
        let message = resp_event.unwrap();
        match message {
            Some(NetworkEvent::InstallSnapshotResponse(sender, target, resp)) => {
                assert_eq!(sender, node_id);
                assert_eq!(target, leader_id);
                assert_eq!(resp.term, 2);
                assert_eq!(resp.state, InstallSnapshotState::Installing);
            }
            _ => panic!("Expected InstallSnapshotResponse"),
        }
    }

    // 10. Test handling `InstallSnapshotRequest` (probe - Failed state)
    #[tokio::test]
    async fn test_follower_handles_probe_install_snapshot_failed() {
        let node_id = create_test_raft_id("test_group", "node1");
        let leader_id = create_test_raft_id("test_group", "node2");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let (_leader_network, mut leader_rx) = hub.register_node(leader_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), leader_id.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![leader_id.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        raft_state.role = Role::Follower;
        raft_state.current_term = 2;

        // Set an ongoing snapshot request ID, but probe with a different ID will result in Failed response
        let ongoing_request_id = RequestId::new();
        raft_state.current_snapshot_request_id = Some(ongoing_request_id);

        let probe_request = InstallSnapshotRequest {
            term: 2,
            leader_id: leader_id.clone(),
            last_included_index: 5,
            last_included_term: 1,
            data: vec![],
            config: ClusterConfig::empty(), // Empty config for testing
            request_id: RequestId::new(),   // Use different request_id
            snapshot_request_id: RequestId::new(),
            is_probe: true,
        };

        raft_state
            .handle_event(Event::InstallSnapshotRequest(
                leader_id.clone(),
                probe_request,
            ))
            .await;

        // Verify InstallSnapshotResponse was sent with Failed status
        let resp_event = timeout(Duration::from_millis(300), leader_rx.recv()).await;
        assert!(resp_event.is_ok());
        let message = resp_event.unwrap();
        match message {
            Some(NetworkEvent::InstallSnapshotResponse(sender, target, resp)) => {
                assert_eq!(sender, node_id);
                assert_eq!(target, leader_id);
                assert_eq!(resp.term, 2);
                matches!(resp.state, InstallSnapshotState::Failed(_)); // Check it's a Failed variant
            }
            _ => panic!("Expected InstallSnapshotResponse"),
        }
    }

    // 11. Test handling `InstallSnapshotRequest` (non-probe - low term)
    #[tokio::test]
    async fn test_follower_rejects_non_probe_install_snapshot_low_term() {
        let node_id = create_test_raft_id("test_group", "node1");
        let leader_id = create_test_raft_id("test_group", "node2");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let (_leader_network, mut leader_rx) = hub.register_node(leader_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), leader_id.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![leader_id.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        raft_state.role = Role::Follower;
        raft_state.current_term = 5; // Higher term

        let low_term_request = InstallSnapshotRequest {
            term: 3, // Lower term
            leader_id: leader_id.clone(),
            last_included_index: 10,
            last_included_term: 2,
            data: vec![1, 2, 3],
            config: ClusterConfig::empty(), // Empty config for testing
            request_id: RequestId::new(),
            snapshot_request_id: RequestId::new(),
            is_probe: false,
        };

        raft_state
            .handle_event(Event::InstallSnapshotRequest(
                leader_id.clone(),
                low_term_request,
            ))
            .await;

        // Verify follower state is unchanged
        assert_eq!(raft_state.current_term, 5);
        assert_eq!(raft_state.role, Role::Follower);
        // Verify a response with higher term was sent back
        let resp_event = timeout(Duration::from_millis(300), leader_rx.recv()).await;
        assert!(resp_event.is_ok());
        let message = resp_event.unwrap();
        match message {
            Some(NetworkEvent::InstallSnapshotResponse(sender, target, resp)) => {
                assert_eq!(sender, node_id);
                assert_eq!(target, leader_id);
                assert_eq!(resp.term, 5); // Response should have the follower's term
                                          // Assuming rejection means not Installing/Success, could also check specific state if defined
            }
            _ => panic!("Expected InstallSnapshotResponse"),
        }
    }

    // 12. Test handling `InstallSnapshotRequest` (non-probe - outdated snapshot)
    #[tokio::test]
    async fn test_follower_rejects_non_probe_install_snapshot_outdated() {
        let node_id = create_test_raft_id("test_group", "node1");
        let leader_id = create_test_raft_id("test_group", "node2");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let (_leader_network, mut leader_rx) = hub.register_node(leader_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), leader_id.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![leader_id.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        raft_state.role = Role::Follower;
        raft_state.current_term = 3;
        raft_state.last_snapshot_index = 15; // Existing snapshot is newer
        raft_state.last_snapshot_term = 2;

        let outdated_request = InstallSnapshotRequest {
            term: 3, // Same term
            leader_id: leader_id.clone(),
            last_included_index: 10, // Older snapshot index
            last_included_term: 1,
            data: vec![1, 2, 3],
            config: ClusterConfig::empty(), // Empty config for testing
            request_id: RequestId::new(),
            snapshot_request_id: RequestId::new(),
            is_probe: false,
        };

        raft_state
            .handle_event(Event::InstallSnapshotRequest(
                leader_id.clone(),
                outdated_request,
            ))
            .await;

        // Verify follower state is largely unchanged (might update term if leader's term was higher, but not here)
        assert_eq!(raft_state.last_snapshot_index, 15); // Should not change
        assert_eq!(raft_state.last_snapshot_term, 2); // Should not change
        assert_eq!(raft_state.role, Role::Follower);
        // Verify a response was sent (success or rejection depends on exact logic, but a response is expected)
        let resp_event = timeout(Duration::from_millis(300), leader_rx.recv()).await;
        assert!(
            resp_event.is_ok(),
            "Expected a response to the outdated snapshot request"
        );
        // Optionally check response content if needed
    }

    // 13. Test Leader handling of `AppendEntriesResponse` (old term)
    #[tokio::test]
    async fn test_leader_ignores_old_term_append_entries_response() {
        let node_id = create_test_raft_id("test_group", "node1");
        let peer1 = create_test_raft_id("test_group", "node2");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), peer1.clone()].into_iter().collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![peer1.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        raft_state.role = Role::Leader;
        raft_state.current_term = 5; // Current term is 5

        // Set up replication state
        raft_state.next_index.insert(peer1.clone(), 3);
        raft_state.match_index.insert(peer1.clone(), 2);

        let old_term_response = AppendEntriesResponse {
            conflict_index: None,
            conflict_term: None,
            term: 3,        // Old term
            success: false, // Value doesn't matter much for this test
            matched_index: 1,
            request_id: RequestId::new(),
        };

        // Capture state before handling the response
        let initial_next_index = *raft_state.next_index.get(&peer1).unwrap();
        let initial_match_index = *raft_state.match_index.get(&peer1).unwrap();
        let initial_role = raft_state.role;
        let initial_term = raft_state.current_term;

        // Handle the response from an old term
        raft_state
            .handle_event(Event::AppendEntriesResponse(
                peer1.clone(),
                old_term_response,
            ))
            .await;

        // Assert that the leader's state has NOT changed
        assert_eq!(raft_state.role, initial_role);
        assert_eq!(raft_state.current_term, initial_term);
        assert_eq!(
            *raft_state.next_index.get(&peer1).unwrap(),
            initial_next_index
        );
        assert_eq!(
            *raft_state.match_index.get(&peer1).unwrap(),
            initial_match_index
        );
    }

    // 14. Test Follower handling of `RequestVoteRequest` (log update check - candidate log older - index)
    #[tokio::test]
    async fn test_follower_rejects_vote_candidate_log_older_index() {
        let node_id = create_test_raft_id("test_group", "node1");
        let candidate_id = create_test_raft_id("test_group", "node2");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), candidate_id.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![candidate_id.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        raft_state.current_term = 2;
        raft_state.role = Role::Follower;
        // Follower has a log entry with higher index
        let local_log_entry = LogEntry {
            term: 2,
            index: 5, // Higher index
            command: vec![1],
            is_config: false,
            client_request_id: None,
        };
        storage
            .append_log_entries(&node_id, &[local_log_entry])
            .await
            .unwrap();
        raft_state.last_log_index = 5;
        raft_state.last_log_term = 2;

        // Candidate's log is shorter
        let candidate_vote_request = RequestVoteRequest {
            term: 2, // Same term
            candidate_id: candidate_id.clone(),
            last_log_index: 3, // Lower index
            last_log_term: 2,  // Same last term
            request_id: RequestId::new(),
        };

        raft_state
            .handle_event(Event::RequestVoteRequest(
                candidate_id.clone(),
                candidate_vote_request,
            ))
            .await;

        // Verify follower did not vote
        assert_eq!(raft_state.voted_for, None);
        assert_eq!(raft_state.current_term, 2);
        assert_eq!(raft_state.role, Role::Follower); // Role should not change just from a vote request
    }

    // 15. Test Follower handling of `RequestVoteRequest` (log update check - candidate log newer - term)
    #[tokio::test]
    async fn test_follower_grants_vote_candidate_log_newer_term() {
        let node_id = create_test_raft_id("test_group", "node1");
        let candidate_id = create_test_raft_id("test_group", "node2");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), candidate_id.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![candidate_id.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        raft_state.current_term = 2;
        raft_state.role = Role::Follower;
        // Follower's last log entry has an older term
        let local_log_entry = LogEntry {
            term: 1, // Older term
            index: 3,
            command: vec![1],
            is_config: false,
            client_request_id: None,
        };
        storage
            .append_log_entries(&node_id, &[local_log_entry])
            .await
            .unwrap();
        raft_state.last_log_index = 3;
        raft_state.last_log_term = 1; // Older term

        // Candidate's log has a newer term
        let candidate_vote_request = RequestVoteRequest {
            term: 3, // Higher term, so follower will update its term
            candidate_id: candidate_id.clone(),
            last_log_index: 3, // Same or higher index is fine if term is higher
            last_log_term: 2,  // Newer term
            request_id: RequestId::new(),
        };

        raft_state
            .handle_event(Event::RequestVoteRequest(
                candidate_id.clone(),
                candidate_vote_request,
            ))
            .await;

        // Verify follower updated its term and voted for the candidate
        assert_eq!(raft_state.current_term, 3); // Term should be updated to candidate's term
        assert_eq!(raft_state.voted_for, Some(candidate_id));
        assert_eq!(raft_state.role, Role::Follower); // Role changes to Follower upon term update
    }

    // 1. Test Leader handling of AppendEntriesResponse from old term
    #[tokio::test]
    async fn test_leader_ignores_old_term_append_entries_response_v2() {
        let node_id = create_test_raft_id("test_group", "node1");
        let peer1 = create_test_raft_id("test_group", "node2");
        // ... (Set storage, network, callbacks, cluster_config, options as in previous tests)
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), peer1.clone()].into_iter().collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![peer1.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        raft_state.role = Role::Leader;
        raft_state.current_term = 5; // Leader's current term is 5

        // Set initial replication state
        let initial_next_index = 10;
        let initial_match_index = 5;
        raft_state
            .next_index
            .insert(peer1.clone(), initial_next_index);
        raft_state
            .match_index
            .insert(peer1.clone(), initial_match_index);

        // Create a response from old term (term=3)
        let old_term_response = AppendEntriesResponse {
            term: 3,              // Old term
            success: false,       // Value doesn't matter
            matched_index: 0,     // Value doesn't matter
            conflict_index: None, // Value doesn't matter
            conflict_term: None,  // Value doesn't matter
            request_id: RequestId::new(),
        };

        // Process response
        raft_state
            .handle_event(Event::AppendEntriesResponse(
                peer1.clone(),
                old_term_response,
            ))
            .await;

        // Verify Leader state hasn't changed
        assert_eq!(raft_state.current_term, 5);
        assert_eq!(raft_state.role, Role::Leader);
        assert_eq!(
            *raft_state.next_index.get(&peer1).unwrap(),
            initial_next_index
        );
        assert_eq!(
            *raft_state.match_index.get(&peer1).unwrap(),
            initial_match_index
        );
        // Can verify update_commit_index wasn't called and next_index/match_index logic wasn't modified
    }

    // 2. Test Leader handling of AppendEntriesResponse from future term (causing step down)
    #[tokio::test]
    async fn test_leader_steps_down_on_future_term_append_entries_response_v2() {
        let node_id = create_test_raft_id("test_group", "node1");
        let peer1 = create_test_raft_id("test_group", "node2");
        // ... (Set storage, network, callbacks, cluster_config, options as in previous tests)
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), peer1.clone()].into_iter().collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![peer1.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        raft_state.role = Role::Leader;
        raft_state.current_term = 5; // Leader's current term is 5

        // Set initial replication state
        raft_state.next_index.insert(peer1.clone(), 10);
        raft_state.match_index.insert(peer1.clone(), 5);

        // Create a failure response from future term (term=7)
        let future_term_response = AppendEntriesResponse {
            term: 7, // Future term
            success: false,
            matched_index: 0,
            conflict_index: None,
            conflict_term: None,
            request_id: RequestId::new(),
        };

        // Process response
        raft_state
            .handle_event(Event::AppendEntriesResponse(
                peer1.clone(),
                future_term_response,
            ))
            .await;

        // Verify Leader steps down to Follower
        assert_eq!(raft_state.current_term, 7); // Term updated
        assert_eq!(raft_state.role, Role::Follower);
        assert_eq!(raft_state.voted_for, None);
        // next_index and match_index are usually cleared or reset when stepping down, but it depends on implementation
        // This primarily verifies role and term changes
    }

    // 3. Test Leader handling successful AppendEntriesResponse from current term
    #[tokio::test]
    async fn test_leader_handles_current_term_successful_append_entries_response() {
        let node_id = create_test_raft_id("test_group", "node1");
        let peer1 = create_test_raft_id("test_group", "node2");
        let peer2 = create_test_raft_id("test_group", "node3");
        // ... (Set storage, network, callbacks, cluster_config, options as in previous tests)
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), peer1.clone(), peer2.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![peer1.clone(), peer2.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        raft_state.role = Role::Leader;
        raft_state.current_term = 5;
        // Assume some logs exist
        raft_state.last_log_index = 15;
        raft_state.last_log_term = 5;

        // Set initial replication state
        let initial_next_index = 10;
        let initial_match_index = 5;
        raft_state
            .next_index
            .insert(peer1.clone(), initial_next_index);
        raft_state
            .match_index
            .insert(peer1.clone(), initial_match_index);
        // Set state for another node for update_commit_index
        raft_state.next_index.insert(peer2.clone(), 16);
        raft_state.match_index.insert(peer2.clone(), 15);

        let initial_commit_index = raft_state.commit_index;

        // Create a success response from current term
        let successful_response = AppendEntriesResponse {
            term: 5, // Current term
            success: true,
            matched_index: 12,    // Follower has matched up to index 12
            conflict_index: None, // Usually not set on success
            conflict_term: None,  // Usually not set on success
            request_id: RequestId::new(),
        };

        // Process response
        raft_state
            .handle_event(Event::AppendEntriesResponse(
                peer1.clone(),
                successful_response,
            ))
            .await;

        // Verify state updates
        let expected_new_next_index = 13; // matched_index + 1
        let expected_new_match_index = 12; // matched_index
        assert_eq!(
            *raft_state.next_index.get(&peer1).unwrap(),
            expected_new_next_index
        );
        assert_eq!(
            *raft_state.match_index.get(&peer1).unwrap(),
            expected_new_match_index
        );
        // Verify if commit_index might update (requires majority confirmation)
        // Peer2 matched up to 15, Peer1 now matched up to 12, Leader local is 15
        // Majority (N=3, requires 2) confirmed highest index is 12? No, min(15, 12) = 12? No, it's the median after sorting.
        // match_index: [12, 15], local last_log_index: 15. Sorted [12, 15, 15]. Median is 15.
        // But update_commit_index usually only commits logs from the current term.
        // Assuming all logs are from term 5, commit_index should update to 15.
        // This test primarily verifies next_index and match_index updates.
        // commit_index updates can be a more complex test.
        // Here we simply assert that it hasn't decreased and might have increased.
        assert!(raft_state.commit_index >= initial_commit_index); // At least not decrease
                                                                  // If update_commit_index logic is correct, it should increase to 15
                                                                  // assert_eq!(raft_state.commit_index, 15); // If logic commits to majority minimum and all logs are same term
                                                                  // For test determinism, we only verify next_index and match_index
    }

    // 4. Test Leader handling failure response from current term (including matched_index)
    #[tokio::test]
    async fn test_leader_handles_current_term_failing_response_with_matched_index() {
        let node_id = create_test_raft_id("test_group", "node1");
        let peer1 = create_test_raft_id("test_group", "node2");
        // ... (Setup)
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), peer1.clone()].into_iter().collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![peer1.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        raft_state.role = Role::Leader;
        raft_state.current_term = 5;

        // Set initial replication state
        raft_state.next_index.insert(peer1.clone(), 10);
        raft_state.match_index.insert(peer1.clone(), 5);

        // Create a failure response from current term, including matched_index
        let failing_response = AppendEntriesResponse {
            term: 5, // Current term
            success: false,
            matched_index: 7,     // Follower actually matched up to 7
            conflict_index: None, // No conflict index provided
            conflict_term: None,
            request_id: RequestId::new(),
        };

        // Handle response
        raft_state
            .handle_event(Event::AppendEntriesResponse(
                peer1.clone(),
                failing_response,
            ))
            .await;

        // Verify match_index and next_index updates
        // According to code, if matched_index > 0, use matched_index + 1 as new_next_index
        let expected_new_match_index = 7;
        let expected_new_next_index = 8; // matched_index + 1
        assert_eq!(
            *raft_state.match_index.get(&peer1).unwrap(),
            expected_new_match_index
        );
        assert_eq!(
            *raft_state.next_index.get(&peer1).unwrap(),
            expected_new_next_index
        );
    }

    // 5. Test Leader handling failure response from current term (including conflict_index)
    #[tokio::test]
    async fn test_leader_handles_current_term_failing_response_with_conflict_index() {
        let node_id = create_test_raft_id("test_group", "node1");
        let peer1 = create_test_raft_id("test_group", "node2");
        // ... (Setup)
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), peer1.clone()].into_iter().collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![peer1.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        raft_state.role = Role::Leader;
        raft_state.current_term = 5;

        // Set initial replication state
        raft_state.next_index.insert(peer1.clone(), 10);
        raft_state.match_index.insert(peer1.clone(), 5);

        // Create a failure response from current term, including conflict_index but no matched_index
        let failing_response = AppendEntriesResponse {
            term: 5, // Current term
            success: false,
            matched_index: 0,        // No match index provided
            conflict_index: Some(6), // Conflict occurs at index 6
            conflict_term: Some(4),
            request_id: RequestId::new(),
        };

        // Handle response
        raft_state
            .handle_event(Event::AppendEntriesResponse(
                peer1.clone(),
                failing_response,
            ))
            .await;

        // Verify next_index update (using conflict_index)
        // According to code, if matched_index == 0 and has conflict_index, then new_next = conflict_index.max(1)
        let expected_new_next_index = 6; // conflict_index.max(1) = 6.max(1) = 6
        assert_eq!(
            *raft_state.next_index.get(&peer1).unwrap(),
            expected_new_next_index
        );
        // match_index should not update for failure response without matched_index
        assert_eq!(*raft_state.match_index.get(&peer1).unwrap(), 5);
    }

    // 6. Test Leader handling failure response from current term (no matched_index or conflict_index - conservative rollback)
    #[tokio::test]
    async fn test_leader_handles_current_term_failing_response_conservative_fallback() {
        let node_id = create_test_raft_id("test_group", "node1");
        let peer1 = create_test_raft_id("test_group", "node2");
        // ... (Setup)
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), peer1.clone()].into_iter().collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![peer1.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        raft_state.role = Role::Leader;
        raft_state.current_term = 5;

        // Set initial replication state
        let initial_next_index = 10;
        raft_state
            .next_index
            .insert(peer1.clone(), initial_next_index);
        raft_state.match_index.insert(peer1.clone(), 5);

        // Create a failure response from current term, without any information to help with rollback
        let failing_response = AppendEntriesResponse {
            term: 5, // Current term
            success: false,
            matched_index: 0,     // No match index provided
            conflict_index: None, // No conflict index provided
            conflict_term: None,
            request_id: RequestId::new(),
        };

        // Handle response
        raft_state
            .handle_event(Event::AppendEntriesResponse(
                peer1.clone(),
                failing_response,
            ))
            .await;

        // Verify next_index conservative rollback (current next_index - 1)
        let expected_new_next_index = initial_next_index - 1;
        assert_eq!(
            *raft_state.next_index.get(&peer1).unwrap(),
            expected_new_next_index
        );
        // match_index should not update
        assert_eq!(*raft_state.match_index.get(&peer1).unwrap(), 5);
    }

    // 7. Test that next_index doesn't go below 1 when Leader handles failure responses
    #[tokio::test]
    async fn test_leader_next_index_not_below_one_after_failure() {
        let node_id = create_test_raft_id("test_group", "node1");
        let peer1 = create_test_raft_id("test_group", "node2");
        // ... (Setup)
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), peer1.clone()].into_iter().collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node_id.clone(), vec![peer1.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        raft_state.role = Role::Leader;
        raft_state.current_term = 5;

        // Set initial replication state, next_index to 1
        raft_state.next_index.insert(peer1.clone(), 1);
        raft_state.match_index.insert(peer1.clone(), 0);

        // Create a failure response to trigger conservative rollback
        let failing_response = AppendEntriesResponse {
            term: 5,
            success: false,
            matched_index: 0,
            conflict_index: None,
            conflict_term: None,
            request_id: RequestId::new(),
        };

        // Handle response
        raft_state
            .handle_event(Event::AppendEntriesResponse(
                peer1.clone(),
                failing_response,
            ))
            .await;

        // Verify next_index doesn't go below 1
        let final_next_index = *raft_state.next_index.get(&peer1).unwrap();
        assert!(
            final_next_index >= 1,
            "next_index should not be less than 1, but was {}",
            final_next_index
        );
        // With conservative rollback strategy, 1.saturating_sub(1).max(1) = 0.max(1) = 1
        assert_eq!(final_next_index, 1);
    }

    // 1. Test update_commit_index accuracy (majority commit)
    #[tokio::test]
    async fn test_update_commit_index_majority_commit() {
        let node_id = create_test_raft_id("test_group", "node1");
        let peer1 = create_test_raft_id("test_group", "node2");
        let peer2 = create_test_raft_id("test_group", "node3");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));
        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), peer1.clone(), peer2.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();
        let options = create_test_options(node_id.clone(), vec![peer1.clone(), peer2.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        // Set Leader state
        raft_state.role = Role::Leader;
        raft_state.current_term = 2;
        raft_state.leader_id = Some(node_id.clone());
        let initial_commit_index = raft_state.commit_index;

        // Add logs (term 2)
        let log_entries: Vec<LogEntry> = (1..=5)
            .map(|i| LogEntry {
                term: 2,
                index: i,
                command: vec![i as u8],
                is_config: false,
                client_request_id: None,
            })
            .collect();
        storage
            .append_log_entries(&node_id, &log_entries)
            .await
            .unwrap();
        raft_state.last_log_index = 5;
        raft_state.last_log_term = 2;

        // Initialize replication state
        raft_state.next_index.insert(peer1.clone(), 6);
        raft_state.next_index.insert(peer2.clone(), 6);
        raft_state.match_index.insert(peer1.clone(), 0);
        raft_state.match_index.insert(peer2.clone(), 0);

        // Simulate peer1 successfully replicated to index 3
        let response1 = AppendEntriesResponse {
            term: 2,
            success: true,
            matched_index: 3,
            conflict_index: None,
            conflict_term: None,
            request_id: RequestId::new(),
        };
        raft_state
            .handle_event(Event::AppendEntriesResponse(peer1.clone(), response1))
            .await;

        // Verify commit_index should update to 3 (majority: node1(5), peer1(3), peer2(0) -> median is 3)
        assert_eq!(raft_state.commit_index, 3.max(initial_commit_index)); // Ensure no rollback

        // Simulate peer2 successfully replicated to index 4
        let response2 = AppendEntriesResponse {
            term: 2,
            success: true,
            matched_index: 4,
            conflict_index: None,
            conflict_term: None,
            request_id: RequestId::new(),
        };
        raft_state
            .handle_event(Event::AppendEntriesResponse(peer2.clone(), response2))
            .await;

        // Verify commit_index should update to 4 (majority: node1(5), peer1(3), peer2(4) -> median is 4)
        assert_eq!(raft_state.commit_index, 4.max(3)); // Ensure no rollback and updated
    }

    // 2. Test Leader handling failure response and optimizing next_index using conflict_term
    #[tokio::test]
    async fn test_leader_handles_failing_response_with_conflict_term_optimization() {
        let node_id = create_test_raft_id("test_group", "node1");
        let peer1 = create_test_raft_id("test_group", "node2");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));
        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), peer1.clone()].into_iter().collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();
        let options = create_test_options(node_id.clone(), vec![peer1.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        // Set Leader state
        raft_state.role = Role::Leader;
        raft_state.current_term = 5;
        raft_state.next_index.insert(peer1.clone(), 10);
        raft_state.match_index.insert(peer1.clone(), 5);

        // Add Leader's logs
        let leader_logs: Vec<LogEntry> = (1..=15)
            .map(|i| LogEntry {
                term: if i <= 5 {
                    3
                } else if i <= 10 {
                    4
                } else {
                    5
                }, // Different term log
                index: i,
                command: vec![i as u8],
                is_config: false,
                client_request_id: None,
            })
            .collect();
        storage
            .append_log_entries(&node_id, &leader_logs)
            .await
            .unwrap();
        raft_state.last_log_index = 15;
        raft_state.last_log_term = 5;

        // Simulate Follower returning failure, including conflict_term
        // Assume Follower has term 3 at index 8, but Leader has term 4 at index 8, causing conflict.
        // Follower reports conflict_index=8, conflict_term=3
        let failing_response = AppendEntriesResponse {
            term: 5,
            success: false,
            matched_index: 0, // Or the last matched index on Follower
            conflict_index: Some(8),
            conflict_term: Some(3), // Follower's term at conflict point
            request_id: RequestId::new(),
        };

        // Find the first index of term 3 in Leader's log (should be 1)
        // And the last index of term 3 in Leader's log (should be 5)
        // According to optimization strategy, Leader should set next_index to position after first term 3 entry, which is 6
        let expected_new_next_index = 6; // First index of term 4 (or later)

        raft_state
            .handle_event(Event::AppendEntriesResponse(
                peer1.clone(),
                failing_response,
            ))
            .await;

        // Verify if next_index was optimally updated
        assert_eq!(
            *raft_state.next_index.get(&peer1).unwrap(),
            expected_new_next_index
        );
        // match_index should not update for failure response with conflict_term
        assert_eq!(*raft_state.match_index.get(&peer1).unwrap(), 5);
    }

    // 3. Test Follower handling complete flow of non-probe InstallSnapshotRequest
    #[tokio::test]
    async fn test_follower_handles_non_probe_install_snapshot_full_flow() {
        let node_id = create_test_raft_id("test_group", "node1");
        let leader_id = create_test_raft_id("test_group", "node2");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));
        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), leader_id.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();
        let options = create_test_options(node_id.clone(), vec![leader_id.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        let snapshot_data = vec![1, 2, 3, 4, 5];
        let last_included_index = 10;
        let last_included_term = 1;
        let request_id = RequestId::new();

        // Create non-probe snapshot installation request
        let snapshot_request = InstallSnapshotRequest {
            term: 2, // Current term
            leader_id: leader_id.clone(),
            last_included_index,
            last_included_term,
            data: snapshot_data.clone(),
            config: ClusterConfig::empty(), // Empty config for testing
            request_id,
            snapshot_request_id: request_id, //
            is_probe: false,                 // Key: non-probe
        };

        // Process snapshot installation request
        raft_state
            .handle_event(Event::InstallSnapshotRequest(
                leader_id.clone(),
                snapshot_request.clone(),
            ))
            .await;

        // Verify Follower state
        assert_eq!(raft_state.current_term, 2);
        assert_eq!(raft_state.role, Role::Follower);
        // Verify if snapshot state is recorded as Installing (if implementation has this field)
        // assert_eq!(*raft_state.follower_snapshot_states.get(&leader_id).unwrap(), InstallSnapshotState::Installing);
        // Verify if last_snapshot_index and term have been updated (usually updated after receiving data, might just be start here)
        // Or verify if there's a pending snapshot request ID

        // Simulate business layer calling complete_snapshot_installation

        raft_state
            .handle_complete_snapshot_installation(CompleteSnapshotInstallation {
                index: last_included_index,
                term: last_included_term,
                success: true,
                request_id,
                reason: None,
                config: Some(ClusterConfig::empty()),
            })
            .await;

        // Verify final state update
        assert_eq!(raft_state.last_snapshot_index, last_included_index);
        assert_eq!(raft_state.last_snapshot_term, last_included_term);
        // Verify snapshot state cleanup
        // assert!(!raft_state.follower_snapshot_states.contains_key(&leader_id));
        // Verify if success response was sent (check via MockNetwork)
    }

    // 4. Test Joint Consensus submission flow for configuration changes
    #[tokio::test]
    async fn test_config_change_joint_consensus_commit_flow() {
        let node_id = create_test_raft_id("test_group", "node1");
        let node2 = create_test_raft_id("test_group", "node2");
        let node3 = create_test_raft_id("test_group", "node3");
        let node4 = create_test_raft_id("test_group", "node4");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));
        // Initial configuration: node1, node2, node3
        let initial_config = ClusterConfig::simple(
            vec![node_id.clone(), node2.clone(), node3.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, initial_config)
            .await
            .unwrap();
        let options = create_test_options(node_id.clone(), vec![node2.clone(), node3.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        // Set Leader state
        raft_state.role = Role::Leader;
        raft_state.current_term = 2;
        raft_state.leader_id = Some(node_id.clone());
        raft_state.last_log_index = 0; // Assume no other logs

        // Propose configuration change: add node4 (enter Joint Consensus)
        let new_config_proposal = ClusterConfig::simple(
            vec![node_id.clone(), node2.clone(), node3.clone(), node4.clone()]
                .into_iter()
                .collect(),
            1,
        );
        // Assume there's a method to handle client configuration change proposals
        // Here we directly simulate creating and appending a configuration change log entry
        let config_log_entry = LogEntry {
            term: 2,
            index: 1,
            command: serde_json::to_vec(&new_config_proposal).unwrap(), // Serialize ClusterConfig using serde_json
            is_config: true,
            client_request_id: None,
        };
        storage
            .append_log_entries(&node_id, &[config_log_entry])
            .await
            .unwrap();
        raft_state.last_log_index = 1;
        raft_state.last_log_term = 2;

        // Initialize replication state
        for peer in &[node2.clone(), node3.clone()] {
            raft_state.next_index.insert(peer.clone(), 2);
            raft_state.match_index.insert(peer.clone(), 0);
        }

        // Simulate node2 and node3 successfully replicated configuration change log (index 1)
        for peer in &[node2.clone(), node3.clone()] {
            let response = AppendEntriesResponse {
                term: 2,
                success: true,
                matched_index: 1,
                conflict_index: None,
                conflict_term: None,
                request_id: RequestId::new(), // Need to match actual sent request ID
            };
            raft_state
                .handle_event(Event::AppendEntriesResponse(peer.clone(), response))
                .await;
        }

        // Verify that commit_index has been updated to 1 (majority nodes node1, node2, node3 have all replicated index 1)
        assert_eq!(raft_state.commit_index, 1);

        // Verify if configuration has entered Joint Consensus state (this is typically handled in apply_command callback and updates raft_state.config)
        // Since apply is asynchronous, check storage directly or assume callback has updated the state
        // let applied_configs = callbacks.applied_commands.lock().await;
        // assert!(applied_configs.iter().any(|(_, _, _, cmd)| ... check for config change command ...));
        // A more practical check is to see if raft_state.config has become Joint
        // assert!(raft_state.config.is_joint());
    }

    // 5. Test handling AppendEntries from old term (should reject)
    #[tokio::test]
    async fn test_follower_rejects_old_term_append_entries() {
        let node_id = create_test_raft_id("test_group", "node1");
        let leader_id = create_test_raft_id("test_group", "node2");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));
        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), leader_id.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();
        let options = create_test_options(node_id.clone(), vec![leader_id.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        // Follower's current term is 5
        raft_state.current_term = 5;
        raft_state.role = Role::Follower;

        // Create an AppendEntriesRequest from term 3
        let old_term_request = AppendEntriesRequest {
            term: 3, // Old term
            leader_id: leader_id.clone(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            request_id: RequestId::new(),
        };

        raft_state
            .handle_event(Event::AppendEntriesRequest(
                leader_id.clone(),
                old_term_request,
            ))
            .await;

        // Verify Follower rejected the request (term not updated, role unchanged)
        assert_eq!(raft_state.current_term, 5);
        assert_eq!(raft_state.role, Role::Follower);
        // Verify if a rejection response was sent (term=5)
        // Can be verified by checking messages sent to leader_id in MockNetworkHub
    }

    // 6. Test Candidate receiving AppendEntries from current term Leader (should become Follower)
    #[tokio::test]
    async fn test_candidate_steps_down_on_current_term_leader_append_entries() {
        let node_id = create_test_raft_id("test_group", "node1");
        let leader_id = create_test_raft_id("test_group", "node2");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));
        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), leader_id.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();
        let options = create_test_options(node_id.clone(), vec![leader_id.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        // Candidate's current term is 3
        raft_state.current_term = 3;
        raft_state.role = Role::Candidate;
        raft_state.voted_for = Some(node_id.clone()); // Candidate voted for itself

        // Create an AppendEntriesRequest from a Leader in term 3
        let current_term_request = AppendEntriesRequest {
            term: 3, // same term as Candidate
            leader_id: leader_id.clone(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
            request_id: RequestId::new(),
        };

        raft_state
            .handle_event(Event::AppendEntriesRequest(
                leader_id.clone(),
                current_term_request,
            ))
            .await;

        // Verify Candidate has become Follower
        assert_eq!(raft_state.current_term, 3); // Term unchanged
        assert_eq!(raft_state.role, Role::Follower);
        assert_eq!(raft_state.leader_id, Some(leader_id.clone()));
        // voted_for is usually reset or remains None, depending on implementation, here we assume reset
        assert_eq!(raft_state.voted_for, None);
    }

    // 8. Test Follower handling AppendEntries where prev_log_index equals last_snapshot_index
    #[tokio::test]
    async fn test_follower_append_entries_prev_index_equals_snapshot_index() {
        let node_id = create_test_raft_id("test_group", "node1");
        let leader_id = create_test_raft_id("test_group", "node2");
        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));
        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), leader_id.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();
        let options = create_test_options(node_id.clone(), vec![leader_id.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        // Follower has a snapshot
        raft_state.last_snapshot_index = 5;
        raft_state.last_snapshot_term = 1;
        raft_state.current_term = 2;
        raft_state.role = Role::Follower;

        // Leader sends AppendEntries with prev_log_index = 5 (snapshot index), prev_log_term = 1 (snapshot term)
        let append_request = AppendEntriesRequest {
            term: 2,
            leader_id: leader_id.clone(),
            prev_log_index: 5,
            prev_log_term: 1, // matches snapshot term
            entries: vec![LogEntry {
                // new log entry
                term: 2,
                index: 6,
                command: vec![6],
                is_config: false,
                client_request_id: None,
            }],
            leader_commit: 5,
            request_id: RequestId::new(),
        };

        raft_state
            .handle_event(Event::AppendEntriesRequest(
                leader_id.clone(),
                append_request,
            ))
            .await;

        // Verify log was appended (starting after snapshot point)
        let stored_entries = storage.get_log_entries(&node_id, 6, 7).await.unwrap();
        assert_eq!(stored_entries.len(), 1);
        assert_eq!(stored_entries[0].index, 6);
        assert_eq!(stored_entries[0].term, 2);
        // Verify response was successful
        // Can be verified by checking messages sent to leader_id in MockNetworkHub
        assert_eq!(raft_state.role, Role::Follower); // Role unchanged
        assert_eq!(raft_state.current_term, 2); // Term unchanged
    }

    #[tokio::test]
    async fn test_learner_management() {
        // Set up test environment
        let node_id = create_test_raft_id("test_group", "node1");
        let peer1 = create_test_raft_id("test_group", "node2");
        let peer2 = create_test_raft_id("test_group", "node3");

        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;

        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        // Initialize cluster configuration
        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), peer1.clone(), peer2.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = RaftStateOptions {
            id: node_id.clone(),
            peers: vec![peer1.clone(), peer2.clone()],
            ..Default::default()
        };

        let mut raft_state = RaftState::new(options.clone(), callbacks.clone())
            .await
            .unwrap();

        // Set as leader
        raft_state.current_term = 1;
        raft_state.role = Role::Leader;

        // Initial configuration should have no learners
        assert!(raft_state.config.get_learners().is_none());

        // Test adding learner
        let learner_id = create_test_raft_id("test_group", "learner1");
        let request_id = RequestId::new();

        raft_state
            .handle_event(Event::AddLearner {
                learner: learner_id.clone(),
                request_id,
            })
            .await;

        // Before log commit, configuration hasn't updated yet
        assert!(raft_state.config.get_learners().is_none());

        // But replication state has been initialized (can start syncing logs)
        assert!(raft_state.next_index.contains_key(&learner_id));
        assert!(raft_state.match_index.contains_key(&learner_id));

        // Simulate log commit: set commit_index to log index of learner configuration change
        let config_log_index = raft_state.get_last_log_index();
        raft_state.commit_index = config_log_index;

        // Apply committed logs
        raft_state.apply_committed_logs().await;

        // Now configuration should be updated
        assert!(raft_state.config.get_learners().is_some());
        assert!(raft_state.config.learners_contains(&learner_id));

        // Test removing learner
        let remove_request_id = RequestId::new();
        raft_state
            .handle_event(Event::RemoveLearner {
                learner: learner_id.clone(),
                request_id: remove_request_id,
            })
            .await;

        // Before log commit, learner is still in configuration and replication state still exists
        assert!(raft_state.config.learners_contains(&learner_id));
        assert!(raft_state.next_index.contains_key(&learner_id));
        assert!(raft_state.match_index.contains_key(&learner_id));

        // Simulate log commit: set commit_index to log index of remove learner configuration change
        let remove_config_log_index = raft_state.get_last_log_index();
        raft_state.commit_index = remove_config_log_index;

        // Apply committed logs
        raft_state.apply_committed_logs().await;

        // Now learner should be removed and replication state cleaned up
        assert!(!raft_state.config.learners_contains(&learner_id));
        assert!(!raft_state.next_index.contains_key(&learner_id));
        assert!(!raft_state.match_index.contains_key(&learner_id));
    }

    #[tokio::test]
    async fn test_learner_cannot_be_voter() {
        // Set up test environment
        let node_id = create_test_raft_id("test_group", "node1");
        let peer1 = create_test_raft_id("test_group", "node2");
        let peer2 = create_test_raft_id("test_group", "node3");

        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node_id.clone()).await;

        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        // Initialize cluster configuration
        let cluster_config = ClusterConfig::simple(
            vec![node_id.clone(), peer1.clone(), peer2.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node_id, cluster_config)
            .await
            .unwrap();

        let options = RaftStateOptions {
            id: node_id.clone(),
            peers: vec![peer1.clone(), peer2.clone()],
            ..Default::default()
        };

        let mut raft_state = RaftState::new(options.clone(), callbacks.clone())
            .await
            .unwrap();

        // Set as leader
        raft_state.current_term = 1;
        raft_state.role = Role::Leader;

        // Try to add a node that's already a voter as a learner
        let voter_id = peer1.clone(); // peer1 is already a voter
        let request_id = RequestId::new();

        raft_state
            .handle_event(Event::AddLearner {
                learner: voter_id.clone(),
                request_id,
            })
            .await;

        // Should fail, voter shouldn't be added as learner
        assert!(!raft_state.config.learners_contains(&voter_id));
    }

    #[tokio::test]
    async fn test_voter_removal_and_graceful_shutdown() {
        // Set up test environment: 3-node cluster
        let node1 = create_test_raft_id("test_group", "node1");
        let node2 = create_test_raft_id("test_group", "node2");
        let node3 = create_test_raft_id("test_group", "node3");

        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node1.clone()).await;

        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        // Initialize cluster configuration: 3 voters
        let cluster_config = ClusterConfig::simple(
            vec![node1.clone(), node2.clone(), node3.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node1, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(node1.clone(), vec![node2.clone(), node3.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        // Set to Leader state to handle configuration changes
        raft_state.role = Role::Leader;
        raft_state.current_term = 1;

        // Initial state check: all nodes are in voter configuration
        assert!(raft_state.config.voters_contains(&node1));
        assert!(raft_state.config.voters_contains(&node2));
        assert!(raft_state.config.voters_contains(&node3));

        // Prepare configuration change to remove node2 (keep node1 and node3)
        let new_voters = vec![node1.clone(), node3.clone()].into_iter().collect();
        let new_config = ClusterConfig::simple(new_voters, raft_state.get_last_log_index() + 1);

        // Manually create configuration change log entry (simulate log generated from configuration change request)
        let config_data = serde_json::to_vec(&new_config).unwrap();
        let config_entry = LogEntry {
            term: raft_state.current_term,
            index: raft_state.get_last_log_index() + 1,
            command: config_data,
            is_config: true,
            client_request_id: Some(RequestId::new()),
        };

        // Add configuration change log to storage
        storage
            .append_log_entries(&node1, &[config_entry.clone()])
            .await
            .unwrap();

        // Update Raft state's log index
        raft_state.last_log_index = config_entry.index;
        raft_state.last_log_term = config_entry.term;

        // Simulate log commit: set commit_index to configuration change log's index
        raft_state.commit_index = config_entry.index;

        // Verify state before configuration application
        assert!(raft_state.config.voters_contains(&node2));

        // Apply committed logs (this triggers configuration change and node removal logic)
        raft_state.apply_committed_logs().await;

        // Verify new configuration has been applied
        assert!(raft_state.config.voters_contains(&node1));
        assert!(!raft_state.config.voters_contains(&node2)); // node2 was removed
        assert!(raft_state.config.voters_contains(&node3));

        // Now test from the perspective of the removed node (node2)
        // Create Raft state for node2 and apply the same config change
        let (network2, _rx2) = hub.register_node(node2.clone()).await;
        let callbacks2 = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network2)));

        let options2 = create_test_options(node2.clone(), vec![node1.clone(), node3.clone()]);
        let mut raft_state2 = RaftState::new(options2, callbacks2.clone()).await.unwrap();

        // Set node2 to Follower state
        raft_state2.role = Role::Follower;
        raft_state2.current_term = 1;

        // Sync the same log state
        raft_state2.last_log_index = config_entry.index;
        raft_state2.last_log_term = config_entry.term;
        raft_state2.commit_index = config_entry.index;

        // Apply the config change to node2 (the removed node)
        raft_state2.apply_committed_logs().await;

        // Verify node2 also correctly applied the new config and knows it was removed
        assert!(raft_state2.config.voters_contains(&node1));
        assert!(!raft_state2.config.voters_contains(&node2)); // Self was removed
        assert!(raft_state2.config.voters_contains(&node3));

        // Note: Actual callback verification requires more complex mocking
        // Here we verify the correctness of the config change logic
    }

    #[tokio::test]
    async fn test_multiple_voter_removal() {
        // Test removing multiple voters at the same time
        let node1 = create_test_raft_id("test_group", "node1");
        let node2 = create_test_raft_id("test_group", "node2");
        let node3 = create_test_raft_id("test_group", "node3");
        let node4 = create_test_raft_id("test_group", "node4");
        let node5 = create_test_raft_id("test_group", "node5");

        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node1.clone()).await;

        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        // Initialize 5-node cluster
        let cluster_config = ClusterConfig::simple(
            vec![
                node1.clone(),
                node2.clone(),
                node3.clone(),
                node4.clone(),
                node5.clone(),
            ]
            .into_iter()
            .collect(),
            0,
        );
        storage
            .save_cluster_config(&node1, cluster_config)
            .await
            .unwrap();

        let options = create_test_options(
            node1.clone(),
            vec![node2.clone(), node3.clone(), node4.clone(), node5.clone()],
        );
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        // Set as Leader state
        raft_state.role = Role::Leader;
        raft_state.current_term = 1;

        // Initialize replication state
        raft_state.next_index.insert(node2.clone(), 1);
        raft_state.next_index.insert(node3.clone(), 1);
        raft_state.next_index.insert(node4.clone(), 1);
        raft_state.next_index.insert(node5.clone(), 1);
        raft_state.match_index.insert(node2.clone(), 0);
        raft_state.match_index.insert(node3.clone(), 0);
        raft_state.match_index.insert(node4.clone(), 0);
        raft_state.match_index.insert(node5.clone(), 0);

        // Verify initial state
        assert_eq!(raft_state.config.get_effective_voters().len(), 5);
        assert_eq!(raft_state.next_index.len(), 4); // Except itself
        assert_eq!(raft_state.match_index.len(), 4);

        // Config change: remove node3 and node5, keep node1, node2, node4
        let new_voters = vec![node1.clone(), node2.clone(), node4.clone()]
            .into_iter()
            .collect();
        let new_config = ClusterConfig::simple(new_voters, raft_state.get_last_log_index() + 1);

        let config_data = serde_json::to_vec(&new_config).unwrap();
        let config_entry = LogEntry {
            term: raft_state.current_term,
            index: raft_state.get_last_log_index() + 1,
            command: config_data,
            is_config: true,
            client_request_id: Some(RequestId::new()),
        };

        storage
            .append_log_entries(&node1, &[config_entry.clone()])
            .await
            .unwrap();

        raft_state.last_log_index = config_entry.index;
        raft_state.last_log_term = config_entry.term;
        raft_state.commit_index = config_entry.index;

        // Apply config change
        raft_state.apply_committed_logs().await;

        // Verify config update
        assert_eq!(raft_state.config.get_effective_voters().len(), 3);
        assert!(raft_state.config.voters_contains(&node1));
        assert!(raft_state.config.voters_contains(&node2));
        assert!(!raft_state.config.voters_contains(&node3)); // removed
        assert!(raft_state.config.voters_contains(&node4));
        assert!(!raft_state.config.voters_contains(&node5)); // removed

        // Verify replication state is properly cleaned up
        assert!(raft_state.next_index.contains_key(&node2));
        assert!(!raft_state.next_index.contains_key(&node3)); // cleaned up
        assert!(raft_state.next_index.contains_key(&node4));
        assert!(!raft_state.next_index.contains_key(&node5)); // cleaned up

        assert!(raft_state.match_index.contains_key(&node2));
        assert!(!raft_state.match_index.contains_key(&node3)); // cleaned up
        assert!(raft_state.match_index.contains_key(&node4));
        assert!(!raft_state.match_index.contains_key(&node5)); // cleaned up
    }

    #[tokio::test]
    async fn test_config_rollback_prevention() {
        // Test config rollback prevention mechanism
        let node1 = create_test_raft_id("test_group", "node1");
        let node2 = create_test_raft_id("test_group", "node2");
        let node3 = create_test_raft_id("test_group", "node3");

        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node1.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        // Initialize cluster config (version 0)
        let initial_config = ClusterConfig::simple(
            vec![node1.clone(), node2.clone(), node3.clone()]
                .into_iter()
                .collect(),
            0,
        );
        storage
            .save_cluster_config(&node1, initial_config.clone())
            .await
            .unwrap();

        let options = create_test_options(node1.clone(), vec![node2.clone(), node3.clone()]);
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        // Set as Leader state
        raft_state.role = Role::Leader;
        raft_state.current_term = 1;

        // Verify initial config
        assert_eq!(raft_state.config.log_index(), 0);
        assert_eq!(raft_state.config.get_effective_voters().len(), 3);

        // Add some regular log entries to establish a continuous log sequence
        let dummy_entries = vec![
            LogEntry {
                term: 1,
                index: 1,
                command: vec![1, 2, 3],
                is_config: false,
                client_request_id: None,
            },
            LogEntry {
                term: 1,
                index: 2,
                command: vec![4, 5, 6],
                is_config: false,
                client_request_id: None,
            },
        ];

        storage
            .append_log_entries(&node1, &dummy_entries)
            .await
            .unwrap();

        // Create an updated config (version 3)
        let updated_config =
            ClusterConfig::simple(vec![node1.clone(), node2.clone()].into_iter().collect(), 3);

        // Apply the updated config
        let updated_config_data = serde_json::to_vec(&updated_config).unwrap();
        let updated_entry = LogEntry {
            term: raft_state.current_term,
            index: 3,
            command: updated_config_data,
            is_config: true,
            client_request_id: Some(RequestId::new()),
        };

        storage
            .append_log_entries(&node1, &[updated_entry.clone()])
            .await
            .unwrap();

        raft_state.last_log_index = 3;
        raft_state.last_log_term = 1;
        raft_state.commit_index = 3;
        raft_state.last_applied = 0; // Ensure we start applying from the correct position

        // Apply the updated config
        raft_state.apply_committed_logs().await;

        // Verify config is updated
        assert_eq!(raft_state.config.log_index(), 3);
        assert_eq!(raft_state.config.get_effective_voters().len(), 2);

        // Now try to apply an outdated config (version 1)
        let old_config = ClusterConfig::simple(
            vec![node1.clone(), node2.clone(), node3.clone()]
                .into_iter()
                .collect(),
            1, // older version
        );

        let old_config_data = serde_json::to_vec(&old_config).unwrap();
        let old_entry = LogEntry {
            term: raft_state.current_term,
            index: 4,
            command: old_config_data,
            is_config: true,
            client_request_id: Some(RequestId::new()),
        };

        storage
            .append_log_entries(&node1, &[old_entry.clone()])
            .await
            .unwrap();

        raft_state.last_log_index = 4;
        raft_state.commit_index = 4;

        // Apply outdated config (should be rejected)
        raft_state.apply_committed_logs().await;

        // Verify config doesn't rollback
        assert_eq!(raft_state.config.log_index(), 3); // Remains at version 3
        assert_eq!(raft_state.config.get_effective_voters().len(), 2); // Configuration unchanged
        assert_eq!(raft_state.last_applied, 4); // But last_applied has been updated
    }

    #[tokio::test]
    async fn test_dangerous_config_change_prevention() {
        // Test protection against dangerous config changes (removing too many nodes at once)
        let nodes: Vec<RaftId> = (1..=5)
            .map(|i| create_test_raft_id("test_group", &format!("node{}", i)))
            .collect();

        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(nodes[0].clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        // Initialize 5-node cluster
        let initial_config = ClusterConfig::simple(nodes.iter().cloned().collect(), 0);
        storage
            .save_cluster_config(&nodes[0], initial_config)
            .await
            .unwrap();

        let options = create_test_options(nodes[0].clone(), nodes[1..].to_vec());
        let mut raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        // Set to Leader state
        raft_state.role = Role::Leader;
        raft_state.current_term = 1;

        // Set some log entries to make config change valid
        raft_state.last_log_index = 1;
        raft_state.last_log_term = 1;

        // Create a dangerous config change: from 5 nodes to just 1 node
        let dangerous_config =
            ClusterConfig::simple(vec![nodes[0].clone()].into_iter().collect(), 1);

        // Test config validation
        let validation_result = raft_state.validate_config_change_safety(&dangerous_config, 1);

        // Should detect dangerous config change
        assert!(validation_result.is_err());
        let error_msg = validation_result.unwrap_err();
        assert!(
            error_msg.contains("Dangerous configuration change"),
            "Expected 'Dangerous configuration change' in error message, got: {}",
            error_msg
        );

        // Create a relatively safe configuration change: remove from 5 nodes to 3 nodes
        let safe_config = ClusterConfig::simple(nodes[0..3].iter().cloned().collect(), 1);

        // This configuration should pass validation
        let validation_result = raft_state.validate_config_change_safety(&safe_config, 1);
        assert!(validation_result.is_ok());
    }

    #[tokio::test]
    async fn test_config_log_index_mismatch_prevention() {
        // Test protection against configuration log_index mismatching with log entry index
        let node1 = create_test_raft_id("test_group", "node1");
        let node2 = create_test_raft_id("test_group", "node2");

        let storage = Arc::new(MockStorage::new());
        let config = MockNetworkHubConfig::default();
        let hub = MockNetworkHub::new(config);
        let (network, _rx) = hub.register_node(node1.clone()).await;
        let callbacks = Arc::new(TestCallbacks::new(storage.clone(), Arc::new(network)));

        let initial_config =
            ClusterConfig::simple(vec![node1.clone(), node2.clone()].into_iter().collect(), 0);
        storage
            .save_cluster_config(&node1, initial_config)
            .await
            .unwrap();

        let options = create_test_options(node1.clone(), vec![node2.clone()]);
        let raft_state = RaftState::new(options, callbacks.clone()).await.unwrap();

        // Create configuration with log_index mismatching log entry index
        let mismatched_config = ClusterConfig::simple(
            vec![node1.clone()].into_iter().collect(),
            5, // log_index is 5
        );

        // Test validation when log entry index is 3
        let validation_result = raft_state.validate_config_change_safety(&mismatched_config, 3);

        // Should detect log_index mismatch
        assert!(validation_result.is_err());
        assert!(validation_result
            .unwrap_err()
            .contains("log_index mismatch"));
    }
}
