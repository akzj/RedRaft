use super::*;

// === 回调接口的默认实现（示例）===
#[derive(Clone)]
pub struct DefaultCallbacks {
    network: Arc<dyn Network + Send + Sync>,
    // 实际实现中可以包含存储逻辑
}

impl DefaultCallbacks {
    pub fn new(network: Arc<dyn Network + Send + Sync>) -> Self {
        Self { network }
    }
}

impl RaftCallbacks for DefaultCallbacks {
    fn send_request_vote_request(
        &self,
        target: NodeId,
        args: RequestVoteRequest,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let network = self.network.clone();
        Box::pin(async move {
            let _ = network.send_request_vote(target, args).await;
        })
    }

    fn send_request_vote_response(
        &self,
        target: NodeId,
        _args: RequestVoteResponse,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move {
            // 实际实现应将响应发送给目标节点
        })
    }

    fn send_append_entries_request(
        &self,
        target: NodeId,
        args: AppendEntriesRequest,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let network = self.network.clone();
        Box::pin(async move {
            let _ = network.send_append_entries(target, args).await;
        })
    }

    fn send_append_entries_response(
        &self,
        target: NodeId,
        _args: AppendEntriesResponse,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move {
            // 实际实现应将响应发送给目标节点
        })
    }

    fn send_install_snapshot_request(
        &self,
        target: NodeId,
        args: InstallSnapshotRequest,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let network = self.network.clone();
        Box::pin(async move {
            let _ = network.send_install_snapshot(target, args).await;
        })
    }

    fn send_install_snapshot_reply(
        &self,
        target: NodeId,
        _args: InstallSnapshotResponse,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move {
            // 实际实现应将响应发送给目标节点
        })
    }

    fn save_hard_state(
        &self,
        _term: u64,
        _voted_for: Option<NodeId>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async {})
    }

    fn load_hard_state(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<(u64, Option<NodeId>), Error>> + Send>> {
        Box::pin(async { Ok((0, None)) })
    }

    fn append_log_entries(
        &self,
        _entries: &[LogEntry],
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>> {
        Box::pin(async { Ok(()) })
    }

    fn get_log_entries(
        &self,
        _low: u64,
        _high: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<LogEntry>, Error>> + Send>> {
        Box::pin(async { Ok(vec![]) })
    }

    fn truncate_log_suffix(
        &self,
        _idx: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>> {
        Box::pin(async { Ok(()) })
    }

    fn truncate_log_prefix(
        &self,
        _idx: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>> {
        Box::pin(async { Ok(()) })
    }

    fn get_last_log_index(&self) -> Pin<Box<dyn Future<Output = Result<u64, Error>> + Send>> {
        Box::pin(async { Ok(0) })
    }

    fn get_log_term(&self, _idx: u64) -> Pin<Box<dyn Future<Output = Result<u64, Error>> + Send>> {
        Box::pin(async { Ok(0) })
    }

    fn save_snapshot(
        &self,
        _snap: Snapshot,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>> {
        Box::pin(async { Ok(()) })
    }

    fn load_snapshot(&self) -> Pin<Box<dyn Future<Output = Result<Snapshot, Error>> + Send>> {
        Box::pin(async {
            Ok(Snapshot {
                index: 0,
                term: 0,
                data: vec![],
                config: ClusterConfig::empty(),
            })
        })
    }

    fn save_cluster_config(
        &self,
        _conf: ClusterConfig,
    ) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>> {
        Box::pin(async { Ok(()) })
    }

    fn load_cluster_config(
        &self,
    ) -> Pin<Box<dyn Future<Output = Result<ClusterConfig, Error>> + Send>> {
        Box::pin(async { Ok(ClusterConfig::empty()) })
    }

    fn set_election_timer(&self, _dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async {})
    }

    fn set_heartbeat_timer(&self, _dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async {})
    }

    fn set_apply_timer(&self, _dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async {})
    }

    fn client_response(
        &self,
        _request_id: RequestId,
        _result: Result<u64, Error>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async {})
    }

    fn state_changed(&self, _role: Role) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async {})
    }

    fn apply_command(
        &self,
        _index: u64,
        _term: u64,
        _cmd: Command,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async {})
    }

    fn set_config_change_timer(&self, dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        todo!()
    }

    fn process_snapshot(
        &self,
        index: u64,
        term: u64,
        data: Vec<u8>,
        request_id: RequestId,
    ) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        todo!()
    }
}
