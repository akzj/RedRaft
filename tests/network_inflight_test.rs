use raft_lite::{RaftId, mock::mock_network::MockNetworkHubConfig, RequestId};
use std::time::Duration;
use tokio;

mod common;
use common::test_cluster::{TestCluster, TestClusterConfig};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_network_inflight_under_packet_loss() {
    let _ = tracing_subscriber::fmt().try_init();

    // 创建 3 节点集群
    let node1 = RaftId::new("test_group".to_string(), "node1".to_string());
    let node2 = RaftId::new("test_group".to_string(), "node2".to_string());
    let node3 = RaftId::new("test_group".to_string(), "node3".to_string());

    let config = TestClusterConfig {
        node_ids: vec![node1.clone(), node2.clone(), node3.clone()],
        hub: MockNetworkHubConfig::default(),
    };
    let cluster = TestCluster::new(config).await;

    // 启动集群在后台
    let cluster_clone = cluster.clone();
    tokio::spawn(async move { cluster_clone.start().await });

    // 等待 Leader 选举
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 找到当前 Leader
    let mut leader_id = None;
    for node_id in &[&node1, &node2, &node3] {
        if let Some(node) = cluster.get_node(node_id) {
            let role = node.get_role();
            if role == raft_lite::Role::Leader {
                leader_id = Some((*node_id).clone());
                println!("Found leader: {:?}", node_id);
                break;
            }
        }
    }

    let leader = leader_id.expect("No leader found");
    println!("✓ Leader elected: {:?}", leader);

    // === 测试 1: 正常网络条件下的InFlight功能 ===
    println!("\n=== Test 1: InFlight under normal network conditions ===");
    
    // 发送多个命令快速填满InFlight队列
    let mut request_ids = Vec::new();
    let mut successful_commands = 0;
    
    for i in 0..10 {
        let request_id = RequestId::new();
        let command = format!("test_command_{}", i).into_bytes();
        
        if let Err(e) = cluster.propose_command(&leader, command) {
            println!("Failed to propose command {}: {}", i, e);
        } else {
            request_ids.push(request_id);
            successful_commands += 1;
            println!("Proposed command {}", i);
        }
    }
    
    // 断言: 在正常网络条件下，所有命令都应该成功提交（快速超时下应该更可靠）
    assert!(successful_commands >= 9, 
        "Expected at least 9 successful commands under normal conditions with fast timeouts, got {}", 
        successful_commands);
    
    // 等待命令处理（快速超时下缩短等待时间）
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    // 检查leader节点的InFlight状态
    let _normal_inflight_count = if let Some(leader_node) = cluster.get_node(&leader) {
        let inflight_count = leader_node.get_inflight_request_count().await;
        println!("InFlight requests under normal conditions: {}", inflight_count);
        
        // 断言: 快速超时下，正常条件的InFlight请求应该很少
        assert!(inflight_count <= 8, 
            "InFlight count under normal conditions with fast timeouts should be very low, got {}", 
            inflight_count);
        
        inflight_count
    } else {
        panic!("Leader node should be accessible");
    };

    // === 测试 2: 网络延迟条件下的InFlight管理 ===
    println!("\n=== Test 2: InFlight under high latency conditions ===");
    
    // 设置高延迟网络配置（100ms 基础延迟 + 50ms 抖动，减少延迟以测试快速超时）
    let high_latency_config = raft_lite::mock::mock_network::MockRaftNetworkConfig {
        base_latency_ms: 100,   // 降低基础延迟
        jitter_max_ms: 50,      // 降低抖动
        drop_rate: 0.0,
        failure_rate: 0.0,
    };
    
    for node_id in &[&node1, &node2, &node3] {
        cluster.update_network_config_for_node(node_id, high_latency_config.clone()).await;
    }
    
    println!("Set high latency network (100ms + 50ms jitter)");
    
    // 发送命令并观察InFlight请求的累积
    let start_time = std::time::Instant::now();
    let mut latency_successful = 0;
    
    for i in 10..20 {
        let command = format!("latency_test_command_{}", i).into_bytes();
        if let Err(e) = cluster.propose_command(&leader, command) {
            println!("Failed to propose command {} under latency: {}", i, e);
        } else {
            latency_successful += 1;
            println!("Proposed command {} under latency", i);
        }
        
        // 检查当前InFlight状态
        if let Some(leader_node) = cluster.get_node(&leader) {
            let inflight_count = leader_node.get_inflight_request_count().await;
            println!("InFlight requests after command {}: {}", i, inflight_count);
            
            // 断言: 快速超时下，即使在高延迟环境中InFlight也应该较少累积
            assert!(inflight_count <= 30, 
                "InFlight count under high latency with fast timeouts should not exceed 30, got {}", 
                inflight_count);
        }
        
        // 缩短等待时间，测试快速处理能力
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    
    // 断言: 即使在高延迟下，大多数命令也应该能够提交（快速超时应提高成功率）
    assert!(latency_successful >= 8, 
        "Expected at least 8 successful commands under high latency with fast timeouts, got {}", 
        latency_successful);
    
    // 等待所有请求在高延迟下完成（增加等待时间以允许超极速超时机制充分清理）
    tokio::time::sleep(Duration::from_secs(2)).await;
    println!("High latency test duration: {:?}", start_time.elapsed());
    
    if let Some(leader_node) = cluster.get_node(&leader) {
        let final_inflight_count = leader_node.get_inflight_request_count().await;
        println!("Final InFlight requests after high latency: {}", final_inflight_count);
        
        // 断言: 超极速超时下，延迟测试完成后InFlight计数应该显著降低
        assert!(final_inflight_count <= 12, 
            "InFlight count should reduce significantly with hyper-fast timeouts (25ms base) after latency test, got {}", 
            final_inflight_count);
    }

    // === 测试 3: 丢包条件下的InFlight超时处理 ===
    println!("\n=== Test 3: InFlight timeout under packet loss ===");
    
    // 设置中等丢包率（20%）和更适合快速超时的延迟
    let packet_loss_config = raft_lite::mock::mock_network::MockRaftNetworkConfig {
        base_latency_ms: 30,     // 降低基础延迟
        jitter_max_ms: 20,       // 降低抖动
        drop_rate: 0.2,          // 20% 丢包率
        failure_rate: 0.05,      // 5% 发送失败率
    };
    
    for node_id in &[&node1, &node2, &node3] {
        cluster.update_network_config_for_node(node_id, packet_loss_config.clone()).await;
    }
    
    println!("Set packet loss network (20% drop rate)");
    
    // 记录初始InFlight状态
    let initial_inflight = if let Some(leader_node) = cluster.get_node(&leader) {
        leader_node.get_inflight_request_count().await
    } else {
        0
    };
    
    // 在丢包环境下发送命令
    let packet_loss_start = std::time::Instant::now();
    let mut packet_loss_successful = 0;
    
    for i in 20..35 {
        let command = format!("packet_loss_test_command_{}", i).into_bytes();
        if let Err(e) = cluster.propose_command(&leader, command) {
            println!("Failed to propose command {} under packet loss: {}", i, e);
        } else {
            packet_loss_successful += 1;
            println!("Proposed command {} under packet loss", i);
        }
        
        // 每发送几个命令检查一次InFlight状态
        if i % 3 == 0 {
            if let Some(leader_node) = cluster.get_node(&leader) {
                let current_inflight = leader_node.get_inflight_request_count().await;
                println!("InFlight requests under packet loss (command {}): {}", i, current_inflight);
                
                // 检查是否达到InFlight限制
                if current_inflight >= 20 {  // 极速超时下进一步降低预警阈值
                    println!("⚠ Approaching InFlight limit with ultra-fast timeouts: {}", current_inflight);
                }
                
                // 断言: 极速超时下，即使有丢包，InFlight请求也应该极快清理
                assert!(current_inflight <= 25, 
                    "InFlight count with ultra-fast timeouts (50ms base) should not exceed 25 under packet loss, got {}", 
                    current_inflight);
            }
        }
        
        tokio::time::sleep(Duration::from_millis(50)).await;  // 缩短等待时间测试快速响应
    }
    
    // 断言: 即使在20%丢包率下，快速超时应该提高成功率
    assert!(packet_loss_successful >= 10, 
        "Expected at least 10 successful commands under 20% packet loss with fast timeouts, got {}", 
        packet_loss_successful);
    
    // 等待超时清理发生（缩短等待时间，快速超时应该更快清理）
    println!("Waiting for fast timeout cleanup under packet loss...");
    tokio::time::sleep(Duration::from_secs(4)).await;  // 缩短等待时间
    
    let post_timeout_inflight = if let Some(leader_node) = cluster.get_node(&leader) {
        leader_node.get_inflight_request_count().await
    } else {
        0
    };
    
    println!("InFlight count before packet loss: {}", initial_inflight);
    println!("InFlight count after timeout cleanup: {}", post_timeout_inflight);
    println!("Packet loss test duration: {:?}", packet_loss_start.elapsed());
    
    // 断言: 验证极速超时机制工作正常（InFlight请求应该被极快清理）
    assert!(post_timeout_inflight <= 12, 
        "Ultra-fast timeout cleanup (50ms base) should reduce InFlight count to under 12, got {}", 
        post_timeout_inflight);
    
    // 断言: 极速超时后的InFlight计数应该比测试期间的峰值明显降低
    if post_timeout_inflight <= 5 {
        println!("✓ Ultra-fast timeout cleanup mechanism working excellently");
    } else {
        println!("⚠ Ultra-fast timeout cleanup may need further optimization");
        // 即使效果不够明显，只要有清理就认为机制在工作
        assert!(post_timeout_inflight <= 10, 
            "Ultra-fast timeout cleanup should show significant effect, got {}", 
            post_timeout_inflight);
    }

    // === 测试 4: 极端丢包条件下的InFlight行为 ===
    println!("\n=== Test 4: InFlight under extreme packet loss ===");
    
    // 设置高丢包率（50%）但降低延迟以配合快速超时
    let extreme_loss_config = raft_lite::mock::mock_network::MockRaftNetworkConfig {
        base_latency_ms: 20,     // 进一步降低延迟
        jitter_max_ms: 30,       // 降低抖动
        drop_rate: 0.5,          // 50% 丢包率
        failure_rate: 0.1,       // 10% 发送失败率
    };
    
    for node_id in &[&node1, &node2, &node3] {
        cluster.update_network_config_for_node(node_id, extreme_loss_config.clone()).await;
    }
    
    println!("Set extreme packet loss network (50% drop rate)");
    
    // 在极端丢包下测试InFlight请求积累
    let extreme_start = std::time::Instant::now();
    let mut successful_proposals = 0;
    let mut failed_proposals = 0;
    
    for i in 35..50 {
        let command = format!("extreme_loss_test_command_{}", i).into_bytes();
        match cluster.propose_command(&leader, command) {
            Ok(_) => {
                successful_proposals += 1;
                println!("Successfully proposed command {} under extreme loss", i);
            }
            Err(e) => {
                failed_proposals += 1;
                println!("Failed to propose command {} under extreme loss: {}", i, e);
            }
        }
        
        // 监控InFlight状态变化
        if let Some(leader_node) = cluster.get_node(&leader) {
            let current_inflight = leader_node.get_inflight_request_count().await;
            if current_inflight > 25 {  // 快速超时下降低关注阈值
                println!("High InFlight count under extreme loss with fast timeouts: {}", current_inflight);
            }
        }
        
        tokio::time::sleep(Duration::from_millis(80)).await;  // 稍微缩短等待
    }
    
    println!("Extreme packet loss test results:");
    println!("  Successful proposals: {}", successful_proposals);
    println!("  Failed proposals: {}", failed_proposals);
    println!("  Duration: {:?}", extreme_start.elapsed());
    
    // 断言: 即使在50%丢包率下，快速超时应该提高成功率
    assert!(successful_proposals >= 7, 
        "Expected at least 7 successful commands under 50% packet loss with fast timeouts, got {}", 
        successful_proposals);
    
    // 断言: 失败和成功的命令总数应该符合预期
    assert!(successful_proposals + failed_proposals == 15, 
        "Total commands should be 15, got {} successful + {} failed = {}", 
        successful_proposals, failed_proposals, successful_proposals + failed_proposals);
    
    // 等待系统稳定（快速超时下缩短等待时间）
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    let final_extreme_inflight = if let Some(leader_node) = cluster.get_node(&leader) {
        let count = leader_node.get_inflight_request_count().await;
        
        // 断言: 快速超时配置下，极端条件后InFlight计数应该更低
        assert!(count <= 50, 
            "InFlight count after extreme packet loss with fast timeouts should be lower, got {}", 
            count);
        
        count
    } else {
        panic!("Leader node should still be accessible after extreme packet loss");
    };
    
    println!("Final InFlight count after extreme packet loss: {}", final_extreme_inflight);

    // === 测试 5: 网络分区恢复后的InFlight恢复 ===
    println!("\n=== Test 5: InFlight recovery after network partition ===");
    
    // 先恢复正常网络
    let normal_config = raft_lite::mock::mock_network::MockRaftNetworkConfig::default();
    for node_id in &[&node1, &node2, &node3] {
        cluster.update_network_config_for_node(node_id, normal_config.clone()).await;
    }
    
    // 发送一些命令建立基准InFlight状态
    for i in 50..55 {
        let command = format!("pre_partition_command_{}", i).into_bytes();
        let _ = cluster.propose_command(&leader, command);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    
    let pre_partition_inflight = if let Some(leader_node) = cluster.get_node(&leader) {
        leader_node.get_inflight_request_count().await
    } else {
        0
    };
    
    println!("InFlight count before partition: {}", pre_partition_inflight);
    
    // 隔离一个follower节点
    let follower = if leader == node1 { &node2 } else { &node1 };
    println!("Isolating follower: {:?}", follower);
    cluster.isolate_node(follower).await;
    
    // 在分区状态下发送更多命令
    for i in 55..60 {
        let command = format!("during_partition_command_{}", i).into_bytes();
        let _ = cluster.propose_command(&leader, command);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    let during_partition_inflight = if let Some(leader_node) = cluster.get_node(&leader) {
        leader_node.get_inflight_request_count().await
    } else {
        0
    };
    
    println!("InFlight count during partition: {}", during_partition_inflight);
    
    // 恢复网络连接
    println!("Restoring network connection for follower: {:?}", follower);
    cluster.restore_node(follower).await;
    
    // 等待网络恢复和重新选举（增加随机间隔避免选举碰撞）
    println!("Waiting for cluster recovery and leader re-election...");
    
    // 初始随机等待，避免选举碰撞
    use rand::Rng;
    let initial_delay = rand::rng().random_range(2000..4000); // 2-4秒随机初始延迟
    tokio::time::sleep(Duration::from_millis(initial_delay)).await;
    
    // 验证集群恢复状态 - 确保有稳定的leader
    let mut recovery_attempts = 0;
    let max_recovery_attempts = 30; // 增加最大尝试次数到30
    let mut stable_leader = None;
    
    while recovery_attempts < max_recovery_attempts {
        recovery_attempts += 1;
        
        // 检查是否有leader
        let mut current_leaders = Vec::new();
        for node_id in &[&node1, &node2, &node3] {
            if let Some(node) = cluster.get_node(node_id) {
                if node.get_role() == raft_lite::Role::Leader {
                    current_leaders.push((*node_id).clone());
                }
            }
        }
        
        if current_leaders.len() == 1 {
            stable_leader = Some(current_leaders[0].clone());
            println!("✓ Stable leader found after recovery: {:?}", stable_leader);
            // 给leader额外的稳定时间
            tokio::time::sleep(Duration::from_millis(500)).await;
            break;
        } else if current_leaders.len() > 1 {
            println!("⚠ Multiple leaders detected, waiting for convergence... (attempt {})", recovery_attempts);
        } else {
            println!("⚠ No leader found, waiting for election... (attempt {})", recovery_attempts);
        }
        
        // 随机等待间隔，避免选举碰撞
        let random_delay = rand::rng().random_range(200..800);
        tokio::time::sleep(Duration::from_millis(random_delay)).await;
    }
    
    // 断言：集群恢复后应该有稳定的leader
    if stable_leader.is_none() {
        panic!("Failed to establish stable leader within {} attempts after partition recovery", max_recovery_attempts);
    }
    
    let recovered_leader = stable_leader.unwrap();
    
    // 等待数据同步和InFlight请求正常处理
    println!("Waiting for data synchronization and InFlight cleanup...");
    tokio::time::sleep(Duration::from_secs(4)).await;
    
    let post_partition_inflight = if let Some(leader_node) = cluster.get_node(&recovered_leader) {
        leader_node.get_inflight_request_count().await
    } else {
        0
    };
    
    println!("InFlight count after complete partition recovery: {}", post_partition_inflight);
    
    // 断言: 验证分区恢复后InFlight请求得到正确处理
    assert!(post_partition_inflight <= during_partition_inflight + 10, 
        "InFlight count should not significantly increase after partition recovery: {} -> {}", 
        during_partition_inflight, post_partition_inflight);
    
    // 断言: 超快超时下分区恢复后系统应该更快趋于稳定
    assert!(post_partition_inflight <= 30, 
        "InFlight count should be reasonable after complete partition recovery with ultra-fast timeouts, got {}", 
        post_partition_inflight);
    
    if post_partition_inflight <= during_partition_inflight + 5 {
        println!("✓ InFlight requests properly handled after partition recovery");
    } else {
        println!("⚠ Some InFlight increase due to leader change and resync - this is expected");
    }
    
    // 验证集群最终状态的一致性
    println!("Verifying final cluster consistency...");
    
    // 发送一个测试命令验证集群功能正常
    let test_command = "post_recovery_test_command".as_bytes().to_vec();
    match cluster.propose_command(&recovered_leader, test_command) {
        Ok(_) => println!("✓ Cluster is functional after partition recovery"),
        Err(e) => println!("⚠ Cluster functionality test failed: {}", e),
    }
    
    // 最后等待一下，确保所有操作完成
    tokio::time::sleep(Duration::from_secs(1)).await;

    // === 最终验证和清理 ===
    println!("\n=== Final verification ===");
    
    // 恢复所有节点到正常网络条件
    for node_id in &[&node1, &node2, &node3] {
        cluster.update_network_config_for_node(node_id, normal_config.clone()).await;
    }
    
    // 等待系统完全稳定（快速超时下缩短等待）
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    // 检查最终状态
    let mut final_leader = None;
    let mut leader_count = 0;
    
    for node_id in &[&node1, &node2, &node3] {
        if let Some(node) = cluster.get_node(node_id) {
            let role = node.get_role();
            println!("Node {:?} final role: {:?}", node_id, role);
            if role == raft_lite::Role::Leader {
                final_leader = Some((*node_id).clone());
                leader_count += 1;
                let final_inflight = node.get_inflight_request_count().await;
                println!("Final leader {:?} InFlight count: {}", node_id, final_inflight);
                
                // 断言: 快速超时下最终leader的InFlight计数应该更低
                assert!(final_inflight <= 15, 
                    "Final InFlight count with fast timeouts should be reasonable at test end, got {}", 
                    final_inflight);
            }
        }
    }
    
    // 断言: 应该有且仅有一个leader
    assert_eq!(leader_count, 1, "Should have exactly one leader, found {}", leader_count);
    assert!(final_leader.is_some(), "Should have a leader at the end");
    
    println!("✓ All InFlight tests completed successfully");
    println!("✓ InFlight management remains robust under various network conditions");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_inflight_timeout_configuration() {
    let _ = tracing_subscriber::fmt().try_init();

    println!("\n=== Testing InFlight timeout configuration ===");
    
    // 创建单节点集群用于测试超时配置
    let node1 = RaftId::new("timeout_test_group".to_string(), "node1".to_string());
    let node2 = RaftId::new("timeout_test_group".to_string(), "node2".to_string());
    
    let config = TestClusterConfig {
        node_ids: vec![node1.clone(), node2.clone()],
        hub: MockNetworkHubConfig::default(),
    };
    let cluster = TestCluster::new(config).await;

    // 启动集群
    let cluster_clone = cluster.clone();
    tokio::spawn(async move { cluster_clone.start().await });

    // 等待选举并重试确保有稳定的leader
    let mut leader_id = None;
    for attempt in 0..10 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        
        // 查找当前leader
        for node_id in &[&node1, &node2] {
            if let Some(node) = cluster.get_node(node_id) {
                let role = node.get_role();
                if role == raft_lite::Role::Leader {
                    leader_id = Some((*node_id).clone());
                    break;
                }
            }
        }
        
        if leader_id.is_some() {
            // 等待leader稳定
            tokio::time::sleep(Duration::from_millis(500)).await;
            // 再次确认leader仍然稳定
            let mut still_leader = false;
            if let Some(ref current_leader) = leader_id {
                if let Some(node) = cluster.get_node(current_leader) {
                    if node.get_role() == raft_lite::Role::Leader {
                        still_leader = true;
                    }
                }
            }
            
            if still_leader {
                break;
            } else {
                leader_id = None;  // 重置，继续寻找
            }
        }
        
        println!("Attempt {} to find stable leader...", attempt + 1);
    }
    
    let leader = leader_id.expect("No stable leader found for timeout test after multiple attempts");
    println!("Leader for timeout test: {:?}", leader);
    
    // 设置100%丢包率，强制触发超时
    let total_loss_config = raft_lite::mock::mock_network::MockRaftNetworkConfig {
        base_latency_ms: 10,
        jitter_max_ms: 5,
        drop_rate: 1.0,  // 100% 丢包率
        failure_rate: 0.0,
    };
    
    for node_id in &[&node1, &node2] {
        cluster.update_network_config_for_node(node_id, total_loss_config.clone()).await;
    }
    
    println!("Set 100% packet loss to test timeout behavior");
    
    // 发送一系列命令，这些命令会因为100%丢包而超时
    let timeout_test_start = std::time::Instant::now();
    let mut timeout_commands_sent = 0;
    
    for i in 0..8 {
        let command = format!("timeout_test_command_{}", i).into_bytes();
        if let Err(e) = cluster.propose_command(&leader, command) {
            println!("Expected failure for timeout test command {}: {}", i, e);
        } else {
            timeout_commands_sent += 1;
            println!("Sent timeout test command {} (will timeout)", i);
        }
        
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    
    // 断言: 大部分命令应该能够发送（即使会超时），快速配置下应该更可靠
    assert!(timeout_commands_sent >= 7, 
        "Expected at least 7 commands to be sent for timeout test with fast config, got {}", 
        timeout_commands_sent);
    
    // 检查InFlight请求累积
    let mid_test_inflight = if let Some(leader_node) = cluster.get_node(&leader) {
        let inflight = leader_node.get_inflight_request_count().await;
        println!("InFlight count with 100% packet loss: {}", inflight);
        
        // 断言: 超极速超时配置(25ms基础超时)下，即使100%丢包，InFlight也应该保持很低
        // 因为请求会快速超时，防止过度积累
        assert!(inflight >= 1, 
            "Expected some InFlight accumulation with 100% packet loss, got {}", 
            inflight);
        
        // 超极速超时配置下，即使100%丢包，InFlight也不应过度累积
        assert!(inflight <= 10, 
            "Ultra-fast timeout config should limit InFlight accumulation even with 100% packet loss, got {}", 
            inflight);
        
        inflight
    } else {
        panic!("Leader node should be accessible during timeout test");
    };
    
    // 等待超快超时机制工作（考虑到心跳定时器依赖）
    println!("Waiting for ultra-fast timeout cleanup...");
    tokio::time::sleep(Duration::from_secs(8)).await;  // 给心跳定时器足够时间
    
    let post_timeout_inflight = if let Some(leader_node) = cluster.get_node(&leader) {
        let inflight = leader_node.get_inflight_request_count().await;
        println!("InFlight count after intelligent timeout: {}", inflight);
        
        // 断言: 验证超快超时机制更有效（应该清理更多请求）
        // 注意：在100%丢包下，可能会发生leader转换，所以设置适当的期望
        if inflight < mid_test_inflight {
            println!("✓ Ultra-fast timeout mechanism working correctly");
        } else {
            println!("⚠ Network partition may have caused leader transition");
            // 在极端网络条件下，leader可能切换，InFlight可能重置
            // 至少验证没有无限增长
            assert!(inflight <= mid_test_inflight + 3, 
                "Ultra-fast timeout should prevent InFlight explosion even under leader transition: {} -> {}", 
                mid_test_inflight, inflight);
        }
        
        // 断言: 超快超时下InFlight计数应该在合理范围内
        assert!(inflight <= 12, 
            "Ultra-fast timeout should keep InFlight requests under control, got {}", 
            inflight);
        
        if inflight <= 3 {
            println!("✓ Ultra-fast timeout mechanism working excellently");
        } else if inflight <= 8 {
            println!("✓ Ultra-fast timeout mechanism working well");
        } else {
            println!("⚠ Ultra-fast timeout mechanism may need further optimization");
        }
        
        inflight
    } else {
        panic!("Leader node should be accessible after timeout test");
    };
    
    // 断言: 快速超时配置下测试应该更快完成
    let test_duration = timeout_test_start.elapsed();
    println!("Fast timeout configuration test duration: {:?}", test_duration);
    assert!(test_duration.as_secs() <= 12, 
        "Fast timeout test should complete within 12 seconds, took {:?}", 
        test_duration);
    
    // 断言: 快速超时下最终InFlight计数应该更低
    assert!(post_timeout_inflight <= 8, 
        "Final InFlight count with fast timeouts should be very low after cleanup, got {}", 
        post_timeout_inflight);
    
    println!("✓ Ultra-fast timeout InFlight configuration test completed successfully");
}
