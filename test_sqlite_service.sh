#!/bin/bash
# 测试SQLite服务的简单脚本

echo "开始测试SQLite服务..."

# 创建测试目录
mkdir -p /tmp/sqlite_test
cd /tmp/sqlite_test

# 创建简单的Rust测试文件
cat > test_sqlite.rs << 'EOF'
use std::path::Path;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("开始测试SQLite服务...");
    
    // 创建SQLite服务
    let service = scx_agent::sinks::util::sqlite_service::SqliteService::new("test_events.db", "events").await?;
    println!("✅ SQLite服务创建成功");
    
    // 测试单个事件写入
    let rows = service.write_event(
        r#"{"message": "Hello SQLite", "level": "info", "service": "test"}"#,
        Some("log"),
        Some("test_service")
    ).await?;
    println!("✅ 单个事件写入成功，影响行数: {}", rows);
    
    // 测试批量事件写入
    let events = vec![
        (
            r#"{"message": "Event 1", "count": 1}"#.to_string(),
            Some("metric".to_string()),
            Some("batch_test".to_string()),
        ),
        (
            r#"{"message": "Event 2", "count": 2}"#.to_string(),
            Some("metric".to_string()),
            Some("batch_test".to_string()),
        ),
    ];
    
    let batch_rows = service.write_events_batch(events).await?;
    println!("✅ 批量事件写入成功，影响行数: {}", batch_rows);
    
    // 获取统计信息
    let (count, size) = service.get_stats().await?;
    println!("✅ 统计信息: {} 条记录，数据库大小: {} 字节", count, size);
    
    // 关闭服务
    service.close().await?;
    println!("✅ SQLite服务关闭成功");
    
    println!("\n🎉 所有测试通过！");
    
    Ok(())
}
EOF

# 编译并运行测试
echo "编译测试程序..."
cargo build --example sqlite_test 2>/dev/null || {
    echo "创建示例程序..."
    mkdir -p examples
    cp test_sqlite.rs examples/sqlite_test.rs
    
    echo "运行测试..."
    cargo run --example sqlite_test
}

echo "测试完成！"
