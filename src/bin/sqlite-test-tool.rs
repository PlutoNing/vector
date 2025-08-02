use scx_agent::sinks::util::sqlite_service::SqliteService;
use tempfile::tempdir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("开始测试SQLite服务...");
    
    // 使用临时文件
    let temp_dir = tempdir()?;
    let db_path = temp_dir.path().join("test_events.db");
    
    println!("数据库路径: {:?}", db_path);
    
    // 创建SQLite服务
    let service = SqliteService::new(&db_path, "events").await?;
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
        (
            r#"{"message": "Event 3", "count": 3}"#.to_string(),
            Some("metric".to_string()),
            Some("batch_test".to_string()),
        ),
    ];
    
    let batch_rows = service.write_events_batch(events).await?;
    println!("✅ 批量事件写入成功，影响行数: {}", batch_rows);
    
    // 获取统计信息
    let (count, size) = service.get_stats().await?;
    println!("✅ 统计信息: {} 条记录，数据库大小: {} 字节", count, size);
    
    // 验证数据
    let conn = rusqlite::Connection::open(&db_path)?;
    let mut stmt = conn.prepare("SELECT data, event_type, source FROM events ORDER BY id")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
        ))
    })?;
    
    println!("\n数据库内容:");
    for (i, row) in rows.enumerate() {
        let (data, event_type, source) = row?;
        println!("  {}. data: {}, type: {:?}, source: {:?}", 
                 i + 1, data, event_type, source);
    }
    
    // 关闭服务
    service.close().await?;
    println!("SQLite服务关闭成功");
    
    // 清理临时目录
    drop(temp_dir);
    
    println!("\n所有测试通过！");
    
    Ok(())
}