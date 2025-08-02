use scx_agent::sinks::util::sqlite_service::SqliteService;
use tempfile::NamedTempFile;

#[tokio::test]
async fn test_sqlite_integration() {
    let temp_file = NamedTempFile::new().unwrap();
    let service = SqliteService::new(temp_file.path(), "integration_test")
        .await
        .unwrap();
    
    // 测试各种数据类型
    let test_cases = vec![
        (r#"{"string": "value", "number": 42, "bool": true}"#, "json", "test"),
        (r#"{"nested": {"key": "value"}}"#, "complex", "test"),
        (r#"[]"#, "array", "test"),
        (r#""simple string""#, "string", "test"),
    ];
    
    for (data, event_type, source) in &test_cases {
        let rows = service.write_event(data, Some(*event_type), Some(*source)).await.unwrap();
        assert_eq!(rows, 1);
    }
    
    let (count, _) = service.get_stats().await.unwrap();
    assert_eq!(count, test_cases.len());
    
    service.close().await.unwrap();
}

#[tokio::test]
async fn test_batch_operations() {
    let temp_file = NamedTempFile::new().unwrap();
    let service = SqliteService::new(temp_file.path(), "batch_test")
        .await
        .unwrap();
    
    let mut events = Vec::new();
    for i in 0..10 {
        events.push((
            format!(r#"{{"id": {}, "message": "test{}"}}"#, i, i),
            Some("log".to_string()),
            Some("batch_test".to_string()),
        ));
    }
    
    let rows = service.write_events_batch(events).await.unwrap();
    assert_eq!(rows, 10);
    
    let (count, _) = service.get_stats().await.unwrap();
    assert_eq!(count, 10);
}
