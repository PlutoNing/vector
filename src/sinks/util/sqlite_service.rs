//! SQLite service for writing events to SQLite database
//! 模仿FileSink的设计，提供SQLite数据库写入功能

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::Mutex;
use rusqlite::{Connection, OpenFlags};

/// SQLite数据库连接包装器
/// 提供线程安全的SQLite操作
#[derive(Debug)]
pub struct SqliteConnection {
    conn: Arc<Mutex<Connection>>,
    #[allow(dead_code)]
    path: String,
}

impl SqliteConnection {
    /// 创建新的SQLite连接
    pub async fn new(path: impl AsRef<Path>) -> Result<Self, rusqlite::Error> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        
        // 确保目录存在
        if let Some(parent) = path.as_ref().parent() {
            tokio::fs::create_dir_all(parent).await
                .map_err(|e| rusqlite::Error::InvalidPath(PathBuf::from(format!("Failed to create directory: {}", e))))?;
        }

        // 使用tokio::task::spawn_blocking来在阻塞线程中打开连接
        let path_clone = path_str.clone();
        let conn = tokio::task::spawn_blocking(move || {
            Connection::open_with_flags(
                path_clone,
                OpenFlags::SQLITE_OPEN_READ_WRITE 
                    | OpenFlags::SQLITE_OPEN_CREATE 
                    | OpenFlags::SQLITE_OPEN_FULL_MUTEX
                    | OpenFlags::SQLITE_OPEN_URI,
            )
        }).await.map_err(|e| rusqlite::Error::InvalidPath(PathBuf::from(format!("Failed to spawn blocking task: {}", e))))??;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            path: path_str,
        })
    }

    /// 初始化数据库表结构
    pub async fn initialize_table(&self, table_name: &str) -> Result<(), rusqlite::Error> {
        let conn = self.conn.clone();
        let table_name = table_name.to_string();
        
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            conn.execute(
                &format!(
                    "CREATE TABLE IF NOT EXISTS {} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                        data TEXT NOT NULL,
                        event_type TEXT,
                        source TEXT
                    )",
                    table_name
                ),
                [],
            )?;
            Ok(())
        }).await.map_err(|e| rusqlite::Error::InvalidPath(PathBuf::from(format!("Failed to initialize table: {}", e))))?
    }

    /// 插入事件数据
    pub async fn insert_event(
        &self,
        table_name: &str,
        data: &str,
        event_type: Option<&str>,
        source: Option<&str>,
    ) -> Result<usize, rusqlite::Error> {
        let conn = self.conn.clone();
        let table_name = table_name.to_string();
        let data = data.to_string();
        let event_type = event_type.map(|s| s.to_string());
        let source = source.map(|s| s.to_string());
        
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            let mut stmt = conn.prepare(&format!(
                "INSERT INTO {} (data, event_type, source) VALUES (?, ?, ?)",
                table_name
            ))?;
            
            let rows = stmt.execute((
                &data,
                event_type.as_deref(),
                source.as_deref(),
            ))?;
            
            Ok(rows)
        }).await.map_err(|e| rusqlite::Error::InvalidPath(PathBuf::from(format!("Failed to insert event: {}", e))))?
    }

    /// 批量插入事件数据
    pub async fn insert_events_batch(
        &self,
        table_name: &str,
        events: Vec<(String, Option<String>, Option<String>)>,
    ) -> Result<usize, rusqlite::Error> {
        let conn = self.conn.clone();
        let table_name = table_name.to_string();
        
        tokio::task::spawn_blocking(move || {
            let mut conn = conn.blocking_lock();
            let tx = conn.transaction()?;
        
            let mut total_rows = 0;
{
    let mut stmt = tx.prepare(&format!(
        "INSERT INTO {} (data, event_type, source) VALUES (?, ?, ?)",
        table_name
    ))?;
    
    for (data, event_type, source) in events {
        let rows = stmt.execute((
            &data,
            event_type.as_deref(),
            source.as_deref(),
        ))?;
        total_rows += rows;
    }
} 
            
            tx.commit()?;
            Ok(total_rows)
        }).await.map_err(|e| rusqlite::Error::InvalidPath(PathBuf::from(format!("Failed to insert events batch: {}", e))))?
    }

    /// 获取数据库统计信息
    pub async fn get_stats(&self, table_name: &str) -> Result<(usize, usize), rusqlite::Error> {
        let conn = self.conn.clone();
        let table_name = table_name.to_string();
        
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            
            let count: i64 = conn.query_row(
                &format!("SELECT COUNT(*) FROM {}", table_name),
                [],
                |row| row.get(0),
            )?;
            
            let size: i64 = conn.query_row(
                "SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()",
                [],
                |row| row.get(0),
            ).unwrap_or(0);
            
            Ok((count as usize, size as usize))
        }).await.map_err(|e| rusqlite::Error::InvalidPath(PathBuf::from(format!("Failed to get stats: {}", e))))?
    }

    /// 关闭连接
    pub async fn close(&self) -> Result<(), rusqlite::Error> {
        let _conn = self.conn.clone();
        
        tokio::task::spawn_blocking(move || {
            // 我们不实际关闭连接，只是释放锁
            Ok(())
        }).await.map_err(|e| rusqlite::Error::InvalidPath(PathBuf::from(format!("Failed to close connection: {}", e))))?
    }
}

/// SQLite服务，模仿OutFile的设计
#[derive(Debug)]
pub struct SqliteService {
    connection: SqliteConnection,
    table_name: String,
}

impl SqliteService {
    /// 创建新的SQLite服务
    pub async fn new(path: impl AsRef<Path>, table_name: &str) -> Result<Self, rusqlite::Error> {
        let connection = SqliteConnection::new(path).await?;
        connection.initialize_table(table_name).await?;
        
        Ok(Self {
            connection,
            table_name: table_name.to_string(),
        })
    }

    /// 写入单个事件
    pub async fn write_event(
        &self,
        data: &str,
        event_type: Option<&str>,
        source: Option<&str>,
    ) -> Result<usize, rusqlite::Error> {
        self.connection
            .insert_event(&self.table_name, data, event_type, source)
            .await
    }

    /// 批量写入事件
    pub async fn write_events_batch(
        &self,
        events: Vec<(String, Option<String>, Option<String>)>,
    ) -> Result<usize, rusqlite::Error> {
        self.connection
            .insert_events_batch(&self.table_name, events)
            .await
    }

    /// 获取统计信息
    pub async fn get_stats(&self) -> Result<(usize, usize), rusqlite::Error> {
        self.connection.get_stats(&self.table_name).await
    }

    /// 关闭服务
    pub async fn close(&self) -> Result<(), rusqlite::Error> {
        self.connection.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_sqlite_connection() {
        let temp_file = NamedTempFile::new().unwrap();
        let conn = SqliteConnection::new(temp_file.path()).await.unwrap();
        
        conn.initialize_table("test_events").await.unwrap();
        
        let rows = conn.insert_event("test_events", r#"{"test": "data"}"#, Some("log"), Some("test")).await.unwrap();
        assert_eq!(rows, 1);
        
        let (count, size) = conn.get_stats("test_events").await.unwrap();
        assert_eq!(count, 1);
        assert!(size > 0);
    }

    #[tokio::test]
    async fn test_sqlite_service() {
        let temp_file = NamedTempFile::new().unwrap();
        let service = SqliteService::new(temp_file.path(), "events").await.unwrap();
        
        // 测试单个事件写入
        let rows = service.write_event(r#"{"message": "hello"}"#, Some("log"), Some("app")).await.unwrap();
        assert_eq!(rows, 1);
        
        // 测试批量写入
        let events = vec![
            (r#"{"message": "event1"}"#.to_string(), Some("log".to_string()), Some("app".to_string())),
            (r#"{"message": "event2"}"#.to_string(), Some("log".to_string()), Some("app".to_string())),
        ];
        let rows = service.write_events_batch(events).await.unwrap();
        assert_eq!(rows, 2);
        
        // 测试统计信息
        let (count, size) = service.get_stats().await.unwrap();
        assert_eq!(count, 3);
        assert!(size > 0);
        
        service.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_batch_insert() {
        let temp_file = NamedTempFile::new().unwrap();
        let service = SqliteService::new(temp_file.path(), "batch_test").await.unwrap();
        
        let mut events = Vec::new();
        for i in 0..100 {
            events.push((
                format!(r#"{{"id": {}, "message": "test{}"}}"#, i, i),
                Some("log".to_string()),
                Some("batch_test".to_string()),
            ));
        }
        
        let rows = service.write_events_batch(events).await.unwrap();
        assert_eq!(rows, 100);
        
        let (count, _) = service.get_stats().await.unwrap();
        assert_eq!(count, 100);
    }
}
