use scx_agent::sinks::util::sqlite_service::SqliteService;
use std::path::Path;
use std::time::Instant;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{transport::Server, Request, Response, Status};
pub mod query {
    tonic::include_proto!("query");
}

use query::{
    query_service_server::{QueryService, QueryServiceServer},
    EmptyRequest, EventData, EventTypesResponse, QueryMetadata, QueryRequest, QueryResponse,
};

#[derive(Debug)]
pub struct QueryServiceImpl {
    sqlite_service: SqliteService,
}

impl QueryServiceImpl {
    async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let sqlite_service = SqliteService::new("/tmp/scx_agent-2025-08-04.db", "events").await?;
        Ok(Self { sqlite_service })
    }
}
#[tonic::async_trait]
impl QueryService for QueryServiceImpl {
    async fn query_events(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let start_time = Instant::now();
        let req = request.into_inner();

        // 参数验证
        if req.limit < 0 || req.limit > 10000 {
            return Err(Status::invalid_argument("limit必须在0-10000之间"));
        }

        // 构建查询条件
        let event_type_filter = if req.event_type_filter.is_empty() {
            None
        } else {
            Some(req.event_type_filter.as_str())
        };

        let source_filter = if req.source_filter.is_empty() {
            None
        } else {
            Some(req.source_filter.as_str())
        };

        // 执行查询
        let events = self
            .sqlite_service
            .query_events(
                Some(req.limit as usize),
                Some(req.offset as usize),
                event_type_filter,
                source_filter,
            )
            .await
            .map_err(|e| Status::internal(format!("数据库查询失败: {}", e)))?;

        // 获取总数
        let total_count = self
            .sqlite_service
            .get_stats()
            .await
            .map_err(|e| Status::internal(format!("获取统计信息失败: {}", e)))?
            .0;

        let event_data: Vec<EventData> = events
            .into_iter()
            .map(|(id, timestamp, data, event_type, source)| EventData {
                id,
                timestamp,
                data,
                event_type: event_type.unwrap_or_default(),
                source: source.unwrap_or_default(),
            })
            .collect();

        let returned_count = event_data.len() as i32; // 先保存长度

        let response = QueryResponse {
            events: event_data,
            total_count: total_count as i32,
            has_more: (req.offset + req.limit) < total_count as i32,
            metadata: Some(QueryMetadata {
                returned_count,
                query_time_ms: start_time.elapsed().as_millis() as f64,
            }),
        };

        Ok(Response::new(response))
    }

    async fn get_event_types(
        &self,
        _request: Request<EmptyRequest>,
    ) -> Result<Response<EventTypesResponse>, Status> {
        let stats = self
            .sqlite_service
            .get_event_type_stats()
            .await
            .map_err(|e| Status::internal(format!("获取事件类型失败: {}", e)))?;

        let event_types: Vec<String> = stats
            .into_iter()
            .map(|(event_type, _)| event_type)
            .collect();

        Ok(Response::new(EventTypesResponse { event_types }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket_path = "/tmp/scx_agent/agent_rpc.sock";

    // 删除已存在的socket文件
    if Path::new(socket_path).exists() {
        std::fs::remove_file(socket_path)?;
    }

    let uds = UnixListener::bind(socket_path)?;
    let uds_stream = UnixListenerStream::new(uds);

    println!("gRPC服务器启动，监听: {}", socket_path);
    println!("可用服务: Calculator, QueryService");

    let query_service = QueryServiceImpl::new().await?;

    Server::builder()
        .add_service(QueryServiceServer::new(query_service))
        .serve_with_incoming(uds_stream)
        .await?;

    Ok(())
}
