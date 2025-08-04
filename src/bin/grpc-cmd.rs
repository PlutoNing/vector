use clap::Parser;
use std::path::Path;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
use serde_json;

use scx_agent::core::rpc_cli::{QueryCli, QueryGetArgs, QueryOutputFormat, QuerySubCommands};
pub mod query {
    tonic::include_proto!("query");
}

use query::{query_service_client::QueryServiceClient, QueryRequest, QueryResponse};

use crate::query::TimeRange;

async fn handle_get_command(
    mut client: QueryServiceClient<Channel>,
    args: QueryGetArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    // 解析时间范围
    let (start_time, end_time) = parse_time_range(&args)?;

    // 构建查询请求
    let request = build_query_request(&args, start_time, end_time)?;

    // 发送查询
    let response = client.query_events(request).await?;

    // 格式化输出
    format_output(response.into_inner(), &args.format)?;

    Ok(())
}
fn format_output(
    response: QueryResponse,
    format: &QueryOutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    match format {
        QueryOutputFormat::Table => {
            // 表格格式输出
            println!("查询结果:");
            println!("总记录数: {}, 返回记录数: {}, 是否有更多: {}", 
                response.total_count, 
                response.metadata.as_ref().map(|m| m.returned_count).unwrap_or(0),
                response.has_more
            );
            
            if let Some(metadata) = &response.metadata {
                println!("查询耗时: {:.2}ms", metadata.query_time_ms);
            }
            
            println!();
            println!("{:<5} {:<20} {:<15} {:<15} {}", "ID", "时间", "类型", "来源", "数据");
            println!("{:-<5} {:-<20} {:-<15} {:-<15} {}", "", "", "", "", "");
            
            for event in response.events {
                println!("{:<5} {:<20} {:<15} {:<15} {}", 
                    event.id, 
                    event.timestamp, 
                    event.event_type, 
                    event.source, 
                    event.data
                );
            }
        }
        QueryOutputFormat::Json => {
            // 手动构建 JSON 输出
            let json_data = serde_json::json!({
                "total_count": response.total_count,
                "has_more": response.has_more,
                "events": response.events.iter().map(|e| {
                    serde_json::json!({
                        "id": e.id,
                        "timestamp": e.timestamp,
                        "event_type": e.event_type,
                        "source": e.source,
                        "data": e.data
                    })
                }).collect::<Vec<_>>(),
                "metadata": response.metadata.as_ref().map(|m| {
                    serde_json::json!({
                        "returned_count": m.returned_count,
                        "query_time_ms": m.query_time_ms
                    })
                })
            });
            println!("{}", serde_json::to_string_pretty(&json_data)?);
        }
        QueryOutputFormat::Csv => {
            // CSV格式输出
            println!("id,timestamp,event_type,source,data");
            for event in response.events {
                println!("{},{},{},{},{}", 
                    event.id, 
                    event.timestamp, 
                    event.event_type, 
                    event.source, 
                    event.data.replace('"', "\"\"")
                );
            }
        }
        QueryOutputFormat::Yaml => todo!("not implemented"),
    }
    
    Ok(())
}

fn parse_time_range(
    args: &QueryGetArgs,
) -> Result<(Option<String>, Option<String>), Box<dyn std::error::Error>> {
    let now = chrono::Utc::now();

    let start_time = match (&args.since, &args.start_time) {
        (Some(since), None) => {
            // 解析相对时间
            let duration = parse_duration(since)?;
            Some((now - duration).to_rfc3339())
        }
        (None, Some(start)) => Some(start.clone()),
        (None, None) => None,
        _ => {
            eprintln!("错误: 不能同时使用 --since 和 --start-time");
            std::process::exit(1);
        }
    };

    let end_time = args.end_time.clone();

    Ok((start_time, end_time))
}

fn parse_duration(duration_str: &str) -> Result<chrono::Duration, Box<dyn std::error::Error>> {
    let re = regex::Regex::new(r"^(\d+)([smhd])$")?;
    let caps = re
        .captures(duration_str)
        .ok_or("时间格式错误，应为: 1h, 30m, 2d")?;

    let num: i64 = caps[1].parse()?;
    let unit = &caps[2];

    match unit {
        "s" => Ok(chrono::Duration::seconds(num)),
        "m" => Ok(chrono::Duration::minutes(num)),
        "h" => Ok(chrono::Duration::hours(num)),
        "d" => Ok(chrono::Duration::days(num)),
        _ => Err("不支持的时间单位".into()),
    }
}
fn build_query_request(
    args: &QueryGetArgs,
    start_time: Option<String>,
    end_time: Option<String>,
) -> Result<QueryRequest, Box<dyn std::error::Error>> {
    let mut request = QueryRequest {
        limit: args.limit as i32,
        offset: args.offset as i32,
        event_type_filter: args.event_type.clone().unwrap_or_default(),
        source_filter: args.source.clone().unwrap_or_default(),
        time_range: None,
        fields: args.fields.clone().unwrap_or_default(),
    };

    // 设置时间范围
    if start_time.is_some() || end_time.is_some() {
        request.time_range = Some(TimeRange {
            start_time: start_time.unwrap_or_default(),
            end_time: end_time.unwrap_or_default(),
        });
    }

    Ok(request)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = QueryCli::parse();

    // 建立gRPC连接
    let client = create_grpc_client().await?;

    match cli.command {
        QuerySubCommands::Get(args) => handle_get_command(client, args).await?,
        QuerySubCommands::Stats(_args) => todo!("Stats command not implemented"),
        QuerySubCommands::Search(_args) => todo!("Stats command not implemented"),
    }

    Ok(())
}

async fn create_grpc_client() -> Result<QueryServiceClient<Channel>, Box<dyn std::error::Error>> {
    let socket_path = "/tmp/scx_agent/agent_rpc.sock";

    if !Path::new(socket_path).exists() {
        eprintln!("错误: gRPC服务器未运行，请先启动 grpc-server");
        std::process::exit(1);
    }

    let channel = Endpoint::try_from("http://[::]:50051")?
        .connect_with_connector(service_fn(move |_: Uri| {
            tokio::net::UnixStream::connect(socket_path)
        }))
        .await?;

    Ok(QueryServiceClient::new(channel))
}
