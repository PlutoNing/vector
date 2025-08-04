use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::Path;
use tonic::transport::{Channel, Endpoint, Uri};
use tower::service_fn;
pub mod calc {
    tonic::include_proto!("calc");
}
use scx_agent::core::rpc_cli::{QueryCli, QueryGetArgs, QuerySubCommands};
use calc::{calculator_client::CalculatorClient, AddRequest};
pub mod query {
    tonic::include_proto!("query");
}

use query::{query_service_client::QueryServiceClient, QueryRequest, QueryResponse};

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

fn parse_time_range(
    args: &GetArgs,
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
    args: &GetArgs,
    start_time: Option<String>,
    end_time: Option<String>,
) -> Result<QueryRequest, Box<dyn std::error::Error>> {
    let mut request = QueryRequest {
        query_type: QueryType::Simple as i32,
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
        QuerySubCommands::Stats(args) => handle_stats_command(client, args).await?,
        QuerySubCommands::Search(args) => handle_search_command(client, args).await?,
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
