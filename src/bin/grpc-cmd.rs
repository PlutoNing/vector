use tonic::transport::{Endpoint, Uri};
use tower::service_fn;
use std::path::Path;

pub mod calc {
    tonic::include_proto!("calc");
}

use calc::{AddRequest, calculator_client::CalculatorClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    
    if args.len() != 3 {
        eprintln!("用法: grpc-cmd <val1> <val2>");
        std::process::exit(1);
    }
    
    let val1: i32 = args[1].parse().unwrap_or_else(|_| {
        eprintln!("错误: val1 必须是整数");
        std::process::exit(1);
    });
    
    let val2: i32 = args[2].parse().unwrap_or_else(|_| {
        eprintln!("错误: val2 必须是整数");
        std::process::exit(1);
    });
    
    let socket_path = "/tmp/calc.sock";
    
    if !Path::new(socket_path).exists() {
        eprintln!("错误: gRPC服务器未运行，请先启动 grpc-server");
        std::process::exit(1);
    }
    
    // 创建Unix socket连接
    let channel = Endpoint::try_from("http://[::]:50051")?
        .connect_with_connector(service_fn(move |_: Uri| {
            tokio::net::UnixStream::connect(socket_path)
        }))
        .await?;
    
    let mut client = CalculatorClient::new(channel);
    
    let request = tonic::Request::new(AddRequest { val1, val2 });
    
    match client.add(request).await {
        Ok(response) => {
            let result = response.into_inner().result;
            println!("{}", result);
        }
        Err(e) => {
            eprintln!("gRPC调用失败: {}", e);
            std::process::exit(1);
        }
    }
    
    Ok(())
}
