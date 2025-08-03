use tonic::{transport::Server, Request, Response, Status};
use std::path::Path;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;

pub mod calc {
    tonic::include_proto!("calc");
}

use calc::{AddRequest, AddResponse, calculator_server::{Calculator, CalculatorServer}};

#[derive(Debug, Default)]
pub struct CalculatorService {}

#[tonic::async_trait]
impl Calculator for CalculatorService {
    async fn add(&self, request: Request<AddRequest>) -> Result<Response<AddResponse>, Status> {
        let req = request.into_inner();
        let result = req.val1 + req.val2;
        
        println!("计算: {} + {} = {}", req.val1, req.val2, result);
        
        Ok(Response::new(AddResponse { result }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let socket_path = "/tmp/calc.sock";
    
    // 删除已存在的socket文件
    if Path::new(socket_path).exists() {
        std::fs::remove_file(socket_path)?;
    }
    
    let uds = UnixListener::bind(socket_path)?;
    let uds_stream = UnixListenerStream::new(uds);
    
    println!("gRPC服务器启动，监听: {}", socket_path);
    
    Server::builder()
        .add_service(CalculatorServer::new(CalculatorService::default()))
        .serve_with_incoming(uds_stream)
        .await?;
    
    Ok(())
}
