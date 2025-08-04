fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile(&["proto/query.proto"], &["proto"])?;
    
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile(&["proto/calc.proto"], &["proto"])?;
    
    Ok(())
}
