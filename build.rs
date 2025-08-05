use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        // .out_dir("src/proto")
        .compile(&["proto/query.proto", "proto/calc.proto"], &["proto"])?;
    
    Ok(())
}
