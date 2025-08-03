fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=proto/calc.proto");
    
    let mut prost_build = prost_build::Config::new();
    prost_build.btree_map(&["."]);
    
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_with_config(
            prost_build,
            &["proto/calc.proto"],
            &["proto"],
        )?;
    
    Ok(())
}
