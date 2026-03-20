fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("proto");
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(
            &[
                proto_dir.join("health.proto"),
                proto_dir.join("schema.proto"),
                proto_dir.join("graph.proto"),
                proto_dir.join("features.proto"),
            ],
            &[proto_dir],
        )?;
    Ok(())
}
