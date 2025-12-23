// build.rs
use tonic_prost_build::configure;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile node-to-node protocols
    configure()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile_protos(
            &[
                "proto/node.proto",
                "proto/snapshot_service.proto",
            ],
            &["proto/"],
        )?;

    Ok(())
}

