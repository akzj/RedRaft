// build.rs
use tonic_prost_build::configure;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    configure()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile_protos(&["proto/pb.proto"], &["proto"])?;

    Ok(())
}
