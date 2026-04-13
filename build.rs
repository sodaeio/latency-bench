use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let include_path = protoc_bin_vendored::include_path()?;
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    unsafe {
        env::set_var("PROTOC", &protoc);
        env::set_var("PROTOC_INCLUDE", &include_path);
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let include_dir = include_path.to_string_lossy().into_owned();

    tonic_prost_build::configure()
        .file_descriptor_set_path(out_dir.join("proto_descriptors.bin"))
        .compile_protos(
            &["proto/geyser.proto", "proto/shredstream.proto", "proto/richat.proto", "proto/soda_stream.proto"],
            &["proto", include_dir.as_str()],
        )?;

    Ok(())
}
