// ========== DOSYA: sentinel-execution/build.rs ==========
fn main() -> std::io::Result<()> {
    println!("cargo:rerun-if-changed=proto/execution.proto");
    println!("cargo:rerun-if-changed=proto/market_data.proto");

    prost_build::compile_protos(
        &["proto/execution.proto", "proto/market_data.proto"],
        &["proto/"],
    )?;
    Ok(())
}
