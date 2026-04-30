// ========== DOSYA: sentinel-execution/build.rs ==========
fn main() -> std::io::Result<()> {
    prost_build::compile_protos(
        &[
            "sentinel-spec/proto/sentinel/market/v1/market_data.proto",
            "sentinel-spec/proto/sentinel/execution/v1/execution.proto",
            "sentinel-spec/proto/sentinel/wallet/v1/wallet.proto",
            "sentinel-spec/proto/sentinel/api/v1/bundle.proto", // 🔥 EKLENDİ: Kontrol komutlarını dinlemek için
        ],
        &["sentinel-spec/proto/"],
    )?;
    Ok(())
}
