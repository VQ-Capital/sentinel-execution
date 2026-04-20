// ========== DOSYA: sentinel-execution/src/main.rs ==========
use anyhow::{Result};
use futures_util::StreamExt;
use prost::Message;
use tracing::{info, warn, error};

pub mod sentinel_protos {
    pub mod execution {
        include!(concat!(env!("OUT_DIR"), "/sentinel.execution.rs"));
    }
}
use sentinel_protos::execution::{TradeSignal, trade_signal::SignalType};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    info!("⚡ Sentinel-Execution (Cellat) başlatılıyor...");

    // 1. NATS BAĞLANTISI
    let nats_url = std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let nats_client = async_nats::connect(nats_url).await?;
    
    // 2. SİNYAL KANALINA ABONE OL (Tüm sembollerden gelen sinyalleri dinle)
    let mut subscriber = nats_client.subscribe("signal.trade.>").await?;
    info!("🛡️ Risk motoru devrede. Sinyaller bekleniyor...");

    // 3. EMİR DÖNGÜSÜ
    while let Some(message) = subscriber.next().await {
        if let Ok(signal) = TradeSignal::decode(message.payload) {
            
            // RİSK FİLTRESİ: Güven skoru 0.40'ın altındaysa işlem yapma
            if signal.confidence_score < 0.40 {
                info!("⚠️ Sinyal reddedildi: Düşük Güven ({:.2})", signal.confidence_score);
                continue;
            }

            let symbol = signal.symbol;
            let signal_type = SignalType::try_from(signal.r#type).unwrap_or(SignalType::Hold);

            match signal_type {
                SignalType::Buy | SignalType::StrongBuy => {
                    execute_order(&symbol, "BUY", signal.confidence_score).await;
                }
                SignalType::Sell | SignalType::StrongSell => {
                    execute_order(&symbol, "SELL", signal.confidence_score).await;
                }
                _ => {} // Hold durumunda bir şey yapma
            }
        }
    }

    Ok(())
}

// GERÇEK EMİR İLETİM FONKSİYONU (Simülasyon/Paper Trading Modu)
async fn execute_order(symbol: &str, side: &str, confidence: f64) {
    // BURASI ÇOK KRİTİK: Şu an sadece log atıyoruz (Paper Trading)
    // Gerçek API anahtarlarını girdiğinde burası Binance'e request atacak.
    
    info!("💰 [PAPER TRADE] EMİR GÖNDERİLDİ!");
    info!("   | Varlık: {}", symbol);
    info!("   | Yön: {}", side);
    info!("   | Güven: {:.2}", confidence);
    info!("   | Durum: Başarılı (Simüle Edildi)");
    info!("--------------------------------------------");
}