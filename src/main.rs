use anyhow::Result;
use futures_util::StreamExt;
use prost::Message;
use tracing::info;

pub mod sentinel_protos {
    pub mod execution {
        include!(concat!(env!("OUT_DIR"), "/sentinel.execution.v1.rs"));
    }
}
use sentinel_protos::execution::{trade_signal::SignalType, ExecutionReport, TradeSignal};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let nats_client = async_nats::connect(&nats_url).await?;

    let mut subscriber = nats_client.subscribe("signal.trade.>").await?;
    info!("🛡️ Risk Motoru ve Paper Trading devrede.");

    // Çok basit bir sanal cüzdan durumu (Demo amaçlı)
    let mut mock_price = 65000.0; // Gerçekte bunu market data'dan almalıyız

    while let Some(message) = subscriber.next().await {
        if let Ok(signal) = TradeSignal::decode(message.payload) {
            if signal.confidence_score < 0.40 {
                continue;
            }

            let signal_type =
                SignalType::try_from(signal.r#type).unwrap_or(SignalType::Unspecified);

            let side = match signal_type {
                SignalType::Buy | SignalType::StrongBuy => "BUY",
                SignalType::Sell | SignalType::StrongSell => "SELL",
                _ => continue,
            };

            // PAPER TRADE MANTIK SİMÜLASYONU
            let quantity = 0.1; // 0.1 BTC
            let commission = (mock_price * quantity) * 0.0004; // %0.04 Binance VIP0 Taker fee

            // Rastgele PnL simülasyonu (Gerçek sistemde giriş-çıkış fiyat farkından hesaplanır)
            let pnl = if side == "BUY" {
                5.0 - commission
            } else {
                -2.0 - commission
            };

            let report = ExecutionReport {
                symbol: signal.symbol.clone(),
                side: side.to_string(),
                execution_price: mock_price,
                quantity,
                realized_pnl: pnl,
                commission,
                timestamp: chrono::Utc::now().timestamp_millis(),
            };

            let mut buf = Vec::new();
            report.encode(&mut buf)?;
            let _ = nats_client
                .publish(format!("execution.report.{}", signal.symbol), buf.into())
                .await;

            info!(
                "💰 [PAPER TRADE] {} {} - Kâr/Zarar: {:.2}$ (Komisyon: {:.2}$)",
                side, signal.symbol, pnl, commission
            );

            mock_price += if side == "BUY" { 10.0 } else { -10.0 }; // Sahte fiyat hareketi
        }
    }
    Ok(())
}
