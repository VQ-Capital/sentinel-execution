// ========== DOSYA: sentinel-execution/src/main.rs ==========
use anyhow::{Context, Result};
use futures_util::StreamExt;
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};

// Protobuf modüllerini içe aktarıyoruz
pub mod sentinel_protos {
    pub mod execution {
        include!(concat!(env!("OUT_DIR"), "/sentinel.execution.v1.rs"));
    }
    pub mod market {
        include!(concat!(env!("OUT_DIR"), "/sentinel.market.v1.rs"));
    }
}

use sentinel_protos::execution::{trade_signal::SignalType, ExecutionReport, TradeSignal};
use sentinel_protos::market::AggTrade;

// =====================================================================
// ARCHITECTURE: GATEWAY ABSTRACTION
// =====================================================================

/// Tüm borsa bağlantıları bu arayüze (Trait) uymak zorundadır.
/// Yarın Binance eklendiğinde sistem kodu değişmeyecek, sadece struct eklenecek.
pub trait ExchangeGateway: Send + Sync {
    fn send_order(
        &self,
        symbol: &str,
        side: &str,
        quantity: f64,
        expected_price: f64,
    ) -> impl std::future::Future<Output = Result<ExecutionReport>> + Send;
}

// =====================================================================
// SHADOW EXCHANGE (GERÇEĞE ÖZDEŞ SİMÜLASYON MOTORU)
// =====================================================================

pub struct ShadowExchange {
    fee_rate: f64,
    base_latency_ms: u64,
    // (Miktar, Ortalama Giriş Fiyatı) -> Basit portföy takibi
    positions: Mutex<HashMap<String, (f64, f64)>>,
}

impl ShadowExchange {
    pub fn new(fee_rate: f64, base_latency_ms: u64) -> Self {
        Self {
            fee_rate,
            base_latency_ms,
            positions: Mutex::new(HashMap::new()),
        }
    }
}

impl ExchangeGateway for ShadowExchange {
    async fn send_order(
        &self,
        symbol: &str,
        side: &str,
        quantity: f64,
        expected_price: f64,
    ) -> Result<ExecutionReport> {
        // 1. Ağ gecikmesi (Latency) simülasyonu
        sleep(Duration::from_millis(self.base_latency_ms)).await;

        // 2. Gerçekçi Slippage (Fiyat kayması) simülasyonu (Örn: %0.02)
        let slippage = expected_price * 0.0002;
        let execution_price = if side == "BUY" {
            expected_price + slippage
        } else {
            expected_price - slippage
        };

        // 3. Komisyon Hesabı
        let commission = execution_price * quantity * self.fee_rate;

        // 4. Portföy Yönetimi ve Gerçekleşen (Realized) PnL Hesabı
        let mut positions = self.positions.lock().await;
        let (pos_qty, avg_price) = positions.entry(symbol.to_string()).or_insert((0.0, 0.0));
        let mut realized_pnl = 0.0;

        if side == "SELL" && *pos_qty > 0.0 {
            // Long pozisyonu kapatma veya küçültme
            let close_qty = quantity.min(*pos_qty);
            realized_pnl = (execution_price - *avg_price) * close_qty;
            *pos_qty -= close_qty;
            if *pos_qty <= 0.0001 {
                *avg_price = 0.0;
            } // Sıfırlandı
        } else if side == "BUY" && *pos_qty < 0.0 {
            // Short pozisyonu kapatma veya küçültme
            let close_qty = quantity.min(pos_qty.abs());
            realized_pnl = (*avg_price - execution_price) * close_qty;
            *pos_qty += close_qty;
            if pos_qty.abs() <= 0.0001 {
                *avg_price = 0.0;
            }
        } else {
            // Yeni pozisyon açma veya ekleme yapma
            let new_qty = if side == "BUY" {
                *pos_qty + quantity
            } else {
                *pos_qty - quantity
            };
            let total_value = (*pos_qty).abs() * (*avg_price) + quantity * execution_price;
            *avg_price = total_value / new_qty.abs();
            *pos_qty = new_qty;
        }

        // Net PnL (Komisyon Düşülmüş)
        let net_pnl = realized_pnl - commission;

        Ok(ExecutionReport {
            symbol: symbol.to_string(),
            side: side.to_string(),
            expected_price,
            execution_price,
            quantity,
            realized_pnl: net_pnl,
            commission,
            latency_ms: self.base_latency_ms as i64,
            timestamp: chrono::Utc::now().timestamp_millis(),
            is_simulated: true, // Gölge Borsa
        })
    }
}

// =====================================================================
// MAIN APPLICATION LOOP
// =====================================================================

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let nats_client = async_nats::connect(&nats_url)
        .await
        .context("Failed to connect to NATS")?;

    info!("🛡️ Risk Motoru ve Gölge Borsa (Shadow Exchange) devrede.");

    // Borsa adaptörümüzü başlatıyoruz (İleride burası BinanceGateway olabilir)
    let exchange_gateway = Arc::new(ShadowExchange::new(0.0004, 15)); // VIP0 Taker fee, 15ms ping

    // Canlı Piyasa Fiyatlarını Tutacak Bellek İçi Önbellek (Cache)
    let live_prices: Arc<RwLock<HashMap<String, f64>>> = Arc::new(RwLock::new(HashMap::new()));

    // GÖREV 1: CANLI FİYAT DİNLEYİCİSİ (Sürekli Güncellenir)
    let prices_clone = live_prices.clone();
    let nats_prices_clone = nats_client.clone();
    tokio::spawn(async move {
        match nats_prices_clone.subscribe("market.trade.>").await {
            Ok(mut sub) => {
                while let Some(msg) = sub.next().await {
                    if let Ok(trade) = AggTrade::decode(msg.payload) {
                        let mut cache = prices_clone.write().await;
                        cache.insert(trade.symbol, trade.price);
                    }
                }
            }
            Err(e) => error!("Fiyat kanalı dinlenemedi: {:?}", e),
        }
    });

    // GÖREV 2: SİNYAL DİNLEYİCİSİ VE İŞLEM YÜRÜTÜCÜ (Execution)
    let mut signal_sub = nats_client
        .subscribe("signal.trade.>")
        .await
        .context("Failed to subscribe to signals")?;

    while let Some(msg) = signal_sub.next().await {
        let signal = match TradeSignal::decode(msg.payload) {
            Ok(s) => s,
            Err(_) => {
                warn!("⚠️ Hatalı TradeSignal paketi (Poison Pill). Drop edildi.");
                continue; // unwrap() yerine devam ediyoruz (Zero-Tolerance Policy)
            }
        };

        if signal.confidence_score < 0.40 {
            continue; // Düşük güven skorlu sinyalleri reddet
        }

        let signal_type = SignalType::try_from(signal.r#type).unwrap_or(SignalType::Unspecified);
        let side = match signal_type {
            SignalType::Buy | SignalType::StrongBuy => "BUY",
            SignalType::Sell | SignalType::StrongSell => "SELL",
            _ => continue,
        };

        // En güncel fiyatı NATS cache'inden çekiyoruz (Yoksa işlemi iptal et)
        let expected_price = {
            let cache = live_prices.read().await;
            match cache.get(&signal.symbol) {
                Some(&p) => p,
                None => {
                    warn!(
                        "⚠️ {} için anlık fiyat bulunamadı, emir reddedildi.",
                        signal.symbol
                    );
                    continue;
                }
            }
        };

        let quantity = 0.1; // Sabit işlem hacmi (İleride risk motorundan hesaplanacak)

        info!(
            "Sinyal Alındı: {} {} (Beklenen Fiyat: {:.2}$)",
            side, signal.symbol, expected_price
        );

        // Emri Adaptör (Gateway) üzerinden gönder
        let gateway = exchange_gateway.clone();
        let nats_publish_client = nats_client.clone();

        // Emri asenkron spawnlıyoruz ki motor tıkanmasın
        tokio::spawn(async move {
            match gateway
                .send_order(&signal.symbol, side, quantity, expected_price)
                .await
            {
                Ok(report) => {
                    let mut buf = Vec::new();
                    if report.encode(&mut buf).is_ok() {
                        let subject = format!("execution.report.{}", report.symbol);
                        let _ = nats_publish_client.publish(subject, buf.into()).await;

                        info!(
                            "💰 [SHADOW] İşlem: {} {} | Fiyat: {:.2}$ | PnL: {:.2}$ | Gecikme: {}ms",
                            report.side, report.symbol, report.execution_price, report.realized_pnl, report.latency_ms
                        );
                    }
                }
                Err(e) => error!("❌ Emir İletim Hatası: {:?}", e),
            }
        });
    }

    Ok(())
}
