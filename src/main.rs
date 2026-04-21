// ========== DOSYA: sentinel-execution/src/main.rs ==========
use anyhow::{Context, Result};
use futures_util::StreamExt;
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, warn};

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
// SHADOW EXCHANGE (SİMÜLASYON)
// =====================================================================
pub struct ShadowExchange {
    fee_rate: f64,
    base_latency_ms: u64,
}

impl ShadowExchange {
    pub fn new(fee_rate: f64, base_latency_ms: u64) -> Self {
        Self {
            fee_rate,
            base_latency_ms,
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
        sleep(Duration::from_millis(self.base_latency_ms)).await;
        // %0.02 Kayma (Slippage)
        let slippage = expected_price * 0.0002;
        let execution_price = if side == "BUY" {
            expected_price + slippage
        } else {
            expected_price - slippage
        };
        let commission = execution_price * quantity * self.fee_rate;

        Ok(ExecutionReport {
            symbol: symbol.to_string(),
            side: side.to_string(),
            expected_price,
            execution_price,
            quantity,
            realized_pnl: 0.0, // Risk Motoru belirleyecek
            commission,
            latency_ms: self.base_latency_ms as i64,
            timestamp: chrono::Utc::now().timestamp_millis(),
            is_simulated: true,
        })
    }
}

// =====================================================================
// RISK ENGINE (DINAMIK LOT, TP/SL, MIKRO-CUZDAN)
// =====================================================================
#[derive(Clone)]
struct Position {
    quantity: f64,
    avg_price: f64,
}

pub struct RiskEngine {
    initial_balance: f64,
    wallet_balance: f64,

    // Strateji Parametreleri
    max_drawdown_usd: f64,
    cooldown_ms: i64,
    risk_per_trade_pct: f64, // Cüzdanın % kaçı riske edilecek
    leverage: f64,           // Kaldıraç oranı
    take_profit_pct: f64,    // % Kâr Alma Seviyesi
    stop_loss_pct: f64,      // % Zarar Kes Seviyesi

    positions: HashMap<String, Position>,
    last_trade_time: HashMap<String, i64>,
    kill_switch_active: bool,
}

impl RiskEngine {
    pub fn new(
        initial_balance: f64,
        max_drawdown_usd: f64,
        cooldown_ms: i64,
        risk_per_trade_pct: f64,
        leverage: f64,
        take_profit_pct: f64,
        stop_loss_pct: f64,
    ) -> Self {
        Self {
            initial_balance,
            wallet_balance: initial_balance,
            max_drawdown_usd,
            cooldown_ms,
            risk_per_trade_pct,
            leverage,
            take_profit_pct,
            stop_loss_pct,
            positions: HashMap::new(),
            last_trade_time: HashMap::new(),
            kill_switch_active: false,
        }
    }

    /// Sinyali onaylar ve o anki bakiye/fiyata göre DINAMİK MİKTARI (LOT) hesaplar.
    pub fn evaluate_signal(
        &mut self,
        symbol: &str,
        side: &str,
        price: f64,
    ) -> std::result::Result<f64, &'static str> {
        if self.kill_switch_active {
            return Err("KILL SWITCH AKTİF.");
        }

        let now = chrono::Utc::now().timestamp_millis();
        if let Some(&last_time) = self.last_trade_time.get(symbol) {
            if now - last_time < self.cooldown_ms {
                return Err("COOLDOWN aktif.");
            }
        }

        let pos = self.positions.get(symbol).cloned().unwrap_or(Position {
            quantity: 0.0,
            avg_price: 0.0,
        });
        if side == "BUY" && pos.quantity > 0.000001 {
            return Err("Zaten LONG pozisyondasınız.");
        }
        if side == "SELL" && pos.quantity < -0.000001 {
            return Err("Zaten SHORT pozisyondasınız.");
        }

        // MİKRO-LOT HESAPLAMA (Dinamik Position Sizing)
        // Örn: 10$ bakiye * %10 risk = 1$. 1$ * 50x Kaldıraç = 50$ Alım Gücü.
        let risk_amount = self.wallet_balance * self.risk_per_trade_pct;
        let buying_power = risk_amount * self.leverage;
        let raw_quantity = buying_power / price;

        // Kripto borsaları için miktarı 5 ondalık basamağa yuvarla (Örn: 0.00065)
        let quantity = (raw_quantity * 100_000.0).trunc() / 100_000.0;

        if quantity <= 0.00001 {
            return Err("Hesaplanan lot boyutu borsanın minimum limitinden düşük!");
        }

        self.last_trade_time.insert(symbol.to_string(), now);
        Ok(quantity)
    }

    /// O anki fiyatlara bakarak Kâr/Zarar (TP/SL) limitlerini kontrol eder.
    pub fn check_tp_sl(
        &mut self,
        current_prices: &HashMap<String, f64>,
    ) -> Vec<(String, &'static str, f64, f64)> {
        let mut close_orders = Vec::new();
        if self.kill_switch_active {
            return close_orders;
        }

        for (symbol, pos) in self.positions.iter() {
            if pos.quantity.abs() < 0.000001 {
                continue;
            } // Pozisyon yok
            if let Some(&current_price) = current_prices.get(symbol) {
                let mut pnl_pct = 0.0;
                let mut close_side = "";

                if pos.quantity > 0.0 {
                    // LONG isek
                    pnl_pct = (current_price - pos.avg_price) / pos.avg_price;
                    close_side = "SELL";
                } else if pos.quantity < 0.0 {
                    // SHORT isek
                    pnl_pct = (pos.avg_price - current_price) / pos.avg_price;
                    close_side = "BUY";
                }

                // Hedef Kâr (Take Profit) veya Zarar Kes (Stop Loss) Vuruldu mu?
                if pnl_pct >= self.take_profit_pct {
                    info!(
                        "🎯 TAKE PROFIT TETİKLENDİ: {} (+%{:.2})",
                        symbol,
                        pnl_pct * 100.0
                    );
                    close_orders.push((
                        symbol.clone(),
                        close_side,
                        pos.quantity.abs(),
                        current_price,
                    ));
                } else if pnl_pct <= -self.stop_loss_pct {
                    warn!(
                        "🛑 STOP LOSS TETİKLENDİ: {} (-%{:.2})",
                        symbol,
                        pnl_pct.abs() * 100.0
                    );
                    close_orders.push((
                        symbol.clone(),
                        close_side,
                        pos.quantity.abs(),
                        current_price,
                    ));
                }
            }
        }
        close_orders
    }

    pub fn process_execution(&mut self, report: &mut ExecutionReport) {
        let pos = self
            .positions
            .entry(report.symbol.clone())
            .or_insert(Position {
                quantity: 0.0,
                avg_price: 0.0,
            });
        let mut realized_pnl = 0.0;

        if report.side == "SELL" && pos.quantity > 0.0 {
            let close_qty = report.quantity.min(pos.quantity);
            realized_pnl = (report.execution_price - pos.avg_price) * close_qty;
            pos.quantity -= close_qty;
            if pos.quantity <= 0.000001 {
                pos.avg_price = 0.0;
            }
        } else if report.side == "BUY" && pos.quantity < 0.0 {
            let close_qty = report.quantity.min(pos.quantity.abs());
            realized_pnl = (pos.avg_price - report.execution_price) * close_qty;
            pos.quantity += close_qty;
            if pos.quantity.abs() <= 0.000001 {
                pos.avg_price = 0.0;
            }
        } else {
            let new_qty = if report.side == "BUY" {
                pos.quantity + report.quantity
            } else {
                pos.quantity - report.quantity
            };
            let total_value =
                (pos.quantity.abs() * pos.avg_price) + (report.quantity * report.execution_price);
            pos.avg_price = total_value / new_qty.abs();
            pos.quantity = new_qty;
        }

        let net_pnl = realized_pnl - report.commission;
        report.realized_pnl = net_pnl;
        self.wallet_balance += net_pnl;

        let total_drawdown = self.wallet_balance - self.initial_balance;
        if total_drawdown <= -self.max_drawdown_usd {
            error!(
                "🚨 KILL SWITCH: Maksimum kayıp ({:.2}$) aşıldı!",
                total_drawdown
            );
            self.kill_switch_active = true;
        }

        info!(
            "💼 [CÜZDAN] Bakiye: {:.4} USDT | {} Pozisyonu: {:.5}",
            self.wallet_balance, report.symbol, pos.quantity
        );
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
        .context("CRITICAL: NATS bağlanılamadı")?;

    info!("🛡️ Kurumsal Risk Motoru (Mikro-Bakiye ve TP/SL Korumalı) devrede.");

    let exchange_gateway = Arc::new(ShadowExchange::new(0.0004, 15));

    // MİKRO-CÜZDAN TESTİ (AŞIRI ZORLAMA):
    // 10$ Başlangıç, 5$ Max Kayıp, 5sn Cooldown
    // Riske Edilen: Kasanın %20'si (2$). Kaldıraç: 50x. (Yani her işlem 100$ gücünde olacak)
    // Take Profit: %0.5, Stop Loss: %0.2
    let risk_engine = Arc::new(Mutex::new(RiskEngine::new(
        10.0, 5.0, 5000, 0.20, 50.0, 0.005, 0.002,
    )));

    let live_prices: Arc<RwLock<HashMap<String, f64>>> = Arc::new(RwLock::new(HashMap::new()));

    // 1. CANLI FİYAT DİNLEYİCİSİ
    let prices_clone = live_prices.clone();
    let nats_prices_clone = nats_client.clone();
    tokio::spawn(async move {
        if let Ok(mut sub) = nats_prices_clone.subscribe("market.trade.>").await {
            while let Some(msg) = sub.next().await {
                if let Ok(trade) = AggTrade::decode(msg.payload) {
                    let mut cache = prices_clone.write().await;
                    cache.insert(trade.symbol, trade.price);
                }
            }
        }
    });

    // 2. BACKGROUND TP/SL MONITOR (Bekçi - Her 100ms'de bir kontrol eder)
    let engine_monitor = risk_engine.clone();
    let prices_monitor = live_prices.clone();
    let gateway_monitor = exchange_gateway.clone();
    let nats_monitor = nats_client.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_millis(100)).await;
            let current_prices = prices_monitor.read().await.clone();

            let close_orders = {
                let mut engine = engine_monitor.lock().await;
                engine.check_tp_sl(&current_prices)
            };

            for (symbol, side, quantity, price) in close_orders {
                match gateway_monitor
                    .send_order(&symbol, side, quantity, price)
                    .await
                {
                    Ok(mut report) => {
                        let mut engine = engine_monitor.lock().await;
                        engine.process_execution(&mut report);
                        let mut buf = Vec::new();
                        if report.encode(&mut buf).is_ok() {
                            let _ = nats_monitor
                                .publish(format!("execution.report.{}", report.symbol), buf.into())
                                .await;
                        }
                    }
                    Err(e) => error!("TP/SL Kapatma Hatası: {:?}", e),
                }
            }
        }
    });

    // 3. SİNYAL DİNLEYİCİSİ VE YÜRÜTÜCÜ
    let mut signal_sub = nats_client.subscribe("signal.trade.>").await?;

    while let Some(msg) = signal_sub.next().await {
        let signal = match TradeSignal::decode(msg.payload) {
            Ok(s) => s,
            Err(_) => continue,
        };

        if signal.confidence_score < 0.95 {
            continue;
        }

        let signal_type = SignalType::try_from(signal.r#type).unwrap_or(SignalType::Unspecified);
        let side = match signal_type {
            SignalType::Buy | SignalType::StrongBuy => "BUY",
            SignalType::Sell | SignalType::StrongSell => "SELL",
            _ => continue,
        };

        let expected_price = {
            let cache = live_prices.read().await;
            match cache.get(&signal.symbol) {
                Some(&p) => p,
                None => continue,
            }
        };

        // Risk Motoru Denetimi ve DINAMIK LOT HESAPLAMA
        let quantity = {
            let mut engine = risk_engine.lock().await;
            match engine.evaluate_signal(&signal.symbol, side, expected_price) {
                Ok(q) => q,
                Err(reason) => {
                    debug!(
                        "🛑 Sinyal Reddedildi: {} - {} - Neden: {}",
                        side, signal.symbol, reason
                    );
                    continue;
                }
            }
        };

        info!(
            "✅ Cüzdan Onayı: {} {} | Lot: {} | Fiyat: {:.2}$",
            side, signal.symbol, quantity, expected_price
        );

        let gateway = exchange_gateway.clone();
        let nats_publish_client = nats_client.clone();
        let risk_engine_clone = risk_engine.clone();
        let symbol_clone = signal.symbol.clone();

        tokio::spawn(async move {
            match gateway
                .send_order(&symbol_clone, side, quantity, expected_price)
                .await
            {
                Ok(mut report) => {
                    {
                        let mut engine = risk_engine_clone.lock().await;
                        engine.process_execution(&mut report);
                    }
                    let mut buf = Vec::new();
                    if report.encode(&mut buf).is_ok() {
                        let subject = format!("execution.report.{}", report.symbol);
                        let _ = nats_publish_client.publish(subject, buf.into()).await;
                    }
                }
                Err(e) => error!("❌ Emir İletim Hatası: {:?}", e),
            }
        });
    }
    Ok(())
}
