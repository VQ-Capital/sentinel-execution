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

pub trait ExchangeGateway: Send + Sync {
    fn send_order(
        &self,
        symbol: &str,
        side: &str,
        quantity: f64,
        expected_price: f64,
    ) -> impl std::future::Future<Output = Result<ExecutionReport>> + Send;
}

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
            realized_pnl: 0.0,
            commission,
            latency_ms: self.base_latency_ms as i64,
            timestamp: chrono::Utc::now().timestamp_millis(),
            is_simulated: true,
        })
    }
}

#[derive(Clone)]
struct Position {
    quantity: f64,
    avg_price: f64,
}

pub struct RiskEngine {
    initial_balance: f64,
    pub wallet_balance: f64,

    // Parametrik Ayarlar
    max_drawdown_usd: f64,
    cooldown_ms: i64,
    min_hold_time_ms: i64,
    base_risk_pct: f64,
    base_leverage: f64,
    take_profit_pct: f64,
    stop_loss_pct: f64,

    // State (Durumlar)
    positions: HashMap<String, Position>,
    last_trade_time: HashMap<String, i64>,
    kill_switch_active: bool,
    is_defensive_mode: bool, // SELF HEALING STATE
}

impl RiskEngine {
    pub fn new(
        init_bal: f64,
        max_dd: f64,
        cooldown: i64,
        min_hold: i64,
        risk_pct: f64,
        lev: f64,
        tp: f64,
        sl: f64,
    ) -> Self {
        Self {
            initial_balance: init_bal,
            wallet_balance: init_bal,
            max_drawdown_usd: max_dd,
            cooldown_ms: cooldown,
            min_hold_time_ms: min_hold,
            base_risk_pct: risk_pct,
            base_leverage: lev,
            take_profit_pct: tp,
            stop_loss_pct: sl,
            positions: HashMap::new(),
            last_trade_time: HashMap::new(),
            kill_switch_active: false,
            is_defensive_mode: false,
        }
    }

    // SELF HEALING MEKANİZMASI
    fn auto_tune_risk(&mut self) {
        let drawdown_pct = (self.initial_balance - self.wallet_balance) / self.initial_balance;

        if drawdown_pct > 0.15 && !self.is_defensive_mode {
            // Kasa %15 eridi -> DEFANS MODU
            self.is_defensive_mode = true;
            warn!("🚑 SELF-HEALING AKTİF: Kasa %15 eridi. Defansif moda geçiliyor. Risk ve Kaldıraç yarıya indirildi!");
        } else if drawdown_pct < -0.05 && self.is_defensive_mode {
            // Kasa başlangıcın %5 üstüne çıktı (Toparlandı) -> HÜCUM MODU
            self.is_defensive_mode = false;
            info!("🦅 SELF-HEALING İYİLEŞME: Kasa toparlandı. Normal saldırı moduna dönülüyor.");
        }
    }

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
        let last_time = self.last_trade_time.get(symbol).copied().unwrap_or(0);

        if now - last_time < self.cooldown_ms {
            return Err("COOLDOWN aktif.");
        }

        let pos = self.positions.get(symbol).cloned().unwrap_or(Position {
            quantity: 0.0,
            avg_price: 0.0,
        });

        // WHIPSAW KORUMASI (TESTERE)
        let is_reversal = (side == "SELL" && pos.quantity > 0.000001)
            || (side == "BUY" && pos.quantity < -0.000001);
        if is_reversal && now - last_time < self.min_hold_time_ms {
            return Err(
                "WHIPSAW KORUMASI: Pozisyon çok yeni, komisyon ödememek için sinyal reddedildi.",
            );
        }

        if side == "BUY" && pos.quantity > 0.000001 {
            return Err("Zaten LONG pozisyondasınız.");
        }
        if side == "SELL" && pos.quantity < -0.000001 {
            return Err("Zaten SHORT pozisyondasınız.");
        }

        // Dinamik Risk Çarpanları
        let active_risk = if self.is_defensive_mode {
            self.base_risk_pct / 2.0
        } else {
            self.base_risk_pct
        };
        let active_leverage = if self.is_defensive_mode {
            self.base_leverage / 2.0
        } else {
            self.base_leverage
        };

        let risk_amount = self.wallet_balance * active_risk;
        let buying_power = risk_amount * active_leverage;
        let raw_quantity = buying_power / price;
        let quantity = (raw_quantity * 100_000.0).trunc() / 100_000.0;

        if quantity <= 0.00001 {
            return Err("Hesaplanan lot boyutu yetersiz!");
        }

        self.last_trade_time.insert(symbol.to_string(), now);
        Ok(quantity)
    }

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
            }
            if let Some(&current_price) = current_prices.get(symbol) {
                let pnl_pct = if pos.quantity > 0.0 {
                    (current_price - pos.avg_price) / pos.avg_price
                } else {
                    (pos.avg_price - current_price) / pos.avg_price
                };
                let close_side = if pos.quantity > 0.0 { "SELL" } else { "BUY" };

                // Defans modundaysak kâr hedefini kısalt (Erken kâr alıp kaç)
                let active_tp = if self.is_defensive_mode {
                    self.take_profit_pct * 0.8
                } else {
                    self.take_profit_pct
                };

                if pnl_pct >= active_tp {
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
        // Rust Borrow Checker kuralı: Mutable referansın kapsamını (scope) daraltıyoruz.
        let current_pos_qty = {
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
                let total_value = (pos.quantity.abs() * pos.avg_price)
                    + (report.quantity * report.execution_price);
                pos.avg_price = total_value / new_qty.abs();
                pos.quantity = new_qty;
            }

            let net_pnl = realized_pnl - report.commission;
            report.realized_pnl = net_pnl;
            self.wallet_balance += net_pnl;

            // Loglama için son miktarı kopyalayıp dışarı aktarıyoruz.
            pos.quantity
        }; // <-- `self.positions` üzerindeki mutable referans (pos) BURADA BİTİYOR.

        // Artık `self`'i başka bir metod için güvenle kullanabiliriz.
        self.auto_tune_risk();

        let total_drawdown = self.initial_balance - self.wallet_balance;
        if total_drawdown >= self.max_drawdown_usd {
            error!(
                "🚨 KILL SWITCH: Maksimum kayıp ({:.2}$) aşıldı! Sistem kilitlendi.",
                total_drawdown
            );
            self.kill_switch_active = true;
        }

        info!(
            "💼 [CÜZDAN] Bakiye: {:.4} USDT | {} Pozisyonu: {:.5}",
            self.wallet_balance, report.symbol, current_pos_qty
        );
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // Ortam Değişkenleri ile Parametrik Yapı
    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());

    let init_bal: f64 = std::env::var("INITIAL_BALANCE")
        .unwrap_or_else(|_| "10.0".to_string())
        .parse()
        .unwrap_or(10.0);
    let max_dd: f64 = std::env::var("MAX_DRAWDOWN")
        .unwrap_or_else(|_| "5.0".to_string())
        .parse()
        .unwrap_or(5.0);
    let cooldown: i64 = std::env::var("COOLDOWN_MS")
        .unwrap_or_else(|_| "10000".to_string())
        .parse()
        .unwrap_or(10000);
    let min_hold: i64 = std::env::var("MIN_HOLD_MS")
        .unwrap_or_else(|_| "45000".to_string())
        .parse()
        .unwrap_or(45000);
    let risk_pct: f64 = std::env::var("RISK_PCT")
        .unwrap_or_else(|_| "0.20".to_string())
        .parse()
        .unwrap_or(0.20);
    let lev: f64 = std::env::var("LEVERAGE")
        .unwrap_or_else(|_| "20.0".to_string())
        .parse()
        .unwrap_or(20.0);
    let tp: f64 = std::env::var("TAKE_PROFIT")
        .unwrap_or_else(|_| "0.005".to_string())
        .parse()
        .unwrap_or(0.005);
    let sl: f64 = std::env::var("STOP_LOSS")
        .unwrap_or_else(|_| "0.002".to_string())
        .parse()
        .unwrap_or(0.002);

    let fee_rate: f64 = std::env::var("FEE_RATE")
        .unwrap_or_else(|_| "0.0001".to_string())
        .parse()
        .unwrap_or(0.0001);

    let nats_client = async_nats::connect(&nats_url)
        .await
        .context("CRITICAL: NATS bağlanılamadı")?;

    info!("🛡️ Kurumsal Risk Motoru (Self-Healing & Whipsaw Korumalı) devrede.");

    let exchange_gateway = Arc::new(ShadowExchange::new(fee_rate, 15));
    let risk_engine = Arc::new(Mutex::new(RiskEngine::new(
        init_bal, max_dd, cooldown, min_hold, risk_pct, lev, tp, sl,
    )));
    let live_prices: Arc<RwLock<HashMap<String, f64>>> = Arc::new(RwLock::new(HashMap::new()));

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
                if let Ok(mut report) = gateway_monitor
                    .send_order(&symbol, side, quantity, price)
                    .await
                {
                    let mut engine = engine_monitor.lock().await;
                    engine.process_execution(&mut report);
                    let mut buf = Vec::new();
                    if report.encode(&mut buf).is_ok() {
                        let _ = nats_monitor
                            .publish(format!("execution.report.{}", report.symbol), buf.into())
                            .await;
                    }
                }
            }
        }
    });

    let mut signal_sub = nats_client.subscribe("signal.trade.>").await?;

    while let Some(msg) = signal_sub.next().await {
        let signal = match TradeSignal::decode(msg.payload) {
            Ok(s) => s,
            Err(_) => continue,
        };

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

        let quantity = {
            let mut engine = risk_engine.lock().await;
            match engine.evaluate_signal(&signal.symbol, side, expected_price) {
                Ok(q) => q,
                Err(reason) => {
                    debug!("🛑 Reddedildi: {} - Neden: {}", signal.symbol, reason);
                    continue;
                }
            }
        };

        let active_mode_str = {
            if risk_engine.lock().await.is_defensive_mode {
                "🛡️ DEFANS"
            } else {
                "⚔️ HÜCUM"
            }
        };
        info!(
            "{} Onay: {} {} | Lot: {} | Fiyat: {:.2}$",
            active_mode_str, side, signal.symbol, quantity, expected_price
        );

        let gateway = exchange_gateway.clone();
        let nats_pub = nats_client.clone();
        let risk_clone = risk_engine.clone();
        let symbol_clone = signal.symbol.clone();

        tokio::spawn(async move {
            if let Ok(mut report) = gateway
                .send_order(&symbol_clone, side, quantity, expected_price)
                .await
            {
                {
                    let mut engine = risk_clone.lock().await;
                    engine.process_execution(&mut report);
                }
                let mut buf = Vec::new();
                if report.encode(&mut buf).is_ok() {
                    let _ = nats_pub
                        .publish(format!("execution.report.{}", report.symbol), buf.into())
                        .await;
                }
            }
        });
    }
    Ok(())
}
