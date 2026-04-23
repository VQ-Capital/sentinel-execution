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
    pub mod wallet {
        include!(concat!(env!("OUT_DIR"), "/sentinel.wallet.v1.rs"));
    }
}

use sentinel_protos::execution::{trade_signal::SignalType, ExecutionReport, TradeSignal};
use sentinel_protos::market::AggTrade;
use sentinel_protos::wallet::EquitySnapshot;

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
        // Dinamik Kayma (Slippage) Modeli
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
    entry_time: i64, // Time-Decay (Funding) koruması için
}

pub struct RiskEngine {
    initial_balance: f64,
    // DİKKAT: wallet_balance BURADAN SİLİNDİ! Artık NATS EquitySnapshot'tan okunacak.

    // Parametrik Ayarlar
    max_drawdown_usd: f64,
    cooldown_ms: i64,
    min_hold_time_ms: i64,
    max_hold_time_ms: i64, // YENİ: Maksimum pozisyon tutma süresi (Funding yememek için)
    base_risk_pct: f64,
    base_leverage: f64,
    take_profit_pct: f64,
    stop_loss_pct: f64,
    max_signal_latency_ms: i64, // YENİ: Bayat sinyal koruması

    // State (Durumlar)
    positions: HashMap<String, Position>,
    last_trade_time: HashMap<String, i64>,
    kill_switch_active: bool,
    is_defensive_mode: bool,
}

impl RiskEngine {
    pub fn new(
        init_bal: f64,
        max_dd: f64,
        cooldown: i64,
        min_hold: i64,
        max_hold: i64,
        risk_pct: f64,
        lev: f64,
        tp: f64,
        sl: f64,
        max_lat: i64,
    ) -> Self {
        Self {
            initial_balance: init_bal,
            max_drawdown_usd: max_dd,
            cooldown_ms: cooldown,
            min_hold_time_ms: min_hold,
            max_hold_time_ms: max_hold,
            base_risk_pct: risk_pct,
            base_leverage: lev,
            take_profit_pct: tp,
            stop_loss_pct: sl,
            max_signal_latency_ms: max_lat,
            positions: HashMap::new(),
            last_trade_time: HashMap::new(),
            kill_switch_active: false,
            is_defensive_mode: false,
        }
    }

    // YENİ: HASSAS LOT YÖNETİMİ (PRECISION)
    fn format_lot_size(symbol: &str, raw_qty: f64) -> f64 {
        match symbol {
            "BTCUSDT" => (raw_qty * 100_000.0).trunc() / 100_000.0, // 5 decimal
            "ETHUSDT" => (raw_qty * 10_000.0).trunc() / 10_000.0,   // 4 decimal
            "SOLUSDT" | "BNBUSDT" => (raw_qty * 100.0).trunc() / 100.0, // 2 decimal
            _ => (raw_qty * 1000.0).trunc() / 1000.0,               // Default 3 decimal
        }
    }

    fn auto_tune_risk(&mut self, current_equity: f64) {
        let drawdown_pct = (self.initial_balance - current_equity) / self.initial_balance;

        if drawdown_pct > 0.15 && !self.is_defensive_mode {
            self.is_defensive_mode = true;
            warn!("🚑 SELF-HEALING AKTİF: Kasa %15 eridi. Defansif moda geçiliyor. Risk/Kaldıraç yarıya indirildi!");
        } else if drawdown_pct < -0.05 && self.is_defensive_mode {
            self.is_defensive_mode = false;
            info!("🦅 SELF-HEALING İYİLEŞME: Kasa toparlandı. Normal saldırı moduna dönülüyor.");
        }

        let total_drawdown_usd = self.initial_balance - current_equity;
        if total_drawdown_usd >= self.max_drawdown_usd && !self.kill_switch_active {
            error!(
                "🚨 KILL SWITCH: Maksimum kayıp ({:.2}$) aşıldı! Sistem kilitlendi.",
                total_drawdown_usd
            );
            self.kill_switch_active = true;
        }
    }

    pub fn evaluate_signal(
        &mut self,
        signal: &TradeSignal,
        side: &str,
        price: f64,
        current_equity: f64,
    ) -> std::result::Result<f64, &'static str> {
        if self.kill_switch_active {
            return Err("KILL SWITCH AKTİF.");
        }

        let now = chrono::Utc::now().timestamp_millis();

        // YENİ: BAYAT SİNYAL KORUMASI (LATENCY TELEMETRY)
        let signal_age = now - signal.timestamp;
        if signal_age > self.max_signal_latency_ms {
            return Err("STALE SIGNAL: Sinyal çok gecikmeli ulaştı, reddedildi.");
        }

        let last_time = self
            .last_trade_time
            .get(&signal.symbol)
            .copied()
            .unwrap_or(0);

        if now - last_time < self.cooldown_ms {
            return Err("COOLDOWN aktif.");
        }

        let pos = self
            .positions
            .get(&signal.symbol)
            .cloned()
            .unwrap_or(Position {
                quantity: 0.0,
                avg_price: 0.0,
                entry_time: 0,
            });

        // WHIPSAW KORUMASI
        let is_reversal = (side == "SELL" && pos.quantity > 0.000001)
            || (side == "BUY" && pos.quantity < -0.000001);
        if is_reversal && now - pos.entry_time < self.min_hold_time_ms {
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

        let risk_amount = current_equity * active_risk;
        let buying_power = risk_amount * active_leverage;
        let raw_quantity = buying_power / price;
        let quantity = Self::format_lot_size(&signal.symbol, raw_quantity);

        if quantity <= 0.0 {
            return Err("Hesaplanan lot boyutu borsa limitlerinin altında!");
        }

        self.last_trade_time.insert(signal.symbol.clone(), now);
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

        let now = chrono::Utc::now().timestamp_millis();

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

                let active_tp = if self.is_defensive_mode {
                    self.take_profit_pct * 0.8
                } else {
                    self.take_profit_pct
                };

                // YENİ: TIME-DECAY EXIT (FUNDING WATCHDOG)
                let hold_duration = now - pos.entry_time;
                if hold_duration > self.max_hold_time_ms {
                    warn!(
                        "⏳ TIME-DECAY TETİKLENDİ: {} (Funding/Risk koruması için kapatılıyor)",
                        symbol
                    );
                    close_orders.push((
                        symbol.clone(),
                        close_side,
                        pos.quantity.abs(),
                        current_price,
                    ));
                    continue;
                }

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
        let now = chrono::Utc::now().timestamp_millis();
        let pos = self
            .positions
            .entry(report.symbol.clone())
            .or_insert(Position {
                quantity: 0.0,
                avg_price: 0.0,
                entry_time: 0,
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
            pos.entry_time = now; // Yeni pozisyon giriş zamanı
        }

        let net_pnl = realized_pnl - report.commission;
        report.realized_pnl = net_pnl;

        info!("💼 [EXECUTION] {} İşlemi: Yön: {} | Miktar: {} | Gerçekleşen Fiyat: {:.2}$ | PnL: {:.4}$",
            report.symbol, report.side, report.quantity, report.execution_price, net_pnl
        );
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());

    // Varsayılan Parametreler
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
    let max_hold: i64 = std::env::var("MAX_HOLD_MS")
        .unwrap_or_else(|_| "900000".to_string())
        .parse()
        .unwrap_or(900000); // 15 Dk
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
    let max_lat: i64 = std::env::var("MAX_LATENCY_MS")
        .unwrap_or_else(|_| "2000".to_string())
        .parse()
        .unwrap_or(2000);

    let nats_client = async_nats::connect(&nats_url)
        .await
        .context("CRITICAL: NATS bağlanılamadı")?;
    info!("🛡️ Kurumsal Execution Motoru (Muhasebe Ayrılmış Mod) devrede.");

    let exchange_gateway = Arc::new(ShadowExchange::new(fee_rate, 15));
    let risk_engine = Arc::new(Mutex::new(RiskEngine::new(
        init_bal, max_dd, cooldown, min_hold, max_hold, risk_pct, lev, tp, sl, max_lat,
    )));

    // NATS üzerinden beslenen Canlı Durum Önbellekleri (Caches)
    let live_prices: Arc<RwLock<HashMap<String, f64>>> = Arc::new(RwLock::new(HashMap::new()));
    let current_equity: Arc<RwLock<Option<f64>>> = Arc::new(RwLock::new(None));

    // 1. DİNLEYİCİ: Piyasa Fiyatları (TP/SL kontrolü için)
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

    // 2. DİNLEYİCİ: Merkezi Hazine (Wallet) Bakiyesi
    let equity_clone = current_equity.clone();
    let nats_wallet_clone = nats_client.clone();
    tokio::spawn(async move {
        if let Ok(mut sub) = nats_wallet_clone.subscribe("wallet.equity.snapshot").await {
            while let Some(msg) = sub.next().await {
                if let Ok(snapshot) = EquitySnapshot::decode(msg.payload) {
                    let mut eq = equity_clone.write().await;
                    *eq = Some(snapshot.total_equity_usd);
                    // DİKKAT: Burada Risk motoruna doğrudan erişip auto_tune çağrısı yapmıyoruz.
                    // İşlemler kilitlenmesin diye sadece okuma cache'ini güncelliyoruz.
                }
            }
        }
    });

    // WORKER: TP/SL ve Time-Decay Kontrolcüsü
    let engine_monitor = risk_engine.clone();
    let prices_monitor = live_prices.clone();
    let equity_monitor = current_equity.clone();
    let gateway_monitor = exchange_gateway.clone();
    let nats_monitor = nats_client.clone();

    tokio::spawn(async move {
        loop {
            sleep(Duration::from_millis(100)).await;

            // Eğer cüzdan servisi henüz çalışmıyorsa TP/SL kontrolü yapma
            let eq_val = *equity_monitor.read().await;
            if eq_val.is_none() {
                continue;
            }
            let eq = eq_val.unwrap();

            let current_prices = prices_monitor.read().await.clone();

            let close_orders = {
                let mut engine = engine_monitor.lock().await;
                engine.auto_tune_risk(eq); // Her döngüde risk korumasını kalibre et
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

    // ANA DÖNGÜ: Sinyalleri Dinle ve İcra Et
    let mut signal_sub = nats_client.subscribe("signal.trade.>").await?;

    while let Some(msg) = signal_sub.next().await {
        let signal = match TradeSignal::decode(msg.payload) {
            Ok(s) => s,
            Err(_) => continue,
        };

        let eq_opt = *current_equity.read().await;
        if eq_opt.is_none() {
            warn!("🛑 Sinyal Reddedildi ({}): sentinel-wallet servisi ayakta değil, bakiye doğrulanamıyor!", signal.symbol);
            continue;
        }
        let equity = eq_opt.unwrap();

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
            match engine.evaluate_signal(&signal, side, expected_price, equity) {
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
            "{} Onay: {} {} | Lot: {} | Fiyat: {:.2}$ | Equity: {:.2}$",
            active_mode_str, side, signal.symbol, quantity, expected_price, equity
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
