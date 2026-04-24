// ========== DOSYA: sentinel-execution/src/main.rs ==========
use anyhow::{Context, Result};
use futures_util::StreamExt;
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{sleep, timeout, Duration};
use tracing::{error, info, warn};

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

// ==============================================================================
// 1. DİNAMİK MALİYET MATRİSİ VE STATİK DAĞITIM (ZERO-ALLOCATION HFT)
// ==============================================================================

#[derive(Clone, Copy)]
pub struct CostMatrix {
    pub fee_rate: f64,
    pub base_slippage_pct: f64,
    pub base_latency_ms: u64,
}

pub struct ShadowExchange {
    pub cost_matrix: CostMatrix,
}
impl ShadowExchange {
    pub fn new(cost_matrix: CostMatrix) -> Self {
        Self { cost_matrix }
    }
    async fn send_order(
        &self,
        symbol: &str,
        side: &str,
        quantity: f64,
        expected_price: f64,
    ) -> Result<ExecutionReport> {
        // Asenkron gecikme (Cancellation safe'dir, timeout tetiklenirse hemen iptal olur)
        sleep(Duration::from_millis(self.cost_matrix.base_latency_ms)).await;

        let slippage = expected_price * self.cost_matrix.base_slippage_pct;
        let execution_price = if side == "BUY" {
            expected_price + slippage
        } else {
            expected_price - slippage
        };
        let commission = execution_price * quantity * self.cost_matrix.fee_rate;

        Ok(ExecutionReport {
            symbol: symbol.to_string(),
            side: side.to_string(),
            expected_price,
            execution_price,
            quantity,
            realized_pnl: 0.0,
            commission,
            latency_ms: self.cost_matrix.base_latency_ms as i64,
            timestamp: chrono::Utc::now().timestamp_millis(),
            is_simulated: true,
        })
    }
}

pub struct HyperliquidGateway {
    pub cost_matrix: CostMatrix,
}
impl Default for HyperliquidGateway {
    fn default() -> Self {
        Self::new()
    }
}

impl HyperliquidGateway {
    pub fn new() -> Self {
        Self {
            cost_matrix: CostMatrix {
                fee_rate: 0.0001,
                base_slippage_pct: 0.00005,
                base_latency_ms: 15,
            },
        }
    }
    async fn send_order(
        &self,
        symbol: &str,
        side: &str,
        quantity: f64,
        expected_price: f64,
    ) -> Result<ExecutionReport> {
        sleep(Duration::from_millis(self.cost_matrix.base_latency_ms)).await;
        let slippage = expected_price * self.cost_matrix.base_slippage_pct;
        let execution_price = if side == "BUY" {
            expected_price + slippage
        } else {
            expected_price - slippage
        };
        let commission = execution_price * quantity * self.cost_matrix.fee_rate;

        Ok(ExecutionReport {
            symbol: symbol.to_string(),
            side: side.to_string(),
            expected_price,
            execution_price,
            quantity,
            realized_pnl: 0.0,
            commission,
            latency_ms: self.cost_matrix.base_latency_ms as i64,
            timestamp: chrono::Utc::now().timestamp_millis(),
            is_simulated: false, // GERÇEK
        })
    }
}

pub enum ActiveGateway {
    Shadow(ShadowExchange),
    Hyperliquid(HyperliquidGateway),
}

impl ActiveGateway {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Shadow(_) => "SHADOW_SIMULATOR",
            Self::Hyperliquid(_) => "HYPERLIQUID_LIVE",
        }
    }

    pub fn is_live(&self) -> bool {
        match self {
            Self::Shadow(_) => false,
            Self::Hyperliquid(_) => true,
        }
    }

    pub async fn send_order(
        &self,
        symbol: &str,
        side: &str,
        quantity: f64,
        expected_price: f64,
    ) -> Result<ExecutionReport> {
        match self {
            Self::Shadow(g) => g.send_order(symbol, side, quantity, expected_price).await,
            Self::Hyperliquid(g) => g.send_order(symbol, side, quantity, expected_price).await,
        }
    }
}

// ==============================================================================
// 2. RISK ENGINE & SLA WATCHDOG YÖNETİMİ
// ==============================================================================

#[derive(Clone, Default)]
struct Position {
    quantity: f64,
    avg_price: f64,
    entry_time: i64,
}

pub struct RiskEngine {
    initial_balance: f64,
    max_drawdown_usd: f64,
    cooldown_ms: i64,
    min_hold_time_ms: i64,
    base_risk_pct: f64,
    base_leverage: f64,
    take_profit_pct: f64,
    stop_loss_pct: f64,

    pub paper_trades_count: i32,
    pub paper_winning_trades: i32,
    pub paper_cumulative_pnl: f64,

    positions: HashMap<String, Position>,
    last_trade_time: HashMap<String, i64>,
    kill_switch_active: bool,
    pub is_defensive_mode: bool,

    // SLA WATCHDOG
    sla_violations: u32,
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
            max_drawdown_usd: max_dd,
            cooldown_ms: cooldown,
            min_hold_time_ms: min_hold,
            base_risk_pct: risk_pct,
            base_leverage: lev,
            take_profit_pct: tp,
            stop_loss_pct: sl,
            paper_trades_count: 0,
            paper_winning_trades: 0,
            paper_cumulative_pnl: 0.0,
            positions: HashMap::new(),
            last_trade_time: HashMap::new(),
            kill_switch_active: false,
            is_defensive_mode: false,
            sla_violations: 0,
        }
    }

    // SLA Watchdog: Eğer timeout alınırsa bu fonksiyon çağrılır.
    pub fn record_sla_violation(&mut self) {
        self.sla_violations += 1;
        if self.sla_violations >= 3 && !self.is_defensive_mode {
            self.is_defensive_mode = true;
            error!("🛑 SLA WATCHDOG: Peş peşe 3 kez ağ gecikmesi yaşandı! Sistem DEFANSİF MODA çekildi.");
        }
    }

    pub fn reset_sla_violations(&mut self) {
        if self.sla_violations > 0 {
            self.sla_violations = 0;
            if self.is_defensive_mode {
                info!("🟢 SLA WATCHDOG: Ağ stabil. İhlaller sıfırlandı.");
            }
        }
    }

    fn format_lot_size(symbol: &str, raw_qty: f64) -> f64 {
        match symbol {
            "BTCUSDT" => (raw_qty * 100_000.0).trunc() / 100_000.0,
            "ETHUSDT" => (raw_qty * 10_000.0).trunc() / 10_000.0,
            "SOLUSDT" | "BNBUSDT" => (raw_qty * 100.0).trunc() / 100.0,
            _ => (raw_qty * 1000.0).trunc() / 1000.0,
        }
    }

    pub fn auto_tune_risk(&mut self, current_equity: f64) {
        let drawdown_pct = (self.initial_balance - current_equity) / self.initial_balance;

        if drawdown_pct > 0.15 && !self.is_defensive_mode {
            self.is_defensive_mode = true;
            warn!("🚑 SELF-HEALING: Kasa %15 eridi. Defansif moda geçiliyor!");
        } else if drawdown_pct < -0.05 && self.is_defensive_mode && self.sla_violations < 3 {
            self.is_defensive_mode = false;
            info!(
                "🦅 SELF-HEALING: Kasa toparlandı ve Ağ Stabil. Normal saldırı moduna dönülüyor."
            );
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
    ) -> Result<f64, &'static str> {
        if self.kill_switch_active {
            return Err("KILL SWITCH AKTİF.");
        }

        let now = chrono::Utc::now().timestamp_millis();
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
            .unwrap_or_default();
        let is_reversal = (side == "SELL" && pos.quantity > 0.000001)
            || (side == "BUY" && pos.quantity < -0.000001);
        if is_reversal && now - pos.entry_time < self.min_hold_time_ms {
            return Err("WHIPSAW KORUMASI: Çok erken.");
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
            return Err("Lot boyutu limitlerin altında!");
        }

        self.last_trade_time.insert(signal.symbol.clone(), now);
        Ok(quantity)
    }

    pub fn process_execution(&mut self, report: &mut ExecutionReport) {
        let now = chrono::Utc::now().timestamp_millis();
        let pos = self.positions.entry(report.symbol.clone()).or_default();
        let mut realized_pnl = 0.0;
        let mut is_closing_trade = false;

        if report.side == "SELL" && pos.quantity > 0.0 {
            let close_qty = report.quantity.min(pos.quantity);
            realized_pnl = (report.execution_price - pos.avg_price) * close_qty;
            pos.quantity -= close_qty;
            if pos.quantity <= 0.000001 {
                pos.avg_price = 0.0;
            }
            is_closing_trade = true;
        } else if report.side == "BUY" && pos.quantity < 0.0 {
            let close_qty = report.quantity.min(pos.quantity.abs());
            realized_pnl = (pos.avg_price - report.execution_price) * close_qty;
            pos.quantity += close_qty;
            if pos.quantity.abs() <= 0.000001 {
                pos.avg_price = 0.0;
            }
            is_closing_trade = true;
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
            pos.entry_time = now;
        }

        let net_pnl = realized_pnl - report.commission;
        report.realized_pnl = net_pnl;

        if is_closing_trade {
            self.paper_trades_count += 1;
            self.paper_cumulative_pnl += net_pnl;
            if net_pnl > 0.0 {
                self.paper_winning_trades += 1;
            }
        }

        info!(
            "💼 [{}] {} İşlemi: {} | PnL: {:.4}$",
            if report.is_simulated { "PAPER" } else { "LIVE" },
            report.symbol,
            report.side,
            net_pnl
        );
    }
}

// ==============================================================================
// 3. MAIN RUNTIME
// ==============================================================================
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());

    let nats_client = async_nats::connect(&nats_url)
        .await
        .context("CRITICAL: NATS bağlanılamadı")?;

    let hl_matrix = CostMatrix {
        fee_rate: 0.0001,
        base_slippage_pct: 0.00005,
        base_latency_ms: 15,
    };
    let active_gateway = Arc::new(RwLock::new(ActiveGateway::Shadow(ShadowExchange::new(
        hl_matrix,
    ))));

    info!("🛡️ Kurumsal Execution Motoru (SLA WATCHDOG AKTİF) devrede.");

    let risk_engine = Arc::new(Mutex::new(RiskEngine::new(
        10.0, 5.0, 10000, 45000, 0.20, 20.0, 0.005, 0.002,
    )));

    let live_prices: Arc<RwLock<HashMap<String, f64>>> = Arc::new(RwLock::new(HashMap::new()));
    let current_equity: Arc<RwLock<Option<f64>>> = Arc::new(RwLock::new(None));

    // Piyasa Dinleyicisi
    let prices_clone = live_prices.clone();
    let nats_prices = nats_client.clone();
    tokio::spawn(async move {
        if let Ok(mut sub) = nats_prices.subscribe("market.trade.>").await {
            while let Some(msg) = sub.next().await {
                if let Ok(trade) = AggTrade::decode(msg.payload) {
                    prices_clone.write().await.insert(trade.symbol, trade.price);
                }
            }
        }
    });

    // Cüzdan Dinleyicisi
    let equity_clone = current_equity.clone();
    let nats_wallet = nats_client.clone();
    tokio::spawn(async move {
        if let Ok(mut sub) = nats_wallet.subscribe("wallet.equity.snapshot").await {
            while let Some(msg) = sub.next().await {
                if let Ok(snapshot) = EquitySnapshot::decode(msg.payload) {
                    *equity_clone.write().await = Some(snapshot.total_equity_usd);
                }
            }
        }
    });

    // Sinyal Dinleyicisi (SLA Watchdog ile İzole)
    let mut signal_sub = nats_client.subscribe("signal.trade.>").await?;

    // HFT Kuralı: Emirin borsaya ulaşıp dönmesi için kabul edilebilir MİNİMUM süre 50 milisaniyedir.
    let sla_timeout_limit = Duration::from_millis(50);

    while let Some(msg) = signal_sub.next().await {
        let signal = match TradeSignal::decode(msg.payload) {
            Ok(s) => s,
            Err(_) => continue,
        };

        let eq_opt = *current_equity.read().await;
        if eq_opt.is_none() {
            continue;
        }

        let signal_type = SignalType::try_from(signal.r#type).unwrap_or(SignalType::Unspecified);
        let side = match signal_type {
            SignalType::Buy | SignalType::StrongBuy => "BUY",
            SignalType::Sell | SignalType::StrongSell => "SELL",
            _ => continue,
        };

        let expected_price = match live_prices.read().await.get(&signal.symbol) {
            Some(&p) => p,
            None => continue,
        };

        let quantity = match risk_engine.lock().await.evaluate_signal(
            &signal,
            side,
            expected_price,
            eq_opt.unwrap(),
        ) {
            Ok(q) => q,
            Err(e) => {
                warn!("Sinyal Reddedildi ({}): {}", signal.symbol, e);
                continue;
            }
        };

        let is_live = active_gateway.read().await.is_live();
        let def_mode = risk_engine.lock().await.is_defensive_mode;

        info!(
            "{} {} | {} {} | Lot: {} | Fiyat: {:.2}$",
            if is_live { "🔥 CANLI" } else { "🧪 TEST" },
            if def_mode {
                "🛡️ DEFANS"
            } else {
                "⚔️ HÜCUM"
            },
            side,
            signal.symbol,
            quantity,
            expected_price
        );

        let gateway = active_gateway.clone();
        let nats_pub = nats_client.clone();
        let risk_clone = risk_engine.clone();
        let symbol_clone = signal.symbol.clone();

        // Asenkron Emir Gönderimi ve SLA Watchdog (Cancellation Safety)
        tokio::spawn(async move {
            let gw = gateway.read().await;

            // 🔥 SLA WATCHDOG DEVREYE GİRİYOR 🔥
            let execution_result = timeout(
                sla_timeout_limit,
                gw.send_order(&symbol_clone, side, quantity, expected_price),
            )
            .await;

            match execution_result {
                Ok(Ok(mut report)) => {
                    // BAŞARILI: SLA içerisinde tamamlandı.
                    let mut engine = risk_clone.lock().await;
                    engine.reset_sla_violations(); // İhlalleri sıfırla
                    engine.process_execution(&mut report);

                    let mut buf = Vec::new();
                    if report.encode(&mut buf).is_ok() {
                        let _ = nats_pub
                            .publish(format!("execution.report.{}", report.symbol), buf.into())
                            .await;
                    }
                }
                Ok(Err(e)) => {
                    // GATEWAY HATASI (Borsa reddetti, bakiye yetersiz vb.)
                    warn!("⚠️ Borsa İletim Hatası ({}): {}", symbol_clone, e);
                }
                Err(_) => {
                    // 🚨 TIMEOUT: SLA İHLALİ (Ağ çok yavaş!)
                    error!(
                        "⏳ [SLA İHLALİ] Borsa yanıtı {}ms'yi aştı! Emir havada İPTAL EDİLDİ.",
                        sla_timeout_limit.as_millis()
                    );
                    let mut engine = risk_clone.lock().await;
                    engine.record_sla_violation();
                }
            }
        });
    }
    Ok(())
}
