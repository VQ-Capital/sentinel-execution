// ========== DOSYA: sentinel-execution/src/main.rs ==========
use anyhow::{Context, Result};
use futures_util::StreamExt;
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{sleep, timeout, Duration};
use tracing::{error, info, warn};
use uuid::Uuid;

mod config;
use config::RiskConfig;

pub mod sentinel {
    pub mod execution {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/sentinel.execution.v1.rs"));
        }
    }
    pub mod market {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/sentinel.market.v1.rs"));
        }
    }
    pub mod wallet {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/sentinel.wallet.v1.rs"));
        }
    }
    pub mod api {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/sentinel.api.v1.rs"));
        }
    }
}

use sentinel::api::v1::{control_command::CommandType, ControlCommand};
use sentinel::execution::v1::{
    trade_signal::SignalType, ExecutionRejection, ExecutionReport, TradeSignal,
};
use sentinel::market::v1::AggTrade;
use sentinel::wallet::v1::EquitySnapshot;

#[derive(Clone, Copy)]
pub struct SymbolRules {
    pub tick_size: f64,
    pub step_size: f64,
    pub min_notional: f64,
}

fn get_symbol_rules(symbol: &str) -> SymbolRules {
    match symbol {
        "BTCUSDT" => SymbolRules {
            tick_size: 0.1,
            step_size: 0.00001,
            min_notional: 5.0,
        },
        "ETHUSDT" => SymbolRules {
            tick_size: 0.01,
            step_size: 0.0001,
            min_notional: 5.0,
        },
        "BNBUSDT" => SymbolRules {
            tick_size: 0.1,
            step_size: 0.001,
            min_notional: 5.0,
        },
        "SOLUSDT" => SymbolRules {
            tick_size: 0.01,
            step_size: 0.01,
            min_notional: 5.0,
        },
        _ => SymbolRules {
            tick_size: 0.001,
            step_size: 0.1,
            min_notional: 5.0,
        },
    }
}

fn format_precision(val: f64, step: f64) -> f64 {
    let inv = 1.0 / step;
    (val * inv).trunc() / inv
}

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
        sleep(Duration::from_millis(self.cost_matrix.base_latency_ms)).await;
        let rules = get_symbol_rules(symbol);
        let exec_price = format_precision(
            if side == "BUY" {
                expected_price * (1.0 + self.cost_matrix.base_slippage_pct)
            } else {
                expected_price * (1.0 - self.cost_matrix.base_slippage_pct)
            },
            rules.tick_size,
        );
        Ok(ExecutionReport {
            symbol: symbol.to_string(),
            side: side.to_string(),
            expected_price,
            execution_price: exec_price,
            quantity,
            realized_pnl: 0.0,
            commission: exec_price * quantity * self.cost_matrix.fee_rate,
            latency_ms: self.cost_matrix.base_latency_ms as i64,
            timestamp: chrono::Utc::now().timestamp_millis(),
            is_simulated: true,
            order_id: format!("SIM-{:x}", Uuid::new_v4().as_fields().0),
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
        let rules = get_symbol_rules(symbol);
        let exec_price = format_precision(
            if side == "BUY" {
                expected_price * (1.0 + self.cost_matrix.base_slippage_pct)
            } else {
                expected_price * (1.0 - self.cost_matrix.base_slippage_pct)
            },
            rules.tick_size,
        );
        Ok(ExecutionReport {
            symbol: symbol.to_string(),
            side: side.to_string(),
            expected_price,
            execution_price: exec_price,
            quantity,
            realized_pnl: 0.0,
            commission: exec_price * quantity * self.cost_matrix.fee_rate,
            latency_ms: self.cost_matrix.base_latency_ms as i64,
            timestamp: chrono::Utc::now().timestamp_millis(),
            is_simulated: false,
            order_id: format!("HYP-{:x}", Uuid::new_v4().as_fields().0),
        })
    }
}

pub enum ActiveGateway {
    Shadow(ShadowExchange),
    Hyperliquid(HyperliquidGateway),
}
impl ActiveGateway {
    pub async fn send_order(
        &self,
        symbol: &str,
        side: &str,
        qty: f64,
        price: f64,
    ) -> Result<ExecutionReport> {
        match self {
            Self::Shadow(g) => g.send_order(symbol, side, qty, price).await,
            Self::Hyperliquid(g) => g.send_order(symbol, side, qty, price).await,
        }
    }
}

#[derive(Clone, Default, Debug)]
struct Position {
    quantity: f64,
    avg_price: f64,
    entry_time: i64,
}

pub struct RiskEngine {
    config: RiskConfig,
    positions: HashMap<String, Position>,
    last_trade_time: HashMap<String, i64>,
    pub kill_switch_active: bool,
    pub is_defensive_mode: bool,
    sla_violations: u32,
}

impl RiskEngine {
    pub fn new(config: RiskConfig) -> Self {
        Self {
            config,
            positions: HashMap::new(),
            last_trade_time: HashMap::new(),
            kill_switch_active: false,
            is_defensive_mode: false,
            sla_violations: 0,
        }
    }

    pub fn record_sla_violation(&mut self) {
        self.sla_violations += 1;
        if self.sla_violations >= self.config.max_sla_violations && !self.is_defensive_mode {
            self.is_defensive_mode = true;
            error!(
                "🛑 SLA BREACH: {} consecutive network delays! Defensive Mode ON.",
                self.config.max_sla_violations
            );
        }
    }

    pub fn reset_sla(&mut self) {
        if self.sla_violations > 0 {
            self.sla_violations = 0;
            if self.is_defensive_mode {
                info!("🟢 SLA Recovered. Defensive mode OFF.");
                self.is_defensive_mode = false;
            }
        }
    }

    pub fn auto_tune_risk(&mut self, current_equity: f64) {
        if self.kill_switch_active {
            return;
        }
        let drawdown_usd = self.config.initial_balance - current_equity;

        if drawdown_usd > self.config.defensive_drawdown_usd && !self.is_defensive_mode {
            self.is_defensive_mode = true;
            warn!("🚑 SELF-HEALING: Defensive Drawdown limit reached. Leverage & Risk halved.");
        }

        if drawdown_usd >= self.config.max_drawdown_usd && !self.kill_switch_active {
            self.kill_switch_active = true;
            error!(
                "🚨 FATAL DRAWDOWN DETECTED (Losing ${:.2})! KILL SWITCH ENGAGED!",
                drawdown_usd
            );
        }
    }

    pub fn evaluate_signal(
        &mut self,
        signal: &TradeSignal,
        side: &str,
        price: f64,
        equity: f64,
    ) -> Result<f64, (&'static str, String)> {
        if self.kill_switch_active {
            return Err((
                "KILL_SWITCH_ENGAGED",
                "Sistem kill switch modunda, işlem yapılamaz.".to_string(),
            ));
        }

        let now = chrono::Utc::now().timestamp_millis();
        let time_diff = (now - signal.timestamp).abs();
        let is_backtest = time_diff > 3600000;

        if !is_backtest && time_diff > self.config.max_signal_latency_ms {
            return Err(("STALE_SIGNAL", format!("Signal was {}ms old", time_diff)));
        }

        if let Some(pos) = self.positions.get(&signal.symbol) {
            if pos.quantity.abs() > 1e-6
                && ((side == "BUY" && pos.quantity > 0.0) || (side == "SELL" && pos.quantity < 0.0))
            {
                return Err((
                    "ANTI_MARTINGALE_REJECT",
                    "Aynı yönde pozisyon büyütme engellendi.".to_string(),
                ));
            }
        }

        let last_time = self
            .last_trade_time
            .get(&signal.symbol)
            .copied()
            .unwrap_or(0);
        if now - last_time < self.config.cooldown_ms {
            return Err(("COOLDOWN_ACTIVE", "Sık işlem filtresi devrede.".to_string()));
        }

        let signal_strength = match SignalType::try_from(signal.r#type).unwrap_or(SignalType::Hold)
        {
            SignalType::StrongBuy | SignalType::StrongSell => 1.0,
            _ => 0.5,
        };

        let active_risk = if self.is_defensive_mode {
            self.config.base_risk_pct * 0.5
        } else {
            self.config.base_risk_pct
        };
        let active_leverage = if self.is_defensive_mode {
            self.config.base_leverage * 0.5
        } else {
            self.config.base_leverage
        };

        let raw_quantity = (equity * active_risk * signal_strength * active_leverage) / price;
        let rules = get_symbol_rules(&signal.symbol);
        let notional_value = raw_quantity * price;

        if notional_value < rules.min_notional {
            return Err((
                "MIN_NOTIONAL_REJECTED",
                format!("İşlem hacmi {} dolardan az", rules.min_notional),
            ));
        }

        let formatted_qty = format_precision(raw_quantity, rules.step_size);
        if formatted_qty <= 0.0 {
            return Err(("INSUFFICIENT_MARGIN", "Hesaplanan miktar 0.".to_string()));
        }

        self.last_trade_time.insert(signal.symbol.clone(), now);
        Ok(formatted_qty)
    }

    pub fn check_tp_sl(
        &mut self,
        current_prices: &HashMap<String, f64>,
    ) -> Vec<(String, &'static str, f64, f64)> {
        let mut orders = Vec::new();
        let now = chrono::Utc::now().timestamp_millis();

        for (symbol, pos) in self.positions.iter() {
            if pos.quantity.abs() < 1e-6 {
                continue;
            }
            if let Some(&price) = current_prices.get(symbol) {
                if self.kill_switch_active {
                    orders.push((
                        symbol.clone(),
                        if pos.quantity > 0.0 { "SELL" } else { "BUY" },
                        pos.quantity.abs(),
                        price,
                    ));
                    continue;
                }

                let pnl = if pos.quantity > 0.0 {
                    (price - pos.avg_price) / pos.avg_price
                } else {
                    (pos.avg_price - price) / pos.avg_price
                };
                let time_held = now - pos.entry_time;

                if time_held < self.config.min_hold_time_ms && pnl > -self.config.stop_loss_pct {
                    continue;
                }

                if pnl >= self.config.take_profit_pct
                    || pnl <= -self.config.stop_loss_pct
                    || (time_held > self.config.max_hold_time_ms)
                {
                    orders.push((
                        symbol.clone(),
                        if pos.quantity > 0.0 { "SELL" } else { "BUY" },
                        pos.quantity.abs(),
                        price,
                    ));
                }
            }
        }
        orders
    }

    pub fn get_extinction_orders(
        &self,
        current_prices: &HashMap<String, f64>,
    ) -> Vec<(String, &'static str, f64, f64)> {
        let mut orders = Vec::new();
        for (symbol, pos) in self.positions.iter() {
            if pos.quantity.abs() < 1e-6 {
                continue;
            }
            if let Some(&price) = current_prices.get(symbol) {
                orders.push((
                    symbol.clone(),
                    if pos.quantity > 0.0 { "SELL" } else { "BUY" },
                    pos.quantity.abs(),
                    price,
                ));
            }
        }
        orders
    }

    pub fn process_execution(&mut self, report: &mut ExecutionReport) {
        let pos = self.positions.entry(report.symbol.clone()).or_default();
        let mut realized = 0.0;
        if (report.side == "SELL" && pos.quantity > 0.0)
            || (report.side == "BUY" && pos.quantity < 0.0)
        {
            let qty = report.quantity.min(pos.quantity.abs());
            realized = if pos.quantity > 0.0 {
                (report.execution_price - pos.avg_price) * qty
            } else {
                (pos.avg_price - report.execution_price) * qty
            };
            pos.quantity = if pos.quantity > 0.0 {
                pos.quantity - qty
            } else {
                pos.quantity + qty
            };
            if pos.quantity.abs() < 1e-6 {
                pos.avg_price = 0.0;
            }
        } else {
            let new_qty = if report.side == "BUY" {
                pos.quantity + report.quantity
            } else {
                pos.quantity - report.quantity
            };
            pos.avg_price = ((pos.quantity.abs() * pos.avg_price)
                + (report.quantity * report.execution_price))
                / new_qty.abs();
            pos.quantity = new_qty;
            pos.entry_time = report.timestamp;
        }
        report.realized_pnl = realized - report.commission;
        info!(
            "💼 [{}] {} {} | PnL: {:.4}$ | ID: {}",
            if report.is_simulated { "PAPER" } else { "LIVE" },
            report.symbol,
            report.side,
            report.realized_pnl,
            report.order_id
        );
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // ⚙️ Konfigürasyonu Çek (Env-Driven)
    let config = RiskConfig::from_env();

    info!(
        "📡 Service: {} | Version: 1.1.0 (V7 ENV-DRIVEN EXTINCTION)",
        env!("CARGO_PKG_NAME")
    );
    info!(
        "🛠️ Risk Config: Bal=${} | Risk={}% | Lev={}x | TP={}% | SL={}%",
        config.initial_balance,
        config.base_risk_pct * 100.0,
        config.base_leverage,
        config.take_profit_pct * 100.0,
        config.stop_loss_pct * 100.0
    );

    let nats_client = async_nats::connect(&config.nats_url)
        .await
        .context("NATS Fail")?;

    let active_gateway = Arc::new(RwLock::new(ActiveGateway::Shadow(ShadowExchange::new(
        CostMatrix {
            fee_rate: config.fee_rate,
            base_slippage_pct: 0.00005,
            base_latency_ms: 15,
        },
    ))));

    let risk_engine = Arc::new(Mutex::new(RiskEngine::new(config.clone())));

    let live_prices = Arc::new(RwLock::new(HashMap::<String, f64>::new()));
    let current_equity = Arc::new(RwLock::new(config.initial_balance));

    // 1. Piyasa Verilerini Oku
    let (lp, ce, n1, n2) = (
        live_prices.clone(),
        current_equity.clone(),
        nats_client.clone(),
        nats_client.clone(),
    );
    tokio::spawn(async move {
        if let Ok(mut sub) = n1.subscribe("market.trade.>").await {
            while let Some(msg) = sub.next().await {
                if let Ok(t) = AggTrade::decode(msg.payload) {
                    lp.write().await.insert(t.symbol.to_uppercase(), t.price);
                }
            }
        }
    });

    tokio::spawn(async move {
        if let Ok(mut sub) = n2.subscribe("wallet.equity.snapshot").await {
            while let Some(msg) = sub.next().await {
                if let Ok(s) = EquitySnapshot::decode(msg.payload) {
                    *ce.write().await = s.total_equity_usd;
                }
            }
        }
    });

    // 2. CONTROL COMMAND LISTENER
    let (cm_em, cm_nm, cm_pm, cm_gw) = (
        risk_engine.clone(),
        nats_client.clone(),
        live_prices.clone(),
        active_gateway.clone(),
    );
    tokio::spawn(async move {
        if let Ok(mut sub) = cm_nm.subscribe("control.command").await {
            while let Some(msg) = sub.next().await {
                if let Ok(cmd) = ControlCommand::decode(msg.payload) {
                    if cmd.r#type == CommandType::ExtinctionProtocol as i32 {
                        error!(
                            "🚨 [EXTINCTION PROTOCOL] TRIGGERED! DUMPING ALL POSITIONS TO USDC!"
                        );
                        let mut em = cm_em.lock().await;
                        em.kill_switch_active = true;

                        let prices = cm_pm.read().await;
                        let dump_orders = em.get_extinction_orders(&prices);

                        for (symbol, side, qty, price) in dump_orders {
                            let gw = cm_gw.read().await;
                            if let Ok(mut report) = gw.send_order(&symbol, side, qty, price).await {
                                em.process_execution(&mut report);
                                let _ = cm_nm
                                    .publish(
                                        format!("execution.report.{}", symbol),
                                        report.encode_to_vec().into(),
                                    )
                                    .await;
                            }
                        }
                        error!("💀 SİYAH KUĞU PROTOKOLÜ TAMAMLANDI. SİSTEM KENDİNİ İMHA EDİYOR.");
                        std::process::exit(1);
                    } else if cmd.r#type == CommandType::StopAll as i32 {
                        warn!("🛑 KILL SWITCH ACTIVATED VIA TERMINAL.");
                        cm_em.lock().await.kill_switch_active = true;
                    }
                }
            }
        }
    });

    // 3. Risk Yönetimi Döngüsü (TP/SL/SLA İzleme)
    let (em, pm, eqm, gm, nm) = (
        risk_engine.clone(),
        live_prices.clone(),
        current_equity.clone(),
        active_gateway.clone(),
        nats_client.clone(),
    );
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_millis(100)).await;
            let equity = *eqm.read().await;
            let prices = pm.read().await.clone();

            let (close_orders, _is_fatally_dead) = {
                let mut re = em.lock().await;
                re.auto_tune_risk(equity);
                (re.check_tp_sl(&prices), re.kill_switch_active)
            };

            for (symbol, side, qty, price) in close_orders {
                let gw = gm.read().await;
                if let Ok(Ok(mut report)) = timeout(
                    Duration::from_millis(50),
                    gw.send_order(&symbol, side, qty, price),
                )
                .await
                {
                    let mut re = em.lock().await;
                    re.reset_sla();
                    re.process_execution(&mut report);
                    let _ = nm
                        .publish(
                            format!("execution.report.{}", symbol),
                            report.encode_to_vec().into(),
                        )
                        .await;
                } else {
                    em.lock().await.record_sla_violation();
                }
            }

            if _is_fatally_dead {
                error!("💀 FATAL: KILL SWITCH ENGAGED! Stopping execution loop.");
                sleep(Duration::from_millis(500)).await;
                std::process::exit(1);
            }
        }
    });

    // 4. MAIN SIGNAL INGESTION (LIFO Backpressure & RCA Publishing)
    let mut signal_sub = nats_client.subscribe("signal.trade.>").await?;
    while let Some(msg) = signal_sub.next().await {
        if let Ok(signal) = TradeSignal::decode(msg.payload) {
            let symbol = signal.symbol.to_uppercase();
            let price = *live_prices.read().await.get(&symbol).unwrap_or(&0.0);
            let equity = *current_equity.read().await;

            if price == 0.0 {
                continue;
            }

            let side = match SignalType::try_from(signal.r#type).unwrap_or(SignalType::Hold) {
                SignalType::Buy | SignalType::StrongBuy => "BUY",
                SignalType::Sell | SignalType::StrongSell => "SELL",
                _ => continue,
            };

            let eval_result = {
                let mut re = risk_engine.lock().await;
                re.evaluate_signal(&signal, side, price, equity)
            };

            match eval_result {
                Ok(qty) => {
                    info!(
                        "🚀 SIGNAL ACCEPTED! Executing {} {} at ${}",
                        side, symbol, price
                    );
                    let gw = active_gateway.clone();
                    let nm = nats_client.clone();
                    let rm = risk_engine.clone();

                    tokio::spawn(async move {
                        let g = gw.read().await;
                        if let Ok(Ok(mut report)) = timeout(
                            Duration::from_millis(50),
                            g.send_order(&symbol, side, qty, price),
                        )
                        .await
                        {
                            let mut r = rm.lock().await;
                            r.reset_sla();
                            r.process_execution(&mut report);
                            let _ = nm
                                .publish(
                                    format!("execution.report.{}", symbol),
                                    report.encode_to_vec().into(),
                                )
                                .await;
                        } else {
                            rm.lock().await.record_sla_violation();
                        }
                    });
                }
                Err((reason_code, desc)) => {
                    tracing::debug!("⛔ Order Rejected [{}]: {} - {}", symbol, reason_code, desc);
                    let rejection = ExecutionRejection {
                        symbol: symbol.clone(),
                        original_side: side.to_string(),
                        intended_quantity: 0.0,
                        reason_code: reason_code.to_string(),
                        description: desc,
                        timestamp: chrono::Utc::now().timestamp_millis(),
                    };
                    let mut buf = Vec::new();
                    if rejection.encode(&mut buf).is_ok() {
                        let _ = nats_client
                            .publish(format!("execution.rejection.{}", symbol), buf.into())
                            .await;
                    }
                }
            }
        }
    }
    Ok(())
}
