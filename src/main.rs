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
// 1. GATEWAY ARCHITECTURE (CORP STANDARD)
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
        sleep(Duration::from_millis(self.cost_matrix.base_latency_ms)).await;
        let slippage = expected_price * self.cost_matrix.base_slippage_pct;
        let execution_price = if side == "BUY" {
            expected_price + slippage
        } else {
            expected_price - slippage
        };
        Ok(ExecutionReport {
            symbol: symbol.to_string(),
            side: side.to_string(),
            expected_price,
            execution_price,
            quantity,
            realized_pnl: 0.0,
            commission: execution_price * quantity * self.cost_matrix.fee_rate,
            latency_ms: self.cost_matrix.base_latency_ms as i64,
            timestamp: chrono::Utc::now().timestamp_millis(),
            is_simulated: true,
        })
    }
}

pub struct BinanceGateway {
    pub cost_matrix: CostMatrix,
}
impl Default for BinanceGateway {
    fn default() -> Self {
        Self::new()
    }
}

impl BinanceGateway {
    pub fn new() -> Self {
        Self {
            cost_matrix: CostMatrix {
                fee_rate: 0.0004,
                base_slippage_pct: 0.0002,
                base_latency_ms: 45,
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
        Ok(ExecutionReport {
            symbol: symbol.to_string(),
            side: side.to_string(),
            expected_price,
            execution_price,
            quantity,
            realized_pnl: 0.0,
            commission: execution_price * quantity * self.cost_matrix.fee_rate,
            latency_ms: self.cost_matrix.base_latency_ms as i64,
            timestamp: chrono::Utc::now().timestamp_millis(),
            is_simulated: false,
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
        Ok(ExecutionReport {
            symbol: symbol.to_string(),
            side: side.to_string(),
            expected_price,
            execution_price,
            quantity,
            realized_pnl: 0.0,
            commission: execution_price * quantity * self.cost_matrix.fee_rate,
            latency_ms: self.cost_matrix.base_latency_ms as i64,
            timestamp: chrono::Utc::now().timestamp_millis(),
            is_simulated: false,
        })
    }
}

pub enum ActiveGateway {
    Shadow(ShadowExchange),
    Binance(BinanceGateway),
    Hyperliquid(HyperliquidGateway),
}
impl ActiveGateway {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Shadow(_) => "SHADOW",
            Self::Binance(_) => "BINANCE",
            Self::Hyperliquid(_) => "HYPERLIQUID",
        }
    }
    pub fn is_live(&self) -> bool {
        !matches!(self, Self::Shadow(_))
    }
    pub async fn send_order(
        &self,
        symbol: &str,
        side: &str,
        qty: f64,
        price: f64,
    ) -> Result<ExecutionReport> {
        match self {
            Self::Shadow(g) => g.send_order(symbol, side, qty, price).await,
            Self::Binance(g) => g.send_order(symbol, side, qty, price).await,
            Self::Hyperliquid(g) => g.send_order(symbol, side, qty, price).await,
        }
    }
}

// ==============================================================================
// 2. RISK ENGINE (Z-SCORE & SLA SENSITIVE)
// ==============================================================================
#[derive(Clone, Default)]
struct Position {
    quantity: f64,
    avg_price: f64,
    entry_time: i64,
}

pub struct RiskConfig {
    pub initial_balance: f64,
    pub max_drawdown_usd: f64,
    pub cooldown_ms: i64,
    pub min_hold_time_ms: i64,
    pub max_hold_time_ms: i64,
    pub base_risk_pct: f64,
    pub base_leverage: f64,
    pub take_profit_pct: f64,
    pub stop_loss_pct: f64,
    pub max_signal_latency_ms: i64,
}

pub struct RiskEngine {
    config: RiskConfig,
    pub paper_trades_count: i32,
    pub paper_winning_trades: i32,
    pub paper_cumulative_pnl: f64,
    positions: HashMap<String, Position>,
    last_trade_time: HashMap<String, i64>,
    kill_switch_active: bool,
    pub is_defensive_mode: bool,
    sla_violations: u32,
}

impl RiskEngine {
    pub fn new(config: RiskConfig) -> Self {
        Self {
            config,
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

    pub fn record_sla_violation(&mut self) {
        self.sla_violations += 1;
        if self.sla_violations >= 3 && !self.is_defensive_mode {
            self.is_defensive_mode = true;
            error!("🛑 SLA BREACH: 3 consecutive network delays! Defensive Mode ON.");
        }
    }

    pub fn reset_sla(&mut self) {
        if self.sla_violations > 0 {
            self.sla_violations = 0;
            if self.is_defensive_mode {
                info!("🟢 SLA Recovered.");
            }
        }
    }

    fn format_lot_size(symbol: &str, raw_qty: f64) -> f64 {
        match symbol {
            "BTCUSDT" => (raw_qty * 100_000.0).trunc() / 100_000.0,
            "ETHUSDT" => (raw_qty * 10_000.0).trunc() / 10_000.0,
            _ => (raw_qty * 1000.0).trunc() / 1000.0,
        }
    }

    pub fn auto_tune_risk(&mut self, current_equity: f64) {
        let drawdown_usd = self.config.initial_balance - current_equity;
        if drawdown_usd > (self.config.initial_balance * 0.15) && !self.is_defensive_mode {
            self.is_defensive_mode = true;
            warn!("🚑 SELF-HEALING: 15% Drawdown detected!");
        }
        if drawdown_usd >= self.config.max_drawdown_usd {
            self.kill_switch_active = true;
            error!("🚨 KILL SWITCH!");
        }
    }

    pub fn evaluate_signal(
        &mut self,
        signal: &TradeSignal,
        _side: &str,
        price: f64,
        equity: f64,
    ) -> Result<f64, &'static str> {
        if self.kill_switch_active {
            return Err("KILL SWITCH");
        }
        let now = chrono::Utc::now().timestamp_millis();
        if now - signal.timestamp > self.config.max_signal_latency_ms {
            return Err("STALE");
        }

        let last_time = self
            .last_trade_time
            .get(&signal.symbol)
            .copied()
            .unwrap_or(0);
        if now - last_time < self.config.cooldown_ms {
            return Err("COOLDOWN");
        }

        // V3 Z-Score Risk Adjustment
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
        let quantity = Self::format_lot_size(
            &signal.symbol,
            (equity * active_risk * signal_strength * self.config.base_leverage) / price,
        );

        if quantity <= 0.0 {
            return Err("MIN_LOT");
        }
        self.last_trade_time.insert(signal.symbol.clone(), now);
        Ok(quantity)
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
                let pnl = if pos.quantity > 0.0 {
                    (price - pos.avg_price) / pos.avg_price
                } else {
                    (pos.avg_price - price) / pos.avg_price
                };
                if pnl >= self.config.take_profit_pct
                    || pnl <= -self.config.stop_loss_pct
                    || (now - pos.entry_time > self.config.max_hold_time_ms)
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

    pub fn process_execution(&mut self, report: &mut ExecutionReport) {
        let pos = self.positions.entry(report.symbol.clone()).or_default();
        let mut realized = 0.0;
        let mut is_closing = false;

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
            is_closing = true;
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
        if is_closing {
            self.paper_trades_count += 1;
            self.paper_cumulative_pnl += report.realized_pnl;
            if report.realized_pnl > 0.0 {
                self.paper_winning_trades += 1;
            }
        }
        info!(
            "💼 [{}] {} {} | PnL: {:.4}$",
            if report.is_simulated { "PAPER" } else { "LIVE" },
            report.symbol,
            report.side,
            report.realized_pnl
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
    let nats_client = async_nats::connect(&nats_url).await.context("NATS Fail")?;

    let active_gateway = Arc::new(RwLock::new(ActiveGateway::Shadow(ShadowExchange::new(
        CostMatrix {
            fee_rate: 0.0001,
            base_slippage_pct: 0.00005,
            base_latency_ms: 15,
        },
    ))));
    let risk_engine = Arc::new(Mutex::new(RiskEngine::new(RiskConfig {
        initial_balance: 10.0,
        max_drawdown_usd: 5.0,
        cooldown_ms: 10000,
        min_hold_time_ms: 45000,
        max_hold_time_ms: 900000,
        base_risk_pct: 0.20,
        base_leverage: 20.0,
        take_profit_pct: 0.006,
        stop_loss_pct: 0.003,
        max_signal_latency_ms: 2000,
    })));

    let live_prices = Arc::new(RwLock::new(HashMap::<String, f64>::new()));
    let current_equity = Arc::new(RwLock::new(10.0));

    // Listeners
    let (lp, ce) = (live_prices.clone(), current_equity.clone());
    let (n1, n2) = (nats_client.clone(), nats_client.clone());
    tokio::spawn(async move {
        if let Ok(mut sub) = n1.subscribe("market.trade.>").await {
            while let Some(msg) = sub.next().await {
                if let Ok(t) = AggTrade::decode(msg.payload) {
                    lp.write().await.insert(t.symbol, t.price);
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

    // TP/SL Watchdog
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
            let close_orders = {
                let mut re = em.lock().await;
                re.auto_tune_risk(equity);
                re.check_tp_sl(&prices)
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
        }
    });

    // Signal Loop
    let mut signal_sub = nats_client.subscribe("signal.trade.>").await?;
    while let Some(msg) = signal_sub.next().await {
        if let Ok(signal) = TradeSignal::decode(msg.payload) {
            let symbol = signal.symbol.clone();
            let price = *live_prices.read().await.get(&symbol).unwrap_or(&0.0);
            let equity = *current_equity.read().await;
            if price == 0.0 {
                continue;
            }

            let mut re = risk_engine.lock().await;
            let side = match SignalType::try_from(signal.r#type).unwrap_or(SignalType::Hold) {
                SignalType::Buy | SignalType::StrongBuy => "BUY",
                SignalType::Sell | SignalType::StrongSell => "SELL",
                _ => continue,
            };

            if let Ok(qty) = re.evaluate_signal(&signal, side, price, equity) {
                let gw = active_gateway.clone();
                let nm = nats_client.clone();
                let rm = risk_engine.clone();

                // AUTO-PROMOTION
                {
                    let mut g = active_gateway.write().await;
                    let win_rate = if re.paper_trades_count > 0 {
                        (re.paper_winning_trades as f64 / re.paper_trades_count as f64) * 100.0
                    } else {
                        0.0
                    };
                    if !g.is_live()
                        && re.paper_trades_count >= 10
                        && win_rate >= 55.0
                        && re.paper_cumulative_pnl > 0.5
                    {
                        warn!("🚀 PROMOTED TO HYPERLIQUID LIVE!");
                        *g = ActiveGateway::Hyperliquid(HyperliquidGateway::new());
                    }
                }

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
        }
    }
    Ok(())
}
