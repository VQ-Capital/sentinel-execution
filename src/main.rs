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
// 1. EXCHANGE RULES & PRECISION ENGINE (FAZ 2)
// ==============================================================================
#[derive(Clone, Copy)]
pub struct SymbolRules {
    pub tick_size: f64,    // Fiyat Hassasiyeti
    pub step_size: f64,    // Miktar Hassasiyeti (MIKRO-LOT UYUMLU)
    pub min_notional: f64, // Minimum İşlem Tutarı (Örn: Binance için 5.0$)
}

// 🔥 CERRAHİ DÜZELTME: 10$ Kasanın işlem açabilmesi için mikro hassasiyetler girildi.
fn get_symbol_rules(symbol: &str) -> SymbolRules {
    match symbol {
        "BTCUSDT" => SymbolRules {
            tick_size: 0.1,
            step_size: 0.00001, // DÜZELTİLDİ (Eski: 0.001)
            min_notional: 5.0,
        },
        "ETHUSDT" => SymbolRules {
            tick_size: 0.01,
            step_size: 0.0001, // DÜZELTİLDİ (Eski: 0.01)
            min_notional: 5.0,
        },
        "BNBUSDT" => SymbolRules {
            tick_size: 0.1,
            step_size: 0.001, // DÜZELTİLDİ (Eski: 0.01)
            min_notional: 5.0,
        },
        "SOLUSDT" => SymbolRules {
            tick_size: 0.01,
            step_size: 0.01, // DÜZELTİLDİ (Eski: 0.1)
            min_notional: 5.0,
        },
        _ => SymbolRules {
            tick_size: 0.001,
            step_size: 0.1,
            min_notional: 5.0,
        },
    }
}

// Matematiksel Yuvarlama (Truncate) - Borsanın kabul edeceği formata çevirir
fn format_precision(val: f64, step: f64) -> f64 {
    let inv = 1.0 / step;
    (val * inv).trunc() / inv
}

// ==============================================================================
// 2. GATEWAY ARCHITECTURE (CORP STANDARD)
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
            order_id: format!(
                "SIM-{}",
                Uuid::new_v4()
                    .to_string()
                    .chars()
                    .take(8)
                    .collect::<String>()
            ),
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
            order_id: format!(
                "HYP-{}",
                Uuid::new_v4()
                    .to_string()
                    .chars()
                    .take(12)
                    .collect::<String>()
            ),
        })
    }
}

pub enum ActiveGateway {
    Shadow(ShadowExchange),
    Hyperliquid(HyperliquidGateway),
}
impl ActiveGateway {
    pub fn is_live(&self) -> bool {
        matches!(self, Self::Hyperliquid(_))
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
            Self::Hyperliquid(g) => g.send_order(symbol, side, qty, price).await,
        }
    }
}

// ==============================================================================
// 3. RISK ENGINE (Z-SCORE & PRECISION SENSITIVE)
// ==============================================================================
#[derive(Clone, Default, Debug)]
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
    pub kill_switch_active: bool,
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

        if drawdown_usd > (self.config.initial_balance * 0.15) && !self.is_defensive_mode {
            self.is_defensive_mode = true;
            warn!("🚑 SELF-HEALING: 15% Drawdown detected. Leverage & Risk halved.");
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
        _side: &str,
        price: f64,
        equity: f64,
    ) -> Result<f64, &'static str> {
        if self.kill_switch_active {
            return Err("KILL SWITCH ENGAGED");
        }

        let now = chrono::Utc::now().timestamp_millis();
        if now - signal.timestamp > self.config.max_signal_latency_ms {
            return Err("STALE SIGNAL");
        }

        let last_time = self
            .last_trade_time
            .get(&signal.symbol)
            .copied()
            .unwrap_or(0);
        if now - last_time < self.config.cooldown_ms {
            return Err("COOLDOWN ACTIVE");
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

        // 1. Ham Miktar Hesaplama
        let raw_quantity = (equity * active_risk * signal_strength * active_leverage) / price;

        // 2. Borsa Kuralları (Precision Engine)
        let rules = get_symbol_rules(&signal.symbol);
        let notional_value = raw_quantity * price;

        if notional_value < rules.min_notional {
            return Err("MIN_NOTIONAL_REJECTED"); // Limitlere takılırsa işlemi düşürür
        }

        let formatted_qty = format_precision(raw_quantity, rules.step_size);
        if formatted_qty <= 0.0 {
            return Err("INSUFFICIENT_MARGIN_AFTER_FORMAT");
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
            "💼 [{}] {} {} | PnL: {:.4}$ | ID: {}",
            if report.is_simulated { "PAPER" } else { "LIVE" },
            report.symbol,
            report.side,
            report.realized_pnl,
            report.order_id
        );
    }
}

// ==============================================================================
// 4. MAIN RUNTIME
// ==============================================================================
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!(
        "📡 Service: {} | Version: {} (Precision Engine Active)",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION")
    );

    let nats_url =
        std::env::var("NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".to_string());
    let nats_client = async_nats::connect(&nats_url).await.context("NATS Fail")?;

    let active_gateway = Arc::new(RwLock::new(ActiveGateway::Shadow(ShadowExchange::new(
        CostMatrix {
            fee_rate: std::env::var("FEE_RATE")
                .unwrap_or_else(|_| "0.0002".to_string())
                .parse()
                .unwrap_or(0.0002),
            base_slippage_pct: 0.00005,
            base_latency_ms: 15,
        },
    ))));

    let initial_balance = std::env::var("INITIAL_BALANCE")
        .unwrap_or_else(|_| "1000.0".to_string())
        .parse()
        .unwrap_or(1000.0);

    let risk_engine = Arc::new(Mutex::new(RiskEngine::new(RiskConfig {
        initial_balance,
        max_drawdown_usd: initial_balance
            * std::env::var("MAX_DRAWDOWN")
                .unwrap_or_else(|_| "0.05".to_string())
                .parse()
                .unwrap_or(0.05),
        cooldown_ms: std::env::var("COOLDOWN_MS")
            .unwrap_or_else(|_| "25000".to_string())
            .parse()
            .unwrap_or(25000),
        min_hold_time_ms: std::env::var("MIN_HOLD_MS")
            .unwrap_or_else(|_| "15000".to_string())
            .parse()
            .unwrap_or(15000),
        max_hold_time_ms: std::env::var("MAX_HOLD_MS")
            .unwrap_or_else(|_| "900000".to_string())
            .parse()
            .unwrap_or(900000),
        base_risk_pct: std::env::var("RISK_PCT")
            .unwrap_or_else(|_| "0.10".to_string())
            .parse()
            .unwrap_or(0.10),
        base_leverage: std::env::var("LEVERAGE")
            .unwrap_or_else(|_| "10.0".to_string())
            .parse()
            .unwrap_or(10.0),
        take_profit_pct: std::env::var("TAKE_PROFIT")
            .unwrap_or_else(|_| "0.003".to_string())
            .parse()
            .unwrap_or(0.003),
        stop_loss_pct: std::env::var("STOP_LOSS")
            .unwrap_or_else(|_| "0.002".to_string())
            .parse()
            .unwrap_or(0.002),
        max_signal_latency_ms: 2000,
    })));

    let live_prices = Arc::new(RwLock::new(HashMap::<String, f64>::new()));
    let current_equity = Arc::new(RwLock::new(initial_balance));

    let (lp, ce) = (live_prices.clone(), current_equity.clone());
    let (n1, n2) = (nats_client.clone(), nats_client.clone());

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

            let (close_orders, is_fatally_dead) = {
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

            if is_fatally_dead {
                error!("💀 FATAL: KILL SWITCH ENGAGED!");
                sleep(Duration::from_millis(500)).await;
                std::process::exit(1);
            }
        }
    });

    let mut signal_sub = nats_client.subscribe("signal.trade.>").await?;
    while let Some(msg) = signal_sub.next().await {
        if let Ok(signal) = TradeSignal::decode(msg.payload) {
            let symbol = signal.symbol.to_uppercase();
            let price = *live_prices.read().await.get(&symbol).unwrap_or(&0.0);
            let equity = *current_equity.read().await;

            if price == 0.0 {
                continue;
            }

            let mut re = risk_engine.lock().await;
            if re.kill_switch_active {
                continue;
            }

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

            let side = match SignalType::try_from(signal.r#type).unwrap_or(SignalType::Hold) {
                SignalType::Buy | SignalType::StrongBuy => "BUY",
                SignalType::Sell | SignalType::StrongSell => "SELL",
                _ => continue,
            };

            match re.evaluate_signal(&signal, side, price, equity) {
                Ok(qty) => {
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
                Err(reason) => {
                    // Min Notional (5$ altı limitleri) gibi ret durumları burada loglanır
                    tracing::debug!("⛔ Order Rejected [{}]: {}", symbol, reason);
                }
            }
        }
    }
    Ok(())
}
