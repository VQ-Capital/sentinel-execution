// ========== DOSYA: sentinel-execution/src/main.rs ==========
use anyhow::{Context, Result};
use futures_util::StreamExt;
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{sleep, timeout, Duration};
use tracing::{error, info};
use uuid::Uuid;

mod config;
use config::RiskConfig as EnvRiskConfig;

use sentinel_core::risk::engine::{RiskConfig as CoreRiskConfig, RiskEngine};
use sentinel_core::types::{
    format_precision, get_symbol_rules, SignalType as CoreSignalType,
    TradeSignal as CoreTradeSignal,
};

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
    // 🔥 CERRAHİ: Artık Utc::now() kullanmıyoruz! Borsa saatini (market_time) baz alıyoruz.
    async fn send_order(
        &self,
        symbol: &str,
        side: &str,
        quantity: f64,
        expected_price: f64,
        market_time: i64,
    ) -> Result<ExecutionReport> {
        // Gerçek dünya (Wall Clock) gecikmesi simüle ediliyor (SLA için gerekli)
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
            timestamp: market_time, // ⏱️ ZAMAN MAKİNESİ UYUMU
            is_simulated: true,
            order_id: format!("SIM-{:x}", Uuid::new_v4().as_fields().0),
        })
    }
}

pub enum ActiveGateway {
    Shadow(ShadowExchange),
}
impl ActiveGateway {
    pub async fn send_order(
        &self,
        symbol: &str,
        side: &str,
        qty: f64,
        price: f64,
        market_time: i64,
    ) -> Result<ExecutionReport> {
        match self {
            Self::Shadow(g) => g.send_order(symbol, side, qty, price, market_time).await,
        }
    }
}

pub struct ExecutionWatchdog {
    pub engine: RiskEngine,
    max_sla_violations: u32,
    sla_violations: u32,
    max_signal_latency_ms: i64,
}

impl ExecutionWatchdog {
    pub fn new(env_cfg: EnvRiskConfig) -> Self {
        let core_cfg = CoreRiskConfig {
            initial_balance: env_cfg.initial_balance,
            max_drawdown_usd: env_cfg.max_drawdown_usd,
            defensive_drawdown_usd: env_cfg.defensive_drawdown_usd,
            cooldown_ms: env_cfg.cooldown_ms,
            min_hold_time_ms: env_cfg.min_hold_time_ms,
            max_hold_time_ms: env_cfg.max_hold_time_ms,
            base_risk_pct: env_cfg.base_risk_pct,
            base_leverage: env_cfg.base_leverage,
            take_profit_pct: env_cfg.take_profit_pct,
            stop_loss_pct: env_cfg.stop_loss_pct,
        };

        Self {
            engine: RiskEngine::new(core_cfg),
            max_sla_violations: env_cfg.max_sla_violations,
            sla_violations: 0,
            max_signal_latency_ms: env_cfg.max_signal_latency_ms,
        }
    }

    pub fn record_sla_violation(&mut self) {
        self.sla_violations += 1;
        if self.sla_violations >= self.max_sla_violations && !self.engine.is_defensive_mode {
            self.engine.is_defensive_mode = true;
            error!(
                "🛑 SLA BREACH: {} consecutive delays! Defensive Mode ON.",
                self.max_sla_violations
            );
        }
    }

    pub fn reset_sla(&mut self) {
        if self.sla_violations > 0 {
            self.sla_violations = 0;
            if self.engine.is_defensive_mode {
                info!("🟢 SLA Recovered. Defensive mode OFF.");
                self.engine.is_defensive_mode = false;
            }
        }
    }

    pub fn get_extinction_orders(
        &self,
        current_prices: &HashMap<String, f64>,
    ) -> Vec<(String, &'static str, f64, f64)> {
        let mut orders = Vec::new();
        for (symbol, pos) in self.engine.positions.iter() {
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
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let config = EnvRiskConfig::from_env();

    info!(
        "📡 Service: {} | Version: 0.9.8 (V9 MARKET-CLOCK SYNC)",
        env!("CARGO_PKG_NAME")
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

    let watchdog = Arc::new(Mutex::new(ExecutionWatchdog::new(config.clone())));

    let live_prices = Arc::new(RwLock::new(HashMap::<String, f64>::new()));
    let current_equity = Arc::new(RwLock::new(config.initial_balance));

    // ⏱️ SİSTEMİN YENİ KALBİ: Borsa Saati
    let latest_market_time = Arc::new(RwLock::new(0i64));

    // 1. Piyasa Verilerini Oku ve Borsa Saatini Güncelle
    let (lp, ce, mt, n1, n2) = (
        live_prices.clone(),
        current_equity.clone(),
        latest_market_time.clone(),
        nats_client.clone(),
        nats_client.clone(),
    );
    tokio::spawn(async move {
        if let Ok(mut sub) = n1.subscribe("market.trade.>").await {
            while let Some(msg) = sub.next().await {
                if let Ok(t) = AggTrade::decode(msg.payload) {
                    lp.write().await.insert(t.symbol.to_uppercase(), t.price);

                    // Saati daima ileriye doğru güncelle (Geriye gitmeyi önler)
                    let mut time_lock = mt.write().await;
                    if t.timestamp > *time_lock {
                        *time_lock = t.timestamp;
                    }
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
    let (cm_em, cm_nm, cm_pm, cm_gw, cm_mt) = (
        watchdog.clone(),
        nats_client.clone(),
        live_prices.clone(),
        active_gateway.clone(),
        latest_market_time.clone(),
    );
    tokio::spawn(async move {
        if let Ok(mut sub) = cm_nm.subscribe("control.command").await {
            while let Some(msg) = sub.next().await {
                if let Ok(cmd) = ControlCommand::decode(msg.payload) {
                    if cmd.r#type == CommandType::ExtinctionProtocol as i32 {
                        error!("🚨 [EXTINCTION PROTOCOL] TRIGGERED!");
                        let mut wd = cm_em.lock().await;
                        wd.engine.kill_switch_active = true;

                        let prices = cm_pm.read().await;
                        let market_clock = *cm_mt.read().await;
                        let dump_orders = wd.get_extinction_orders(&prices);

                        for (symbol, side, qty, price) in dump_orders {
                            let gw = cm_gw.read().await;
                            if let Ok(mut report) =
                                gw.send_order(&symbol, side, qty, price, market_clock).await
                            {
                                let realized = wd.engine.process_execution(
                                    &symbol,
                                    side,
                                    report.execution_price,
                                    qty,
                                    report.timestamp,
                                );
                                report.realized_pnl = realized - report.commission;
                                let _ = cm_nm
                                    .publish(
                                        format!("execution.report.{}", symbol),
                                        report.encode_to_vec().into(),
                                    )
                                    .await;
                            }
                        }
                        std::process::exit(1);
                    } else if cmd.r#type == CommandType::StopAll as i32 {
                        cm_em.lock().await.engine.kill_switch_active = true;
                    }
                }
            }
        }
    });

    // 3. Risk Yönetimi Döngüsü (TP/SL/SLA İzleme)
    let (em, pm, eqm, gm, nm, rmt) = (
        watchdog.clone(),
        live_prices.clone(),
        current_equity.clone(),
        active_gateway.clone(),
        nats_client.clone(),
        latest_market_time.clone(),
    );
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_millis(100)).await;
            let equity = *eqm.read().await;
            let prices = pm.read().await.clone();

            // ⏱️ Utc::now() YERİNE, BORSA SAATİ KULLANILIYOR
            let market_clock = *rmt.read().await;
            if market_clock == 0 {
                continue;
            } // Sistem veri alana kadar bekle

            let (close_orders, _is_fatally_dead) = {
                let mut wd = em.lock().await;
                wd.engine.auto_tune_risk(equity);
                (
                    wd.engine.check_tp_sl(&prices, market_clock),
                    wd.engine.kill_switch_active,
                )
            };

            for (symbol, side, qty, price) in close_orders {
                let gw = gm.read().await;
                if let Ok(Ok(mut report)) = timeout(
                    Duration::from_millis(50),
                    gw.send_order(&symbol, side, qty, price, market_clock),
                )
                .await
                {
                    let mut wd = em.lock().await;
                    wd.reset_sla();
                    let realized = wd.engine.process_execution(
                        &symbol,
                        side,
                        report.execution_price,
                        qty,
                        report.timestamp,
                    );
                    report.realized_pnl = realized - report.commission;
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

    // 4. MAIN SIGNAL INGESTION
    let mut signal_sub = nats_client.subscribe("signal.trade.>").await?;
    while let Some(msg) = signal_sub.next().await {
        if let Ok(signal) = TradeSignal::decode(msg.payload) {
            let symbol = signal.symbol.to_uppercase();
            let price = *live_prices.read().await.get(&symbol).unwrap_or(&0.0);
            let equity = *current_equity.read().await;

            if price == 0.0 {
                continue;
            }

            let core_sig_type =
                match SignalType::try_from(signal.r#type).unwrap_or(SignalType::Hold) {
                    SignalType::Buy => CoreSignalType::Buy,
                    SignalType::StrongBuy => CoreSignalType::StrongBuy,
                    SignalType::Sell => CoreSignalType::Sell,
                    SignalType::StrongSell => CoreSignalType::StrongSell,
                    _ => CoreSignalType::Hold,
                };

            let core_signal = CoreTradeSignal {
                symbol: symbol.clone(),
                signal_type: core_sig_type,
                confidence_score: signal.confidence_score,
                recommended_leverage: signal.recommended_leverage,
                timestamp: signal.timestamp,
            };

            let eval_result = {
                let mut wd = watchdog.lock().await;

                // Zaman yolculuğu uyumluluğu (Time Travel Compatibility)
                // Sinyalin gecikmesini Market Clock'a göre ölç!
                let market_clock = *latest_market_time.read().await;
                let time_diff = (market_clock - signal.timestamp).abs();

                // BACKTEST BYPASS KALDIRILDI: Artık borsa saati eşzamanlı olduğu için gerçek limitleri kontrol edebiliriz
                if time_diff > wd.max_signal_latency_ms {
                    Err("STALE_SIGNAL")
                } else {
                    wd.engine
                        .evaluate_signal(&core_signal, price, equity, market_clock)
                }
            };

            let side = match core_sig_type {
                CoreSignalType::Buy | CoreSignalType::StrongBuy => "BUY",
                CoreSignalType::Sell | CoreSignalType::StrongSell => "SELL",
                _ => "HOLD",
            };

            match eval_result {
                Ok(qty) => {
                    info!(
                        "🚀 SIGNAL ACCEPTED! Executing {} {} at ${}",
                        side, symbol, price
                    );
                    let gw = active_gateway.clone();
                    let nm = nats_client.clone();
                    let rm = watchdog.clone();
                    let sig_time = signal.timestamp;

                    tokio::spawn(async move {
                        let g = gw.read().await;
                        if let Ok(Ok(mut report)) = timeout(
                            Duration::from_millis(50),
                            g.send_order(&symbol, side, qty, price, sig_time), // Sinyalin anındaki zaman!
                        )
                        .await
                        {
                            let mut wd = rm.lock().await;
                            wd.reset_sla();
                            let realized = wd.engine.process_execution(
                                &symbol,
                                side,
                                report.execution_price,
                                qty,
                                report.timestamp,
                            );
                            report.realized_pnl = realized - report.commission;
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
                Err(reason_code) => {
                    tracing::debug!("⛔ Order Rejected [{}]: {}", symbol, reason_code);
                    let rejection = ExecutionRejection {
                        symbol: symbol.clone(),
                        original_side: side.to_string(),
                        intended_quantity: 0.0,
                        reason_code: reason_code.to_string(),
                        description: "Core Engine Rejected".to_string(),
                        timestamp: chrono::Utc::now().timestamp_millis(), // Bu loglama amaçlı Wall Clock kalabilir
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
