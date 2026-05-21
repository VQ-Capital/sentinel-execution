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
use config::ExecutionConfig;

use sentinel_core::dna; // 🧬 SSOT DNA eklendi
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

use sentinel::execution::v1::{
    trade_signal::SignalType, ExecutionRejection, ExecutionReport, TradeSignal,
};
use sentinel::market::v1::AggTrade;
use sentinel::wallet::v1::EquitySnapshot;

pub struct ShadowExchange;
impl Default for ShadowExchange {
    fn default() -> Self {
        Self::new()
    }
}

impl ShadowExchange {
    pub fn new() -> Self {
        Self
    }

    async fn send_order(
        &self,
        symbol: &str,
        side: &str,
        quantity: f64,
        expected_price: f64,
        market_time: i64,
    ) -> Result<ExecutionReport> {
        // HFT Gerçekçilik Payı (Backtest için 15ms)
        sleep(Duration::from_millis(15)).await;

        let rules = get_symbol_rules(symbol);
        let exec_price = format_precision(
            if side == "BUY" {
                expected_price * (1.0 + dna::BASE_SLIPPAGE_PCT)
            } else {
                expected_price * (1.0 - dna::BASE_SLIPPAGE_PCT)
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
            commission: exec_price * quantity * dna::FEE_RATE,
            latency_ms: 15,
            timestamp: market_time, // ⏱️ ZAMAN MAKİNESİ UYUMU
            is_simulated: true,
            order_id: format!("SIM-{:x}", Uuid::new_v4().as_fields().0),
        })
    }
}

pub struct ExecutionWatchdog {
    pub engine: RiskEngine,
    max_sla_violations: u32,
    sla_violations: u32,
}

impl ExecutionWatchdog {
    pub fn new(env_cfg: &ExecutionConfig) -> Self {
        // 🧬 Çekirdek konfigürasyonu DNA'dan (SSOT) alarak besliyoruz
        let core_cfg = CoreRiskConfig::new_baked(
            env_cfg.initial_balance,
            env_cfg.max_drawdown_pct,
            env_cfg.defensive_drawdown_pct,
        );

        Self {
            engine: RiskEngine::new(core_cfg),
            max_sla_violations: env_cfg.max_sla_violations,
            sla_violations: 0,
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
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let config = ExecutionConfig::from_env();

    info!(
        "📡 Service: {} | Version: 1.0.0 (V10 SINGULARITY EVENT-DRIVEN)",
        env!("CARGO_PKG_NAME")
    );

    let nats_client = async_nats::connect(&config.nats_url)
        .await
        .context("NATS Fail")?;
    let active_gateway = Arc::new(ShadowExchange::new());
    let watchdog = Arc::new(Mutex::new(ExecutionWatchdog::new(&config)));

    let live_prices = Arc::new(RwLock::new(HashMap::<String, f64>::new()));
    let current_equity = Arc::new(RwLock::new(config.initial_balance));
    let latest_market_time = Arc::new(RwLock::new(0i64));

    // 1. Cüzdan Dinleyicisi
    let (ce, n1) = (current_equity.clone(), nats_client.clone());
    tokio::spawn(async move {
        if let Ok(mut sub) = n1.subscribe("wallet.equity.snapshot").await {
            while let Some(msg) = sub.next().await {
                if let Ok(s) = EquitySnapshot::decode(msg.payload) {
                    *ce.write().await = s.total_equity_usd;
                }
            }
        }
    });

    // 2. ⏱️ EVENT-DRIVEN MARKET CLOCK & TICK-BASED TP/SL (ZAMAN PARADOKSU FIX)
    let (lp, eqm, mt, em, gw, n2) = (
        live_prices.clone(),
        current_equity.clone(),
        latest_market_time.clone(),
        watchdog.clone(),
        active_gateway.clone(),
        nats_client.clone(),
    );
    tokio::spawn(async move {
        if let Ok(mut sub) = n2.subscribe("market.trade.>").await {
            while let Some(msg) = sub.next().await {
                if let Ok(t) = AggTrade::decode(msg.payload) {
                    let symbol = t.symbol.to_uppercase();
                    lp.write().await.insert(symbol.clone(), t.price);

                    let market_clock = {
                        let mut time_lock = mt.write().await;
                        if t.timestamp > *time_lock {
                            *time_lock = t.timestamp;
                        }
                        *time_lock
                    };

                    // 🔥 CERRAHİ: Gelen her tick anında TP/SL kontrolü tetikler. (Backtest'te %100 hassasiyet!)
                    let equity = *eqm.read().await;
                    let prices = lp.read().await.clone();

                    let (close_orders, is_dead) = {
                        let mut wd = em.lock().await;
                        wd.engine.auto_tune_risk(equity);
                        (
                            wd.engine.check_tp_sl(&prices, market_clock),
                            wd.engine.kill_switch_active,
                        )
                    };

                    for (sym, side, qty, price) in close_orders {
                        if let Ok(Ok(mut report)) = timeout(
                            Duration::from_millis(50),
                            gw.send_order(&sym, side, qty, price, market_clock),
                        )
                        .await
                        {
                            let mut wd = em.lock().await;
                            wd.reset_sla();
                            let realized = wd.engine.process_execution(
                                &sym,
                                side,
                                report.execution_price,
                                qty,
                                report.timestamp,
                            );
                            report.realized_pnl = realized - report.commission;
                            let _ = n2
                                .publish(
                                    format!("execution.report.{}", sym),
                                    report.encode_to_vec().into(),
                                )
                                .await;
                        } else {
                            em.lock().await.record_sla_violation();
                        }
                    }

                    if is_dead {
                        error!("💀 FATAL: KILL SWITCH ENGAGED! Stopping execution.");
                        std::process::exit(1);
                    }
                }
            }
        }
    });

    // 3. MAIN SIGNAL INGESTION
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
                let market_clock = *latest_market_time.read().await;
                wd.engine
                    .evaluate_signal(&core_signal, price, equity, market_clock)
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
                        if let Ok(Ok(mut report)) = timeout(
                            Duration::from_millis(50),
                            gw.send_order(&symbol, side, qty, price, sig_time),
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
