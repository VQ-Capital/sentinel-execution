// ========== DOSYA: sentinel-execution/src/config.rs ==========

#[derive(Clone, Debug)]
pub struct RiskConfig {
    pub nats_url: String,
    pub fee_rate: f64,
    pub initial_balance: f64,
    pub max_drawdown_usd: f64,
    pub defensive_drawdown_usd: f64,
    pub cooldown_ms: i64,
    pub min_hold_time_ms: i64,
    pub max_hold_time_ms: i64,
    pub base_risk_pct: f64,
    pub base_leverage: f64,
    pub take_profit_pct: f64,
    pub stop_loss_pct: f64,
    pub max_signal_latency_ms: i64,
    pub max_sla_violations: u32,
}

impl RiskConfig {
    pub fn from_env() -> Self {
        let initial_balance: f64 = std::env::var("INITIAL_BALANCE")
            .unwrap_or_else(|_| "50.0".to_string())
            .parse()
            .expect("ENV ERROR: INITIAL_BALANCE");

        let max_dd_pct: f64 = std::env::var("MAX_DRAWDOWN_PCT")
            .unwrap_or_else(|_| "0.15".to_string())
            .parse()
            .expect("ENV ERROR: MAX_DRAWDOWN_PCT");

        let def_dd_pct: f64 = std::env::var("DEFENSIVE_DRAWDOWN_PCT")
            .unwrap_or_else(|_| "0.10".to_string())
            .parse()
            .expect("ENV ERROR: DEFENSIVE_DRAWDOWN_PCT");

        Self {
            nats_url: std::env::var("NATS_URL")
                .unwrap_or_else(|_| "nats://localhost:4222".to_string()),
            fee_rate: std::env::var("FEE_RATE")
                .unwrap_or_else(|_| "0.0002".to_string())
                .parse()
                .expect("ENV ERROR: FEE_RATE"),
            initial_balance,
            max_drawdown_usd: initial_balance * max_dd_pct,
            defensive_drawdown_usd: initial_balance * def_dd_pct,
            cooldown_ms: std::env::var("COOLDOWN_MS")
                .unwrap_or_else(|_| "10000".to_string())
                .parse()
                .expect("ENV ERROR: COOLDOWN_MS"),
            min_hold_time_ms: std::env::var("MIN_HOLD_MS")
                .unwrap_or_else(|_| "5000".to_string())
                .parse()
                .expect("ENV ERROR: MIN_HOLD_MS"),
            max_hold_time_ms: std::env::var("MAX_HOLD_MS")
                .unwrap_or_else(|_| "300000".to_string())
                .parse()
                .expect("ENV ERROR: MAX_HOLD_MS"),
            base_risk_pct: std::env::var("RISK_PCT")
                .unwrap_or_else(|_| "0.10".to_string())
                .parse()
                .expect("ENV ERROR: RISK_PCT"),
            base_leverage: std::env::var("LEVERAGE")
                .unwrap_or_else(|_| "10.0".to_string())
                .parse()
                .expect("ENV ERROR: LEVERAGE"),
            take_profit_pct: std::env::var("TAKE_PROFIT")
                .unwrap_or_else(|_| "0.008".to_string())
                .parse()
                .expect("ENV ERROR: TAKE_PROFIT"),
            stop_loss_pct: std::env::var("STOP_LOSS")
                .unwrap_or_else(|_| "0.004".to_string())
                .parse()
                .expect("ENV ERROR: STOP_LOSS"),
            max_signal_latency_ms: std::env::var("MAX_SIGNAL_LATENCY_MS")
                .unwrap_or_else(|_| "2000".to_string())
                .parse()
                .expect("ENV ERROR: MAX_SIGNAL_LATENCY_MS"),
            max_sla_violations: std::env::var("MAX_SLA_VIOLATIONS")
                .unwrap_or_else(|_| "3".to_string())
                .parse()
                .expect("ENV ERROR: MAX_SLA_VIOLATIONS"),
        }
    }
}
