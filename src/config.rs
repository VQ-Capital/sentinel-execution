// ========== DOSYA: sentinel-execution/src/config.rs ==========
#[derive(Clone, Debug)]
pub struct ExecutionConfig {
    pub nats_url: String,
    pub initial_balance: f64,
    pub max_drawdown_pct: f64,
    pub defensive_drawdown_pct: f64,
    pub max_sla_violations: u32,
}

impl ExecutionConfig {
    pub fn from_env() -> Self {
        Self {
            nats_url: std::env::var("NATS_URL")
                .unwrap_or_else(|_| "nats://localhost:4222".to_string()),
            initial_balance: std::env::var("INITIAL_BALANCE")
                .unwrap_or_else(|_| "1000.0".to_string())
                .parse()
                .expect("ENV ERROR: INITIAL_BALANCE"),
            max_drawdown_pct: std::env::var("MAX_DRAWDOWN_PCT")
                .unwrap_or_else(|_| "0.15".to_string())
                .parse()
                .expect("ENV ERROR: MAX_DRAWDOWN_PCT"),
            defensive_drawdown_pct: std::env::var("DEFENSIVE_DRAWDOWN_PCT")
                .unwrap_or_else(|_| "0.10".to_string())
                .parse()
                .expect("ENV ERROR: DEFENSIVE_DRAWDOWN_PCT"),
            max_sla_violations: std::env::var("MAX_SLA_VIOLATIONS")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .expect("ENV ERROR: MAX_SLA_VIOLATIONS"),
        }
    }
}
