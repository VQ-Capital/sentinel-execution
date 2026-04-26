# ⚡ sentinel-risk-executor (Legacy: sentinel-execution)

**Domain:** Order Routing, Drawdown Protection & SLA Watchdog
**Rol:** Sistemin Elleri ve Kalkanı

Bu servis hiçbir şekilde piyasa analizi veya fiyat tahmini yapmaz. Sadece Quant motorundan gelen sinyalleri körü körüne yerine getirir. ANCAK, işlem açmadan önce "Risk Motoru" kurallarını uygular. Kasa (Equity) %15 erirse veya borsa API'si art arda 50ms üzerinde gecikme verirse "Kill Switch / Self-Healing" moduna geçerek sistemi korur.

- **NATS Girdisi:** `signal.trade.*`
- **NATS Çıktısı:** `execution.report.*`
- **SLA Hedefi:** < 50ms