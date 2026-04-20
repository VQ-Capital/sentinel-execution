# ⚡ sentinel-execution (The Executor)

**Sorumluluk:** `NATS`'taki `signal.trade.*` kanalını dinler. Gelen sinyali alır, cüzdanın risk limitlerini (Drawdown, Exposure) kontrol eder ve saniyeler içinde borsanın API'sine FIX veya WebSocket protokolüyle emir (Buy/Sell) gönderir.
**Kural:** Hiçbir analiz veya tahmin yapmaz. Sadece emre itaat eder ve riski yönetir.
**Dil:** Rust.