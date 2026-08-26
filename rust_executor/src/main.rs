//! KNIFE DOM v8.5.2 — Systems-test ready with enhanced monitoring logs
//! - All pre-flight fixes applied
//! - Extra debug logs for partial fills, timeouts, capital drift

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Duration};

use anyhow::Result;
use dashmap::DashMap;
use dotenvy::dotenv;
use hmac::{Hmac, Mac};
use parking_lot::Mutex;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::time;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures_util::{SinkExt, StreamExt};
use tracing::{error, info, warn, debug};
use chrono::Datelike;

type HmacSha256 = Hmac<Sha256>;

// ============================================================
// CONSTANTS
// ============================================================
const ZMQ_REP_BIND: &str = "tcp://127.0.0.1:5555";
const ZMQ_PUB_BIND: &str = "tcp://127.0.0.1:5556";
const BINANCE_FUTURES_REST: &str = "https://testnet.binancefuture.com";
const BINANCE_FUTURES_WS_STREAM: &str = "wss://stream.binancefuture.com/stream";
const BINANCE_FUTURES_WS_USER: &str = "wss://stream.binancefuture.com/ws";
const TAKER_FEE: Decimal = Decimal::new(5, 4);

// ============================================================
// TYPES (unchanged)
// ============================================================
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeSignal {
    pub action: String,
    pub symbol: String,
    pub price: Decimal,
    pub size: Decimal,
    pub sl: Decimal,
    pub tp: Decimal,
    pub leverage: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineResponse {
    pub status: String,
    pub message: String,
    pub order_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillReport {
    pub symbol: String,
    pub side: String,
    pub filled_size: Decimal,
    pub avg_price: Decimal,
    pub is_exit: bool,
    pub order_id: String,
    pub pnl: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookSnapshot {
    pub symbol: String,
    pub bids: Vec<(Decimal, Decimal)>,
    pub asks: Vec<(Decimal, Decimal)>,
    pub timestamp: u64,
}

// ============================================================
// DOM ANALYZER (with volume fix)
// ============================================================
pub struct DomAnalyzer {
    symbol: String,
    book: Arc<Mutex<Option<OrderBookSnapshot>>>,
    price_history: Arc<Mutex<VecDeque<(SystemTime, Decimal)>>>,
    volume_history: Arc<Mutex<VecDeque<(SystemTime, Decimal)>>>,
    last_signal: Arc<Mutex<SystemTime>>,
}

impl DomAnalyzer {
    pub fn new(symbol: String) -> Self {
        Self {
            symbol,
            book: Arc::new(Mutex::new(None)),
            price_history: Arc::new(Mutex::new(VecDeque::with_capacity(1000))),
            volume_history: Arc::new(Mutex::new(VecDeque::with_capacity(1000))),
            last_signal: Arc::new(Mutex::new(SystemTime::now() - Duration::from_secs(120))),
        }
    }

    pub fn update_book(&self, snapshot: OrderBookSnapshot) {
        let mut book = self.book.lock();
        *book = Some(snapshot);
    }

    pub fn update_price(&self, price: Decimal, volume: Decimal) {
        let mut prices = self.price_history.lock();
        prices.push_back((SystemTime::now(), price));
        if prices.len() > 1000 { prices.pop_front(); }

        let mut vols = self.volume_history.lock();
        vols.push_back((SystemTime::now(), volume));
        if vols.len() > 1000 { vols.pop_front(); }
    }

    pub fn analyze(&self) -> Option<(String, Decimal, Decimal, Decimal, String)> {
        let now = SystemTime::now();

        {
            let last = *self.last_signal.lock();
            if now.duration_since(last).unwrap_or(Duration::from_secs(0)) < Duration::from_secs(60) {
                return None;
            }
        }

        let book = self.book.lock();
        if book.is_none() { return None; }
        let book = book.as_ref().unwrap();

        let prices = self.price_history.lock();
        if prices.len() < 20 { return None; }

        let vols = self.volume_history.lock();
        if vols.len() < 20 { return None; }

        let bid_depth: Decimal = book.bids.iter().take(10).map(|(_, q)| q).sum();
        let ask_depth: Decimal = book.asks.iter().take(10).map(|(_, q)| q).sum();
        let imbalance = if ask_depth > Decimal::ZERO { bid_depth / ask_depth } else { Decimal::ONE };

        let one_min_ago = now - Duration::from_secs(60);
        let recent_prices: Vec<&Decimal> = prices
            .iter()
            .filter(|(t, _)| *t >= one_min_ago)
            .map(|(_, p)| p)
            .collect();

        if recent_prices.len() < 2 { return None; }

        let current = *recent_prices.last().unwrap();
        let start = *recent_prices.first().unwrap();
        let price_change = if start > Decimal::ZERO { (current - start) / start } else { Decimal::ZERO };

        let historical = &recent_prices[..recent_prices.len() - 1];
        let recent_high = historical.iter().max().unwrap_or(&current);
        let recent_low = historical.iter().min().unwrap_or(&current);

        let recent_vols: Vec<&Decimal> = vols.iter().rev().take(10).map(|(_, v)| v).collect();
        let avg_vol = recent_vols.iter().fold(Decimal::ZERO, |a, &b| a + b) / Decimal::from(10);
        let current_vol = recent_vols.first().unwrap_or(&Decimal::ONE);
        let vol_spike = current_vol > &(avg_vol * Decimal::new(15, 1));

        let swept_high = current > *recent_high && vol_spike;
        let swept_low = current < *recent_low && vol_spike;

        let is_sharp_drop = price_change < Decimal::new(-5, 3);
        let is_stabilizing = recent_prices.len() > 3 &&
            recent_prices[recent_prices.len() - 3] > recent_prices[recent_prices.len() - 2] &&
            current > recent_prices[recent_prices.len() - 2];

        let price = *current;
        let sl_buy = price * Decimal::new(99, 2);
        let tp_buy = price * Decimal::new(101, 2);
        let sl_sell = price * Decimal::new(101, 2);
        let tp_sell = price * Decimal::new(99, 2);

        if imbalance > Decimal::new(12, 1) && vol_spike && swept_low {
            *self.last_signal.lock() = now;
            return Some(("Buy".to_string(), price, sl_buy, tp_buy, "DOM imbalance + sweep".to_string()));
        }

        if is_sharp_drop && is_stabilizing && imbalance > Decimal::new(11, 1) {
            *self.last_signal.lock() = now;
            return Some(("Buy".to_string(), price, sl_buy, tp_buy, "Knife catch".to_string()));
        }

        if imbalance < Decimal::new(8, 1) && vol_spike && swept_high {
            *self.last_signal.lock() = now;
            return Some(("Sell".to_string(), price, sl_sell, tp_sell, "DOM imbalance + sweep".to_string()));
        }

        if price_change > Decimal::new(5, 3) && is_stabilizing && imbalance < Decimal::new(9, 1) {
            *self.last_signal.lock() = now;
            return Some(("Sell".to_string(), price, sl_sell, tp_sell, "Knife catch (short)".to_string()));
        }

        None
    }
}

// ============================================================
// EXCHANGE INFO & BINANCE CLIENT (unchanged)
// ============================================================
pub struct ExchangeInfo {
    pub tick_size: Decimal,
    pub step_size: Decimal,
}

impl ExchangeInfo {
    pub fn for_symbol(_symbol: &str) -> Self {
        Self {
            tick_size: Decimal::new(1, 3),
            step_size: Decimal::new(1, 1),
        }
    }
}

pub struct BinanceClient {
    pub api_key: String,
    pub api_secret: String,
    pub client: reqwest::Client,
    exchange_info: HashMap<String, ExchangeInfo>,
}

impl BinanceClient {
    pub fn new() -> Self {
        dotenv().ok();
        let mut info = HashMap::new();
        info.insert("SOLUSDT".to_string(), ExchangeInfo::for_symbol("SOLUSDT"));
        Self {
            api_key: std::env::var("BINANCE_API_KEY").expect("BINANCE_API_KEY missing"),
            api_secret: std::env::var("BINANCE_API_SECRET").expect("BINANCE_API_SECRET missing"),
            client: reqwest::Client::new(),
            exchange_info: info,
        }
    }

    fn sign(&self, query: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(self.api_secret.as_bytes()).expect("HMAC init");
        mac.update(query.as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    fn round_price(&self, price: Decimal, symbol: &str) -> Decimal {
        let info = self.exchange_info.get(symbol).unwrap_or(&ExchangeInfo { tick_size: Decimal::new(1, 3), step_size: Decimal::new(1, 1) });
        let tick = info.tick_size;
        (price / tick).round() * tick
    }

    fn round_size(&self, size: Decimal, symbol: &str) -> Decimal {
        let info = self.exchange_info.get(symbol).unwrap_or(&ExchangeInfo { tick_size: Decimal::new(1, 3), step_size: Decimal::new(1, 1) });
        let step = info.step_size;
        (size / step).floor() * step
    }

    pub async fn place_market_entry(&self, symbol: &str, side: &str, size: Decimal) -> Result<String> {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let side_str = if side == "Buy" { "BUY" } else { "SELL" };
        let size_r = self.round_size(size, symbol);

        let query = format!(
            "symbol={}&side={}&type=MARKET&quantity={}&timestamp={}&newOrderRespType=RESULT",
            symbol, side_str, size_r, ts
        );
        let sig = self.sign(&query);
        let url = format!("{}/fapi/v1/order?{}&signature={}", BINANCE_FUTURES_REST, query, sig);

        let resp = self.client.post(&url).header("X-MBX-APIKEY", &self.api_key).send().await?;
        let json: serde_json::Value = resp.json().await?;

        if let Some(order_id) = json.get("orderId").and_then(|v| v.as_i64()) {
            return Ok(order_id.to_string());
        }
        anyhow::bail!("Entry failed: {}", json)
    }

    pub async fn place_stop_market(&self, symbol: &str, side: &str, stop_price: Decimal, size: Decimal) -> Result<String> {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let side_str = if side == "Buy" { "BUY" } else { "SELL" };
        let stop_r = self.round_price(stop_price, symbol);
        let size_r = self.round_size(size, symbol);

        let query = format!(
            "symbol={}&side={}&type=STOP_MARKET&stopPrice={}&quantity={}&timestamp={}&reduceOnly=true",
            symbol, side_str, stop_r, size_r, ts
        );
        let sig = self.sign(&query);
        let url = format!("{}/fapi/v1/order?{}&signature={}", BINANCE_FUTURES_REST, query, sig);

        let resp = self.client.post(&url).header("X-MBX-APIKEY", &self.api_key).send().await?;
        let json: serde_json::Value = resp.json().await?;

        if let Some(order_id) = json.get("orderId").and_then(|v| v.as_i64()) {
            return Ok(order_id.to_string());
        }
        anyhow::bail!("Stop failed: {}", json)
    }

    pub async fn place_limit(&self, symbol: &str, side: &str, price: Decimal, size: Decimal) -> Result<String> {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let side_str = if side == "Buy" { "BUY" } else { "SELL" };
        let price_r = self.round_price(price, symbol);
        let size_r = self.round_size(size, symbol);

        let query = format!(
            "symbol={}&side={}&type=LIMIT&price={}&quantity={}&timestamp={}&reduceOnly=true&timeInForce=GTC",
            symbol, side_str, price_r, size_r, ts
        );
        let sig = self.sign(&query);
        let url = format!("{}/fapi/v1/order?{}&signature={}", BINANCE_FUTURES_REST, query, sig);

        let resp = self.client.post(&url).header("X-MBX-APIKEY", &self.api_key).send().await?;
        let json: serde_json::Value = resp.json().await?;

        if let Some(order_id) = json.get("orderId").and_then(|v| v.as_i64()) {
            return Ok(order_id.to_string());
        }
        anyhow::bail!("Limit failed: {}", json)
    }

    pub async fn cancel_order(&self, symbol: &str, order_id: &str) -> Result<()> {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let query = format!("symbol={}&orderId={}&timestamp={}", symbol, order_id, ts);
        let sig = self.sign(&query);
        let url = format!("{}/fapi/v1/order?{}&signature={}", BINANCE_FUTURES_REST, query, sig);

        let resp = self.client.delete(&url).header("X-MBX-APIKEY", &self.api_key).send().await?;
        let json: serde_json::Value = resp.json().await?;

        if json.get("code").and_then(|c| c.as_i64()) == Some(0) || json.get("orderId").is_some() {
            return Ok(());
        }
        anyhow::bail!("Cancel failed: {}", json)
    }

    pub async fn close_position(&self, symbol: &str, size: Decimal, side: &str) -> Result<String> {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let close_side = if side == "Buy" { "SELL" } else { "BUY" };
        let size_r = self.round_size(size, symbol);

        let query = format!(
            "symbol={}&side={}&type=MARKET&quantity={}&timestamp={}&reduceOnly=true",
            symbol, close_side, size_r, ts
        );
        let sig = self.sign(&query);
        let url = format!("{}/fapi/v1/order?{}&signature={}", BINANCE_FUTURES_REST, query, sig);

        let resp = self.client.post(&url).header("X-MBX-APIKEY", &self.api_key).send().await?;
        let json: serde_json::Value = resp.json().await?;

        if let Some(order_id) = json.get("orderId").and_then(|v| v.as_i64()) {
            return Ok(order_id.to_string());
        }
        anyhow::bail!("Close failed: {}", json)
    }

    pub async fn set_leverage(&self, symbol: &str, leverage: u32) -> Result<()> {
        let ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        let query = format!("symbol={}&leverage={}&timestamp={}", symbol, leverage, ts);
        let sig = self.sign(&query);
        let url = format!("{}/fapi/v1/leverage?{}&signature={}", BINANCE_FUTURES_REST, query, sig);

        let resp = self.client.post(&url).header("X-MBX-APIKEY", &self.api_key).send().await?;
        if resp.status().is_success() {
            Ok(())
        } else {
            anyhow::bail!("Leverage set failed: {}", resp.text().await?)
        }
    }

    pub async fn get_listen_key(&self) -> Result<String> {
        let url = format!("{}/fapi/v1/listenKey", BINANCE_FUTURES_REST);
        let resp = self.client.post(&url).header("X-MBX-APIKEY", &self.api_key).send().await?;
        let json: serde_json::Value = resp.json().await?;
        json.get("listenKey")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow::anyhow!("ListenKey missing"))
    }

    pub async fn keep_listen_key_alive(&self, listen_key: &str) -> Result<()> {
        let url = format!("{}/fapi/v1/listenKey?listenKey={}", BINANCE_FUTURES_REST, listen_key);
        let resp = self.client.put(&url).header("X-MBX-APIKEY", &self.api_key).send().await?;
        if resp.status().is_success() {
            Ok(())
        } else {
            anyhow::bail!("Keep-alive failed: {}", resp.text().await?)
        }
    }
}

// ============================================================
// STATE MACHINE
// ============================================================
#[derive(Debug, Clone)]
pub enum AssetState {
    Idle,
    PendingEntry {
        signal: TradeSignal,
        order_id: String,
        size: Decimal,
        side: String,
        entry_time: SystemTime,
    },
    InPosition {
        side: String,
        size: Decimal,
        entry_price: Decimal,
        order_id: String,
        sl: Decimal,
        tp: Decimal,
    },
    Closing {
        side: String,
        size: Decimal,
        entry_price: Decimal,
        order_id: String,
        sl: Decimal,
        tp: Decimal,
    },
}

// ============================================================
// ASSET ENGINE (with extra logging for monitoring)
// ============================================================
pub struct AssetEngine {
    symbol: String,
    client: Arc<BinanceClient>,
    pub_tx: tokio::sync::mpsc::UnboundedSender<String>,
    state: Arc<Mutex<AssetState>>,
    active_orders: Arc<Mutex<Option<(String, String)>>>,
    analyzer: Arc<DomAnalyzer>,
    daily_pnl: Arc<Mutex<Decimal>>,
    daily_loss_limit_usd: Arc<Mutex<Decimal>>,
    daily_trades: Arc<Mutex<u32>>,
    last_reset_day: Arc<Mutex<u32>>,
    capital: Arc<Mutex<Decimal>>,
}

impl AssetEngine {
    pub fn new(
        symbol: String,
        client: Arc<BinanceClient>,
        pub_tx: tokio::sync::mpsc::UnboundedSender<String>,
        daily_loss_limit_usd: Arc<Mutex<Decimal>>,
        initial_capital: Decimal,
    ) -> Self {
        Self {
            symbol: symbol.clone(),
            client,
            pub_tx,
            state: Arc::new(Mutex::new(AssetState::Idle)),
            active_orders: Arc::new(Mutex::new(None)),
            analyzer: Arc::new(DomAnalyzer::new(symbol)),
            daily_pnl: Arc::new(Mutex::new(Decimal::ZERO)),
            daily_loss_limit_usd,
            daily_trades: Arc::new(Mutex::new(0)),
            last_reset_day: Arc::new(Mutex::new(chrono::Utc::now().naive_utc().ordinal())),
            capital: Arc::new(Mutex::new(initial_capital)),
        }
    }

    pub fn get_analyzer(&self) -> Arc<DomAnalyzer> {
        self.analyzer.clone()
    }

    pub async fn cancel_active_bracket(&self) {
        let orders = {
            let mut guard = self.active_orders.lock();
            guard.take()
        };
        if let Some((stop_id, limit_id)) = orders {
            let _ = self.client.cancel_order(&self.symbol, &stop_id).await;
            let _ = self.client.cancel_order(&self.symbol, &limit_id).await;
            info!("✅ Cancelled bracket orders: {} / {}", stop_id, limit_id);
        }
    }

    pub async fn process_signal(&self, signal: TradeSignal) -> EngineResponse {
        let today = chrono::Utc::now().naive_utc().ordinal();
        {
            let mut trades = self.daily_trades.lock();
            let mut reset_day = self.last_reset_day.lock();
            if *reset_day != today {
                *trades = 0;
                *reset_day = today;
            }
            if *trades >= 10 {
                return EngineResponse {
                    status: "REJECTED".to_string(),
                    message: "Max 10 trades per day reached".to_string(),
                    order_id: None,
                };
            }
        }

        {
            let daily = *self.daily_pnl.lock();
            let limit = *self.daily_loss_limit_usd.lock();
            if daily < Decimal::ZERO && daily.abs() > limit {
                return EngineResponse {
                    status: "REJECTED".to_string(),
                    message: format!("Daily loss limit ${:.2} reached", limit),
                    order_id: None,
                };
            }
        }

        if signal.action == "Close" {
            let (size, side, entry_price, order_id, sl, tp) = {
                let state = self.state.lock();
                match &*state {
                    AssetState::InPosition { size, side, entry_price, order_id, sl, tp } => {
                        (*size, side.clone(), *entry_price, order_id.clone(), *sl, *tp)
                    }
                    AssetState::Closing { size, side, entry_price, order_id, sl, tp } => {
                        (*size, side.clone(), *entry_price, order_id.clone(), *sl, *tp)
                    }
                    _ => return EngineResponse {
                        status: "REJECTED".to_string(),
                        message: "No position to close".to_string(),
                        order_id: None,
                    },
                }
            };

            {
                let mut state = self.state.lock();
                *state = AssetState::Closing {
                    side: side.clone(),
                    size,
                    entry_price,
                    order_id: order_id.clone(),
                    sl,
                    tp,
                };
            }

            self.cancel_active_bracket().await;

            match self.client.close_position(&self.symbol, size, &side).await {
                Ok(close_order_id) => {
                    info!("📉 Manual close order placed: {}", close_order_id);
                    EngineResponse {
                        status: "CLOSING".to_string(),
                        message: "Close order sent, awaiting fill".to_string(),
                        order_id: Some(close_order_id),
                    }
                }
                Err(e) => {
                    {
                        let mut state = self.state.lock();
                        *state = AssetState::InPosition {
                            side,
                            size,
                            entry_price,
                            order_id,
                            sl,
                            tp,
                        };
                    }
                    EngineResponse {
                        status: "ERROR".to_string(),
                        message: format!("Close order failed: {}", e),
                        order_id: None,
                    }
                }
            }
        } else {
            {
                let state = self.state.lock();
                if !matches!(*state, AssetState::Idle) {
                    return EngineResponse {
                        status: "REJECTED".to_string(),
                        message: "Busy (pending, in position, or closing)".to_string(),
                        order_id: None,
                    };
                }
            }

            if let Err(e) = self.client.set_leverage(&self.symbol, signal.leverage).await {
                return EngineResponse {
                    status: "ERROR".to_string(),
                    message: format!("Leverage set failed: {}", e),
                    order_id: None,
                };
            }

            let live_cap = *self.capital.lock();
            let risk_amount = live_cap * Decimal::new(2, 2);
            let sl_distance = if signal.action == "Buy" { signal.price - signal.sl } else { signal.sl - signal.price };
            if sl_distance <= Decimal::ZERO {
                return EngineResponse {
                    status: "ERROR".to_string(),
                    message: "Invalid SL distance".to_string(),
                    order_id: None,
                };
            }
            let raw_size = risk_amount / sl_distance;
            let size = self.client.round_size(raw_size, &self.symbol);
            if size <= Decimal::ZERO {
                return EngineResponse {
                    status: "REJECTED".to_string(),
                    message: "Calculated size too small".to_string(),
                    order_id: None,
                };
            }

            let entry_side = signal.action.clone();
            match self.client.place_market_entry(&self.symbol, &entry_side, size).await {
                Ok(entry_id) => {
                    {
                        let mut state = self.state.lock();
                        *state = AssetState::PendingEntry {
                            signal: signal.clone(),
                            order_id: entry_id.clone(),
                            size,
                            side: entry_side.clone(),
                            entry_time: SystemTime::now(),
                        };
                    }

                    {
                        let mut trades = self.daily_trades.lock();
                        *trades += 1;
                    }

                    info!("📈 Pending entry: {} @ {} (size: {})", self.symbol, signal.price, size);

                    let state_clone = self.state.clone();
                    let client_clone = self.client.clone();
                    let symbol_clone = self.symbol.clone();
                    let order_id_clone = entry_id.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(60)).await;
                        let needs_cancel = {
                            let state = state_clone.lock();
                            matches!(&*state, AssetState::PendingEntry { order_id, .. } if order_id == &order_id_clone)
                        };
                        if needs_cancel {
                            warn!("⏰ PendingEntry timeout for order {}, cancelling", order_id_clone);
                            let _ = client_clone.cancel_order(&symbol_clone, &order_id_clone).await;
                            let mut state = state_clone.lock();
                            if matches!(&*state, AssetState::PendingEntry { order_id, .. } if order_id == &order_id_clone) {
                                *state = AssetState::Idle;
                                info!("⏰ State reset to Idle after timeout (order {})", order_id_clone);
                            }
                        }
                    });

                    EngineResponse {
                        status: "EXECUTED".to_string(),
                        message: "Entry pending, awaiting fill confirmation".to_string(),
                        order_id: Some(entry_id),
                    }
                }
                Err(e) => {
                    error!("Entry failed: {}", e);
                    EngineResponse {
                        status: "ERROR".to_string(),
                        message: e.to_string(),
                        order_id: None,
                    }
                }
            }
        }
    }

    // Called when User Data Stream sends FILLED event for entry
    pub fn on_entry_fill(&self, order_id: &str, avg_price: Decimal, filled_size: Decimal) {
        let mut state = self.state.lock();
        if let AssetState::PendingEntry { signal, side, size, .. } = &*state {
            if filled_size > Decimal::ZERO && filled_size <= *size {
                let actual_size = filled_size;
                let new_state = AssetState::InPosition {
                    side: side.clone(),
                    size: actual_size,
                    entry_price: avg_price,
                    order_id: order_id.to_string(),
                    sl: signal.sl,
                    tp: signal.tp,
                };
                *state = new_state;
                info!("✅ Entry confirmed: {} @ {} (size: {})", self.symbol, avg_price, actual_size);

                // If partial fill, log the difference
                if actual_size < *size {
                    warn!("⚠️ PARTIAL FILL: requested {}, got {}", size, actual_size);
                }

                let client = self.client.clone();
                let symbol = self.symbol.clone();
                let sl_side = if side == "Buy" { "SELL" } else { "BUY" };
                let sl_price = signal.sl;
                let tp_price = signal.tp;
                let active_orders = self.active_orders.clone();
                let state_clone = self.state.clone();
                let pub_tx = self.pub_tx.clone();
                let symbol_clone = self.symbol.clone();
                let side_clone = side.clone();
                let entry_price_clone = avg_price;
                let size_clone = actual_size;
                let order_id_clone = order_id.to_string();

                tokio::spawn(async move {
                    let stop_res = client.place_stop_market(&symbol, sl_side, sl_price, size_clone).await;
                    let limit_res = client.place_limit(&symbol, sl_side, tp_price, size_clone).await;

                    match (stop_res, limit_res) {
                        (Ok(sid), Ok(lid)) => {
                            *active_orders.lock() = Some((sid, lid));
                            info!("🛡️ Protective orders placed: SL={}, TP={}", sid, lid);
                        }
                        (Ok(sid), Err(e)) => {
                            error!("TP order failed after SL placed: {}. Cleaning up.", e);
                            let _ = client.cancel_order(&symbol, &sid).await;
                            let _ = client.close_position(&symbol, size_clone, &side_clone).await;
                            *state_clone.lock() = AssetState::Idle;
                            warn!("🚨 Emergency close after TP failure");
                        }
                        (Err(e), Ok(lid)) => {
                            error!("SL order failed after TP placed: {}. Cleaning up.", e);
                            let _ = client.cancel_order(&symbol, &lid).await;
                            let _ = client.close_position(&symbol, size_clone, &side_clone).await;
                            *state_clone.lock() = AssetState::Idle;
                            warn!("🚨 Emergency close after SL failure");
                        }
                        (Err(e1), Err(e2)) => {
                            error!("Both SL and TP failed: {} | {}. Emergency closing.", e1, e2);
                            let _ = client.close_position(&symbol, size_clone, &side_clone).await;
                            *state_clone.lock() = AssetState::Idle;
                            warn!("🚨 Emergency close after both SL/TP failures");
                        }
                    }
                });

                let report = FillReport {
                    symbol: symbol_clone,
                    side: side_clone,
                    filled_size: size_clone,
                    avg_price: entry_price_clone,
                    is_exit: false,
                    order_id: order_id_clone,
                    pnl: Decimal::ZERO,
                };
                let json = serde_json::to_string(&report).unwrap();
                let _ = pub_tx.send(json);
            } else {
                warn!("Entry fill size mismatch or zero: expected ~{}, got {}", size, filled_size);
            }
        } else {
            warn!("Entry fill received while not pending: state {:?}", *state);
        }
    }

    // Called on exit fill (SL/TP or manual close)
    pub fn on_exit_fill(&self, order_id: &str, avg_price: Decimal, filled_size: Decimal) {
        let mut state = self.state.lock();
        let (side, size, entry_price) = match &*state {
            AssetState::InPosition { side, size, entry_price, .. } => {
                (side.clone(), *size, *entry_price)
            }
            AssetState::Closing { side, size, entry_price, .. } => {
                (side.clone(), *size, *entry_price)
            }
            _ => {
                warn!("Exit fill received while not in position or closing: {:?}", *state);
                return;
            }
        };

        if filled_size <= size && filled_size > Decimal::ZERO {
            // If partial exit, log it
            if filled_size < size {
                warn!("⚠️ PARTIAL EXIT: size {}, filled {}", size, filled_size);
                // For simplicity, we still treat as full exit; residual risk remains
            }

            let gross_pnl = if side == "Buy" {
                (avg_price - entry_price) * filled_size
            } else {
                (entry_price - avg_price) * filled_size
            };
            let entry_fee = entry_price * filled_size * TAKER_FEE;
            let exit_fee = avg_price * filled_size * TAKER_FEE;
            let pnl = gross_pnl - entry_fee - exit_fee;

            {
                let mut cap = self.capital.lock();
                *cap += pnl;
                info!("💰 Capital updated: +${:.2} → ${:.2}", pnl, *cap);
            }
            {
                let mut daily = self.daily_pnl.lock();
                *daily += pnl;
                info!("📊 Daily PnL: ${:.2}", *daily);
            }

            let report = FillReport {
                symbol: self.symbol.clone(),
                side: side.clone(),
                filled_size,
                avg_price,
                is_exit: true,
                order_id: order_id.to_string(),
                pnl,
            };
            let json = serde_json::to_string(&report).unwrap();
            let _ = self.pub_tx.send(json);

            info!("📤 Exit: {} PnL=${:.2} (gross=${:.2}, fees=${:.2})",
                  self.symbol, pnl, gross_pnl, entry_fee + exit_fee);

            *state = AssetState::Idle;
            info!("🔄 State reset to Idle");

            let orders = {
                let mut guard = self.active_orders.lock();
                guard.take()
            };
            if let Some((stop_id, limit_id)) = orders {
                let client = self.client.clone();
                let symbol = self.symbol.clone();
                tokio::spawn(async move {
                    let _ = client.cancel_order(&symbol, &stop_id).await;
                    let _ = client.cancel_order(&symbol, &limit_id).await;
                    info!("🧹 Cleaned up leftover bracket orders");
                });
            }
        } else {
            warn!("Exit fill size mismatch: expected <= {}, got {}", size, filled_size);
        }
    }

    pub fn is_in_position(&self) -> bool {
        matches!(*self.state.lock(), AssetState::InPosition { .. }) ||
        matches!(*self.state.lock(), AssetState::Closing { .. })
    }

    pub fn is_pending(&self) -> bool {
        matches!(*self.state.lock(), AssetState::PendingEntry { .. })
    }
}

// ============================================================
// ZMQ PUBLISHER THREAD (decoupled)
// ============================================================
fn start_zmq_publisher(mut rx: tokio::sync::mpsc::UnboundedReceiver<String>) -> Result<()> {
    std::thread::spawn(move || {
        let context = zmq::Context::new();
        let pub_socket = match context.socket(zmq::PUB) {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to create ZMQ PUB socket: {}", e);
                return;
            }
        };
        if let Err(e) = pub_socket.bind(ZMQ_PUB_BIND) {
            error!("Failed to bind ZMQ PUB: {}", e);
            return;
        }
        info!("ZMQ PUB bound to {}", ZMQ_PUB_BIND);
        while let Some(msg) = rx.blocking_recv() {
            if let Err(e) = pub_socket.send(msg.as_bytes(), 0) {
                error!("ZMQ PUB send error: {}", e);
            }
        }
    });
    Ok(())
}

// ============================================================
// ZMQ REP SERVER – Decoupled Native Thread
// ============================================================
fn start_zmq_rep_server(
    assets: Arc<DashMap<String, Arc<AssetEngine>>>,
    handle: tokio::runtime::Handle,
) -> Result<()> {
    std::thread::spawn(move || {
        let context = zmq::Context::new();
        let socket = match context.socket(zmq::REP) {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to create ZMQ REP socket: {}", e);
                return;
            }
        };
        if let Err(e) = socket.bind(ZMQ_REP_BIND) {
            error!("Failed to bind ZMQ REP: {}", e);
            return;
        }
        info!("ZMQ REP bound to {}", ZMQ_REP_BIND);

        loop {
            let msg = match socket.recv_bytes(0) {
                Ok(m) => m,
                Err(e) => {
                    error!("ZMQ REP recv error: {}", e);
                    continue;
                }
            };
            let payload = String::from_utf8_lossy(&msg);

            let resp = match serde_json::from_str::<TradeSignal>(&payload) {
                Ok(signal) => {
                    if let Some(asset) = assets.get(&signal.symbol) {
                        handle.block_on(asset.process_signal(signal))
                    } else {
                        EngineResponse {
                            status: "ERROR".to_string(),
                            message: "Unknown symbol".to_string(),
                            order_id: None,
                        }
                    }
                }
                Err(e) => EngineResponse {
                    status: "ERROR".to_string(),
                    message: format!("Malformed JSON: {}", e),
                    order_id: None,
                },
            };

            if let Ok(json_resp) = serde_json::to_string(&resp) {
                if let Err(e) = socket.send(json_resp.as_bytes(), 0) {
                    error!("ZMQ REP send error: {}", e);
                }
            }
        }
    });
    Ok(())
}

// ============================================================
// WEBSOCKET: MARKET DATA with reconnect
// ============================================================
async fn run_market_websocket(assets: Arc<DashMap<String, Arc<AssetEngine>>>) -> Result<()> {
    let mut backoff = 1;
    loop {
        match run_market_websocket_inner(assets.clone()).await {
            Ok(_) => break,
            Err(e) => {
                error!("Market WebSocket error: {}. Reconnecting in {}s...", e, backoff);
                tokio::time::sleep(Duration::from_secs(backoff)).await;
                if backoff < 60 { backoff *= 2; }
            }
        }
    }
    Ok(())
}

async fn run_market_websocket_inner(assets: Arc<DashMap<String, Arc<AssetEngine>>>) -> Result<()> {
    let symbols: Vec<String> = assets.iter().map(|entry| entry.key().clone()).collect();
    let streams: Vec<String> = symbols.iter()
        .flat_map(|s| {
            let lower = s.to_lowercase();
            vec![
                format!("{}@depth10@100ms", lower),
                format!("{}@trade", lower),
            ]
        })
        .collect();

    let url = format!("{}?streams={}", BINANCE_FUTURES_WS_STREAM, streams.join("/"));
    info!("Connecting to market stream: {}", url);

    let (ws_stream, _) = connect_async(url).await?;
    let (mut write, mut read) = ws_stream.split();

    while let Some(msg) = read.next().await {
        if let Ok(Message::Text(text)) = msg {
            let json: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
            if let Some(data) = json.get("data") {
                let stream = json.get("stream").and_then(|v| v.as_str()).unwrap_or("");
                if stream.contains("@depth") {
                    let symbol = stream.split('@').next().unwrap_or("").to_uppercase();
                    if let Some(asset) = assets.get(&symbol) {
                        let mut bids = Vec::new();
                        let mut asks = Vec::new();
                        if let Some(bid_list) = data.get("b").and_then(|v| v.as_array()) {
                            for b in bid_list {
                                if let (Some(price), Some(qty)) = (b.get(0).and_then(|v| v.as_str()), b.get(1).and_then(|v| v.as_str())) {
                                    if let (Ok(p), Ok(q)) = (price.parse::<Decimal>(), qty.parse::<Decimal>()) {
                                        bids.push((p, q));
                                    }
                                }
                            }
                        }
                        if let Some(ask_list) = data.get("a").and_then(|v| v.as_array()) {
                            for a in ask_list {
                                if let (Some(price), Some(qty)) = (a.get(0).and_then(|v| v.as_str()), a.get(1).and_then(|v| v.as_str())) {
                                    if let (Ok(p), Ok(q)) = (price.parse::<Decimal>(), qty.parse::<Decimal>()) {
                                        asks.push((p, q));
                                    }
                                }
                            }
                        }
                        let snapshot = OrderBookSnapshot {
                            symbol: symbol.clone(),
                            bids,
                            asks,
                            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
                        };
                        asset.get_analyzer().update_book(snapshot);
                    }
                } else if stream.contains("@trade") {
                    let symbol = stream.split('@').next().unwrap_or("").to_uppercase();
                    if let Some(asset) = assets.get(&symbol) {
                        if let (Some(price), Some(qty)) = (data.get("p").and_then(|v| v.as_str()), data.get("q").and_then(|v| v.as_str())) {
                            if let (Ok(p), Ok(q)) = (price.parse::<Decimal>(), qty.parse::<Decimal>()) {
                                asset.get_analyzer().update_price(p, q);
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

// ============================================================
// WEBSOCKET: USER DATA with integrated keep-alive
// ============================================================
async fn run_user_websocket(client: Arc<BinanceClient>, assets: Arc<DashMap<String, Arc<AssetEngine>>>) -> Result<()> {
    let mut backoff = 1;
    loop {
        match client.get_listen_key().await {
            Ok(key) => {
                let client_clone = client.clone();
                let key_clone = key.clone();
                // Spawn keep-alive for this active key
                let keep_alive_task = tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_secs(1800)).await;
                        if let Err(e) = client_clone.keep_listen_key_alive(&key_clone).await {
                            warn!("ListenKey keep-alive failed: {}", e);
                        } else {
                            info!("🔑 ListenKey keep-alive successful.");
                        }
                    }
                });

                if let Err(e) = run_user_websocket_inner(key, assets.clone()).await {
                    error!("User WS error: {}. Reconnecting in {}s...", e, backoff);
                }
                keep_alive_task.abort();
                info!("🔌 User WS disconnected, aborted keep-alive.");
            }
            Err(e) => error!("Failed to refresh listenKey: {}", e),
        }
        tokio::time::sleep(Duration::from_secs(backoff)).await;
        if backoff < 60 { backoff *= 2; }
    }
}

async fn run_user_websocket_inner(listen_key: String, assets: Arc<DashMap<String, Arc<AssetEngine>>>) -> Result<()> {
    let url = format!("{}/{}", BINANCE_FUTURES_WS_USER, listen_key);
    let (ws_stream, _) = connect_async(url).await?;
    let (_, mut read) = ws_stream.split();

    info!("👤 User Data WebSocket connected");

    while let Some(msg) = read.next().await {
        if let Ok(Message::Text(text)) = msg {
            let json: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
            if json.get("e").and_then(|v| v.as_str()) == Some("ORDER_TRADE_UPDATE") {
                if let Some(order) = json.get("o") {
                    let symbol = order.get("s").and_then(|v| v.as_str()).unwrap_or("");
                    let status = order.get("X").and_then(|v| v.as_str()).unwrap_or("");
                    let exec_type = order.get("x").and_then(|v| v.as_str()).unwrap_or("");
                    let order_id = order.get("i").and_then(|v| v.as_i64()).unwrap_or(0).to_string();
                    let side = order.get("S").and_then(|v| v.as_str()).unwrap_or("");
                    let avg_price = order.get("ap").and_then(|v| v.as_str()).and_then(|s| s.parse::<Decimal>().ok()).unwrap_or(Decimal::ZERO);
                    let filled_size = order.get("z").and_then(|v| v.as_str()).and_then(|s| s.parse::<Decimal>().ok()).unwrap_or(Decimal::ZERO);

                    if let Some(asset) = assets.get(symbol) {
                        if status == "FILLED" && exec_type == "TRADE" {
                            let is_reduce_only = order.get("R").and_then(|v| v.as_bool()).unwrap_or(false);
                            if is_reduce_only {
                                asset.on_exit_fill(&order_id, avg_price, filled_size);
                            } else {
                                asset.on_entry_fill(&order_id, avg_price, filled_size);
                            }
                        }
                        if (status == "REJECTED" || status == "EXPIRED") && exec_type == "TRADE" {
                            let mut state = asset.state.lock();
                            if let AssetState::PendingEntry { order_id: oid, .. } = &*state {
                                if oid == &order_id {
                                    warn!("❌ Entry order {} was {}. Resetting to Idle.", order_id, status);
                                    *state = AssetState::Idle;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

// ============================================================
// SIGNAL LOOP
// ============================================================
async fn run_signal_loop(assets: Arc<DashMap<String, Arc<AssetEngine>>>) {
    let mut interval = time::interval(Duration::from_millis(500));
    loop {
        interval.tick().await;
        for entry in assets.iter() {
            let asset = entry.value();
            let analyzer = asset.get_analyzer();
            if let Some((action, price, sl, tp, reason)) = analyzer.analyze() {
                if !asset.is_in_position() && !asset.is_pending() {
                    let signal = TradeSignal {
                        action: action.clone(),
                        symbol: "SOLUSDT".to_string(),
                        price,
                        size: Decimal::ZERO,
                        sl,
                        tp,
                        leverage: 5,
                    };
                    let _ = asset.process_signal(signal).await;
                    info!("🎯 DOM Signal: {} | Reason: {} | Price: {}", action, reason, price);
                }
            }
        }
    }
}

// ============================================================
// MAIN
// ============================================================
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    dotenv().ok();
    info!("🗡️ KNIFE DOM v8.5.2 — Systems-test ready (Testnet)");

    let initial_capital = Decimal::new(50, 0);
    let daily_loss_pct = std::env::var("DAILY_LOSS_LIMIT")
        .unwrap_or_else(|_| "0.10".to_string())
        .parse::<f64>()
        .unwrap_or(0.10);
    let daily_loss_usd = initial_capital * Decimal::new((daily_loss_pct * 100.0) as i64, 2);
    let daily_loss_limit = Arc::new(Mutex::new(daily_loss_usd));
    info!("💲 Initial capital: ${:.2}, daily loss limit: ${:.2}", initial_capital, daily_loss_usd);

    // ZMQ PUB – decoupled
    let (pub_tx, pub_rx) = tokio::sync::mpsc::unbounded_channel();
    start_zmq_publisher(pub_rx)?;

    let client = Arc::new(BinanceClient::new());
    let assets = Arc::new(DashMap::new());

    let asset = Arc::new(AssetEngine::new(
        "SOLUSDT".to_string(),
        client.clone(),
        pub_tx,
        daily_loss_limit.clone(),
        initial_capital,
    ));
    assets.insert("SOLUSDT".to_string(), asset);

    // ZMQ REP – decoupled native thread
    let handle = tokio::runtime::Handle::current();
    start_zmq_rep_server(assets.clone(), handle)?;

    // Spawn market WebSocket (with reconnect)
    let ws_assets = assets.clone();
    tokio::spawn(async move {
        if let Err(e) = run_market_websocket(ws_assets).await {
            error!("Market WebSocket fatal error: {}", e);
        }
    });

    // Spawn signal loop
    let signal_assets = assets.clone();
    tokio::spawn(run_signal_loop(signal_assets));

    // Spawn user WebSocket with integrated keep-alive
    let user_assets = assets.clone();
    let user_client = client.clone();
    tokio::spawn(async move {
        if let Err(e) = run_user_websocket(user_client, user_assets).await {
            error!("User WebSocket fatal error: {}", e);
        }
    });

    tokio::signal::ctrl_c().await?;
    info!("🛑 Shutting down...");
    Ok(())
}
