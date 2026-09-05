//! KNIFE DOM v8.12.1 — Final production build with all fixes
//! - Dual‑mode: Base (1.5% risk, 12 trades, 0.35/0.65) and Boost (3% risk, 4 trades/day, 0.30/0.70)
//! - Boost triggers on: slope >0.01%, extreme imbalance, ≥2 wins in last 90 min
//! - Daily loss limit correctly $5.00 (10% of $50)
//! - Per-trade metadata propagated to logs (reason, imbalance, slope, ATR)
//! - True MFE/MAE tracked every 500ms and reported on exit
//! - Maker order support (compile flag), fixed fee accounting

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::LazyLock;
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
use futures_util::StreamExt;
use tracing::{error, info, warn};
use chrono::{Datelike, Timelike, Utc};

type HmacSha256 = Hmac<Sha256>;

// ============================================================
// CONSTANTS
// ============================================================
static TAKER_FEE: LazyLock<Decimal> = LazyLock::new(|| Decimal::new(5, 4));
static MAKER_FEE: LazyLock<Decimal> = LazyLock::new(|| Decimal::new(2, 4));

// Compile‑time flag: false for testnet (market orders), true for mainnet (limit orders)
const USE_LIMIT_ORDERS: bool = false;

static DEFAULT_TICK_SIZE: LazyLock<Decimal> = LazyLock::new(|| Decimal::new(1, 2));
static DEFAULT_STEP_SIZE: LazyLock<Decimal> = LazyLock::new(|| Decimal::new(1, 1));

const ZMQ_REP_BIND: &str = "tcp://127.0.0.1:5555";
const ZMQ_PUB_BIND: &str = "tcp://127.0.0.1:5556";
const BINANCE_FUTURES_REST: &str = "https://testnet.binancefuture.com";
const BINANCE_FUTURES_WS_STREAM: &str = "wss://stream.binancefuture.com/stream";
const BINANCE_FUTURES_WS_USER: &str = "wss://stream.binancefuture.com/ws";

const DAILY_LOSS_LIMIT_PCT: Decimal = Decimal::new(10, 2); // 10%
const CONSECUTIVE_LOSS_LIMIT: u32 = 4;
const PAUSE_DURATION: Duration = Duration::from_secs(3600); // 60 min
const BOOST_MAX_TRADES: u32 = 4;

// ============================================================
// TYPES
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
    pub boost: bool,
    pub reason: String,
    pub imbalance: Decimal,
    pub slope: Decimal,
    pub atr: Decimal,
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
    pub mode: String,
    pub signal_reason: String,
    pub imbalance: Decimal,
    pub slope: Decimal,
    pub atr: Decimal,
    pub mfe: Decimal,
    pub mae: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookSnapshot {
    pub symbol: String,
    pub bids: Vec<(Decimal, Decimal)>,
    pub asks: Vec<(Decimal, Decimal)>,
    pub timestamp: u64,
}

// ============================================================
// SECOND‑BAR AGGREGATION
// ============================================================
#[derive(Debug, Clone, Copy)]
struct SecBar {
    time: u64,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: Decimal,
}

impl SecBar {
    fn new(time: u64, price: Decimal, volume: Decimal) -> Self {
        Self { time, open: price, high: price, low: price, close: price, volume }
    }
    fn update(&mut self, price: Decimal, volume: Decimal) {
        self.high = self.high.max(price);
        self.low = self.low.min(price);
        self.close = price;
        self.volume += volume;
    }
}

// ============================================================
// DOM ANALYZER
// ============================================================
pub struct DomAnalyzer {
    symbol: String,
    book: Arc<Mutex<Option<OrderBookSnapshot>>>,
    price_history: Arc<Mutex<VecDeque<(SystemTime, Decimal)>>>,
    volume_history: Arc<Mutex<VecDeque<(SystemTime, Decimal)>>>,
    last_entry: Arc<Mutex<SystemTime>>,
    last_exit: Arc<Mutex<SystemTime>>,
    last_result: Arc<Mutex<bool>>,
    minute_candles: Arc<Mutex<VecDeque<(SystemTime, Decimal, Decimal, Decimal)>>>,
    sec_bars: Arc<Mutex<VecDeque<SecBar>>>,
    recent_wins: Arc<Mutex<u32>>,
    last_win_time: Arc<Mutex<SystemTime>>,
}

impl DomAnalyzer {
    pub fn new(symbol: String) -> Self {
        let now = SystemTime::now();
        Self {
            symbol,
            book: Arc::new(Mutex::new(None)),
            price_history: Arc::new(Mutex::new(VecDeque::with_capacity(2000))),
            volume_history: Arc::new(Mutex::new(VecDeque::with_capacity(2000))),
            last_entry: Arc::new(Mutex::new(now - Duration::from_secs(120))),
            last_exit: Arc::new(Mutex::new(now - Duration::from_secs(120))),
            last_result: Arc::new(Mutex::new(true)),
            minute_candles: Arc::new(Mutex::new(VecDeque::with_capacity(200))),
            sec_bars: Arc::new(Mutex::new(VecDeque::with_capacity(400))),
            recent_wins: Arc::new(Mutex::new(0)),
            last_win_time: Arc::new(Mutex::new(now)),
        }
    }

    pub fn update_book(&self, snapshot: OrderBookSnapshot) {
        let mut book = self.book.lock();
        *book = Some(snapshot);
    }

    pub fn update_price(&self, price: Decimal, volume: Decimal) {
        let now = SystemTime::now();
        let sec = now.duration_since(UNIX_EPOCH).unwrap().as_secs();

        {
            let mut bars = self.sec_bars.lock();
            if let Some(last) = bars.back_mut() {
                if last.time == sec {
                    last.update(price, volume);
                } else {
                    bars.push_back(SecBar::new(sec, price, volume));
                    if bars.len() > 400 { bars.pop_front(); }
                }
            } else {
                bars.push_back(SecBar::new(sec, price, volume));
            }
        }

        {
            let mut prices = self.price_history.lock();
            prices.push_back((now, price));
            if prices.len() > 2000 { prices.pop_front(); }
        }
        {
            let mut vols = self.volume_history.lock();
            vols.push_back((now, volume));
            if vols.len() > 2000 { vols.pop_front(); }
        }

        let current_minute = sec / 60;
        let mut candles = self.minute_candles.lock();
        if let Some(last) = candles.back_mut() {
            let last_minute = last.0.duration_since(UNIX_EPOCH).unwrap().as_secs() / 60;
            if last_minute == current_minute {
                if price > last.2 { last.2 = price; }
                if price < last.3 { last.3 = price; }
            } else {
                candles.push_back((now, price, price, price));
                if candles.len() > 200 { candles.pop_front(); }
            }
        } else {
            candles.push_back((now, price, price, price));
        }
    }

    fn atr(&self, period: usize) -> Decimal {
        let candles = self.minute_candles.lock();
        if candles.len() < period + 1 { return Decimal::ZERO; }
        let mut sum = Decimal::ZERO;
        let mut count = 0;
        let mut prev_close = candles[0].1;
        for i in 1..candles.len() {
            let (_, _, high, low) = candles[i];
            let tr = (high - low).max((high - prev_close).abs()).max((low - prev_close).abs());
            sum += tr;
            count += 1;
            prev_close = candles[i].1;
            if count >= period { break; }
        }
        if count == 0 { Decimal::ZERO } else { sum / Decimal::from(count) }
    }

    fn ema_5m_trend(&self) -> Option<(Decimal, Decimal, Decimal)> {
        let candles = self.minute_candles.lock();
        if candles.len() < 100 { return None; }
        let mut five_min_closes = Vec::new();
        let mut current_bucket = Vec::new();
        for c in candles.iter() {
            let minute_bucket = c.0.duration_since(UNIX_EPOCH).unwrap().as_secs() / 300;
            if current_bucket.is_empty() {
                current_bucket.push(*c);
            } else if minute_bucket == current_bucket[0].0.duration_since(UNIX_EPOCH).unwrap().as_secs() / 300 {
                current_bucket.push(*c);
            } else {
                let close = current_bucket.last().unwrap().1;
                five_min_closes.push(close);
                current_bucket.clear();
                current_bucket.push(*c);
            }
        }
        if five_min_closes.len() < 20 { return None; }
        let k = Decimal::from(2) / Decimal::from(21);
        let mut ema = five_min_closes[0];
        let mut prev_ema = ema;
        for i in 1..five_min_closes.len() {
            prev_ema = ema;
            ema = (five_min_closes[i] - ema) * k + ema;
        }
        let current_price = candles.last().unwrap().1;
        Some((current_price, ema, prev_ema))
    }

    fn ema_slope(&self) -> Option<Decimal> {
        let (_, current_ema, prev_ema) = self.ema_5m_trend()?;
        if prev_ema == Decimal::ZERO { return None; }
        Some((current_ema - prev_ema).abs() / current_ema)
    }

    fn is_good_session(&self) -> bool {
        let now = Utc::now();
        let hour = now.hour();
        (hour >= 13 && hour < 21) || (hour >= 0 && hour < 8)
    }

    fn weighted_imbalance(&self, book: &OrderBookSnapshot) -> Decimal {
        let mut bid_sum = Decimal::ZERO;
        let mut ask_sum = Decimal::ZERO;
        for (i, (_, qty)) in book.bids.iter().take(10).enumerate() {
            let weight = Decimal::from(1) / Decimal::from(i + 1);
            bid_sum += qty * weight;
        }
        for (i, (_, qty)) in book.asks.iter().take(10).enumerate() {
            let weight = Decimal::from(1) / Decimal::from(i + 1);
            ask_sum += qty * weight;
        }
        if ask_sum == Decimal::ZERO { return Decimal::ONE; }
        bid_sum / ask_sum
    }

    fn volume_spike(&self) -> bool {
        let bars = self.sec_bars.lock();
        if bars.len() < 32 { return false; }
        let last_complete = &bars[bars.len() - 2];
        let mut sum = Decimal::ZERO;
        let mut count = 0;
        for bar in bars.iter().rev().skip(2).take(30) {
            sum += bar.volume;
            count += 1;
        }
        if count == 0 { return false; }
        let avg = sum / Decimal::from(count);
        last_complete.volume > avg * Decimal::new(20, 1) // 2.0×
    }

    fn sweep(&self, current: Decimal) -> (bool, bool) {
        let bars = self.sec_bars.lock();
        if bars.len() < 300 { return (false, false); }
        let mut high = current;
        let mut low = current;
        for bar in bars.iter().rev().skip(1).take(300) {
            if bar.high > high { high = bar.high; }
            if bar.low < low { low = bar.low; }
        }
        let swept_high = current > high;
        let swept_low = current < low;
        (swept_high, swept_low)
    }

    fn win_freshness(&self) -> bool {
        let wins = *self.recent_wins.lock();
        let last_win = *self.last_win_time.lock();
        let now = SystemTime::now();
        let within_window = now.duration_since(last_win).unwrap_or(Duration::from_secs(0)) < Duration::from_secs(5400);
        wins >= 2 && within_window
    }

    fn boost_mode(&self, imbalance: Decimal, slope: Decimal) -> bool {
        let strong_trend = slope > Decimal::new(1, 4);
        let extreme_imbalance = imbalance > Decimal::new(35, 1) || imbalance < Decimal::new(25, 2);
        strong_trend && extreme_imbalance && self.win_freshness()
    }

    pub fn set_entry_time(&self, t: SystemTime) {
        *self.last_entry.lock() = t;
    }

    pub fn set_exit_info(&self, win: bool) {
        let now = SystemTime::now();
        *self.last_exit.lock() = now;
        *self.last_result.lock() = win;
        let mut wins = self.recent_wins.lock();
        if win {
            *wins += 1;
            *self.last_win_time.lock() = now;
        } else {
            *wins = 0;
        }
    }

    pub fn reset_daily_state(&self) {
        let mut wins = self.recent_wins.lock();
        *wins = 0;
        *self.last_win_time.lock() = SystemTime::now();
    }

    pub fn analyze(&self) -> Option<(String, Decimal, Decimal, Decimal, String, bool, Decimal, Decimal, Decimal)> {
        let now = SystemTime::now();

        let last_exit = *self.last_exit.lock();
        let last_entry = *self.last_entry.lock();
        let last_activity = if last_exit > last_entry { last_exit } else { last_entry };
        if now.duration_since(last_activity).unwrap_or(Duration::from_secs(0)) < Duration::from_secs(60) {
            return None;
        }

        if !self.is_good_session() { return None; }

        let book = self.book.lock();
        if book.is_none() { return None; }
        let book = book.as_ref().unwrap();

        let prices = self.price_history.lock();
        if prices.len() < 50 { return None; }

        let imbalance = self.weighted_imbalance(book);
        let vol_spike = self.volume_spike();
        let current = *prices.back().unwrap().1;
        let (swept_high, swept_low) = self.sweep(current);

        let atr = self.atr(14);
        if atr == Decimal::ZERO { return None; }

        let trend = self.ema_5m_trend();
        let (current_price, ema, _) = match trend {
            Some((p, e, _)) => (p, e),
            None => return None,
        };
        let is_uptrend = current_price > ema;
        let is_downtrend = current_price < ema;
        let slope = self.ema_slope().unwrap_or(Decimal::ZERO);
        let min_slope = Decimal::new(5, 5);
        let trend_valid = slope > min_slope;

        let boost = self.boost_mode(imbalance, slope);

        let (sl_mult, tp_mult, sl_floor_pct, tp_floor_pct) = if boost {
            (Decimal::new(40, 2), Decimal::new(90, 2), Decimal::new(30, 4), Decimal::new(70, 4))
        } else {
            (Decimal::new(50, 2), Decimal::new(80, 2), Decimal::new(35, 4), Decimal::new(65, 4))
        };

        let stop_distance = (sl_mult / Decimal::from(100) * atr)
            .max(current * sl_floor_pct)
            .min(current * Decimal::new(70, 4));
        let tp_distance = (tp_mult / Decimal::from(100) * atr)
            .max(current * tp_floor_pct)
            .min(current * Decimal::new(120, 4));

        let sl_buy = current - stop_distance;
        let tp_buy = current + tp_distance;
        let sl_sell = current + stop_distance;
        let tp_sell = current - tp_distance;

        let (buy_imbalance, sell_imbalance) = if boost {
            (Decimal::new(35, 1), Decimal::new(25, 2))
        } else {
            (Decimal::new(28, 1), Decimal::new(36, 2))
        };

        if imbalance > buy_imbalance && vol_spike && swept_low && is_uptrend && trend_valid {
            *self.last_entry.lock() = now;
            return Some(("Buy".to_string(), current, sl_buy, tp_buy, "DOM+Sweep+Trend".to_string(), boost, imbalance, slope, atr));
        }

        let five_min_ago = now - Duration::from_secs(300);
        let recent_prices: Vec<&Decimal> = prices
            .iter()
            .filter(|(t, _)| *t >= five_min_ago)
            .map(|(_, p)| p)
            .collect();
        if recent_prices.len() < 10 { return None; }
        let is_sharp_drop = current < **recent_prices.first().unwrap() * Decimal::new(995, 3);
        let is_stabilizing = recent_prices.len() > 5 &&
            recent_prices[recent_prices.len() - 4] > recent_prices[recent_prices.len() - 3] &&
            current > *recent_prices[recent_prices.len() - 2];
        if is_sharp_drop && is_stabilizing && imbalance > buy_imbalance && is_uptrend && trend_valid {
            *self.last_entry.lock() = now;
            return Some(("Buy".to_string(), current, sl_buy, tp_buy, "KnifeCatch".to_string(), boost, imbalance, slope, atr));
        }

        if imbalance < sell_imbalance && vol_spike && swept_high && is_downtrend && trend_valid {
            *self.last_entry.lock() = now;
            return Some(("Sell".to_string(), current, sl_sell, tp_sell, "DOM+Sweep+Trend".to_string(), boost, imbalance, slope, atr));
        }

        let is_sharp_rise = current > **recent_prices.first().unwrap() * Decimal::new(1005, 3);
        if is_sharp_rise && is_stabilizing && imbalance < sell_imbalance && is_downtrend && trend_valid {
            *self.last_entry.lock() = now;
            return Some(("Sell".to_string(), current, sl_sell, tp_sell, "KnifeCatchShort".to_string(), boost, imbalance, slope, atr));
        }

        None
    }
}

// ============================================================
// BINANCE CLIENT
// ============================================================
#[derive(Debug, Clone, Copy)]
pub struct ExchangeInfo {
    pub tick_size: Decimal,
    pub step_size: Decimal,
}

impl ExchangeInfo {
    pub fn for_symbol(_symbol: &str) -> Self {
        Self {
            tick_size: *DEFAULT_TICK_SIZE,
            step_size: *DEFAULT_STEP_SIZE,
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
        let default_info = ExchangeInfo {
            tick_size: *DEFAULT_TICK_SIZE,
            step_size: *DEFAULT_STEP_SIZE,
        };
        let info = self.exchange_info.get(symbol).unwrap_or(&default_info);
        let tick = info.tick_size;
        (price / tick).round() * tick
    }

    fn round_size(&self, size: Decimal, symbol: &str) -> Decimal {
        let default_info = ExchangeInfo {
            tick_size: *DEFAULT_TICK_SIZE,
            step_size: *DEFAULT_STEP_SIZE,
        };
        let info = self.exchange_info.get(symbol).unwrap_or(&default_info);
        let step = info.step_size;
        (size / step).floor() * step
    }

    // Entry order: market or limit depending on USE_LIMIT_ORDERS
    pub async fn place_entry(&self, symbol: &str, side: &str, price: Decimal, size: Decimal) -> Result<String> {
        if USE_LIMIT_ORDERS {
            let ts = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
            let side_str = if side == "Buy" { "BUY" } else { "SELL" };
            let price_r = self.round_price(price, symbol);
            let size_r = self.round_size(size, symbol);

            let query = format!(
                "symbol={}&side={}&type=LIMIT&price={}&quantity={}&timestamp={}&timeInForce=GTX",
                symbol, side_str, price_r, size_r, ts
            );
            let sig = self.sign(&query);
            let url = format!("{}/fapi/v1/order?{}&signature={}", BINANCE_FUTURES_REST, query, sig);

            let resp = self.client.post(&url).header("X-MBX-APIKEY", &self.api_key).send().await?;
            let json: serde_json::Value = resp.json().await?;

            if let Some(order_id) = json.get("orderId").and_then(|v| v.as_i64()) {
                return Ok(order_id.to_string());
            }
            anyhow::bail!("Limit entry failed: {}", json)
        } else {
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
            anyhow::bail!("Market entry failed: {}", json)
        }
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
        mode: String,
        signal_reason: String,
        imbalance: Decimal,
        slope: Decimal,
        atr: Decimal,
    },
    Closing {
        side: String,
        size: Decimal,
        entry_price: Decimal,
        order_id: String,
        sl: Decimal,
        tp: Decimal,
        mode: String,
        signal_reason: String,
        imbalance: Decimal,
        slope: Decimal,
        atr: Decimal,
    },
}

// ============================================================
// ASSET ENGINE
// ============================================================
pub struct AssetEngine {
    symbol: String,
    client: Arc<BinanceClient>,
    pub_tx: tokio::sync::mpsc::UnboundedSender<String>,
    state: Arc<Mutex<AssetState>>,
    analyzer: Arc<DomAnalyzer>,
    daily_pnl: Arc<Mutex<Decimal>>,
    daily_loss_limit_usd: Arc<Mutex<Decimal>>,
    daily_trades: Arc<Mutex<u32>>,
    boost_trades_today: Arc<Mutex<u32>>,
    last_reset_day: Arc<Mutex<u32>>,
    capital: Arc<Mutex<Decimal>>,
    last_price: Arc<Mutex<Decimal>>,
    consecutive_losses: Arc<Mutex<u32>>,
    pause_until: Arc<Mutex<SystemTime>>,
    initial_capital: Decimal,
    mfe: Arc<Mutex<Decimal>>,
    mae: Arc<Mutex<Decimal>>,
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
            analyzer: Arc::new(DomAnalyzer::new(symbol)),
            daily_pnl: Arc::new(Mutex::new(Decimal::ZERO)),
            daily_loss_limit_usd,
            daily_trades: Arc::new(Mutex::new(0)),
            boost_trades_today: Arc::new(Mutex::new(0)),
            last_reset_day: Arc::new(Mutex::new(chrono::Utc::now().naive_utc().ordinal())),
            capital: Arc::new(Mutex::new(initial_capital)),
            last_price: Arc::new(Mutex::new(Decimal::ZERO)),
            consecutive_losses: Arc::new(Mutex::new(0)),
            pause_until: Arc::new(Mutex::new(SystemTime::now())),
            initial_capital,
            mfe: Arc::new(Mutex::new(Decimal::ZERO)),
            mae: Arc::new(Mutex::new(Decimal::ZERO)),
        }
    }

    pub fn get_analyzer(&self) -> Arc<DomAnalyzer> {
        self.analyzer.clone()
    }

    pub fn update_last_price(&self, price: Decimal) {
        let mut last = self.last_price.lock();
        *last = price;
    }

    async fn monitor_sl_tp(&self) {
        let (side, size, entry_price, sl, tp, order_id, mode, signal_reason, imbalance, slope, atr) = {
            let state = self.state.lock();
            match &*state {
                AssetState::InPosition { side, size, entry_price, order_id, sl, tp, mode, signal_reason, imbalance, slope, atr } => {
                    (side.clone(), *size, *entry_price, *sl, *tp, order_id.clone(), mode.clone(), signal_reason.clone(), *imbalance, *slope, *atr)
                }
                _ => return,
            }
        };

        let current_price = *self.last_price.lock();
        if current_price == Decimal::ZERO { return; }

        let fav = if side == "Buy" { current_price - entry_price } else { entry_price - current_price };
        let adv = if side == "Buy" { entry_price - current_price } else { current_price - entry_price };
        {
            let mut mfe = self.mfe.lock();
            if fav > *mfe { *mfe = fav; }
            let mut mae = self.mae.lock();
            if adv > *mae { *mae = adv; }
        }

        let should_close = if side == "Buy" {
            current_price <= sl || current_price >= tp
        } else {
            current_price >= sl || current_price <= tp
        };

        if should_close {
            info!("🎯 SL/TP triggered at price {}", current_price);
            if let Ok(close_id) = self.client.close_position(&self.symbol, size, &side).await {
                info!("📉 Close order placed: {}", close_id);
                {
                    let mut state = self.state.lock();
                    if let AssetState::InPosition { side, size, entry_price, order_id, sl, tp, mode, signal_reason, imbalance, slope, atr } = &*state {
                        *state = AssetState::Closing {
                            side: side.clone(),
                            size: *size,
                            entry_price: *entry_price,
                            order_id: order_id.clone(),
                            sl: *sl,
                            tp: *tp,
                            mode: mode.clone(),
                            signal_reason: signal_reason.clone(),
                            imbalance: *imbalance,
                            slope: *slope,
                            atr: *atr,
                        };
                    }
                }
            } else {
                error!("Failed to close position on SL/TP trigger");
            }
        }
    }

    pub async fn process_signal(&self, signal: TradeSignal) -> EngineResponse {
        let today = chrono::Utc::now().naive_utc().ordinal();
        {
            let mut reset_day = self.last_reset_day.lock();
            if *reset_day != today {
                *reset_day = today;
                let mut trades = self.daily_trades.lock();
                *trades = 0;
                let mut boost_trades = self.boost_trades_today.lock();
                *boost_trades = 0;
                self.analyzer.reset_daily_state();
                let mut pnl = self.daily_pnl.lock();
                *pnl = Decimal::ZERO;
                let mut mfe = self.mfe.lock();
                *mfe = Decimal::ZERO;
                let mut mae = self.mae.lock();
                *mae = Decimal::ZERO;
            }
        }

        let boost = signal.boost;
        let max_trades = if boost { 20 } else { 12 };
        {
            let trades = self.daily_trades.lock();
            if *trades >= max_trades {
                return EngineResponse {
                    status: "REJECTED".to_string(),
                    message: format!("Max {} trades per day reached", max_trades),
                    order_id: None,
                };
            }
        }

        if boost {
            let boost_trades = self.boost_trades_today.lock();
            if *boost_trades >= BOOST_MAX_TRADES {
                return EngineResponse {
                    status: "REJECTED".to_string(),
                    message: format!("Max boost trades per day ({}) reached", BOOST_MAX_TRADES),
                    order_id: None,
                };
            }
        }

        {
            let pause_until = *self.pause_until.lock();
            if SystemTime::now() < pause_until {
                return EngineResponse {
                    status: "REJECTED".to_string(),
                    message: "Paused after consecutive losses".to_string(),
                    order_id: None,
                };
            }
        }

        let daily_loss_limit = *self.daily_loss_limit_usd.lock();
        {
            let daily = *self.daily_pnl.lock();
            if daily < Decimal::ZERO && daily.abs() > daily_loss_limit {
                return EngineResponse {
                    status: "REJECTED".to_string(),
                    message: format!("Daily loss limit ${:.2} reached", daily_loss_limit),
                    order_id: None,
                };
            }
        }

        if signal.action == "Close" {
            let (size, side, entry_price, order_id, sl, tp, mode, signal_reason, imbalance, slope, atr) = {
                let state = self.state.lock();
                match &*state {
                    AssetState::InPosition { size, side, entry_price, order_id, sl, tp, mode, signal_reason, imbalance, slope, atr } => {
                        (*size, side.clone(), *entry_price, order_id.clone(), *sl, *tp, mode.clone(), signal_reason.clone(), *imbalance, *slope, *atr)
                    }
                    AssetState::Closing { size, side, entry_price, order_id, sl, tp, mode, signal_reason, imbalance, slope, atr } => {
                        (*size, side.clone(), *entry_price, order_id.clone(), *sl, *tp, mode.clone(), signal_reason.clone(), *imbalance, *slope, *atr)
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
                    mode: mode.clone(),
                    signal_reason: signal_reason.clone(),
                    imbalance,
                    slope,
                    atr,
                };
            }

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
                            mode,
                            signal_reason,
                            imbalance,
                            slope,
                            atr,
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

            let (risk_frac, cap_frac) = if boost {
                (Decimal::new(3, 2),  Decimal::new(8, 1))
            } else {
                (Decimal::new(15, 3), Decimal::new(5, 1))
            };
            let live_cap = *self.capital.lock();
            let risk_amount = live_cap * risk_frac;
            let sl_distance = if signal.action == "Buy" { signal.price - signal.sl } else { signal.sl - signal.price };
            if sl_distance <= Decimal::ZERO {
                return EngineResponse {
                    status: "ERROR".to_string(),
                    message: "Invalid SL distance".to_string(),
                    order_id: None,
                };
            }

            let raw_notional = risk_amount / sl_distance * signal.price;
            let max_notional = live_cap * Decimal::from(signal.leverage) * cap_frac;
            let capped_notional = if raw_notional > max_notional { max_notional } else { raw_notional };
            let size = self.client.round_size(capped_notional / signal.price, &self.symbol);
            if size <= Decimal::ZERO {
                return EngineResponse {
                    status: "REJECTED".to_string(),
                    message: "Calculated size too small".to_string(),
                    order_id: None,
                };
            }

            let entry_side = signal.action.clone();
            match self.client.place_entry(&self.symbol, &entry_side, signal.price, size).await {
                Ok(entry_id) => {
                    let now = SystemTime::now();
                    self.analyzer.set_entry_time(now);

                    {
                        let mut trades = self.daily_trades.lock();
                        *trades += 1;
                        if boost {
                            let mut boost_trades = self.boost_trades_today.lock();
                            *boost_trades += 1;
                        }
                    }

                    {
                        let mut mfe = self.mfe.lock();
                        *mfe = Decimal::ZERO;
                        let mut mae = self.mae.lock();
                        *mae = Decimal::ZERO;
                    }

                    info!("📈 Pending entry: {} @ {} (size: {}) | Boost: {}", self.symbol, signal.price, size, boost);

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

    pub fn on_entry_fill(&self, order_id: &str, avg_price: Decimal, filled_size: Decimal) {
        let (signal, side, requested_size) = {
            let state = self.state.lock();
            if let AssetState::PendingEntry { signal, side, size, .. } = &*state {
                (signal.clone(), side.clone(), *size)
            } else {
                warn!("Entry fill received while not pending: state {:?}", *state);
                return;
            }
        };

        if filled_size > Decimal::ZERO && filled_size <= requested_size {
            let actual_size = filled_size;
            let mode = if signal.boost { "boost".to_string() } else { "base".to_string() };
            {
                let mut state = self.state.lock();
                *state = AssetState::InPosition {
                    side: side.clone(),
                    size: actual_size,
                    entry_price: avg_price,
                    order_id: order_id.to_string(),
                    sl: signal.sl,
                    tp: signal.tp,
                    mode: mode.clone(),
                    signal_reason: signal.reason.clone(),
                    imbalance: signal.imbalance,
                    slope: signal.slope,
                    atr: signal.atr,
                };
            }
            info!("✅ Entry confirmed: {} @ {} (size: {}) | Mode: {}", self.symbol, avg_price, actual_size, mode);

            if actual_size < requested_size {
                warn!("⚠️ PARTIAL FILL: requested {}, got {}", requested_size, actual_size);
            }

            let report = FillReport {
                symbol: self.symbol.clone(),
                side: side.clone(),
                filled_size: actual_size,
                avg_price,
                is_exit: false,
                order_id: order_id.to_string(),
                pnl: Decimal::ZERO,
                mode: mode.clone(),
                signal_reason: signal.reason.clone(),
                imbalance: signal.imbalance,
                slope: signal.slope,
                atr: signal.atr,
                mfe: Decimal::ZERO,
                mae: Decimal::ZERO,
            };
            let json = serde_json::to_string(&report).unwrap();
            let _ = self.pub_tx.send(json);

            let self_clone = Arc::new(self.clone());
            tokio::spawn(async move {
                let mut interval = time::interval(Duration::from_millis(500));
                loop {
                    interval.tick().await;
                    let in_position = {
                        let state = self_clone.state.lock();
                        matches!(*state, AssetState::InPosition { .. })
                    };
                    if !in_position { break; }
                    self_clone.monitor_sl_tp().await;
                }
            });
        } else {
            warn!("Entry fill size mismatch: expected ~{}, got {}", requested_size, filled_size);
        }
    }

    pub fn on_exit_fill(&self, order_id: &str, avg_price: Decimal, filled_size: Decimal) {
        let mut state = self.state.lock();
        let (side, size, entry_price, sl, tp, mode, signal_reason, imbalance, slope, atr) = match &*state {
            AssetState::InPosition { side, size, entry_price, order_id, sl, tp, mode, signal_reason, imbalance, slope, atr } => {
                (side.clone(), *size, *entry_price, *sl, *tp, mode.clone(), signal_reason.clone(), *imbalance, *slope, *atr)
            }
            AssetState::Closing { side, size, entry_price, order_id, sl, tp, mode, signal_reason, imbalance, slope, atr } => {
                (side.clone(), *size, *entry_price, *sl, *tp, mode.clone(), signal_reason.clone(), *imbalance, *slope, *atr)
            }
            _ => {
                warn!("Exit fill received while not in position or closing: {:?}", *state);
                return;
            }
        };

        if filled_size <= size && filled_size > Decimal::ZERO {
            if filled_size < size {
                warn!("⚠️ PARTIAL EXIT: size {}, filled {}", size, filled_size);
            }

            let entry_fee_rate = if USE_LIMIT_ORDERS { *MAKER_FEE } else { *TAKER_FEE };
            let exit_fee_rate = *TAKER_FEE;
            let gross_pnl = if side == "Buy" {
                (avg_price - entry_price) * filled_size
            } else {
                (entry_price - avg_price) * filled_size
            };
            let entry_fee = entry_price * filled_size * entry_fee_rate;
            let exit_fee = avg_price * filled_size * exit_fee_rate;
            let pnl = gross_pnl - entry_fee - exit_fee;

            let win = pnl > Decimal::ZERO;
            self.analyzer.set_exit_info(win);

            {
                let mut losses = self.consecutive_losses.lock();
                if win {
                    *losses = 0;
                } else {
                    *losses += 1;
                    if *losses >= CONSECUTIVE_LOSS_LIMIT {
                        warn!("🚨 {} consecutive losses reached – pausing for 60 min", CONSECUTIVE_LOSS_LIMIT);
                        *self.pause_until.lock() = SystemTime::now() + PAUSE_DURATION;
                    }
                }
            }

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

            let mfe = *self.mfe.lock();
            let mae = *self.mae.lock();

            let report = FillReport {
                symbol: self.symbol.clone(),
                side: side.clone(),
                filled_size,
                avg_price,
                is_exit: true,
                order_id: order_id.to_string(),
                pnl,
                mode: mode.clone(),
                signal_reason: signal_reason.clone(),
                imbalance,
                slope,
                atr,
                mfe,
                mae,
            };
            let json = serde_json::to_string(&report).unwrap();
            let _ = self.pub_tx.send(json);

            info!("📤 Exit: {} PnL=${:.2} (gross=${:.2}, fees=${:.2}) | Mode: {} | MFE: ${:.2}, MAE: ${:.2}",
                  self.symbol, pnl, gross_pnl, entry_fee + exit_fee, mode, mfe, mae);

            *state = AssetState::Idle;
            info!("🔄 State reset to Idle");
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

impl Clone for AssetEngine {
    fn clone(&self) -> Self {
        Self {
            symbol: self.symbol.clone(),
            client: self.client.clone(),
            pub_tx: self.pub_tx.clone(),
            state: self.state.clone(),
            analyzer: self.analyzer.clone(),
            daily_pnl: self.daily_pnl.clone(),
            daily_loss_limit_usd: self.daily_loss_limit_usd.clone(),
            daily_trades: self.daily_trades.clone(),
            boost_trades_today: self.boost_trades_today.clone(),
            last_reset_day: self.last_reset_day.clone(),
            capital: self.capital.clone(),
            last_price: self.last_price.clone(),
            consecutive_losses: self.consecutive_losses.clone(),
            pause_until: self.pause_until.clone(),
            initial_capital: self.initial_capital,
            mfe: self.mfe.clone(),
            mae: self.mae.clone(),
        }
    }
}

// ============================================================
// ZMQ, WEBSOCKETS, MAIN
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
            match serde_json::from_str::<TradeSignal>(&payload) {
                Ok(signal) => {
                    if let Some(asset) = assets.get(&signal.symbol) {
                        let resp = handle.block_on(asset.process_signal(signal));
                        let json_resp = serde_json::to_string(&resp).unwrap_or_default();
                        if let Err(e) = socket.send(json_resp.as_bytes(), 0) {
                            error!("ZMQ REP send error: {}", e);
                        }
                    } else {
                        let resp = EngineResponse {
                            status: "ERROR".to_string(),
                            message: "Unknown symbol".to_string(),
                            order_id: None,
                        };
                        let json_resp = serde_json::to_string(&resp).unwrap_or_default();
                        if let Err(e) = socket.send(json_resp.as_bytes(), 0) {
                            error!("ZMQ REP send error: {}", e);
                        }
                    }
                }
                Err(e) => {
                    let resp = EngineResponse {
                        status: "ERROR".to_string(),
                        message: format!("Malformed JSON: {}", e),
                        order_id: None,
                    };
                    let json_resp = serde_json::to_string(&resp).unwrap_or_default();
                    if let Err(e) = socket.send(json_resp.as_bytes(), 0) {
                        error!("ZMQ REP send error: {}", e);
                    }
                }
            }
        }
    });
    Ok(())
}

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
    let (_write, mut read) = ws_stream.split();

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
                                asset.update_last_price(p);
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

async fn run_user_websocket(client: Arc<BinanceClient>, assets: Arc<DashMap<String, Arc<AssetEngine>>>) -> Result<()> {
    let mut backoff = 1;
    loop {
        match client.get_listen_key().await {
            Ok(key) => {
                let client_clone = client.clone();
                let key_clone = key.clone();
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

async fn run_signal_loop(assets: Arc<DashMap<String, Arc<AssetEngine>>>) {
    let mut interval = time::interval(Duration::from_millis(500));
    loop {
        interval.tick().await;
        for entry in assets.iter() {
            let asset = entry.value();
            let analyzer = asset.get_analyzer();
            if let Some((action, price, sl, tp, reason, boost, imbalance, slope, atr)) = analyzer.analyze() {
                if !asset.is_in_position() && !asset.is_pending() {
                    let signal = TradeSignal {
                        action: action.clone(),
                        symbol: "SOLUSDT".to_string(),
                        price,
                        size: Decimal::ZERO,
                        sl,
                        tp,
                        leverage: 5,
                        boost,
                        reason,
                        imbalance,
                        slope,
                        atr,
                    };
                    let _ = asset.process_signal(signal).await;
                    info!("🎯 DOM Signal: {} | Reason: {} | Boost: {} | Price: {}", action, reason, boost, price);
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_max_level(tracing::Level::INFO).init();
    dotenv().ok();
    info!("🗡️ KNIFE DOM v8.12.1 — Final Production Build (Testnet)");

    let initial_capital = Decimal::new(50, 0);
    let daily_loss_pct = std::env::var("DAILY_LOSS_LIMIT")
        .unwrap_or_else(|_| "0.10".to_string())
        .parse::<f64>()
        .unwrap_or(0.10);
    let daily_loss_usd = initial_capital * Decimal::new((daily_loss_pct * 100.0) as i64, 2);
    let daily_loss_limit = Arc::new(Mutex::new(daily_loss_usd));
    info!("💲 Initial capital: ${:.2}, daily loss limit: ${:.2}", initial_capital, daily_loss_usd);

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

    let handle = tokio::runtime::Handle::current();
    start_zmq_rep_server(assets.clone(), handle)?;

    let ws_assets = assets.clone();
    tokio::spawn(async move {
        if let Err(e) = run_market_websocket(ws_assets).await {
            error!("Market WebSocket fatal error: {}", e);
        }
    });

    let signal_assets = assets.clone();
    tokio::spawn(run_signal_loop(signal_assets));

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
