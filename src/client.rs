use crate::concurrency::seqlock::LatestCache;
use crate::order_book::{OrderBookSet, SnapshotFetcher};
use crate::primitives::{
    AccountBalanceModel, AccountInfoV2Model, AggTradeModel, BinanceInterval, BookTickerModel,
    ChangeLeverageResponse, ChangeMarginTypeResponse, ClientOrderId, DepthLevel,
    ExchangeInfoModel, Kline, KlineUpdateModel, LeverageBracketModel, MarginType, Market,
    ModifyOrderRequest, NewOrderRequest, OrderBookModel, OrderBookSnapshotModel, OrderModel,
    PartialDepthModel, Symbol, TickerPriceModel,
};
use crate::rate_limiter::{BinanceLimits, BinanceRateLimiter};
use crate::resiliency::{MakeBeforeBreakRotator, NetworkGate, StreamWatchdog};
use crate::transport::rest_api::{RestApiTransport, RestError};
use crate::transport::ws_api::{WsApiError, WsApiTransport};
use crate::transport::ws_streams::{
    MarketStateHandler, StreamHandler, StreamKind, WsStreamRegistry, WsStreamShard,
};
use crate::user_data::ListenKeyManager;
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, OnceCell};

#[derive(Debug, Clone)]
pub struct BinanceClientOptions {
    pub rest_base_url: String,
    pub ws_streams_url: String,
    pub ws_api_url: String,
    pub api_key: String,
    pub api_secret: String,
    pub market: Market,
    pub limits: BinanceLimits,
    pub recv_window_ms: i64,
    pub rest_pool_size: usize,
    pub ws_api_lanes: usize,
    pub max_subscriptions: usize,
    pub depth_snapshot_limit: i32,
}

impl Default for BinanceClientOptions {
    fn default() -> Self {
        Self {
            rest_base_url: "http://127.0.0.1:8080".to_string(),
            ws_streams_url: "ws://127.0.0.1:8080/market/stream".to_string(),
            ws_api_url: "ws://127.0.0.1:8080/ws-fapi/v1".to_string(),
            api_key: String::new(),
            api_secret: String::new(),
            market: Market::FuturesUsdM,
            limits: BinanceLimits::default(),
            recv_window_ms: 5_000,
            rest_pool_size: 8,
            ws_api_lanes: 2,
            max_subscriptions: 256,
            depth_snapshot_limit: 1000,
        }
    }
}

pub struct BinanceClient {
    options: BinanceClientOptions,
    pub rate_limiter: Arc<BinanceRateLimiter>,
    pub rest: Arc<RestApiTransport>,
    ws_api: OnceCell<Arc<WsApiTransport>>,
    pub registry: Arc<WsStreamRegistry>,
    pub network_gate: Arc<NetworkGate>,
    pub reconnect_notify: Arc<Notify>,
    pub active_shard: Arc<Mutex<Option<WsStreamShard>>>,
    pub book_tickers: Arc<LatestCache<BookTickerModel>>,
    pub agg_trades: Arc<LatestCache<AggTradeModel>>,
    pub partial_depths: Arc<LatestCache<PartialDepthModel>>,
    pub klines: Arc<LatestCache<KlineUpdateModel>>,
    pub order_books: Arc<OrderBookSet>,
}

struct RestSnapshotFetcher(Arc<RestApiTransport>);

impl SnapshotFetcher for RestSnapshotFetcher {
    async fn fetch_snapshot<'a>(
        &'a mut self,
        symbol: Symbol,
        limit: usize,
        bids: &'a mut [DepthLevel],
        asks: &'a mut [DepthLevel],
    ) -> Option<(i64, usize, usize)> {
        let snap = self
            .0
            .order_book_into(symbol, limit as i32, bids, asks)
            .await
            .ok()?;
        Some((snap.last_update_id, snap.bid_count, snap.ask_count))
    }
}

impl BinanceClient {
    pub fn new(options: BinanceClientOptions) -> Result<Self, String> {
        let rate_limiter = Arc::new(BinanceRateLimiter::new(&options.limits));
        let rest = Arc::new(
            RestApiTransport::new(
                &options.rest_base_url,
                options.api_key.clone(),
                options.api_secret.clone(),
                options.rest_pool_size,
                Arc::clone(&rate_limiter),
                options.recv_window_ms,
            )
            .map_err(|e| e.to_string())?,
        );
        let cap = options.max_subscriptions.max(16);
        let order_books = OrderBookSet::new(
            cap,
            options.market,
            options.depth_snapshot_limit.max(5) as usize,
        );

        Ok(Self {
            options,
            rate_limiter,
            rest,
            ws_api: OnceCell::new(),
            registry: Arc::new(WsStreamRegistry::new()),
            network_gate: NetworkGate::new(),
            reconnect_notify: Arc::new(Notify::new()),
            active_shard: Arc::new(Mutex::new(None)),
            book_tickers: Arc::new(LatestCache::new(cap)),
            agg_trades: Arc::new(LatestCache::new(cap)),
            partial_depths: Arc::new(LatestCache::new(cap)),
            klines: Arc::new(LatestCache::new(cap)),
            order_books,
        })
    }

    pub async fn warmup_rest(&self, count: usize) -> Result<(), RestError> {
        self.rest.warmup(count).await
    }

    pub async fn connect_ws_api(&self) -> Result<Arc<WsApiTransport>, WsApiError> {
        let transport = self
            .ws_api
            .get_or_try_init(|| async {
                WsApiTransport::connect(
                    &self.options.ws_api_url,
                    self.options.ws_api_lanes,
                    self.options.api_key.clone(),
                    self.options.api_secret.clone(),
                    Arc::clone(&self.rate_limiter),
                    self.options.recv_window_ms,
                )
                .await
                .map(Arc::new)
            })
            .await?;
        Ok(Arc::clone(transport))
    }

    pub fn market_handler(&self) -> MarketStateHandler {
        MarketStateHandler {
            book_tickers: Arc::clone(&self.book_tickers),
            agg_trades: Arc::clone(&self.agg_trades),
            partial_depths: Arc::clone(&self.partial_depths),
            klines: Arc::clone(&self.klines),
            order_books: Arc::clone(&self.order_books),
        }
    }

    pub async fn connect_streams_with<H: StreamHandler>(
        &self,
        handler: H,
    ) -> Result<WsStreamShard, String> {
        WsStreamShard::connect(&self.options.ws_streams_url, Arc::clone(&self.registry), handler)
            .await
    }

    pub async fn connect_streams_to<H: StreamHandler>(
        &self,
        url: &str,
        handler: H,
    ) -> Result<WsStreamShard, String> {
        WsStreamShard::connect(url, Arc::clone(&self.registry), handler).await
    }

    pub async fn connect_market_streams(&self) -> Result<WsStreamShard, String> {
        self.connect_streams_with(self.market_handler()).await
    }

    /// Registers a stream route locally before connecting or before sending a `SUBSCRIBE` batch.
    pub fn register_stream(&self, stream_name: &str, kind: StreamKind) -> Result<u32, &'static str> {
        self.registry.register(stream_name, kind)
    }

    pub fn spawn_depth_snapshot_worker(&self) -> tokio::task::JoinHandle<()> {
        let books = Arc::clone(&self.order_books);
        let fetcher = RestSnapshotFetcher(Arc::clone(&self.rest));
        tokio::spawn(async move {
            books.run_snapshot_worker(fetcher).await;
        })
    }

    #[inline(always)]
    pub fn try_get_book_ticker(&self, sub_id: u32) -> Option<BookTickerModel> {
        self.book_tickers.try_read(sub_id as usize).map(|(m, _)| m)
    }

    #[inline(always)]
    pub fn try_get_agg_trade(&self, sub_id: u32) -> Option<AggTradeModel> {
        self.agg_trades.try_read(sub_id as usize).map(|(m, _)| m)
    }

    #[inline(always)]
    pub fn try_get_partial_depth(&self, sub_id: u32) -> Option<PartialDepthModel> {
        self.partial_depths.try_read(sub_id as usize).map(|(m, _)| m)
    }

    #[inline(always)]
    pub fn try_get_kline(&self, sub_id: u32) -> Option<KlineUpdateModel> {
        self.klines.try_read(sub_id as usize).map(|(m, _)| m)
    }

    #[inline(always)]
    pub fn try_get_best_order_book(&self, sub_id: u32) -> Option<(DepthLevel, DepthLevel, i64)> {
        self.order_books.try_get_best(sub_id as usize)
    }

    #[inline(always)]
    pub fn try_copy_order_book_top(
        &self,
        sub_id: u32,
        bids_dst: &mut [DepthLevel],
        asks_dst: &mut [DepthLevel],
    ) -> Option<(usize, usize, i64)> {
        self.order_books
            .try_copy_top(sub_id as usize, bids_dst, asks_dst)
    }

    // ---- REST endpoints ----

    pub async fn klines_into(
        &self,
        symbol: Symbol,
        interval: BinanceInterval,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<i32>,
        dst: &mut [Kline],
    ) -> Result<usize, RestError> {
        self.rest
            .klines_into(symbol, interval, start_time, end_time, limit, dst)
            .await
    }

    pub async fn order_book_into(
        &self,
        symbol: Symbol,
        limit: i32,
        bids: &mut [DepthLevel],
        asks: &mut [DepthLevel],
    ) -> Result<OrderBookSnapshotModel, RestError> {
        self.rest.order_book_into(symbol, limit, bids, asks).await
    }

    pub async fn order_book(&self, symbol: Symbol, limit: i32) -> Result<OrderBookModel, RestError> {
        self.rest.order_book(symbol, limit).await
    }

    pub async fn exchange_info(&self) -> Result<ExchangeInfoModel, RestError> {
        self.rest.exchange_info().await
    }

    pub async fn account_info_v2(&self) -> Result<AccountInfoV2Model, RestError> {
        self.rest.account_info_v2().await
    }

    pub async fn leverage_bracket(
        &self,
        symbol: Option<Symbol>,
    ) -> Result<LeverageBracketModel, RestError> {
        self.rest.leverage_bracket(symbol).await
    }

    pub async fn new_order(&self, req: &mut NewOrderRequest) -> Result<OrderModel, RestError> {
        self.rest.new_order(req).await
    }

    pub async fn cancel_order(
        &self,
        symbol: Symbol,
        order_id: Option<i64>,
        orig_client_order_id: Option<&ClientOrderId>,
    ) -> Result<OrderModel, RestError> {
        self.rest
            .cancel_order(symbol, order_id, orig_client_order_id)
            .await
    }

    pub async fn modify_order(
        &self,
        req: &ModifyOrderRequest,
    ) -> Result<OrderModel, RestError> {
        self.rest.modify_order(req).await
    }

    pub async fn cancel_all_open_orders(&self, symbol: Symbol) -> Result<bool, RestError> {
        self.rest.cancel_all_open_orders(symbol).await
    }

    pub async fn change_leverage(
        &self,
        symbol: Symbol,
        leverage: i32,
    ) -> Result<ChangeLeverageResponse, RestError> {
        self.rest.change_leverage(symbol, leverage).await
    }

    pub async fn change_margin_type(
        &self,
        symbol: Symbol,
        margin_type: MarginType,
    ) -> Result<ChangeMarginTypeResponse, RestError> {
        self.rest.change_margin_type(symbol, margin_type).await
    }

    pub async fn create_listen_key(&self) -> Result<String, RestError> {
        self.rest.create_listen_key().await
    }

    pub async fn keepalive_listen_key(&self) -> Result<(), RestError> {
        self.rest.keepalive_listen_key().await
    }

    pub async fn close_listen_key(&self) -> Result<(), RestError> {
        self.rest.close_listen_key().await
    }

    pub fn listen_key_manager(&self) -> ListenKeyManager {
        ListenKeyManager::new(Arc::clone(&self.rest))
    }

    pub fn spawn_watchdog(&self, timeout: Duration) -> StreamWatchdog {
        let mut wd = StreamWatchdog::new(
            timeout,
            Arc::clone(&self.network_gate),
            Arc::clone(&self.reconnect_notify),
        );
        wd.start();
        wd
    }

    pub fn spawn_rotator(&self, rotation_interval: Duration) -> MakeBeforeBreakRotator {
        let mut rotator = MakeBeforeBreakRotator::new(
            self.options.ws_streams_url.clone(),
            Arc::clone(&self.registry),
            Arc::clone(&self.active_shard),
            rotation_interval,
        );
        let client_state = self.market_handler();
        rotator.start(move || MarketStateHandler {
            book_tickers: Arc::clone(&client_state.book_tickers),
            agg_trades: Arc::clone(&client_state.agg_trades),
            partial_depths: Arc::clone(&client_state.partial_depths),
            klines: Arc::clone(&client_state.klines),
            order_books: Arc::clone(&client_state.order_books),
        });
        rotator
    }

    // ---- WebSocket API endpoints ----

    pub async fn ws_ticker_price(&self, symbol: Symbol) -> Result<TickerPriceModel, WsApiError> {
        self.connect_ws_api().await?.ticker_price(symbol).await
    }

    pub async fn ws_account_balance(&self) -> Result<AccountBalanceModel, WsApiError> {
        self.connect_ws_api().await?.account_balance().await
    }

    pub async fn ws_account_status(&self) -> Result<AccountInfoV2Model, WsApiError> {
        self.connect_ws_api().await?.account_status().await
    }

    pub async fn ws_new_order(
        &self,
        req: &mut NewOrderRequest,
    ) -> Result<OrderModel, WsApiError> {
        self.connect_ws_api().await?.new_order(req).await
    }

    pub async fn ws_cancel_order(
        &self,
        symbol: Symbol,
        order_id: Option<i64>,
        orig_client_order_id: Option<&ClientOrderId>,
    ) -> Result<OrderModel, WsApiError> {
        self.connect_ws_api()
            .await?
            .cancel_order(symbol, order_id, orig_client_order_id)
            .await
    }

    pub async fn ws_modify_order(
        &self,
        req: &ModifyOrderRequest,
    ) -> Result<OrderModel, WsApiError> {
        self.connect_ws_api().await?.modify_order(req).await
    }

    pub async fn ws_cancel_all_open_orders(&self, symbol: Symbol) -> Result<bool, WsApiError> {
        self.connect_ws_api().await?.cancel_all_open_orders(symbol).await
    }

    pub async fn ws_change_leverage(
        &self,
        symbol: Symbol,
        leverage: i32,
    ) -> Result<ChangeLeverageResponse, WsApiError> {
        self.connect_ws_api().await?.change_leverage(symbol, leverage).await
    }

    pub async fn ws_change_margin_type(
        &self,
        symbol: Symbol,
        margin_type: MarginType,
    ) -> Result<ChangeMarginTypeResponse, WsApiError> {
        self.connect_ws_api().await?.change_margin_type(symbol, margin_type).await
    }

    pub async fn ws_create_listen_key(&self) -> Result<String, WsApiError> {
        self.connect_ws_api().await?.create_listen_key().await
    }

    pub async fn ws_keepalive_listen_key(&self, listen_key: &str) -> Result<(), WsApiError> {
        self.connect_ws_api().await?.keepalive_listen_key(listen_key).await
    }

    pub async fn ws_close_listen_key(&self, listen_key: &str) -> Result<(), WsApiError> {
        self.connect_ws_api().await?.close_listen_key(listen_key).await
    }
}
