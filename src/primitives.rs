//! Core zero-allocation primitives and flat market/order models.

use core::fmt;

/// An exchange ticker of up to 16 ASCII characters packed into a single `u128`.
/// Zero heap allocation, 16-byte `Copy`, single-instruction equality and hashing.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct Symbol(u128);

impl Symbol {
    pub const MAX_LEN: usize = 16;
    pub const EMPTY: Self = Self(0);

    #[inline(always)]
    const fn is_allowed(c: u8) -> bool {
        (c | 0x20).wrapping_sub(b'a') <= (b'z' - b'a')
            || c.wrapping_sub(b'0') <= (b'9' - b'0')
            || c == b'-'
            || c == b'_'
            || c == b'.'
            || c == b'/'
    }

    #[inline(always)]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Length in bytes, computed in O(1) from leading zero bits (little-endian packing).
    #[inline(always)]
    pub const fn len(self) -> usize {
        if self.0 == 0 {
            0
        } else {
            let bits = 128 - self.0.leading_zeros() as usize;
            (bits + 7) / 8
        }
    }

    #[inline]
    pub fn try_from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() > Self::MAX_LEN {
            return None;
        }
        let mut val = 0u128;
        let mut i = 0;
        while i < bytes.len() {
            let b = bytes[i];
            if !Self::is_allowed(b) {
                return None;
            }
            val |= (b as u128) << (i * 8);
            i += 1;
        }
        Some(Self(val))
    }

    #[inline]
    pub fn parse(s: &str) -> Result<Self, &'static str> {
        Self::try_from_bytes(s.as_bytes()).ok_or("invalid symbol")
    }

    #[inline(always)]
    pub const fn from_bytes_const(bytes: &[u8]) -> Self {
        assert!(bytes.len() <= Self::MAX_LEN);
        let mut val = 0u128;
        let mut i = 0;
        while i < bytes.len() {
            let b = bytes[i];
            assert!(Self::is_allowed(b));
            val |= (b as u128) << (i * 8);
            i += 1;
        }
        Self(val)
    }

    #[inline(always)]
    pub fn write_to(&self, dst: &mut [u8]) -> usize {
        let len = self.len();
        let bytes = self.0.to_le_bytes();
        dst[..len].copy_from_slice(&bytes[..len]);
        len
    }

    #[inline(always)]
    pub fn as_raw(&self) -> u128 {
        self.0
    }
}

impl fmt::Debug for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut buf = [0u8; Self::MAX_LEN];
        let len = self.write_to(&mut buf);
        let s = core::str::from_utf8(&buf[..len]).unwrap_or("?");
        f.write_str(s)
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// Client order identifier up to 36 ASCII bytes, stack-only `Copy`.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct ClientOrderId {
    bytes: [u8; Self::MAX_LEN],
    len: u8,
}

impl ClientOrderId {
    pub const MAX_LEN: usize = 36;
    pub const EMPTY: Self = Self {
        bytes: [0; Self::MAX_LEN],
        len: 0,
    };

    #[inline]
    pub fn try_from_bytes(src: &[u8]) -> Option<Self> {
        if src.len() > Self::MAX_LEN {
            return None;
        }
        let mut bytes = [0u8; Self::MAX_LEN];
        bytes[..src.len()].copy_from_slice(src);
        Some(Self {
            bytes,
            len: src.len() as u8,
        })
    }

    #[inline]
    pub fn parse(s: &str) -> Result<Self, &'static str> {
        Self::try_from_bytes(s.as_bytes()).ok_or("invalid client order id")
    }

    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Default for ClientOrderId {
    fn default() -> Self {
        Self::EMPTY
    }
}

impl fmt::Debug for ClientOrderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = core::str::from_utf8(self.as_bytes()).unwrap_or("?");
        f.write_str(s)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum Market {
    #[default]
    FuturesUsdM = 0,
    FuturesCoinM = 1,
    Spot = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum OrderSide {
    #[default]
    Unknown = 0,
    Buy = 1,
    Sell = 2,
}

impl OrderSide {
    #[inline(always)]
    pub const fn as_bytes(self) -> &'static [u8] {
        match self {
            Self::Buy => b"BUY",
            Self::Sell => b"SELL",
            Self::Unknown => b"UNKNOWN",
        }
    }

    #[inline]
    pub fn from_bytes(b: &[u8]) -> Self {
        match b {
            b"BUY" => Self::Buy,
            b"SELL" => Self::Sell,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum OrderType {
    #[default]
    Unknown = 0,
    Limit = 1,
    Market = 2,
    Stop = 3,
    StopMarket = 4,
    TakeProfit = 5,
    TakeProfitMarket = 6,
    TrailingStopMarket = 7,
}

impl OrderType {
    #[inline(always)]
    pub const fn as_bytes(self) -> &'static [u8] {
        match self {
            Self::Limit => b"LIMIT",
            Self::Market => b"MARKET",
            Self::Stop => b"STOP",
            Self::StopMarket => b"STOP_MARKET",
            Self::TakeProfit => b"TAKE_PROFIT",
            Self::TakeProfitMarket => b"TAKE_PROFIT_MARKET",
            Self::TrailingStopMarket => b"TRAILING_STOP_MARKET",
            Self::Unknown => b"UNKNOWN",
        }
    }

    #[inline]
    pub fn from_bytes(b: &[u8]) -> Self {
        match b {
            b"LIMIT" => Self::Limit,
            b"MARKET" => Self::Market,
            b"STOP" => Self::Stop,
            b"STOP_MARKET" => Self::StopMarket,
            b"TAKE_PROFIT" => Self::TakeProfit,
            b"TAKE_PROFIT_MARKET" => Self::TakeProfitMarket,
            b"TRAILING_STOP_MARKET" => Self::TrailingStopMarket,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum TimeInForce {
    #[default]
    Unknown = 0,
    Gtc = 1,
    Ioc = 2,
    Fok = 3,
    Gtx = 4,
    Gtd = 5,
    Rpi = 6,
}

impl TimeInForce {
    #[inline(always)]
    pub const fn as_bytes(self) -> &'static [u8] {
        match self {
            Self::Gtc => b"GTC",
            Self::Ioc => b"IOC",
            Self::Fok => b"FOK",
            Self::Gtx => b"GTX",
            Self::Gtd => b"GTD",
            Self::Rpi => b"RPI",
            Self::Unknown => b"UNKNOWN",
        }
    }

    #[inline]
    pub fn from_bytes(b: &[u8]) -> Self {
        match b {
            b"GTC" => Self::Gtc,
            b"IOC" => Self::Ioc,
            b"FOK" => Self::Fok,
            b"GTX" => Self::Gtx,
            b"GTD" => Self::Gtd,
            b"RPI" => Self::Rpi,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum PositionSide {
    #[default]
    Both = 0,
    Long = 1,
    Short = 2,
    Unknown = 3,
}

impl PositionSide {
    #[inline(always)]
    pub const fn as_bytes(self) -> &'static [u8] {
        match self {
            Self::Both => b"BOTH",
            Self::Long => b"LONG",
            Self::Short => b"SHORT",
            Self::Unknown => b"UNKNOWN",
        }
    }

    #[inline]
    pub fn from_bytes(b: &[u8]) -> Self {
        match b {
            b"BOTH" => Self::Both,
            b"LONG" => Self::Long,
            b"SHORT" => Self::Short,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum OrderStatus {
    #[default]
    Unknown = 0,
    New = 1,
    PartiallyFilled = 2,
    Filled = 3,
    Canceled = 4,
    Rejected = 5,
    Expired = 6,
    ExpiredInMatch = 7,
}

impl OrderStatus {
    #[inline]
    pub fn from_bytes(b: &[u8]) -> Self {
        match b {
            b"NEW" => Self::New,
            b"PARTIALLY_FILLED" => Self::PartiallyFilled,
            b"FILLED" => Self::Filled,
            b"CANCELED" => Self::Canceled,
            b"REJECTED" => Self::Rejected,
            b"EXPIRED" => Self::Expired,
            b"EXPIRED_IN_MATCH" => Self::ExpiredInMatch,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum WorkingType {
    #[default]
    ContractPrice = 0,
    MarkPrice = 1,
    Unknown = 2,
}

impl WorkingType {
    #[inline]
    pub fn from_bytes(b: &[u8]) -> Self {
        match b {
            b"CONTRACT_PRICE" => Self::ContractPrice,
            b"MARK_PRICE" => Self::MarkPrice,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum PriceMatch {
    #[default]
    Unknown = 0,
    None = 1,
    Opponent = 2,
    Opponent5 = 3,
    Opponent10 = 4,
    Opponent20 = 5,
    Queue = 6,
    Queue5 = 7,
    Queue10 = 8,
    Queue20 = 9,
}

impl PriceMatch {
    #[inline(always)]
    pub const fn as_bytes(self) -> &'static [u8] {
        match self {
            Self::None => b"NONE",
            Self::Opponent => b"OPPONENT",
            Self::Opponent5 => b"OPPONENT_5",
            Self::Opponent10 => b"OPPONENT_10",
            Self::Opponent20 => b"OPPONENT_20",
            Self::Queue => b"QUEUE",
            Self::Queue5 => b"QUEUE_5",
            Self::Queue10 => b"QUEUE_10",
            Self::Queue20 => b"QUEUE_20",
            Self::Unknown => b"UNKNOWN",
        }
    }

    #[inline]
    pub fn from_bytes(b: &[u8]) -> Self {
        match b {
            b"NONE" => Self::None,
            b"OPPONENT" => Self::Opponent,
            b"OPPONENT_5" => Self::Opponent5,
            b"OPPONENT_10" => Self::Opponent10,
            b"OPPONENT_20" => Self::Opponent20,
            b"QUEUE" => Self::Queue,
            b"QUEUE_5" => Self::Queue5,
            b"QUEUE_10" => Self::Queue10,
            b"QUEUE_20" => Self::Queue20,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum SelfTradePreventionMode {
    #[default]
    Unknown = 0,
    None = 1,
    ExpireTaker = 2,
    ExpireMaker = 3,
    ExpireBoth = 4,
}

impl SelfTradePreventionMode {
    #[inline(always)]
    pub const fn as_bytes(self) -> &'static [u8] {
        match self {
            Self::None => b"NONE",
            Self::ExpireTaker => b"EXPIRE_TAKER",
            Self::ExpireMaker => b"EXPIRE_MAKER",
            Self::ExpireBoth => b"EXPIRE_BOTH",
            Self::Unknown => b"UNKNOWN",
        }
    }

    #[inline]
    pub fn from_bytes(b: &[u8]) -> Self {
        match b {
            b"NONE" => Self::None,
            b"EXPIRE_TAKER" => Self::ExpireTaker,
            b"EXPIRE_MAKER" => Self::ExpireMaker,
            b"EXPIRE_BOTH" => Self::ExpireBoth,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum OrderResponseType {
    #[default]
    Ack = 0,
    Result = 1,
}

impl OrderResponseType {
    #[inline(always)]
    pub const fn as_bytes(self) -> &'static [u8] {
        match self {
            Self::Ack => b"ACK",
            Self::Result => b"RESULT",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum SymbolStatus {
    #[default]
    Unknown = 0,
    Trading = 1,
    PreTrading = 2,
    PostTrading = 3,
    EndOfDay = 4,
    Halt = 5,
    Break = 6,
    Settling = 7,
    PendingTrading = 8,
    Close = 9,
}

impl SymbolStatus {
    #[inline]
    pub fn from_bytes(b: &[u8]) -> Self {
        match b {
            b"TRADING" => Self::Trading,
            b"PRE_TRADING" => Self::PreTrading,
            b"POST_TRADING" => Self::PostTrading,
            b"END_OF_DAY" => Self::EndOfDay,
            b"HALT" => Self::Halt,
            b"BREAK" => Self::Break,
            b"SETTLING" => Self::Settling,
            b"PENDING_TRADING" => Self::PendingTrading,
            b"CLOSE" => Self::Close,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinanceInterval {
    OneMinute,
    ThreeMinutes,
    FiveMinutes,
    FifteenMinutes,
    ThirtyMinutes,
    OneHour,
}

impl BinanceInterval {
    #[inline(always)]
    pub const fn as_bytes(self) -> &'static [u8] {
        match self {
            Self::OneMinute => b"1m",
            Self::ThreeMinutes => b"3m",
            Self::FiveMinutes => b"5m",
            Self::FifteenMinutes => b"15m",
            Self::ThirtyMinutes => b"30m",
            Self::OneHour => b"1h",
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct DepthLevel {
    pub price: f64,
    pub quantity: f64,
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct DepthUpdateHeader {
    pub symbol: Symbol,
    pub first_update_id: i64,
    pub final_update_id: i64,
    pub previous_final_update_id: i64,
    pub event_time: i64,
    pub transaction_time: i64,
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct PartialDepthModel {
    pub symbol: Symbol,
    pub final_update_id: i64,
    pub event_time: i64,
    pub transaction_time: i64,
    pub bids: [DepthLevel; 20],
    pub asks: [DepthLevel; 20],
    pub bid_count: u8,
    pub ask_count: u8,
}

impl Default for PartialDepthModel {
    fn default() -> Self {
        Self {
            symbol: Symbol::EMPTY,
            final_update_id: 0,
            event_time: 0,
            transaction_time: 0,
            bids: [DepthLevel::default(); 20],
            asks: [DepthLevel::default(); 20],
            bid_count: 0,
            ask_count: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct BookTickerModel {
    pub symbol: Symbol,
    pub update_id: i64,
    pub event_time: i64,
    pub transaction_time: i64,
    pub bid_price: f64,
    pub bid_quantity: f64,
    pub ask_price: f64,
    pub ask_quantity: f64,
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct AggTradeModel {
    pub symbol: Symbol,
    pub aggregate_trade_id: i64,
    pub event_time: i64,
    pub trade_time: i64,
    pub first_trade_id: i64,
    pub last_trade_id: i64,
    pub price: f64,
    pub quantity: f64,
    pub is_buyer_maker: bool,
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct Kline {
    pub open_time: i64,
    pub close_time: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub taker_buy_volume: f64,
    pub taker_buy_quote_volume: f64,
    pub trade_count: i32,
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct KlineUpdateModel {
    pub symbol: Symbol,
    pub event_time: i64,
    pub first_trade_id: i64,
    pub last_trade_id: i64,
    pub kline: Kline,
    pub is_closed: bool,
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct OrderBookSnapshotModel {
    pub last_update_id: i64,
    pub event_time: i64,
    pub transaction_time: i64,
    pub bid_count: usize,
    pub ask_count: usize,
}

#[derive(Clone, Debug, Default)]
pub struct OrderBookModel {
    pub last_update_id: i64,
    pub event_time: i64,
    pub transaction_time: i64,
    pub bids: Vec<DepthLevel>,
    pub asks: Vec<DepthLevel>,
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct SymbolFilters {
    pub tick_size: f64,
    pub min_price: f64,
    pub max_price: f64,
    pub step_size: f64,
    pub min_quantity: f64,
    pub max_quantity: f64,
    pub market_step_size: f64,
    pub market_min_quantity: f64,
    pub market_max_quantity: f64,
    pub min_notional: f64,
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct ExchangeSymbol {
    pub symbol: Symbol,
    pub filters: SymbolFilters,
    pub status: SymbolStatus,
}

#[derive(Clone, Debug, Default)]
pub struct ExchangeInfoModel {
    pub symbols: Vec<ExchangeSymbol>,
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct OrderModel {
    pub symbol: Symbol,
    pub client_order_id: ClientOrderId,
    pub order_id: i64,
    pub create_time: i64,
    pub update_time: i64,
    pub good_till_date: i64,
    pub modify_id: i64,
    pub quantity: f64,
    pub price: f64,
    pub average_price: f64,
    pub stop_price: f64,
    pub filled_quantity: f64,
    pub filled_quote_quantity: f64,
    pub activation_price: f64,
    pub callback_rate: f64,
    pub status: OrderStatus,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub original_type: OrderType,
    pub time_in_force: TimeInForce,
    pub position_side: PositionSide,
    pub working_type: WorkingType,
    pub price_match: PriceMatch,
    pub self_trade_prevention_mode: SelfTradePreventionMode,
    pub is_reduce_only: bool,
    pub is_close_position: bool,
    pub is_price_protect: bool,
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct NewOrderRequest {
    pub symbol: Symbol,
    pub client_order_id: ClientOrderId,
    pub filters: SymbolFilters,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub position_side: PositionSide,
    pub response_type: OrderResponseType,
    pub price_match: PriceMatch,
    pub self_trade_prevention_mode: SelfTradePreventionMode,
    pub quantity: f64,
    pub price: f64,
    pub good_till_date: i64,
    pub is_reduce_only: bool,
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct AccountInfoV2Asset {
    pub asset: Symbol,
    pub update_time: i64,
    pub wallet_balance: f64,
    pub unrealized_profit: f64,
    pub margin_balance: f64,
    pub maintenance_margin: f64,
    pub initial_margin: f64,
    pub position_initial_margin: f64,
    pub open_order_initial_margin: f64,
    pub cross_wallet_balance: f64,
    pub cross_unrealized_profit: f64,
    pub available_balance: f64,
    pub max_withdraw_amount: f64,
    pub margin_available: bool,
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct AccountInfoV2Position {
    pub symbol: Symbol,
    pub update_time: i64,
    pub quantity: f64,
    pub entry_price: f64,
    pub break_even_price: f64,
    pub unrealized_profit: f64,
    pub initial_margin: f64,
    pub maintenance_margin: f64,
    pub position_initial_margin: f64,
    pub open_order_initial_margin: f64,
    pub max_notional: f64,
    pub leverage: i32,
    pub side: PositionSide,
    pub is_isolated: bool,
}

#[derive(Clone, Debug, Default)]
pub struct AccountInfoV2Model {
    pub assets: Vec<AccountInfoV2Asset>,
    pub positions: Vec<AccountInfoV2Position>,
    pub trade_group_id: i64,
    pub total_initial_margin: f64,
    pub total_maintenance_margin: f64,
    pub total_wallet_balance: f64,
    pub total_unrealized_profit: f64,
    pub total_margin_balance: f64,
    pub total_position_initial_margin: f64,
    pub total_open_order_initial_margin: f64,
    pub total_cross_wallet_balance: f64,
    pub total_cross_unrealized_profit: f64,
    pub available_balance: f64,
    pub max_withdraw_amount: f64,
    pub fee_tier: i32,
    pub fee_burn: bool,
    pub can_trade: bool,
    pub can_deposit: bool,
    pub can_withdraw: bool,
    pub is_multi_assets_margin: bool,
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct LeverageBracketLevel {
    pub notional_floor: f64,
    pub notional_cap: f64,
    pub maint_margin_ratio: f64,
    pub cum: f64,
    pub bracket: i32,
    pub initial_leverage: i32,
}

#[derive(Clone, Debug, Default)]
pub struct SymbolBrackets {
    pub symbol: Symbol,
    pub brackets: Vec<LeverageBracketLevel>,
    pub notional_coef: f64,
}

#[derive(Clone, Debug, Default)]
pub struct LeverageBracketModel {
    pub symbols: Vec<SymbolBrackets>,
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct TickerPriceModel {
    pub symbol: Symbol,
    pub price: f64,
    pub time: i64,
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct AccountBalanceAsset {
    pub asset: Symbol,
    pub update_time: i64,
    pub wallet_balance: f64,
    pub cross_wallet_balance: f64,
    pub cross_unrealized_profit: f64,
    pub available_balance: f64,
    pub max_withdraw_amount: f64,
    pub margin_available: bool,
}

#[derive(Clone, Debug, Default)]
pub struct AccountBalanceModel {
    pub assets: Vec<AccountBalanceAsset>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum ExecutionType {
    #[default]
    Unknown = 0,
    New = 1,
    Canceled = 2,
    Calculated = 3,
    Expired = 4,
    Trade = 5,
    Amendment = 6,
}

impl ExecutionType {
    #[inline]
    pub fn from_bytes(b: &[u8]) -> Self {
        match b {
            b"NEW" => Self::New,
            b"CANCELED" => Self::Canceled,
            b"CALCULATED" => Self::Calculated,
            b"EXPIRED" => Self::Expired,
            b"TRADE" => Self::Trade,
            b"AMENDMENT" => Self::Amendment,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum EventReasonType {
    #[default]
    Unknown = 0,
    Deposit = 1,
    Withdraw = 2,
    Order = 3,
    FundingFee = 4,
    WithdrawReject = 5,
    Adjustment = 6,
    InsuranceClear = 7,
    AdminDeposit = 8,
    AdminWithdraw = 9,
    MarginTransfer = 10,
    MarginTypeChange = 11,
    AssetTransfer = 12,
    AutoExchange = 13,
}

impl EventReasonType {
    #[inline]
    pub fn from_bytes(b: &[u8]) -> Self {
        match b {
            b"DEPOSIT" => Self::Deposit,
            b"WITHDRAW" => Self::Withdraw,
            b"ORDER" => Self::Order,
            b"FUNDING_FEE" => Self::FundingFee,
            b"WITHDRAW_REJECT" => Self::WithdrawReject,
            b"ADJUSTMENT" => Self::Adjustment,
            b"INSURANCE_CLEAR" => Self::InsuranceClear,
            b"ADMIN_DEPOSIT" => Self::AdminDeposit,
            b"ADMIN_WITHDRAW" => Self::AdminWithdraw,
            b"MARGIN_TRANSFER" => Self::MarginTransfer,
            b"MARGIN_TYPE_CHANGE" => Self::MarginTypeChange,
            b"ASSET_TRANSFER" => Self::AssetTransfer,
            b"AUTO_EXCHANGE" => Self::AutoExchange,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum MarginType {
    #[default]
    Cross = 0,
    Isolated = 1,
}

impl MarginType {
    #[inline(always)]
    pub const fn as_bytes(self) -> &'static [u8] {
        match self {
            Self::Cross => b"CROSSED",
            Self::Isolated => b"ISOLATED",
        }
    }

    #[inline]
    pub fn from_bytes(b: &[u8]) -> Self {
        match b {
            b"ISOLATED" | b"isolated" => Self::Isolated,
            _ => Self::Cross,
        }
    }
}

/// Order trade update event payload from UserData WebSocket (`ORDER_TRADE_UPDATE`).
#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct OrderTradeUpdateModel {
    pub event_time: i64,
    pub transaction_time: i64,
    pub symbol: Symbol,
    pub client_order_id: ClientOrderId,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub original_quantity: f64,
    pub original_price: f64,
    pub average_price: f64,
    pub stop_price: f64,
    pub execution_type: ExecutionType,
    pub order_status: OrderStatus,
    pub order_id: i64,
    pub last_filled_quantity: f64,
    pub cumulative_filled_quantity: f64,
    pub last_filled_price: f64,
    pub commission_amount: f64,
    pub commission_asset: Symbol,
    pub trade_time: i64,
    pub trade_id: i64,
    pub bids_notional: f64,
    pub asks_notional: f64,
    pub is_maker: bool,
    pub is_reduce_only: bool,
    pub position_side: PositionSide,
    pub realized_profit: f64,
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct AccountUpdateBalance {
    pub asset: Symbol,
    pub wallet_balance: f64,
    pub cross_wallet_balance: f64,
    pub balance_change: f64,
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct AccountUpdatePosition {
    pub symbol: Symbol,
    pub position_amount: f64,
    pub entry_price: f64,
    pub accumulated_realized: f64,
    pub unrealized_pnl: f64,
    pub margin_type: MarginType,
    pub isolated_wallet: f64,
    pub position_side: PositionSide,
}

/// Account update event payload (`ACCOUNT_UPDATE`).
#[derive(Clone, Debug, Default)]
pub struct AccountUpdateModel {
    pub event_time: i64,
    pub transaction_time: i64,
    pub reason: EventReasonType,
    pub balances: Vec<AccountUpdateBalance>,
    pub positions: Vec<AccountUpdatePosition>,
}

#[derive(Clone, Copy, Debug, Default)]
#[repr(C)]
pub struct MarginCallPosition {
    pub symbol: Symbol,
    pub position_side: PositionSide,
    pub position_amount: f64,
    pub margin_type: MarginType,
    pub isolated_wallet: f64,
    pub mark_price: f64,
    pub unrealized_pnl: f64,
    pub maintenance_margin_required: f64,
}

/// Margin call warning payload (`MARGIN_CALL`).
#[derive(Clone, Debug, Default)]
pub struct MarginCallModel {
    pub event_time: i64,
    pub cross_wallet_balance: f64,
    pub positions: Vec<MarginCallPosition>,
}

/// Account configuration update payload (`ACCOUNT_CONFIG_UPDATE`).
#[derive(Clone, Copy, Debug, Default)]
pub struct AccountConfigUpdateModel {
    pub event_time: i64,
    pub transaction_time: i64,
    pub symbol: Symbol,
    pub leverage: i32,
    pub is_multi_assets_margin: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ChangeLeverageResponse {
    pub leverage: i32,
    pub max_notional_value: f64,
    pub symbol: Symbol,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ChangeMarginTypeResponse {
    pub code: i32,
    pub msg: Symbol,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ModifyOrderRequest {
    pub order_id: Option<i64>,
    pub orig_client_order_id: Option<ClientOrderId>,
    pub symbol: Symbol,
    pub side: OrderSide,
    pub quantity: f64,
    pub price: f64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NewAlgoOrderRequest {
    pub symbol: Symbol,
    pub side: OrderSide,
    pub position_side: PositionSide,
    pub algo_type: OrderType,
    pub quantity: f64,
    pub trigger_price: f64,
    pub price: Option<f64>,
    pub client_algo_id: ClientOrderId,
    pub is_reduce_only: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct AlgoOrderModel {
    pub algo_id: i64,
    pub client_algo_id: ClientOrderId,
    pub symbol: Symbol,
    pub side: OrderSide,
    pub position_side: PositionSide,
    pub trigger_price: f64,
    pub price: f64,
    pub quantity: f64,
    pub status: OrderStatus,
    pub actual_order_id: i64,
}


