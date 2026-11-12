pub mod client;
pub mod concurrency;
pub mod order_book;
pub mod order_rules;
pub mod parsing;
pub mod primitives;
pub mod rate_limiter;
pub mod resiliency;
pub mod transport;
pub mod user_data;

pub use client::{BinanceClient, BinanceClientOptions};
pub use order_book::{OrderBook, OrderBookSet};
pub use order_rules::{preflight_order, OrderRuleViolation};
pub use primitives::*;
pub use rate_limiter::{bucket, BinanceLimits, BinanceRateLimiter};
pub use resiliency::{MakeBeforeBreakRotator, NetworkGate, StreamWatchdog};
pub use transport::rest_api::{RestApiTransport, RestError};
pub use transport::ws_api::{WsApiError, WsApiTransport};
pub use transport::ws_streams::{
    MarketStateHandler, StreamHandler, StreamKind, WsStreamRegistry, WsStreamShard,
};
pub use user_data::{ListenKeyManager, UserDataEvent};

#[cfg(test)]
mod integration_unit_tests {
    use super::*;
    use crate::concurrency::seqlock::LatestCache;
    use crate::parsing::depth::read_order_book_into;
    use crate::parsing::rest::parse_klines_into;
    use crate::parsing::streams::parse_book_ticker;

    #[test]
    fn symbol_pack_unpack_and_rules() {
        let sym = Symbol::parse("BTCUSDT").unwrap();
        let mut buf = [0u8; 16];
        let n = sym.write_to(&mut buf);
        assert_eq!(&buf[..n], b"BTCUSDT");

        let mut req = NewOrderRequest {
            symbol: sym,
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            time_in_force: TimeInForce::Gtc,
            quantity: 0.5,
            price: 64250.25,
            ..Default::default()
        };
        assert_eq!(preflight_order(&mut req), OrderRuleViolation::None);
    }

    #[test]
    fn book_ticker_parse_and_latest_cache_roundtrip() {
        let json = br#"{"e":"bookTicker","u":12345,"s":"BTCUSDT","b":"64250.10","B":"1.25","a":"64250.20","A":"0.75","T":1700000000100,"E":1700000000105}"#;
        let (model, seq_id) = parse_book_ticker(json).unwrap();
        assert_eq!(seq_id, 12345);
        assert_eq!(model.event_time, 1_700_000_000_105);
        assert!((model.bid_price - 64250.10).abs() < 1e-9);
        assert!((model.ask_quantity - 0.75).abs() < 1e-9);

        let cache = LatestCache::new(16);
        cache.update(3, model);
        let (got, _seq) = cache.try_read(3).unwrap();
        assert_eq!(got.update_id, 12345);
        assert!((got.bid_price - 64250.10).abs() < 1e-9);
    }

    #[test]
    fn klines_into_zero_alloc_slice_parse() {
        let json = br#"[[1700000000000,"64000.1","64100.5","63950.0","64080.2","123.45",1700000059999,"7900000.1",420,"60.1","3850000.0","0"]]"#;
        let mut out = [Kline::default(); 8];
        let n = parse_klines_into(json, &mut out).unwrap();
        assert_eq!(n, 1);
        assert_eq!(out[0].open_time, 1_700_000_000_000);
        assert!((out[0].close - 64080.2).abs() < 1e-9);
        assert_eq!(out[0].trade_count, 420);
    }

    #[test]
    fn order_book_snapshot_and_delta_sync() {
        let snap_json = br#"{"lastUpdateId":100,"E":1000,"T":999,"bids":[["100.0","2.0"],["99.5","5.0"]],"asks":[["101.0","3.0"],["102.0","4.0"]]}"#;
        let mut bids = [DepthLevel::default(); 8];
        let mut asks = [DepthLevel::default(); 8];
        let snap = read_order_book_into(snap_json, true, &mut bids, &mut asks).unwrap();
        assert_eq!(snap.last_update_id, 100);
        assert_eq!(snap.bid_count, 2);
        assert_eq!(snap.ask_count, 2);

        let book = OrderBook::new(64, 16, 1024, 0);
        let sym = Symbol::parse("BTCUSDT").unwrap();

        let hdr1 = DepthUpdateHeader {
            symbol: sym,
            first_update_id: 98,
            final_update_id: 102,
            previous_final_update_id: 97,
            transaction_time: 1001,
            event_time: 1002,
        };
        let d_bids = [DepthLevel { price: 100.2, quantity: 1.5 }];
        let d_asks = [DepthLevel { price: 101.0, quantity: 0.0 }];
        let (_kind, ticket, _lost) = book.update(&hdr1, &d_bids, &d_asks);
        assert!(book.try_get_best().is_none());

        let (_, lifetime) = book.try_begin_snapshot(ticket).unwrap();
        assert!(book.apply_snapshot(
            lifetime,
            snap.last_update_id,
            &bids[..snap.bid_count],
            &asks[..snap.ask_count],
            100,
        ));
        let (best_bid, best_ask, _) = book.try_get_best().unwrap();
        assert!((best_bid.price - 100.2).abs() < 1e-9);
        assert!((best_ask.price - 102.0).abs() < 1e-9);
    }

    #[tokio::test]
    async fn watchdog_and_network_gate_trip_and_recover() {
        let gate = NetworkGate::new();
        let notify = std::sync::Arc::new(tokio::sync::Notify::new());
        let mut wd = StreamWatchdog::new(
            std::time::Duration::from_millis(50),
            std::sync::Arc::clone(&gate),
            std::sync::Arc::clone(&notify),
        );
        wd.start();

        assert!(!gate.is_tripped());
        // Wait for watchdog to bark after silence
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        assert!(gate.is_tripped());

        // Recovery
        gate.open();
        assert!(!gate.is_tripped());
        wd.stop();
    }
}
