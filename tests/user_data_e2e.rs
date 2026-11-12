use axitor_rs::primitives::{
    ClientOrderId, ExecutionType, ModifyOrderRequest, OrderSide, OrderStatus, Symbol,
};
use axitor_rs::user_data::{parse_user_data_event, UserDataEvent};
use axitor_rs::{BinanceClient, BinanceClientOptions, BinanceLimits};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::test]
async fn full_user_data_and_listen_key_e2e() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 4096];

        // 1. POST /fapi/v1/listenKey
        let mut total = 0;
        loop {
            let n = sock.read(&mut buf[total..]).await.unwrap();
            total += n;
            if buf[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        let req1 = std::str::from_utf8(&buf[..total]).unwrap();
        assert!(req1.starts_with("POST /fapi/v1/listenKey"));
        let resp1 = br#"{"listenKey":"mock_listen_key_12345"}"#;
        let hdr1 = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", resp1.len());
        sock.write_all(hdr1.as_bytes()).await.unwrap();
        sock.write_all(resp1).await.unwrap();

        // 2. PUT /fapi/v1/listenKey (keepalive)
        total = 0;
        loop {
            let n = sock.read(&mut buf[total..]).await.unwrap();
            total += n;
            if buf[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        let req2 = std::str::from_utf8(&buf[..total]).unwrap();
        assert!(req2.starts_with("PUT /fapi/v1/listenKey"));
        let resp2 = b"{}";
        let hdr2 = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", resp2.len());
        sock.write_all(hdr2.as_bytes()).await.unwrap();
        sock.write_all(resp2).await.unwrap();

        // 3. PUT /fapi/v1/order (modify order)
        total = 0;
        loop {
            let n = sock.read(&mut buf[total..]).await.unwrap();
            total += n;
            if buf[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        let req3 = std::str::from_utf8(&buf[..total]).unwrap();
        assert!(req3.starts_with("PUT /fapi/v1/order?"));
        let resp3 = br#"{"orderId":112233,"symbol":"BTCUSDT","status":"NEW","clientOrderId":"mod_1","price":"65000.0","avgPrice":"0","origQty":"1.0","executedQty":"0","cumQty":"0","cumQuote":"0","timeInForce":"GTC","type":"LIMIT","reduceOnly":false,"closePosition":false,"side":"BUY","positionSide":"BOTH","stopPrice":"0","workingType":"CONTRACT_PRICE","priceProtect":false,"origType":"LIMIT","priceMatch":"NONE","selfTradePreventionMode":"NONE","goodTillDate":0,"updateTime":1700000000300}"#;
        let hdr3 = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", resp3.len());
        sock.write_all(hdr3.as_bytes()).await.unwrap();
        sock.write_all(resp3).await.unwrap();

        // 4. POST /fapi/v1/leverage
        total = 0;
        loop {
            let n = sock.read(&mut buf[total..]).await.unwrap();
            total += n;
            if buf[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        let req4 = std::str::from_utf8(&buf[..total]).unwrap();
        assert!(req4.starts_with("POST /fapi/v1/leverage?"));
        let resp4 = br#"{"leverage":50,"maxNotionalValue":"1000000","symbol":"BTCUSDT"}"#;
        let hdr4 = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", resp4.len());
        sock.write_all(hdr4.as_bytes()).await.unwrap();
        sock.write_all(resp4).await.unwrap();
    });

    let client = BinanceClient::new(BinanceClientOptions {
        rest_base_url: format!("http://{addr}"),
        api_key: "key".to_string(),
        api_secret: "sec".to_string(),
        limits: BinanceLimits::unlimited(),
        rest_pool_size: 1,
        ..Default::default()
    })
    .unwrap();

    let mut lk_mgr = client.listen_key_manager();
    let lk = lk_mgr.get_or_create().await.unwrap();
    assert_eq!(lk, "mock_listen_key_12345");

    client.keepalive_listen_key().await.unwrap();

    let sym = Symbol::parse("BTCUSDT").unwrap();
    let mod_req = ModifyOrderRequest {
        order_id: Some(112233),
        orig_client_order_id: Some(ClientOrderId::parse("mod_1").unwrap()),
        symbol: sym,
        side: OrderSide::Buy,
        quantity: 1.0,
        price: 65000.0,
    };
    let modified = client.modify_order(&mod_req).await.unwrap();
    assert_eq!(modified.order_id, 112233);
    assert_eq!(modified.status, OrderStatus::New);

    let lev = client.change_leverage(sym, 50).await.unwrap();
    assert_eq!(lev.leverage, 50);
    assert_eq!(lev.symbol.as_raw(), sym.as_raw());

    // Test User Data WebSocket event stream dispatching
    let order_update_raw = br#"{"e":"ORDER_TRADE_UPDATE","E":1690000000000,"T":1690000000001,"o":{"s":"BTCUSDT","c":"my_cid","S":"BUY","o":"LIMIT","f":"GTC","q":"0.25","p":"64000.0","ap":"64000.0","sp":"0","x":"TRADE","X":"FILLED","i":999888,"l":"0.25","z":"0.25","L":"64000.0","N":"USDT","n":"1.6","T":1690000000001,"t":445566,"b":"0","a":"0","m":false,"R":false,"ps":"BOTH","rp":"0"}}"#;
    let event = parse_user_data_event(order_update_raw).unwrap();
    match event {
        UserDataEvent::OrderTradeUpdate(o) => {
            assert_eq!(o.order_id, 999888);
            assert_eq!(o.execution_type, ExecutionType::Trade);
            assert_eq!(o.order_status, OrderStatus::Filled);
            assert_eq!(o.trade_id, 445566);
            assert!((o.last_filled_price - 64000.0).abs() < 1e-9);
            assert!((o.last_filled_quantity - 0.25).abs() < 1e-9);
        }
        _ => panic!("wrong event"),
    }

    server.await.unwrap();
}
