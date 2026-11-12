use axitor_rs::transport::ws_streams::StreamKind;
use axitor_rs::{
    BinanceClient, BinanceClientOptions, BinanceInterval, BinanceLimits, ClientOrderId,
    DepthLevel, Kline, NewOrderRequest, OrderSide, OrderStatus, OrderType, Symbol, TimeInForce,
};
use base64::Engine as _;
use sha1_helper::websocket_accept;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

mod sha1_helper {
    // Minimal RFC 6455 Sec-WebSocket-Accept SHA-1 implementation for the loopback test server only.
    pub fn sha1_digest(data: &[u8]) -> [u8; 20] {
        let mut h0: u32 = 0x67452301;
        let mut h1: u32 = 0xEFCDAB89;
        let mut h2: u32 = 0x98BADCFE;
        let mut h3: u32 = 0x10325476;
        let mut h4: u32 = 0xC3D2E1F0;

        let bit_len = (data.len() as u64) * 8;
        let mut msg = data.to_vec();
        msg.push(0x80);
        while (msg.len() % 64) != 56 {
            msg.push(0);
        }
        msg.extend_from_slice(&bit_len.to_be_bytes());

        for chunk in msg.chunks_exact(64) {
            let mut w = [0u32; 80];
            for i in 0..16 {
                w[i] = u32::from_be_bytes([
                    chunk[i * 4],
                    chunk[i * 4 + 1],
                    chunk[i * 4 + 2],
                    chunk[i * 4 + 3],
                ]);
            }
            for i in 16..80 {
                w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
            }
            let (mut a, mut b, mut c, mut d, mut e) = (h0, h1, h2, h3, h4);
            for (i, &wi) in w.iter().enumerate() {
                let (f, k) = match i {
                    0..=19 => ((b & c) | ((!b) & d), 0x5A827999u32),
                    20..=39 => (b ^ c ^ d, 0x6ED9EBA1u32),
                    40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDCu32),
                    _ => (b ^ c ^ d, 0xCA62C1D6u32),
                };
                let temp = a
                    .rotate_left(5)
                    .wrapping_add(f)
                    .wrapping_add(e)
                    .wrapping_add(k)
                    .wrapping_add(wi);
                e = d;
                d = c;
                c = b.rotate_left(30);
                b = a;
                a = temp;
            }
            h0 = h0.wrapping_add(a);
            h1 = h1.wrapping_add(b);
            h2 = h2.wrapping_add(c);
            h3 = h3.wrapping_add(d);
            h4 = h4.wrapping_add(e);
        }

        let mut out = [0u8; 20];
        out[0..4].copy_from_slice(&h0.to_be_bytes());
        out[4..8].copy_from_slice(&h1.to_be_bytes());
        out[8..12].copy_from_slice(&h2.to_be_bytes());
        out[12..16].copy_from_slice(&h3.to_be_bytes());
        out[16..20].copy_from_slice(&h4.to_be_bytes());
        out
    }

    pub fn websocket_accept(key: &str) -> String {
        use base64::Engine as _;
        let combined = format!("{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11");
        let digest = sha1_digest(combined.as_bytes());
        base64::engine::general_purpose::STANDARD.encode(digest)
    }
}

fn encode_server_text_frame(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 10);
    out.push(0x81);
    if payload.len() <= 125 {
        out.push(payload.len() as u8);
    } else if payload.len() <= 65535 {
        out.push(126);
        out.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    } else {
        out.push(127);
        out.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    }
    out.extend_from_slice(payload);
    out
}

async fn read_client_masked_frame(sock: &mut tokio::net::TcpStream) -> Option<(u8, Vec<u8>)> {
    let mut hdr = [0u8; 2];
    sock.read_exact(&mut hdr).await.ok()?;
    let opcode = hdr[0] & 0x0F;
    let masked = (hdr[1] & 0x80) != 0;
    let len7 = (hdr[1] & 0x7F) as usize;
    let len = if len7 <= 125 {
        len7
    } else if len7 == 126 {
        let mut b = [0u8; 2];
        sock.read_exact(&mut b).await.ok()?;
        u16::from_be_bytes(b) as usize
    } else {
        let mut b = [0u8; 8];
        sock.read_exact(&mut b).await.ok()?;
        u64::from_be_bytes(b) as usize
    };
    let mut mask = [0u8; 4];
    if masked {
        sock.read_exact(&mut mask).await.ok()?;
    }
    let mut body = vec![0u8; len];
    sock.read_exact(&mut body).await.ok()?;
    if masked {
        for (i, b) in body.iter_mut().enumerate() {
            *b ^= mask[i & 3];
        }
    }
    Some((opcode, body))
}

#[tokio::test]
async fn rest_keepalive_content_length_and_chunked_e2e() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut buf = vec![0u8; 8192];
        for req_idx in 0..3 {
            let mut total = 0;
            loop {
                let n = sock.read(&mut buf[total..]).await.unwrap();
                if n == 0 {
                    return;
                }
                total += n;
                if buf[..total].windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let req = std::str::from_utf8(&buf[..total]).unwrap();
            if req_idx == 0 {
                assert!(req.starts_with("GET /fapi/v1/klines?symbol=BTCUSDT&interval=1m"));
                let body = br#"[[1700000000000,"64000.1","64100.5","63950.0","64080.2","123.45",1700000059999,"7900000.1",420,"60.1","3850000.0","0"]]"#;
                let hdr = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nX-MBX-USED-WEIGHT-1M: 7\r\n\r\n",
                    body.len()
                );
                sock.write_all(hdr.as_bytes()).await.unwrap();
                sock.write_all(body).await.unwrap();
            } else if req_idx == 1 {
                assert!(req.starts_with("GET /fapi/v1/depth?symbol=BTCUSDT&limit=5"));
                let body = br#"{"lastUpdateId":555,"E":1000,"T":999,"bids":[["64200.0","1.5"]],"asks":[["64201.0","2.5"]]}"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n{}\r\n0\r\n\r\n",
                    body.len(),
                    std::str::from_utf8(body).unwrap()
                );
                sock.write_all(resp.as_bytes()).await.unwrap();
            } else {
                assert!(req.starts_with("POST /fapi/v1/order?"));
                assert!(req.contains("signature="));
                let body = br#"{"orderId":98765,"symbol":"BTCUSDT","status":"NEW","clientOrderId":"cid_1","price":"64200.0","avgPrice":"0","origQty":"0.5","executedQty":"0","cumQty":"0","cumQuote":"0","timeInForce":"GTC","type":"LIMIT","reduceOnly":false,"closePosition":false,"side":"BUY","positionSide":"BOTH","stopPrice":"0","workingType":"CONTRACT_PRICE","priceProtect":false,"origType":"LIMIT","priceMatch":"NONE","selfTradePreventionMode":"NONE","goodTillDate":0,"updateTime":1700000000100}"#;
                let hdr = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", body.len());
                sock.write_all(hdr.as_bytes()).await.unwrap();
                sock.write_all(body).await.unwrap();
            }
        }
    });

    let client = BinanceClient::new(BinanceClientOptions {
        rest_base_url: format!("http://{addr}"),
        api_key: "k".to_string(),
        api_secret: "s".to_string(),
        limits: BinanceLimits::default(),
        rest_pool_size: 1,
        ..Default::default()
    })
    .unwrap();
    client.warmup_rest(1).await.unwrap();

    let sym = Symbol::parse("BTCUSDT").unwrap();
    let mut klines = [Kline::default(); 4];
    let kn = client
        .klines_into(sym, BinanceInterval::OneMinute, None, None, Some(10), &mut klines)
        .await
        .unwrap();
    assert_eq!(kn, 1);
    assert!((klines[0].close - 64080.2).abs() < 1e-9);

    let mut bids = [DepthLevel::default(); 5];
    let mut asks = [DepthLevel::default(); 5];
    let snap = client
        .order_book_into(sym, 5, &mut bids, &mut asks)
        .await
        .unwrap();
    assert_eq!(snap.last_update_id, 555);
    assert_eq!(snap.bid_count, 1);
    assert!((bids[0].price - 64200.0).abs() < 1e-9);

    let mut req = NewOrderRequest {
        symbol: sym,
        side: OrderSide::Buy,
        order_type: OrderType::Limit,
        time_in_force: TimeInForce::Gtc,
        quantity: 0.5,
        price: 64200.0,
        client_order_id: ClientOrderId::parse("cid_1").unwrap(),
        ..Default::default()
    };
    let placed = client.new_order(&mut req).await.unwrap();
    assert_eq!(placed.order_id, 98765);
    assert_eq!(placed.status, OrderStatus::New);

    server.await.unwrap();
}

#[tokio::test]
async fn ws_streams_and_ws_api_e2e() {
    let stream_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let stream_addr = stream_listener.local_addr().unwrap();

    let stream_srv = tokio::spawn(async move {
        let (mut sock, _) = stream_listener.accept().await.unwrap();
        let mut buf = [0u8; 4096];
        let n = sock.read(&mut buf).await.unwrap();
        let req = std::str::from_utf8(&buf[..n]).unwrap();
        let key_line = req
            .lines()
            .find(|l| l.to_ascii_lowercase().starts_with("sec-websocket-key:"))
            .unwrap();
        let key = key_line.split(':').nth(1).unwrap().trim();
        let accept = websocket_accept(key);
        let hs = format!(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
        );
        sock.write_all(hs.as_bytes()).await.unwrap();

        let (op, body) = read_client_masked_frame(&mut sock).await.unwrap();
        assert_eq!(op, 0x1);
        assert!(std::str::from_utf8(&body).unwrap().contains("SUBSCRIBE"));
        sock.write_all(&encode_server_text_frame(br#"{"result":null,"id":1}"#))
            .await
            .unwrap();

        let bt = br#"{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":777,"s":"BTCUSDT","b":"64250.5","B":"2.0","a":"64251.0","A":"1.5","T":1000,"E":1001}}"#;
        sock.write_all(&encode_server_text_frame(bt)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    });

    let ws_api_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_api_addr = ws_api_listener.local_addr().unwrap();

    let ws_api_srv = tokio::spawn(async move {
        let (mut sock, _) = ws_api_listener.accept().await.unwrap();
        let mut buf = [0u8; 4096];
        let n = sock.read(&mut buf).await.unwrap();
        let req = std::str::from_utf8(&buf[..n]).unwrap();
        let key_line = req
            .lines()
            .find(|l| l.to_ascii_lowercase().starts_with("sec-websocket-key:"))
            .unwrap();
        let key = key_line.split(':').nth(1).unwrap().trim();
        let accept = websocket_accept(key);
        let hs = format!(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
        );
        sock.write_all(hs.as_bytes()).await.unwrap();

        let (op, body) = read_client_masked_frame(&mut sock).await.unwrap();
        assert_eq!(op, 0x1);
        let txt = std::str::from_utf8(&body).unwrap();
        assert!(txt.contains("ticker.price"));
        let resp = br#"{"id":1,"status":200,"result":{"symbol":"BTCUSDT","price":"64250.75","time":1700000000100}}"#;
        sock.write_all(&encode_server_text_frame(resp)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
    });

    let client = BinanceClient::new(BinanceClientOptions {
        ws_streams_url: format!("ws://{stream_addr}/market/stream"),
        ws_api_url: format!("ws://{ws_api_addr}/ws-fapi/v1"),
        ws_api_lanes: 1,
        limits: BinanceLimits::unlimited(),
        ..Default::default()
    })
    .unwrap();

    let sub_id = client
        .register_stream("btcusdt@bookTicker", StreamKind::BookTicker)
        .unwrap();
    let shard = client.connect_market_streams().await.unwrap();
    shard
        .subscribe_batch(&["btcusdt@bookTicker".to_string()])
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(40)).await;
    let bt = client.try_get_book_ticker(sub_id).unwrap();
    assert_eq!(bt.update_id, 777);
    assert!((bt.bid_price - 64250.5).abs() < 1e-9);
    shard.close().await;

    let sym = Symbol::parse("BTCUSDT").unwrap();
    let tp = client.ws_ticker_price(sym).await.unwrap();
    assert!((tp.price - 64250.75).abs() < 1e-9);

    stream_srv.await.unwrap();
    ws_api_srv.await.unwrap();
}

// Reference base64 import check cleanly
const _: fn(&[u8]) -> String = |b| base64::engine::general_purpose::STANDARD.encode(b);

