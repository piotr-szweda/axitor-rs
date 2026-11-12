use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

fn main() {
    let out_dir = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("../bench-data"));

    let rest_dir = out_dir.join("rest");
    let orders_dir = out_dir.join("orders");
    let ws_api_dir = out_dir.join("ws-api");
    let streams_dir = out_dir.join("ws");

    fs::create_dir_all(&rest_dir).unwrap();
    fs::create_dir_all(&orders_dir).unwrap();
    fs::create_dir_all(&ws_api_dir).unwrap();
    fs::create_dir_all(&streams_dir).unwrap();

    // 1. REST samples
    write_klines(&rest_dir.join("klines_500.json"), 500);
    write_depth_snapshot(&rest_dir.join("depth_1000_BTCUSDT.json"), 1000, 10_000_000);
    fs::write(
        rest_dir.join("exchange_info.json"),
        r#"{"timezone":"UTC","serverTime":1700000000000,"symbols":[{"symbol":"BTCUSDT","pair":"BTCUSDT","contractType":"PERPETUAL","status":"TRADING","baseAsset":"BTC","quoteAsset":"USDT","marginAsset":"USDT","pricePrecision":2,"quantityPrecision":3,"filters":[{"filterType":"PRICE_FILTER","minPrice":"0.10","maxPrice":"1000000.00","tickSize":"0.10"},{"filterType":"LOT_SIZE","minQty":"0.001","maxQty":"1000.000","stepSize":"0.001"},{"filterType":"MIN_NOTIONAL","notional":"5.0"}]}]}"#,
    )
    .unwrap();
    fs::write(
        rest_dir.join("account_v2.json"),
        r#"{"feeTier":0,"canTrade":true,"canDeposit":true,"canWithdraw":true,"totalInitialMargin":"100.0","totalMaintMargin":"10.0","totalWalletBalance":"10000.0","totalUnrealizedProfit":"25.5","totalMarginBalance":"10025.5","totalPositionInitialMargin":"50.0","totalOpenOrderInitialMargin":"50.0","availableBalance":"9925.5","maxWithdrawAmount":"9925.5","assets":[{"asset":"USDT","walletBalance":"10000.0","unrealizedProfit":"25.5","marginBalance":"10025.5","maintMargin":"10.0","initialMargin":"100.0","availableBalance":"9925.5"}],"positions":[{"symbol":"BTCUSDT","initialMargin":"50.0","maintMargin":"5.0","unrealizedProfit":"25.5","positionInitialMargin":"50.0","openOrderInitialMargin":"0.0","leverage":"20","isolated":false,"entryPrice":"64000.0","maxNotional":"5000000","positionSide":"BOTH","positionAmt":"0.100","notional":"6425.5"}]}"#,
    )
    .unwrap();
    fs::write(
        rest_dir.join("leverage_bracket.json"),
        r#"[{"symbol":"BTCUSDT","notionalCoef":1.0,"brackets":[{"bracket":1,"initialLeverage":125,"notionalCap":50000,"notionalFloor":0,"maintMarginRatio":0.004,"cum":0.0}]}]"#,
    )
    .unwrap();

    // 2. Order samples (shared by REST and WS-API templates in ExchangeStub)
    let new_order_json = r#"{"orderId":1234567890,"symbol":"BTCUSDT","status":"NEW","clientOrderId":"axitor_bench_001","price":"64250.10","avgPrice":"0.00","origQty":"0.010","executedQty":"0.000","cumQty":"0.000","cumQuote":"0.0000","timeInForce":"GTC","type":"LIMIT","reduceOnly":false,"closePosition":false,"side":"BUY","positionSide":"BOTH","stopPrice":"0.00","workingType":"CONTRACT_PRICE","priceProtect":false,"origType":"LIMIT","priceMatch":"NONE","selfTradePreventionMode":"NONE","goodTillDate":0,"updateTime":1700000000123}"#;
    let cancel_order_json = r#"{"orderId":1234567890,"symbol":"BTCUSDT","status":"CANCELED","clientOrderId":"axitor_bench_001","price":"64250.10","avgPrice":"0.00","origQty":"0.010","executedQty":"0.000","cumQty":"0.000","cumQuote":"0.0000","timeInForce":"GTC","type":"LIMIT","reduceOnly":false,"closePosition":false,"side":"BUY","positionSide":"BOTH","stopPrice":"0.00","workingType":"CONTRACT_PRICE","priceProtect":false,"origType":"LIMIT","priceMatch":"NONE","selfTradePreventionMode":"NONE","goodTillDate":0,"updateTime":1700000000200}"#;
    fs::write(orders_dir.join("new_order.json"), new_order_json).unwrap();
    fs::write(orders_dir.join("cancel_order.json"), cancel_order_json).unwrap();

    // 3. WS-API templates
    fs::write(
        ws_api_dir.join("ticker_price.json"),
        r#"{"id":1,"status":200,"result":{"symbol":"BTCUSDT","price":"64250.50","time":1700000000100}}"#,
    )
    .unwrap();
    fs::write(
        ws_api_dir.join("account_balance.json"),
        r#"{"id":1,"status":200,"result":[{"accountAlias":"uSgR","asset":"USDT","balance":"10000.00","crossWalletBalance":"10000.00","crossUnPnl":"25.50","availableBalance":"9925.50","maxWithdrawAmount":"9925.50","marginAvailable":true,"updateTime":1700000000100}]}"#,
    )
    .unwrap();
    fs::write(
        ws_api_dir.join("account_status.json"),
        format!(
            r#"{{"id":1,"status":200,"result":{}}}"#,
            fs::read_to_string(rest_dir.join("account_v2.json")).unwrap()
        ),
    )
    .unwrap();

    // 4. AXREC001 streams (400_000 bookTicker frames + 50_000 others)
    write_stream_axrec(&streams_dir.join("book_ticker.axrec"), 400_000, |i, buf| {
        let bp = 64250.0 + ((i % 100) as f64) * 0.1;
        let ap = bp + 0.1;
        buf.clear();
        buf.push_str(&format!(
            r#"{{"stream":"btcusdt@bookTicker","data":{{"e":"bookTicker","u":{},"s":"BTCUSDT","b":"{:.2}","B":"1.250","a":"{:.2}","A":"0.850","T":1700000000100,"E":1700000000100}}}}"#,
            10_000_000 + i,
            bp,
            ap
        ));
    });

    write_stream_axrec(&streams_dir.join("agg_trade.axrec"), 50_000, |i, buf| {
        let p = 64250.0 + ((i % 50) as f64) * 0.1;
        buf.clear();
        buf.push_str(&format!(
            r#"{{"stream":"btcusdt@aggTrade","data":{{"e":"aggTrade","E":1700000000100,"a":{},"s":"BTCUSDT","p":"{:.2}","q":"0.150","f":{},"l":{},"T":1700000000100,"m":{}}}}}"#,
            500_000 + i,
            p,
            900_000 + i * 2,
            900_001 + i * 2,
            if i % 2 == 0 { "true" } else { "false" }
        ));
    });

    write_stream_axrec(&streams_dir.join("diff_depth.axrec"), 50_000, |i, buf| {
        let first_u = 10_000_000 + i * 5 + 1;
        let final_u = first_u + 4;
        let prev_u = first_u - 1;
        let base = 64250.0 + ((i % 20) as f64) * 0.1;
        buf.clear();
        buf.push_str(&format!(
            r#"{{"stream":"btcusdt@depth@100ms","data":{{"e":"depthUpdate","E":1700000000100,"T":1700000000100,"s":"BTCUSDT","U":{},"u":{},"pu":{},"b":[["{:.2}","2.500"],["{:.2}","1.100"]],"a":[["{:.2}","1.800"],["{:.2}","3.200"]]}}}}"#,
            first_u,
            final_u,
            prev_u,
            base - 0.1,
            base - 0.2,
            base + 0.1,
            base + 0.2
        ));
    });

    write_stream_axrec(&streams_dir.join("partial_depth.axrec"), 30_000, |i, buf| {
        let mut s = format!(
            r#"{{"stream":"btcusdt@depth20@100ms","data":{{"e":"depthUpdate","E":1700000000100,"T":1700000000100,"s":"BTCUSDT","U":{},"u":{},"pu":{},"b":["#,
            10_000_000 + i,
            10_000_000 + i,
            9_999_999 + i
        );
        for lvl in 0..20 {
            if lvl > 0 {
                s.push(',');
            }
            s.push_str(&format!(r#"["{:.2}","1.000"]"#, 64250.0 - (lvl as f64) * 0.1));
        }
        s.push_str(r#"],"a":["#);
        for lvl in 0..20 {
            if lvl > 0 {
                s.push(',');
            }
            s.push_str(&format!(r#"["{:.2}","1.000"]"#, 64250.1 + (lvl as f64) * 0.1));
        }
        s.push_str("]}}");
        *buf = s;
    });

    write_stream_axrec(&streams_dir.join("kline.axrec"), 30_000, |i, buf| {
        let close = 64250.0 + ((i % 40) as f64) * 0.5;
        buf.clear();
        buf.push_str(&format!(
            r#"{{"stream":"btcusdt@kline_1m","data":{{"e":"kline","E":1700000000100,"s":"BTCUSDT","k":{{"t":1700000000000,"T":1700000059999,"s":"BTCUSDT","i":"1m","f":100,"L":200,"o":"64200.00","c":"{:.2}","h":"64300.00","l":"64190.00","v":"123.456","n":101,"x":false,"q":"7932000.0","V":"60.0","Q":"3850000.0","B":"0"}}}}}}"#,
            close
        ));
    });

    println!("Generated synthetic benchmark dataset in {}", out_dir.display());
}

fn write_klines(path: &PathBuf, count: usize) {
    let mut s = String::with_capacity(count * 120);
    s.push('[');
    for i in 0..count {
        if i > 0 {
            s.push(',');
        }
        let t = 1_700_000_000_000i64 + (i as i64) * 60_000;
        let o = 64000.0 + ((i % 100) as f64);
        let c = o + 12.5;
        s.push_str(&format!(
            r#"[{t},"{o:.2}","{:.2}","{:.2}","{c:.2}","100.500",{},"{:.2}",250,"50.250","{:.2}","0"]"#,
            c + 5.0,
            o - 5.0,
            t + 59_999,
            c * 100.5,
            c * 50.25
        ));
    }
    s.push(']');
    fs::write(path, s).unwrap();
}

fn write_depth_snapshot(path: &PathBuf, levels: usize, last_update_id: i64) {
    let mut s = format!(
        r#"{{"lastUpdateId":{last_update_id},"E":1700000000000,"T":1700000000000,"bids":["#
    );
    for i in 0..levels {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!(r#"["{:.2}","1.500"]"#, 64250.0 - (i as f64) * 0.1));
    }
    s.push_str(r#"],"asks":["#);
    for i in 0..levels {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!(r#"["{:.2}","1.500"]"#, 64250.1 + (i as f64) * 0.1));
    }
    s.push_str("]}");
    fs::write(path, s).unwrap();
}

fn write_stream_axrec(path: &PathBuf, count: usize, mut make_frame: impl FnMut(usize, &mut String)) {
    let file = File::create(path).unwrap();
    let mut w = BufWriter::new(file);
    w.write_all(b"AXREC001").unwrap();
    let mut buf = String::with_capacity(1024);
    for i in 0..count {
        make_frame(i, &mut buf);
        let micros = (i as i64) * 50;
        let bytes = buf.as_bytes();
        w.write_all(&micros.to_le_bytes()).unwrap();
        w.write_all(&[0u8]).unwrap();
        w.write_all(&(bytes.len() as i32).to_le_bytes()).unwrap();
        w.write_all(bytes).unwrap();
    }
    w.flush().unwrap();
}

