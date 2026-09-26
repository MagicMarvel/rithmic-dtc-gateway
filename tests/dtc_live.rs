use std::{collections::HashSet, env, sync::Arc, time::Duration};

use rithmic_dtc_bridge::{
    dtc::{self, Instrument},
    rithmic_feed::RithmicFeed,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{Instant, timeout},
};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires configured Rithmic Paper credentials and reference-data permission"]
async fn paper_trading_dynamic_multi_symbol_market_data_flows_through_dtc_wire() {
    dotenvy::dotenv().ok();
    let configured_symbol = env::var("RITHMIC_DTC_SYMBOL")
        .or_else(|_| env::var("RITHMIC_PROBE_SYMBOL"))
        .unwrap();
    let configured_exchange = env::var("RITHMIC_DTC_EXCHANGE")
        .or_else(|_| env::var("RITHMIC_PROBE_EXCHANGE"))
        .unwrap();
    let requested_symbol =
        env::var("RITHMIC_CATALOG_TEST_SYMBOL").unwrap_or_else(|_| "NQU6".to_owned());
    let requested_exchange =
        env::var("RITHMIC_CATALOG_TEST_EXCHANGE").unwrap_or_else(|_| "CME".to_owned());

    assert_ne!(
        (configured_symbol.as_str(), configured_exchange.as_str()),
        (requested_symbol.as_str(), requested_exchange.as_str()),
        "catalog test must request a symbol other than the configured fallback"
    );
    let instrument =
        Instrument::es(configured_symbol.clone(), configured_exchange.clone()).unwrap();
    let feed = RithmicFeed::connect_from_env().await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        dtc::handle_connection_with_market(stream, instrument, Some(feed.client()))
            .await
            .unwrap();
    });

    let mut client = TcpStream::connect(address).await.unwrap();
    client.write_all(&encoding_request()).await.unwrap();
    read_message(&mut client).await;
    client.write_all(&logon_request()).await.unwrap();
    read_message(&mut client).await;

    client.write_all(&exchange_list_request(2)).await.unwrap();
    let mut exchanges = HashSet::new();
    let mut published = HashSet::new();
    loop {
        let response = timeout(
            Duration::from_secs(20),
            read_without_session_status(&mut client),
        )
        .await
        .expect("timed out waiting for dynamic exchange list");
        if message_type(&response) == 507
            && i32::from_le_bytes(response[4..8].try_into().unwrap()) == 0
        {
            published.insert(read_string(&response[8..72]));
            continue;
        }
        assert_eq!(message_type(&response), 501);
        assert_eq!(i32::from_le_bytes(response[4..8].try_into().unwrap()), 2);
        let exchange = read_string(&response[8..24]);
        if !exchange.is_empty() {
            exchanges.insert(exchange);
        }
        if response[24] == 1 {
            break;
        }
    }
    assert!(published.contains(&configured_symbol));
    assert!(published.contains(&requested_symbol));
    assert!(
        published.iter().any(|symbol| symbol.starts_with("MES")),
        "published catalog is missing MES: {published:?}"
    );
    assert!(
        published.iter().any(|symbol| symbol.starts_with("MNQ")),
        "published catalog is missing MNQ: {published:?}"
    );
    assert!(
        exchanges.contains(&requested_exchange),
        "Rithmic exchange permissions did not include {requested_exchange}: {exchanges:?}"
    );

    client
        .write_all(&symbol_search_request(
            3,
            &requested_symbol,
            &requested_exchange,
        ))
        .await
        .unwrap();
    let mut search_symbols = HashSet::new();
    loop {
        let response = timeout(
            Duration::from_secs(30),
            read_without_session_status(&mut client),
        )
        .await
        .expect("timed out waiting for dynamic symbol search");
        assert_eq!(message_type(&response), 507);
        assert_eq!(i32::from_le_bytes(response[4..8].try_into().unwrap()), 3);
        let symbol = read_string(&response[8..72]);
        if !symbol.is_empty() {
            search_symbols.insert(symbol);
        }
        if response[168] == 1 {
            break;
        }
    }
    assert!(
        search_symbols.contains(&requested_symbol),
        "Rithmic search did not return {requested_symbol}: {search_symbols:?}"
    );

    client
        .write_all(&security_definition_request(
            &requested_symbol,
            &requested_exchange,
        ))
        .await
        .unwrap();
    let definition = timeout(
        Duration::from_secs(20),
        read_without_session_status(&mut client),
    )
    .await
    .expect("timed out waiting for dynamic security definition");
    assert_eq!(message_type(&definition), 507);
    assert_eq!(read_string(&definition[8..72]), requested_symbol);
    assert_eq!(read_string(&definition[72..88]), requested_exchange);
    assert!(f32::from_le_bytes(definition[156..160].try_into().unwrap()) > 0.0);
    assert!(!read_string(&definition[92..156]).is_empty());
    let underlying = read_string(&definition[180..212]);
    assert!(
        !underlying.is_empty() && requested_symbol.starts_with(&underlying),
        "unexpected dynamic underlying {underlying:?} for {requested_symbol}"
    );
    let mut bbo_symbol_ids = HashSet::new();
    client
        .write_all(&market_data_request(
            76,
            &configured_symbol,
            &configured_exchange,
        ))
        .await
        .unwrap();
    loop {
        let message = read_message(&mut client).await;
        match message_type(&message) {
            104 if u32::from_le_bytes(message[4..8].try_into().unwrap()) == 76 => break,
            148 => {
                bbo_symbol_ids.insert(u32::from_le_bytes(message[4..8].try_into().unwrap()));
            }
            _ => {}
        }
    }
    client
        .write_all(&market_data_request(
            77,
            &requested_symbol,
            &requested_exchange,
        ))
        .await
        .unwrap();
    loop {
        let message = read_message(&mut client).await;
        match message_type(&message) {
            104 if u32::from_le_bytes(message[4..8].try_into().unwrap()) == 77 => break,
            148 => {
                bbo_symbol_ids.insert(u32::from_le_bytes(message[4..8].try_into().unwrap()));
            }
            _ => {}
        }
    }

    let deadline = Instant::now() + Duration::from_secs(30);
    while bbo_symbol_ids.len() < 2 {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "timed out waiting for ES and NQ BBO");
        let message = timeout(remaining, read_message(&mut client))
            .await
            .expect("timed out waiting for multi-symbol market data");
        if message_type(&message) == 148 {
            bbo_symbol_ids.insert(u32::from_le_bytes(message[4..8].try_into().unwrap()));
        }
    }
    assert_eq!(bbo_symbol_ids, HashSet::from([76, 77]));

    for (symbol_id, symbol, exchange) in [
        (86, configured_symbol.as_str(), configured_exchange.as_str()),
        (87, requested_symbol.as_str(), requested_exchange.as_str()),
    ] {
        client
            .write_all(&market_depth_request(symbol_id, 20, symbol, exchange))
            .await
            .unwrap();
        let mut sides = HashSet::new();
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero(), "timed out waiting for depth snapshot");
            let message = timeout(remaining, read_message(&mut client))
                .await
                .expect("timed out waiting for multi-symbol depth");
            if message_type(&message) == 122
                && u32::from_le_bytes(message[4..8].try_into().unwrap()) == symbol_id
            {
                sides.insert(u16::from_le_bytes(message[8..10].try_into().unwrap()));
                if message[35] == 1 {
                    break;
                }
            }
        }
        assert!(sides.contains(&1) && sides.contains(&2));
    }

    client.shutdown().await.unwrap();
    server.await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires configured Rithmic Paper Trading credentials and live ES data"]
async fn paper_trading_es_flows_through_dtc_wire() {
    dotenvy::dotenv().ok();
    let symbol = env::var("RITHMIC_DTC_SYMBOL")
        .or_else(|_| env::var("RITHMIC_PROBE_SYMBOL"))
        .unwrap();
    let exchange = env::var("RITHMIC_DTC_EXCHANGE")
        .or_else(|_| env::var("RITHMIC_PROBE_EXCHANGE"))
        .unwrap();
    let timeout_seconds = env::var("RITHMIC_PROBE_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(45);
    let (address, server) = match env::var("DTC_TEST_ADDR") {
        Ok(address) => (address, None),
        Err(_) => {
            let instrument = Instrument::es(symbol.clone(), exchange.clone()).unwrap();
            let feed = RithmicFeed::connect_from_env().await.unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap().to_string();
            let factory = Arc::new(move || feed.client());
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                dtc::handle_connection_with_market(stream, instrument, Some(factory()))
                    .await
                    .unwrap();
            });
            (address, Some(server))
        }
    };

    let mut client = TcpStream::connect(address).await.unwrap();
    client.write_all(&encoding_request()).await.unwrap();
    assert_eq!(read_message(&mut client).await[2], 7);
    client.write_all(&logon_request()).await.unwrap();
    let logon = read_message(&mut client).await;
    assert_eq!(logon[244], 1, "security definitions must be advertised");
    assert_eq!(logon[252], 1, "market data must be advertised");
    client
        .write_all(&security_definition_request(&symbol, &exchange))
        .await
        .unwrap();
    let definition = read_security_definition(&mut client, 1, Duration::from_secs(60)).await;
    assert_eq!(message_type(&definition), 507);
    assert_eq!(definition[168], 1, "definition must be final");

    client
        .write_all(&market_data_request(1, &symbol, &exchange))
        .await
        .unwrap();
    let snapshot = read_message(&mut client).await;
    assert_eq!(message_type(&snapshot), 104);

    let deadline = Instant::now() + Duration::from_secs(timeout_seconds);
    let mut saw_trade_v2 = false;
    let mut saw_bbo_v2 = false;
    while !(saw_trade_v2 && saw_bbo_v2) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for DTC Trade V2 and BBO V2"
        );
        let message = timeout(remaining, read_message(&mut client))
            .await
            .expect("timed out waiting for DTC market data");
        match message_type(&message) {
            147 => {
                assert!(read_f64(&message, 8).is_finite());
                assert!(read_f64(&message, 16) > 0.0);
                if matches!(message[32], 1 | 2) {
                    saw_trade_v2 = true;
                }
            }
            148 => {
                saw_bbo_v2 = true;
                let bid = read_f64(&message, 8);
                let ask = read_f64(&message, 24);
                assert!(bid.is_finite() && ask.is_finite() && bid <= ask);
            }
            _ => {}
        }
    }

    client.shutdown().await.unwrap();
    if let Some(server) = server {
        server.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires configured Rithmic Paper Trading credentials and live ES DBO data"]
async fn paper_trading_es_dbo_flows_as_aggregated_dtc_l2() {
    dotenvy::dotenv().ok();
    let symbol = env::var("RITHMIC_DTC_SYMBOL")
        .or_else(|_| env::var("RITHMIC_PROBE_SYMBOL"))
        .unwrap();
    let exchange = env::var("RITHMIC_DTC_EXCHANGE")
        .or_else(|_| env::var("RITHMIC_PROBE_EXCHANGE"))
        .unwrap();
    let timeout_seconds = env::var("RITHMIC_PROBE_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(45);
    let (address, server) = match env::var("DTC_TEST_ADDR") {
        Ok(address) => (address, None),
        Err(_) => {
            let instrument = Instrument::es(symbol.clone(), exchange.clone()).unwrap();
            let feed = RithmicFeed::connect_from_env().await.unwrap();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap().to_string();
            let factory = Arc::new(move || feed.client());
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                dtc::handle_connection_with_market(stream, instrument, Some(factory()))
                    .await
                    .unwrap();
            });
            (address, Some(server))
        }
    };

    let mut client = TcpStream::connect(address).await.unwrap();
    client.write_all(&encoding_request()).await.unwrap();
    read_message(&mut client).await;
    client.write_all(&logon_request()).await.unwrap();
    let logon = read_message(&mut client).await;
    assert_eq!(logon[247], 1, "market depth must be advertised");
    client
        .write_all(&market_depth_request(2, 1400, &symbol, &exchange))
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(timeout_seconds);
    let mut bid_levels = Vec::new();
    let mut ask_levels = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for DTC depth snapshot"
        );
        let message = timeout(remaining, read_message(&mut client))
            .await
            .expect("timed out waiting for DTC depth snapshot");
        match message_type(&message) {
            122 => {
                let side = u16::from_le_bytes(message[8..10].try_into().unwrap());
                let price = read_f64(&message, 16);
                let quantity = read_f64(&message, 24);
                let num_orders = u32::from_le_bytes(message[48..52].try_into().unwrap());
                assert!(price.is_finite() && price > 0.0);
                assert!(quantity > 0.0);
                assert!(num_orders > 0);
                match side {
                    1 => bid_levels.push(price),
                    2 => ask_levels.push(price),
                    _ => panic!("invalid DTC depth side {side}"),
                }
                if message[35] == 1 {
                    break;
                }
            }
            121 => panic!("DTC market depth request was rejected"),
            _ => {}
        }
    }
    assert!(!bid_levels.is_empty() && !ask_levels.is_empty());
    assert!(bid_levels.windows(2).all(|prices| prices[0] > prices[1]));
    assert!(ask_levels.windows(2).all(|prices| prices[0] < prices[1]));
    assert!(bid_levels[0] <= ask_levels[0]);

    let mut update_count = 0_usize;
    let mut snapshot_bid_levels = Vec::new();
    let mut snapshot_ask_levels = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for DTC depth update V2"
        );
        let message = timeout(remaining, read_message(&mut client))
            .await
            .expect("timed out waiting for DTC depth update V2");
        if message_type(&message) == 122 {
            if message[34] == 1 {
                snapshot_bid_levels.clear();
                snapshot_ask_levels.clear();
            }
            let side = u16::from_le_bytes(message[8..10].try_into().unwrap());
            let price = read_f64(&message, 16);
            match side {
                1 => snapshot_bid_levels.push(price),
                2 => snapshot_ask_levels.push(price),
                _ => panic!("invalid DTC depth snapshot side {side}"),
            }
            if message[35] == 1 {
                bid_levels = snapshot_bid_levels.clone();
                ask_levels = snapshot_ask_levels.clone();
                assert!(bid_levels.windows(2).all(|prices| prices[0] > prices[1]));
                assert!(ask_levels.windows(2).all(|prices| prices[0] < prices[1]));
                assert!(bid_levels[0] <= ask_levels[0]);
            }
        } else if message_type(&message) == 109 {
            assert_eq!(message.len(), 39);
            let side = message[36];
            let action = message[37];
            let price = read_f64(&message, 16);
            let level = u16::from_le_bytes(message[34..36].try_into().unwrap()) as usize;
            assert!(matches!(side, 1 | 2));
            assert!(matches!(action, 2..=4));
            assert!(price.is_finite());
            assert!(level > 0);
            let levels = if side == 1 {
                &mut bid_levels
            } else {
                &mut ask_levels
            };
            let index = level - 1;
            match action {
                2 => {
                    assert!(index < levels.len(), "delete level {level} is out of range");
                    levels.remove(index);
                }
                3 => {
                    assert!(
                        index <= levels.len(),
                        "insert level {level} is out of range"
                    );
                    levels.insert(index, price);
                }
                4 => {
                    assert!(index < levels.len(), "update level {level} is out of range");
                    levels[index] = price;
                }
                _ => unreachable!(),
            }
            update_count += 1;
            if message[38] == 1 {
                assert!(bid_levels.windows(2).all(|prices| prices[0] > prices[1]));
                assert!(ask_levels.windows(2).all(|prices| prices[0] < prices[1]));
                assert!(
                    bid_levels.is_empty()
                        || ask_levels.is_empty()
                        || bid_levels[0] <= ask_levels[0],
                    "crossed DTC book after batch: bid={:?}, ask={:?}",
                    bid_levels.first(),
                    ask_levels.first(),
                );
            }
            if Instant::now() + Duration::from_secs(10) >= deadline {
                break;
            }
        }
    }
    assert!(update_count > 0, "expected live DTC depth updates");

    client.shutdown().await.unwrap();
    if let Some(server) = server {
        server.await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires configured Rithmic Paper Trading credentials and ES history permission"]
async fn paper_trading_es_tick_history_flows_through_dtc_wire() {
    dotenvy::dotenv().ok();
    let symbol = env::var("RITHMIC_DTC_SYMBOL")
        .or_else(|_| env::var("RITHMIC_PROBE_SYMBOL"))
        .unwrap();
    let exchange = env::var("RITHMIC_DTC_EXCHANGE")
        .or_else(|_| env::var("RITHMIC_PROBE_EXCHANGE"))
        .unwrap();
    let address = env::var("DTC_TEST_ADDR").expect("run dtc_server and set DTC_TEST_ADDR");
    let mut client = TcpStream::connect(address).await.unwrap();
    client.write_all(&encoding_request()).await.unwrap();
    read_message(&mut client).await;
    client.write_all(&logon_request()).await.unwrap();
    let logon = read_message(&mut client).await;
    assert_eq!(logon[245], 1, "historical data must be advertised");
    assert_eq!(
        logon[248], 0,
        "multiple sequential history requests are supported"
    );
    let end = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        - 10;
    let history_seconds = env::var("RITHMIC_DTC_HISTORY_TEST_SECONDS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(120);
    let start = end - history_seconds;
    client
        .write_all(&historical_price_data_request(
            91, &symbol, &exchange, 0, start, end,
        ))
        .await
        .unwrap();

    let timeout_seconds = env::var("RITHMIC_PROBE_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(45);
    let deadline = Instant::now() + Duration::from_secs(timeout_seconds);
    let header = read_until_type(
        &mut client,
        801,
        deadline.saturating_duration_since(Instant::now()),
    )
    .await;
    assert_eq!(message_type(&header), 801);
    assert_eq!(i32::from_le_bytes(header[4..8].try_into().unwrap()), 91);
    assert_eq!(i32::from_le_bytes(header[8..12].try_into().unwrap()), 0);
    assert_eq!(header[13], 0, "recent active ES window must contain ticks");

    let mut count = 0_usize;
    let mut previous_time = f64::NEG_INFINITY;
    let mut classified = 0_usize;
    loop {
        let message = timeout(
            deadline.saturating_duration_since(Instant::now()),
            read_message(&mut client),
        )
        .await
        .expect("timed out waiting for historical tick records");
        if message_type(&message) == 3 {
            continue;
        }
        assert_eq!(message_type(&message), 804);
        let timestamp = read_f64(&message, 8);
        let price = read_f64(&message, 24);
        let volume = read_f64(&message, 32);
        let side = u16::from_le_bytes(message[16..18].try_into().unwrap());
        assert!(timestamp >= previous_time);
        assert!(price.is_finite() && price > 0.0);
        assert!(volume > 0.0);
        assert!(matches!(side, 0..=2));
        classified += usize::from(matches!(side, 1 | 2));
        previous_time = timestamp;
        count += 1;
        if message[40] == 1 {
            break;
        }
    }
    assert!(count > 0);
    assert!(
        classified > 0,
        "history must preserve at-bid/at-ask classification"
    );
    client.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires configured Rithmic Paper Trading credentials and ES history permission"]
async fn paper_trading_es_minute_history_flows_through_dtc_wire() {
    dotenvy::dotenv().ok();
    let symbol = env::var("RITHMIC_DTC_SYMBOL")
        .or_else(|_| env::var("RITHMIC_PROBE_SYMBOL"))
        .unwrap();
    let exchange = env::var("RITHMIC_DTC_EXCHANGE")
        .or_else(|_| env::var("RITHMIC_PROBE_EXCHANGE"))
        .unwrap();
    let address = env::var("DTC_TEST_ADDR").expect("run dtc_server and set DTC_TEST_ADDR");
    let mut client = TcpStream::connect(address).await.unwrap();
    client.write_all(&encoding_request()).await.unwrap();
    read_message(&mut client).await;
    client.write_all(&logon_request()).await.unwrap();
    let logon = read_message(&mut client).await;
    assert_eq!(logon[245], 1);

    let end = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        - 60;
    client
        .write_all(&historical_price_data_request(
            92,
            &symbol,
            &exchange,
            60,
            end - 1_800,
            end,
        ))
        .await
        .unwrap();
    let header = read_until_type(&mut client, 801, Duration::from_secs(60)).await;
    assert_eq!(message_type(&header), 801);
    assert_eq!(i32::from_le_bytes(header[8..12].try_into().unwrap()), 60);
    assert_eq!(header[13], 0);

    let mut count = 0;
    let mut previous_start = i64::MIN;
    loop {
        let message = read_message(&mut client).await;
        if message_type(&message) == 3 {
            continue;
        }
        assert_eq!(message_type(&message), 803);
        let start = i64::from_le_bytes(message[8..16].try_into().unwrap());
        let open = read_f64(&message, 16);
        let high = read_f64(&message, 24);
        let low = read_f64(&message, 32);
        let close = read_f64(&message, 40);
        let volume = read_f64(&message, 48);
        let num_trades = u32::from_le_bytes(message[56..60].try_into().unwrap());
        let bid_volume = read_f64(&message, 64);
        let ask_volume = read_f64(&message, 72);
        assert!(start >= previous_start);
        assert!(low <= open && open <= high);
        assert!(low <= close && close <= high);
        assert!(volume > 0.0 && num_trades > 0);
        assert!(bid_volume >= 0.0 && ask_volume >= 0.0);
        previous_start = start;
        count += 1;
        if message[80] == 1 {
            break;
        }
    }
    assert!(count >= 5, "expected several recent one-minute bars");

    client
        .write_all(&historical_price_data_request(
            93,
            &symbol,
            &exchange,
            60,
            end - 1_800,
            end,
        ))
        .await
        .unwrap();
    let second_header = read_until_type(&mut client, 801, Duration::from_secs(60)).await;
    assert_eq!(
        i32::from_le_bytes(second_header[4..8].try_into().unwrap()),
        93
    );
    let mut second_count = 0;
    loop {
        let message = read_message(&mut client).await;
        if message_type(&message) == 3 {
            continue;
        }
        assert_eq!(message_type(&message), 803);
        assert_eq!(i32::from_le_bytes(message[4..8].try_into().unwrap()), 93);
        second_count += 1;
        if message[80] == 1 {
            break;
        }
    }
    assert!(
        second_count >= 5,
        "the second history request on the same DTC connection must complete"
    );
    client.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a running DTC server and Rithmic Paper Trading daily-history permission"]
async fn paper_trading_daily_history_flows_through_dtc_wire() {
    dotenvy::dotenv().ok();
    let symbol = env::var("RITHMIC_DAILY_TEST_SYMBOL").unwrap_or_else(|_| "NQU6".to_owned());
    let exchange = env::var("RITHMIC_DAILY_TEST_EXCHANGE").unwrap_or_else(|_| "CME".to_owned());
    let address = env::var("DTC_TEST_ADDR").expect("run dtc_server and set DTC_TEST_ADDR");
    let mut client = TcpStream::connect(address).await.unwrap();
    client.write_all(&encoding_request()).await.unwrap();
    read_message(&mut client).await;
    client.write_all(&logon_request()).await.unwrap();
    let logon = read_message(&mut client).await;
    assert_eq!(logon[245], 1);

    let end = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    client
        .write_all(&historical_price_data_request(
            94,
            &symbol,
            &exchange,
            86_400,
            end - 30 * 86_400,
            end,
        ))
        .await
        .unwrap();
    let header = read_until_type(&mut client, 801, Duration::from_secs(60)).await;
    assert_eq!(i32::from_le_bytes(header[4..8].try_into().unwrap()), 94);
    assert_eq!(
        i32::from_le_bytes(header[8..12].try_into().unwrap()),
        86_400
    );
    assert_eq!(header[13], 0, "daily replay must return records");

    let mut count = 0;
    let mut previous_start = i64::MIN;
    loop {
        let message = read_message(&mut client).await;
        if message_type(&message) == 3 {
            continue;
        }
        assert_eq!(message_type(&message), 803);
        let start = i64::from_le_bytes(message[8..16].try_into().unwrap());
        let open = read_f64(&message, 16);
        let high = read_f64(&message, 24);
        let low = read_f64(&message, 32);
        let close = read_f64(&message, 40);
        assert!(start >= previous_start);
        assert!(open > 0.0 && low <= open && open <= high);
        assert!(close > 0.0 && low <= close && close <= high);
        previous_start = start;
        count += 1;
        if message[80] == 1 {
            break;
        }
    }
    assert!(count >= 3, "expected several NQ daily bars, got {count}");
    client.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "SUBMITS a one-lot far-away limit order through a trading-enabled DTC server"]
async fn paper_trading_order_lifecycle_flows_through_dtc_wire() {
    dotenvy::dotenv().ok();
    assert_eq!(
        env::var("RITHMIC_ENV")
            .unwrap_or_else(|_| "demo".to_owned())
            .to_ascii_lowercase(),
        "demo"
    );
    assert!(
        env::var("RITHMIC_RUN_ORDER_LIFECYCLE_TEST")
            .is_ok_and(|value| value.eq_ignore_ascii_case("true"))
    );
    let symbol = env::var("RITHMIC_DTC_SYMBOL")
        .or_else(|_| env::var("RITHMIC_PROBE_SYMBOL"))
        .unwrap();
    let exchange = env::var("RITHMIC_DTC_EXCHANGE")
        .or_else(|_| env::var("RITHMIC_PROBE_EXCHANGE"))
        .unwrap();
    let address = env::var("DTC_TEST_ADDR").expect("run trading-enabled dtc_server");
    let mut client = TcpStream::connect(address).await.unwrap();
    client.write_all(&encoding_request()).await.unwrap();
    read_message(&mut client).await;
    client.write_all(&logon_request()).await.unwrap();
    let logon = read_message(&mut client).await;
    assert_eq!(logon[237], 1, "DTC trading must be advertised");
    assert_eq!(logon[238], 0, "DTC OCO must remain unsupported");
    assert_eq!(logon[239], 1, "cancel/replace must be advertised");

    client
        .write_all(&trade_accounts_request(501))
        .await
        .unwrap();
    let account_message = read_until_type(&mut client, 401, Duration::from_secs(10)).await;
    let account = read_string(&account_message[12..44]);

    assert!(!account.is_empty());
    assert_eq!(
        i32::from_le_bytes(account_message[44..48].try_into().unwrap()),
        501
    );
    assert_eq!(
        i32::from_le_bytes(account_message[48..52].try_into().unwrap()),
        0
    );

    client
        .write_all(&market_data_request(77, &symbol, &exchange))
        .await
        .unwrap();
    read_until_type(&mut client, 104, Duration::from_secs(10)).await;
    let bbo = read_until_type(&mut client, 148, Duration::from_secs(30)).await;
    let bid = read_f64(&bbo, 8);
    let safe_price = ((bid - 100.0) * 4.0).floor() / 4.0;
    let client_id = format!("dtc-{}", std::process::id());
    client
        .write_all(&submit_limit_order(
            &symbol, &exchange, &account, &client_id, safe_price,
        ))
        .await
        .unwrap();

    let opened = read_order_update(
        &mut client,
        &client_id,
        None,
        Duration::from_secs(20),
        |message| {
            matches!(
                i32::from_le_bytes(message[224..228].try_into().unwrap()),
                2 | 4
            )
        },
    )
    .await;
    let basket_id = read_string(&opened[128..160]);
    assert!(!basket_id.is_empty());
    eprintln!("DTC Paper order accepted: basket_id={basket_id} price={safe_price}");

    let modified_price = safe_price - 0.25;
    client
        .write_all(&modify_limit_order(
            &basket_id,
            &client_id,
            &account,
            modified_price,
        ))
        .await
        .unwrap();
    let modified = timeout(Duration::from_secs(20), async {
        loop {
            let message = read_message(&mut client).await;
            if message_type(&message) == 301 {
                eprintln!(
                    "DTC order update: basket={} client={} status={} reason={} price={}",
                    read_string(&message[128..160]),
                    read_string(&message[160..192]),
                    i32::from_le_bytes(message[224..228].try_into().unwrap()),
                    i32::from_le_bytes(message[228..232].try_into().unwrap()),
                    read_f64(&message, 240),
                );
            }
            if message_type(&message) == 301
                && read_string(&message[128..160]) == basket_id
                && i32::from_le_bytes(message[224..228].try_into().unwrap()) == 4
                && i32::from_le_bytes(message[228..232].try_into().unwrap()) == 7
                && (read_f64(&message, 240) - modified_price).abs() < 0.001
            {
                break true;
            }
        }
    })
    .await
    .unwrap_or(false);

    client
        .write_all(&cancel_order(&basket_id, &client_id, &account))
        .await
        .unwrap();
    let canceled = timeout(Duration::from_secs(20), async {
        loop {
            let message = read_message(&mut client).await;
            if message_type(&message) == 301 {
                eprintln!(
                    "DTC cancel update: basket={} status={} reason={}",
                    read_string(&message[128..160]),
                    i32::from_le_bytes(message[224..228].try_into().unwrap()),
                    i32::from_le_bytes(message[228..232].try_into().unwrap()),
                );
            }
            if message_type(&message) == 301
                && read_string(&message[128..160]) == basket_id
                && i32::from_le_bytes(message[224..228].try_into().unwrap()) == 8
            {
                break true;
            }
        }
    })
    .await
    .unwrap_or(false);
    assert!(modified, "DTC cancel/replace confirmation was not received");
    assert!(canceled, "DTC cancellation confirmation was not received");
    eprintln!("DTC Paper order modified to {modified_price} and canceled");
    client.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "SUBMITS and cancels a far-away one-lot NQ Sell Stop on Paper"]
async fn paper_trading_stop_becomes_open_and_cancels_through_dtc_wire() {
    dotenvy::dotenv().ok();
    assert_eq!(
        env::var("RITHMIC_ENV")
            .unwrap_or_else(|_| "demo".to_owned())
            .to_ascii_lowercase(),
        "demo"
    );
    assert!(
        env::var("RITHMIC_RUN_STOP_LIFECYCLE_TEST")
            .is_ok_and(|value| value.eq_ignore_ascii_case("true"))
    );
    let symbol = env::var("RITHMIC_STOP_TEST_SYMBOL").unwrap_or_else(|_| "NQU6".to_owned());
    let exchange = env::var("RITHMIC_STOP_TEST_EXCHANGE").unwrap_or_else(|_| "CME".to_owned());
    let address = env::var("DTC_TEST_ADDR").expect("run trading-enabled dtc_server");
    let mut client = TcpStream::connect(address).await.unwrap();
    client.write_all(&encoding_request()).await.unwrap();
    read_message(&mut client).await;
    client.write_all(&logon_request()).await.unwrap();
    let logon = read_message(&mut client).await;
    assert_eq!(logon[237], 1);

    client
        .write_all(&trade_accounts_request(701))
        .await
        .unwrap();
    let account_message = read_until_type(&mut client, 401, Duration::from_secs(10)).await;
    let account = read_string(&account_message[12..44]);
    client
        .write_all(&market_data_request(79, &symbol, &exchange))
        .await
        .unwrap();
    read_until_type(&mut client, 104, Duration::from_secs(10)).await;
    let bbo = read_until_type(&mut client, 148, Duration::from_secs(30)).await;
    let bid = read_f64(&bbo, 8);
    let trigger = ((bid - 200.0) * 4.0).floor() / 4.0;
    let client_id = format!("dtc-stop-{}", std::process::id());
    client
        .write_all(&submit_stop_order(
            &symbol, &exchange, &account, &client_id, trigger,
        ))
        .await
        .unwrap();

    let opened = read_order_update(
        &mut client,
        &client_id,
        None,
        Duration::from_secs(20),
        |message| {
            i32::from_le_bytes(message[224..228].try_into().unwrap()) == 4
                && i32::from_le_bytes(message[228..232].try_into().unwrap()) == 2
                && i32::from_le_bytes(message[232..236].try_into().unwrap()) == 3
        },
    )
    .await;
    let basket_id = read_string(&opened[128..160]);
    assert!(!basket_id.is_empty());
    eprintln!(
        "DTC Paper Sell Stop is working Open: {}.{} basket={} trigger={trigger}",
        symbol, exchange, basket_id
    );

    client
        .write_all(&cancel_order(&basket_id, &client_id, &account))
        .await
        .unwrap();
    read_order_update(
        &mut client,
        &client_id,
        Some(&basket_id),
        Duration::from_secs(20),
        |message| i32::from_le_bytes(message[224..228].try_into().unwrap()) == 8,
    )
    .await;
    eprintln!("DTC Paper Sell Stop canceled: basket={basket_id}");
    client.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "read-only DTC Paper account, position, and balance snapshot"]
async fn paper_trading_account_position_balance_flow_through_dtc_wire() {
    dotenvy::dotenv().ok();
    let address = env::var("DTC_TEST_ADDR").expect("run trading-enabled dtc_server");
    let mut client = TcpStream::connect(address).await.unwrap();
    client.write_all(&encoding_request()).await.unwrap();
    read_message(&mut client).await;
    client.write_all(&logon_request()).await.unwrap();
    let logon = read_message(&mut client).await;
    assert_eq!(logon[237], 1);
    client
        .write_all(&trade_accounts_request(601))
        .await
        .unwrap();
    let account_message = read_until_type(&mut client, 401, Duration::from_secs(10)).await;
    let account = read_string(&account_message[12..44]);

    client
        .write_all(&open_orders_request(604, &account))
        .await
        .unwrap();
    let orders = read_until_type(&mut client, 301, Duration::from_secs(10)).await;
    assert_eq!(
        orders[520], 1,
        "completed/canceled snapshot orders must not be returned as open"
    );
    eprintln!("DTC Paper open-order snapshot: none");

    client
        .write_all(&current_positions_request(602, &account))
        .await
        .unwrap();
    let position = read_until_type(&mut client, 306, Duration::from_secs(10)).await;
    let no_positions = position[176] != 0;
    if no_positions {
        eprintln!("DTC Paper position snapshot: flat");
    } else {
        eprintln!(
            "DTC Paper position snapshot: {}.{} quantity={} average={} open_pnl={}",
            read_string(&position[16..80]),
            read_string(&position[80..96]),
            read_f64(&position, 96),
            read_f64(&position, 104),
            read_f64(&position, 200),
        );
    }

    client
        .write_all(&account_balance_request(603, &account))
        .await
        .unwrap();
    let balance = read_until_type(&mut client, 600, Duration::from_secs(10)).await;
    eprintln!(
        "DTC Paper balance: cash={} available={} open_pnl={} daily_pnl={}",
        read_f64(&balance, 8),
        read_f64(&balance, 16),
        read_f64(&balance, 96),
        read_f64(&balance, 104),
    );
    client.shutdown().await.unwrap();
}

fn encoding_request() -> [u8; 16] {
    let mut message = [0_u8; 16];
    put_u16(&mut message, 0, 16);
    put_u16(&mut message, 2, 6);
    put_i32(&mut message, 4, 8);
    put_i32(&mut message, 8, 0);
    message[12..15].copy_from_slice(b"DTC");
    message
}

fn logon_request() -> [u8; 284] {
    let mut message = [0_u8; 284];
    put_u16(&mut message, 0, 284);
    put_u16(&mut message, 2, 1);
    put_i32(&mut message, 4, 8);
    put_i32(&mut message, 144, 30);
    message
}

fn security_definition_request(symbol: &str, exchange: &str) -> [u8; 88] {
    let mut message = [0_u8; 88];
    put_u16(&mut message, 0, 88);
    put_u16(&mut message, 2, 506);
    put_i32(&mut message, 4, 1);
    put_string(&mut message[8..72], symbol);
    put_string(&mut message[72..88], exchange);
    message
}

fn exchange_list_request(request_id: i32) -> [u8; 8] {
    let mut message = [0_u8; 8];
    put_u16(&mut message, 0, 8);
    put_u16(&mut message, 2, 500);
    put_i32(&mut message, 4, request_id);
    message
}

fn symbol_search_request(request_id: i32, search_text: &str, exchange: &str) -> [u8; 96] {
    let mut message = [0_u8; 96];
    put_u16(&mut message, 0, 96);
    put_u16(&mut message, 2, 508);
    put_i32(&mut message, 4, request_id);
    put_string(&mut message[8..72], search_text);
    put_string(&mut message[72..88], exchange);
    put_i32(&mut message, 88, 1); // SECURITY_TYPE_FUTURES
    put_i32(&mut message, 92, 1); // SEARCH_BY_SYMBOL
    message
}

fn market_data_request(symbol_id: u32, symbol: &str, exchange: &str) -> [u8; 96] {
    let mut message = [0_u8; 96];
    put_u16(&mut message, 0, 96);
    put_u16(&mut message, 2, 101);
    put_i32(&mut message, 4, 1);
    message[8..12].copy_from_slice(&symbol_id.to_le_bytes());
    put_string(&mut message[12..76], symbol);
    put_string(&mut message[76..92], exchange);
    message
}

fn market_depth_request(symbol_id: u32, levels: i32, symbol: &str, exchange: &str) -> [u8; 96] {
    let mut message = [0_u8; 96];
    put_u16(&mut message, 0, 96);
    put_u16(&mut message, 2, 102);
    put_i32(&mut message, 4, 1);
    message[8..12].copy_from_slice(&symbol_id.to_le_bytes());
    put_string(&mut message[12..76], symbol);
    put_string(&mut message[76..92], exchange);
    put_i32(&mut message, 92, levels);
    message
}

fn historical_price_data_request(
    request_id: i32,
    symbol: &str,
    exchange: &str,
    interval: i32,
    start: i64,
    end: i64,
) -> [u8; 128] {
    let mut message = [0_u8; 128];
    put_u16(&mut message, 0, 128);
    put_u16(&mut message, 2, 800);
    put_i32(&mut message, 4, request_id);
    put_string(&mut message[8..72], symbol);
    put_string(&mut message[72..88], exchange);
    put_i32(&mut message, 88, interval);
    message[96..104].copy_from_slice(&start.to_le_bytes());
    message[104..112].copy_from_slice(&end.to_le_bytes());
    message
}

fn trade_accounts_request(request_id: i32) -> [u8; 8] {
    let mut message = [0_u8; 8];
    put_u16(&mut message, 0, 8);
    put_u16(&mut message, 2, 400);
    put_i32(&mut message, 4, request_id);
    message
}

fn current_positions_request(request_id: i32, account: &str) -> [u8; 40] {
    let mut message = [0_u8; 40];
    put_u16(&mut message, 0, 40);
    put_u16(&mut message, 2, 305);
    put_i32(&mut message, 4, request_id);
    put_string(&mut message[8..40], account);
    message
}

fn open_orders_request(request_id: i32, account: &str) -> [u8; 76] {
    let mut message = [0_u8; 76];
    put_u16(&mut message, 0, 76);
    put_u16(&mut message, 2, 300);
    put_i32(&mut message, 4, request_id);
    put_i32(&mut message, 8, 1);
    put_string(&mut message[44..76], account);
    message
}

fn account_balance_request(request_id: i32, account: &str) -> [u8; 40] {
    let mut message = [0_u8; 40];
    put_u16(&mut message, 0, 40);
    put_u16(&mut message, 2, 601);
    put_i32(&mut message, 4, request_id);
    put_string(&mut message[8..40], account);
    message
}

fn submit_limit_order(
    symbol: &str,
    exchange: &str,
    account: &str,
    client_id: &str,
    price: f64,
) -> [u8; 304] {
    let mut message = [0_u8; 304];
    put_u16(&mut message, 0, 304);
    put_u16(&mut message, 2, 208);
    put_string(&mut message[4..68], symbol);
    put_string(&mut message[68..84], exchange);
    put_string(&mut message[84..116], account);
    put_string(&mut message[116..148], client_id);
    put_i32(&mut message, 148, 2);
    put_i32(&mut message, 152, 1);
    put_f64(&mut message, 160, price);
    put_f64(&mut message, 176, 1.0);
    put_i32(&mut message, 184, 1);
    message
}

fn submit_stop_order(
    symbol: &str,
    exchange: &str,
    account: &str,
    client_id: &str,
    trigger: f64,
) -> [u8; 304] {
    let mut message = [0_u8; 304];
    put_u16(&mut message, 0, 304);
    put_u16(&mut message, 2, 208);
    put_string(&mut message[4..68], symbol);
    put_string(&mut message[68..84], exchange);
    put_string(&mut message[84..116], account);
    put_string(&mut message[116..148], client_id);
    put_i32(&mut message, 148, 3); // DTC STOP_MARKET
    put_i32(&mut message, 152, 2); // SELL
    put_f64(&mut message, 160, trigger);
    put_f64(&mut message, 176, 1.0);
    put_i32(&mut message, 184, 1); // DAY
    message
}

fn modify_limit_order(basket_id: &str, client_id: &str, account: &str, price: f64) -> [u8; 192] {
    let mut message = [0_u8; 192];
    put_u16(&mut message, 0, 192);
    put_u16(&mut message, 2, 204);
    put_string(&mut message[4..36], basket_id);
    put_string(&mut message[36..68], client_id);
    put_f64(&mut message, 72, price);
    put_f64(&mut message, 88, 1.0);
    message[96] = 1;
    put_i32(&mut message, 104, 1);
    put_string(&mut message[121..153], account);
    message
}

fn cancel_order(basket_id: &str, client_id: &str, account: &str) -> [u8; 100] {
    let mut message = [0_u8; 100];
    put_u16(&mut message, 0, 100);
    put_u16(&mut message, 2, 203);
    put_string(&mut message[4..36], basket_id);
    put_string(&mut message[36..68], client_id);
    put_string(&mut message[68..100], account);
    message
}

async fn read_security_definition(
    stream: &mut TcpStream,
    request_id: i32,
    duration: Duration,
) -> Vec<u8> {
    timeout(duration, async {
        loop {
            let message = read_message(stream).await;
            if message_type(&message) == 507
                && i32::from_le_bytes(message[4..8].try_into().unwrap()) == request_id
            {
                break message;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for Security Definition {request_id}"))
}

async fn read_until_type(stream: &mut TcpStream, wanted: u16, duration: Duration) -> Vec<u8> {
    let mut seen = Vec::new();
    timeout(duration, async {
        loop {
            let message = read_message(stream).await;
            let received = message_type(&message);
            if received == wanted {
                break message;
            }
            seen.push(received);
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("timed out waiting for DTC message type {wanted}; received {seen:?}")
    })
}

async fn read_order_update<F>(
    stream: &mut TcpStream,
    client_id: &str,
    basket_id: Option<&str>,
    duration: Duration,
    predicate: F,
) -> Vec<u8>
where
    F: Fn(&[u8]) -> bool,
{
    timeout(duration, async {
        loop {
            let message = read_message(stream).await;
            if message_type(&message) == 301
                && read_string(&message[160..192]) == client_id
                && basket_id.is_none_or(|id| read_string(&message[128..160]) == id)
                && predicate(&message)
            {
                break message;
            }
        }
    })
    .await
    .expect("timed out waiting for matching DTC ORDER_UPDATE")
}

async fn read_message(stream: &mut TcpStream) -> Vec<u8> {
    let mut header = [0_u8; 4];
    stream.read_exact(&mut header).await.unwrap();
    let size = u16::from_le_bytes(header[..2].try_into().unwrap()) as usize;
    let mut message = vec![0_u8; size];
    message[..4].copy_from_slice(&header);
    stream.read_exact(&mut message[4..]).await.unwrap();
    message
}

async fn read_without_session_status(stream: &mut TcpStream) -> Vec<u8> {
    loop {
        let message = read_message(stream).await;
        if !matches!(
            message_type(&message),
            dtc::HEARTBEAT | dtc::MARKET_DATA_FEED_STATUS
        ) {
            return message;
        }
    }
}

fn message_type(message: &[u8]) -> u16 {
    u16::from_le_bytes(message[2..4].try_into().unwrap())
}

fn read_f64(message: &[u8], offset: usize) -> f64 {
    f64::from_le_bytes(message[offset..offset + 8].try_into().unwrap())
}

fn read_string(bytes: &[u8]) -> String {
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    String::from_utf8(bytes[..end].to_vec()).unwrap()
}

fn put_u16(target: &mut [u8], offset: usize, value: u16) {
    target[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_i32(target: &mut [u8], offset: usize, value: i32) {
    target[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_f64(target: &mut [u8], offset: usize, value: f64) {
    target[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn put_string(target: &mut [u8], value: &str) {
    let bytes = value.as_bytes();
    target[..bytes.len()].copy_from_slice(bytes);
}
