use super::*;

#[test]
fn incomplete_tail_fields_use_whole_field_defaults() {
    for (kind, size, offset, width) in [
        (HISTORICAL_PRICE_DATA_REQUEST, 114, 112, 4),
        (MARKET_DEPTH_REQUEST, 94, 92, 4),
        (CANCEL_REPLACE_ORDER, 106, 104, 4),
        (CANCEL_ORDER, 75, 68, 32),
        (SUBMIT_NEW_SINGLE_ORDER, 196, 192, 8),
    ] {
        let mut bytes = request(size, kind);
        bytes[offset..size].fill(0xff);
        let frame = decode_frame(kind, bytes).unwrap();
        assert!(frame.bytes[offset..offset + width].iter().all(|b| *b == 0));
        assert_eq!(
            u16::from_le_bytes(frame.bytes[..2].try_into().unwrap()),
            size as u16
        );
    }
    let mut bytes = request(116, HISTORICAL_PRICE_DATA_REQUEST);
    put_u32(&mut bytes, 112, 2);
    assert_eq!(
        read_u32(
            &decode_frame(HISTORICAL_PRICE_DATA_REQUEST, bytes)
                .unwrap()
                .bytes,
            112
        ),
        2
    );
}

#[tokio::test]
async fn underlying_fallback_does_not_leak_contract_metadata() {
    let mut instrument = Instrument::es("ESU6", "CME").unwrap();
    instrument.expiration_date = 12345;
    instrument.exchange_symbol = "EXCHANGE-ESU6".into();
    let items = catalog_enumerate(None, &instrument, "CME", "", true)
        .await
        .unwrap();
    let bytes = security_definition_response(9, &items[0]);
    assert!(read_fixed_string(&bytes[8..72]).unwrap().is_empty());
    assert!(read_fixed_string(&bytes[260..324]).unwrap().is_empty());
    assert_eq!(read_u32(&bytes, 228), 0);
    assert_eq!(read_i32(&bytes, 160), -1);
    assert_eq!(read_fixed_string(&bytes[180..212]).unwrap(), "ES");
    let bytes = security_definition_response(10, &instrument);
    assert_eq!(read_u32(&bytes, 228), 12345);
    assert_eq!(
        read_fixed_string(&bytes[260..324]).unwrap(),
        "EXCHANGE-ESU6"
    );
}

#[tokio::test]
async fn empty_final_history_batch_marks_last_real_record_and_allows_next_request() {
    let (commands, mut rx) = mpsc::channel(1);
    let mut stream = connect_services(None, Some(HistoryDataClient::new(commands))).await;
    let worker = tokio::spawn(async move {
        for _ in 0..2 {
            let (request, output) = rx.recv().await.unwrap();
            for (records, is_final) in [
                (Vec::new(), false),
                (
                    vec![HistoricalRecord::Tick {
                        datetime_us: 1_700_000_000_000_000,
                        price: 6000.0,
                        volume: 1.0,
                        at_bid_or_ask: 2,
                    }],
                    false,
                ),
                (Vec::new(), true),
            ] {
                output
                    .send(Ok(HistoricalResponse {
                        request_id: request.request_id,
                        record_interval: 0,
                        records,
                        is_final,
                    }))
                    .await
                    .unwrap();
            }
        }
    });
    for id in [91, 92] {
        let mut bytes = request(128, HISTORICAL_PRICE_DATA_REQUEST);
        put_i32(&mut bytes, 4, id);
        put_fixed_string(&mut bytes[8..72], "ESU6");
        put_fixed_string(&mut bytes[72..88], "CME");
        stream.write_all(&bytes).await.unwrap();
        for kind in [
            HISTORICAL_PRICE_DATA_RESPONSE_HEADER,
            HISTORICAL_PRICE_DATA_TICK_RECORD_RESPONSE,
        ] {
            let frame = time::timeout(Duration::from_secs(2), read_frame(&mut stream))
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(frame.message_type, kind);
            assert_eq!(read_i32(&frame.bytes, 4), id);
            if kind == HISTORICAL_PRICE_DATA_TICK_RECORD_RESPONSE {
                assert_eq!(frame.bytes[40], 1);
                assert_eq!(read_f64(&frame.bytes, 24), 6000.0);
            }
        }
    }
    stream
        .write_all(&logoff_message("done", false))
        .await
        .unwrap();
    worker.await.unwrap();
}

async fn connect_services(
    market: Option<MarketDataClient>,
    history: Option<HistoryDataClient>,
) -> TcpStream {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        handle_connection_with_services(
            stream,
            Instrument::es("ESU6", "CME").unwrap(),
            market,
            history,
        )
        .await
        .unwrap();
    });
    let mut stream = TcpStream::connect(address).await.unwrap();
    let mut logon = request(284, LOGON_REQUEST);
    put_i32(&mut logon, 4, 8);
    put_i32(&mut logon, 144, 5);
    stream.write_all(&logon).await.unwrap();
    let response = read_frame(&mut stream).await.unwrap().unwrap();
    assert_eq!(response.message_type, LOGON_RESPONSE);
    stream
}

#[tokio::test]
async fn slow_catalog_query_keeps_heartbeat_and_queued_requests_alive() {
    let (commands, mut rx) = mpsc::channel(1);
    let (_events, events_rx) = mpsc::channel(1);
    let mut stream = connect_services(Some(MarketDataClient::new(commands, events_rx)), None).await;
    let (release, wait) = oneshot::channel();
    let worker = tokio::spawn(async move {
        let Some(MarketCommand::EnumerateCatalog { response, .. }) = rx.recv().await else {
            panic!("enumeration expected")
        };
        wait.await.unwrap();
        response
            .send(Ok(vec![Instrument::es("ESU6", "CME").unwrap()]))
            .unwrap();
    });
    let mut list = request(96, SYMBOLS_FOR_EXCHANGE_REQUEST);
    put_i32(&mut list, 4, 71);
    put_i32(&mut list, 28, 1);
    stream.write_all(&list).await.unwrap();
    let mut exact = request(88, SECURITY_DEFINITION_FOR_SYMBOL_REQUEST);
    put_i32(&mut exact, 4, 72);
    put_fixed_string(&mut exact[8..72], "ESU6");
    put_fixed_string(&mut exact[72..88], "CME");
    stream.write_all(&exact).await.unwrap();
    let heartbeat = time::timeout(Duration::from_secs(7), read_frame(&mut stream))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(heartbeat.message_type, HEARTBEAT);
    stream.write_all(&heartbeat_message()).await.unwrap();
    release.send(()).unwrap();
    for id in [71, 72] {
        let response = time::timeout(Duration::from_secs(2), read_frame(&mut stream))
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(response.message_type, SECURITY_DEFINITION_RESPONSE);
        assert_eq!(read_i32(&response.bytes, 4), id);
    }
    stream
        .write_all(&logoff_message("done", false))
        .await
        .unwrap();
    worker.await.unwrap();
}

#[tokio::test]
async fn history_download_outlives_client_heartbeat_timeout() {
    let (commands, mut rx) = mpsc::channel(1);
    let mut stream = connect_services(None, Some(HistoryDataClient::new(commands))).await;
    let worker = tokio::spawn(async move {
        let (request, output) = rx.recv().await.unwrap();
        let record = HistoricalRecord::Tick {
            datetime_us: 1_700_000_000_000_000,
            price: 6000.0,
            volume: 1.0,
            at_bid_or_ask: 2,
        };
        output
            .send(Ok(HistoricalResponse {
                request_id: request.request_id,
                record_interval: 0,
                records: vec![record.clone()],
                is_final: false,
            }))
            .await
            .unwrap();
        time::sleep(Duration::from_secs(11)).await; // exceeds the negotiated 2 x 5 seconds
        output
            .send(Ok(HistoricalResponse {
                request_id: request.request_id,
                record_interval: 0,
                records: vec![record],
                is_final: true,
            }))
            .await
            .unwrap();
    });
    let mut history = request(120, HISTORICAL_PRICE_DATA_REQUEST);
    put_i32(&mut history, 4, 81);
    put_fixed_string(&mut history[8..72], "ESU6");
    put_fixed_string(&mut history[72..88], "CME");
    stream.write_all(&history).await.unwrap();
    time::timeout(Duration::from_secs(15), async {
        loop {
            let response = read_frame(&mut stream)
                .await
                .unwrap()
                .expect("download connection must remain open");
            assert_ne!(response.message_type, LOGOFF);
            if response.message_type == HISTORICAL_PRICE_DATA_TICK_RECORD_RESPONSE
                && response.bytes[40] == 1
            {
                break;
            }
        }
    })
    .await
    .unwrap();
    stream
        .write_all(&logoff_message("done", false))
        .await
        .unwrap();
    worker.await.unwrap();
}

fn request(size: usize, kind: u16) -> Vec<u8> {
    let mut bytes = vec![0; size];
    put_u16(&mut bytes, 0, size as u16);
    put_u16(&mut bytes, 2, kind);
    bytes
}

fn single_order() -> Vec<u8> {
    let mut bytes = request(304, SUBMIT_NEW_SINGLE_ORDER);
    put_fixed_string(&mut bytes[4..68], "ESU6");
    put_fixed_string(&mut bytes[68..84], "CME");
    put_fixed_string(&mut bytes[84..116], "paper-account");
    put_fixed_string(&mut bytes[116..148], "client-order");
    put_i32(&mut bytes, 148, 2);
    put_i32(&mut bytes, 152, 1);
    put_f64(&mut bytes, 160, 6000.0);
    put_f64(&mut bytes, 176, 1.0);
    put_i32(&mut bytes, 184, 1);
    bytes
}

#[tokio::test]
async fn bracket_parent_is_rejected_before_sending_any_order() {
    let (commands, mut rx) = mpsc::channel(1);
    let mut bytes = single_order();
    bytes[201] = 1;
    let response = handle_trading_request(
        SUBMIT_NEW_SINGLE_ORDER,
        &bytes,
        &Instrument::es("ESU6", "CME").unwrap(),
        None,
        Some(&commands),
    )
    .await
    .unwrap();
    assert_eq!(read_i32(&response[0], 228), 8);
    assert_eq!(
        read_fixed_string(&response[0][160..192]).unwrap(),
        "client-order"
    );
    assert_eq!(read_fixed_string(&response[0][16..80]).unwrap(), "ESU6");
    assert_eq!(read_fixed_string(&response[0][80..96]).unwrap(), "CME");
    assert!(matches!(
        rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn incompatible_contract_tick_is_rejected_before_submission() {
    let (commands, mut rx) = mpsc::channel(1);
    let mut instrument = Instrument::es("ESU6", "CME").unwrap();
    instrument.min_price_increment = 0.01;
    let response = handle_trading_request(
        SUBMIT_NEW_SINGLE_ORDER,
        &single_order(),
        &instrument,
        None,
        Some(&commands),
    )
    .await
    .unwrap();
    assert_eq!(read_i32(&response[0], 228), 8);
    assert!(matches!(
        rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn failed_action_keeps_current_status_and_clears_unknown_server_id() {
    for (reason, status) in [(9, 4), (10, 4), (9, 8)] {
        let (commands, mut rx) = mpsc::channel(1);
        let mut order = tests_order();
        order.order_status = status;
        tokio::spawn(async move {
            let Some(TradingCommand::OrderState(id, response)) = rx.recv().await else {
                panic!("state expected")
            };
            assert_eq!(id, "server-order");
            response.send(Ok(Some(order))).unwrap();
        });
        let response = reject_order_action(
            Some(&commands),
            "server-order",
            "client-order",
            "paper-account",
            reason,
            "cannot modify",
        )
        .await;
        assert_eq!(read_i32(&response, 224), status);
        assert_eq!(read_i32(&response, 228), reason);
        assert_eq!(read_f64(&response, 320), f64::MAX);
    }
    let (commands, mut rx) = mpsc::channel(1);
    tokio::spawn(async move {
        let Some(TradingCommand::OrderState(_, response)) = rx.recv().await else {
            panic!("state expected")
        };
        response.send(Ok(None)).unwrap();
    });
    let response = reject_order_action(
        Some(&commands),
        "unknown",
        "client-order",
        "paper-account",
        9,
        "unknown order",
    )
    .await;
    assert_eq!(read_i32(&response, 224), 9);
    assert!(read_fixed_string(&response[128..160]).unwrap().is_empty());
    assert_eq!(
        read_fixed_string(&response[160..192]).unwrap(),
        "client-order"
    );
}

fn tests_order() -> TradingOrder {
    TradingOrder {
        request_id: 0,
        symbol: "ESU6".into(),
        exchange: "CME".into(),
        account_id: "paper-account".into(),
        client_order_id: "client-order".into(),
        server_order_id: "server-order".into(),
        exchange_order_id: String::new(),
        order_status: 4,
        update_reason: 3,
        order_type: 2,
        buy_sell: 1,
        price1: 6000.0,
        price2: f64::MAX,
        quantity: 1.0,
        filled_quantity: 0.0,
        remaining_quantity: 1.0,
        average_fill_price: f64::MAX,
        last_fill_price: 5999.0,
        last_fill_quantity: 1.0,
        last_fill_datetime_ms: 1,
        last_fill_execution_id: "old-fill".into(),
        info_text: String::new(),
        time_in_force: 1,
        is_snapshot: false,
    }
}

#[tokio::test]
async fn one_shot_market_and_depth_requests_use_no_subscribe_command() {
    let instrument = Instrument::es("ESU6", "CME").unwrap();
    let (commands, mut rx) = mpsc::channel(2);
    let worker = tokio::spawn(async move {
        let Some(MarketCommand::Snapshot { response, .. }) = rx.recv().await else {
            panic!("one-shot snapshot expected")
        };
        response
            .send(Ok(MarketSnapshot {
                bid: Some(6000.0),
                ask: Some(6000.25),
                volume: Some(100.0),
                ..Default::default()
            }))
            .unwrap();
        let Some(MarketCommand::DepthSnapshot {
            response,
            max_levels,
            ..
        }) = rx.recv().await
        else {
            panic!("one-shot depth expected")
        };
        assert_eq!(max_levels, 10);
        response.send(Ok(Vec::new())).unwrap();
    });
    let mut bytes = request(96, MARKET_DATA_REQUEST);
    put_i32(&mut bytes, 4, SNAPSHOT);
    put_u32(&mut bytes, 8, 25);
    put_fixed_string(&mut bytes[12..76], "ESU6");
    put_fixed_string(&mut bytes[76..92], "CME");
    let response = handle_market_data_request(&bytes, &instrument, Some(&commands))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read_u32(&response, 4), 25);
    assert_eq!(read_f64(&response, 56), 6000.0);
    assert_eq!(read_f64(&response, 40), 100.0);
    put_u16(&mut bytes, 2, MARKET_DEPTH_REQUEST);
    put_i32(&mut bytes, 92, 10);
    let response = handle_market_depth_request(&bytes, &instrument, Some(&commands))
        .await
        .unwrap();
    assert_eq!(response.len(), 1);
    assert_eq!(&response[0][34..36], &[1, 1]);
    worker.await.unwrap();
}

#[tokio::test]
async fn enumeration_uses_all_contracts_and_search_honors_description() {
    let es = Instrument::es("ESU6", "CME").unwrap();
    let mut nq = es.clone();
    nq.symbol = "NQU6".into();
    nq.underlying_symbol = "NQ".into();
    nq.description = "Nasdaq futures".into();
    let mut esz = es.clone();
    esz.symbol = "ESZ6".into();
    let (commands, mut rx) = mpsc::channel(4);
    let worker = tokio::spawn(async move {
        for _ in 0..4 {
            match rx.recv().await.unwrap() {
                MarketCommand::EnumerateCatalog {
                    underlying,
                    roots_only,
                    response,
                    ..
                } => {
                    let items = if underlying == "ES" {
                        vec![es.clone(), esz.clone()]
                    } else if roots_only {
                        vec![es.clone(), nq.clone()]
                    } else {
                        vec![es.clone(), esz.clone(), nq.clone()]
                    };
                    response.send(Ok(items)).unwrap();
                }
                MarketCommand::SearchCatalog {
                    search_type,
                    response,
                    ..
                } => {
                    assert_eq!(search_type, 2);
                    response.send(Ok(vec![es.clone(), nq.clone()])).unwrap();
                }
                _ => panic!("unexpected command"),
            }
        }
    });
    let fallback = Instrument::es("ESU6", "CME").unwrap();
    let mut bytes = request(96, SYMBOLS_FOR_EXCHANGE_REQUEST);
    put_i32(&mut bytes, 4, 10);
    put_i32(&mut bytes, 28, SUBSCRIBE);
    put_fixed_string(&mut bytes[8..24], "CME");
    let response = handle_symbol_discovery_request(
        SYMBOLS_FOR_EXCHANGE_REQUEST,
        &bytes,
        &fallback,
        Some(&commands),
    )
    .await
    .unwrap();
    assert_eq!(response.len(), 3);
    assert_eq!(response[0][168], 0);
    assert_eq!(response[2][168], 1);
    let response = handle_symbol_discovery_request(
        UNDERLYING_SYMBOLS_FOR_EXCHANGE_REQUEST,
        &bytes[..28],
        &fallback,
        Some(&commands),
    )
    .await
    .unwrap();
    assert_eq!(response.len(), 2);
    assert!(read_fixed_string(&response[1][8..72]).unwrap().is_empty());
    let mut bytes = request(60, SYMBOLS_FOR_UNDERLYING_REQUEST);
    put_fixed_string(&mut bytes[8..40], "ES");
    put_fixed_string(&mut bytes[40..56], "CME");
    let response = handle_symbol_discovery_request(
        SYMBOLS_FOR_UNDERLYING_REQUEST,
        &bytes,
        &fallback,
        Some(&commands),
    )
    .await
    .unwrap();
    assert_eq!(response.len(), 2);
    assert_eq!(read_fixed_string(&response[1][8..72]).unwrap(), "ESZ6");
    let mut bytes = request(96, SYMBOL_SEARCH_REQUEST);
    put_fixed_string(&mut bytes[8..72], "Nasdaq");
    put_i32(&mut bytes, 92, 2);
    let response =
        handle_symbol_discovery_request(SYMBOL_SEARCH_REQUEST, &bytes, &fallback, Some(&commands))
            .await
            .unwrap();
    assert_eq!(response.len(), 1);
    assert_eq!(read_fixed_string(&response[0][8..72]).unwrap(), "NQU6");
    worker.await.unwrap();
}

#[test]
fn empty_definition_preserves_official_nonzero_defaults() {
    let response = empty_security_definition_response(51);
    assert_eq!(read_i32(&response, 160), -1);
    for offset in [172, 176, 256] {
        assert_eq!(
            f32::from_le_bytes(response[offset..offset + 4].try_into().unwrap()),
            1.0
        );
    }
    assert_eq!(response[168], 1);
}

#[test]
fn partial_statistics_and_quotes_merge_and_keep_unknowns_unset() {
    use rithmic_rs::rti::{
        BestBidOffer, EndOfDayPrices, LastTrade, OpenInterest, TradeStatistics,
        messages::RithmicMessage,
    };
    let mut snapshot = MarketSnapshot::default();
    let mut bid = BestBidOffer::default();
    bid.bid_price = Some(6000.0);
    bid.bid_size = Some(3);
    snapshot.apply(&RithmicMessage::BestBidOffer(bid));
    let mut ask = BestBidOffer::default();
    ask.ask_price = Some(6000.25);
    ask.ask_size = Some(4);
    snapshot.apply(&RithmicMessage::BestBidOffer(ask));
    let mut stats = TradeStatistics::default();
    stats.open_price = Some(5900.0);
    stats.high_price = Some(6010.0);
    stats.low_price = Some(5800.0);
    snapshot.apply(&RithmicMessage::TradeStatistics(stats));
    let mut volume = LastTrade::default();
    volume.volume = Some(12345);
    snapshot.apply(&RithmicMessage::LastTrade(volume));
    let mut oi = OpenInterest::default();
    oi.open_interest = Some(999);
    snapshot.apply(&RithmicMessage::OpenInterest(oi));
    let mut settle = EndOfDayPrices::default();
    settle.settlement_price = Some(5950.0);
    settle.settlement_date = Some("20260903".into());
    snapshot.apply(&RithmicMessage::EndOfDayPrices(settle));
    let bytes = market_data_snapshot(1, &snapshot);
    assert_eq!(read_f64(&bytes, 8), 5950.0);
    assert_eq!(read_f64(&bytes, 16), 5900.0);
    assert_eq!(read_f64(&bytes, 24), 6010.0);
    assert_eq!(read_f64(&bytes, 32), 5800.0);
    assert_eq!(read_f64(&bytes, 40), 12345.0);
    assert_eq!(read_u32(&bytes, 52), 999);
    assert_eq!(read_f64(&bytes, 56), 6000.0);
    assert_eq!(read_f64(&bytes, 64), 6000.25);
    assert_eq!(read_f64(&bytes, 88), f64::MAX); // volume-only messages are not trades
    assert_eq!(read_u32(&bytes, 48), u32::MAX); // no fabricated total trade count
    let mut clear = TradeStatistics::default();
    clear.clear_bits = Some(7);
    snapshot.apply(&RithmicMessage::TradeStatistics(clear));
    assert_eq!(read_f64(&market_data_snapshot(1, &snapshot), 24), f64::MAX);
    assert_eq!(crate::market_data::date_to_unix("19700101"), Some(0));
    assert_eq!(
        crate::market_data::date_to_unix("20240229"),
        Some(1709164800)
    );
    assert_eq!(crate::market_data::date_to_unix("20260229"), None);
    assert_eq!(crate::market_data::date_to_unix("junk20260918"), None);
    assert_eq!(
        crate::market_data::date_to_unix("2026-09-18"),
        crate::market_data::date_to_unix("20260918")
    );
}
