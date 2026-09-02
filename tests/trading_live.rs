use rithmic_dtc_bridge::identity::synthetic_mac;
use rithmic_rs::{
    ConnectStrategy, LoginConfig, ManualOrAutoEntry, OrderSide, OrderType, RithmicAccount,
    RithmicCancelOrder, RithmicConfig, RithmicEnv, RithmicExitPosition, RithmicModifyOrder,
    RithmicOrder, RithmicOrderPlant, RithmicPnlPlant, RithmicTickerPlant,
    rti::messages::RithmicMessage,
};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires configured Rithmic Paper Trading credentials and trading permission"]
async fn paper_trading_order_plant_discovers_account_and_cme_route() {
    dotenvy::dotenv().ok();
    assert_eq!(
        std::env::var("RITHMIC_ENV")
            .unwrap_or_else(|_| "demo".to_owned())
            .to_ascii_lowercase(),
        "demo",
        "live trading tests are restricted to Paper Trading"
    );

    let config = RithmicConfig::from_env(RithmicEnv::Demo).unwrap();
    // Login and account discovery do not need a selected account. Any method that
    // can alter trading state is deliberately absent from this read-only test.
    let discovery_account = RithmicAccount::new("", "", "");
    let plant = RithmicOrderPlant::connect(&config, ConnectStrategy::Simple)
        .await
        .unwrap();
    let handle = plant.get_handle(&discovery_account);
    let mut login = LoginConfig::default();
    login.mac_addr = Some(vec![synthetic_mac()]);
    handle.login_with_config(login).await.unwrap();

    let responses = handle.get_account_list().await.unwrap();
    let accounts: Vec<_> = responses
        .iter()
        .map(|response| {
            if let Some(error) = &response.error {
                panic!("Rithmic account-list permission failed: {error}");
            }
            match &response.message {
                RithmicMessage::ResponseAccountList(account) => account,
                other => panic!("unexpected account-list response: {other:?}"),
            }
        })
        .filter(|account| {
            account
                .account_id
                .as_deref()
                .is_some_and(|id| !id.is_empty())
        })
        .collect();
    assert!(
        !accounts.is_empty(),
        "Order Plant login succeeded but returned no trading account"
    );

    let route = handle.trade_route_for("CME").await.unwrap();
    assert!(
        !route.is_empty(),
        "Rithmic returned an empty CME trade route"
    );
    for account in accounts {
        eprintln!(
            "Paper account discovered: account_id={} fcm_id={} ib_id={} currency={}",
            account.account_id.as_deref().unwrap_or(""),
            account.fcm_id.as_deref().unwrap_or(""),
            account.ib_id.as_deref().unwrap_or(""),
            account.account_currency.as_deref().unwrap_or("")
        );
    }
    eprintln!("CME trade route discovered: {route}");

    let _ = handle.disconnect().await;
    let _ = plant.await_shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires configured Rithmic Paper Trading credentials and PnL permission"]
async fn paper_trading_pnl_plant_accepts_account_snapshot() {
    dotenvy::dotenv().ok();
    assert_demo();
    let config = RithmicConfig::from_env(RithmicEnv::Demo).unwrap();
    let account = discover_account(&config).await;

    let plant = RithmicPnlPlant::connect(&config, ConnectStrategy::Simple)
        .await
        .unwrap();
    let handle = plant.get_handle(&account);
    let mut login = LoginConfig::default();
    login.mac_addr = Some(vec![synthetic_mac()]);
    handle.login_with_config(login).await.unwrap();
    // Subscribe a local receiver before requesting the server-side snapshot;
    // snapshot events can arrive before the acknowledgement future resolves.
    let mut updates = handle.subscription_receiver.resubscribe();
    let subscribe = handle.subscribe_pnl_updates().await.unwrap();
    assert!(
        subscribe.error.is_none(),
        "PnL subscription rejected: {subscribe:?}"
    );
    let snapshot = handle.get_pnl_position_snapshot().await.unwrap();
    assert!(
        snapshot.error.is_none(),
        "PnL snapshot rejected: {snapshot:?}"
    );

    let response = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let response = updates.recv().await.unwrap();
            if matches!(
                response.message,
                RithmicMessage::AccountPnLPositionUpdate(_)
                    | RithmicMessage::InstrumentPnLPositionUpdate(_)
            ) {
                break response;
            }
        }
    })
    .await
    .expect("PnL snapshot produced no account or position update in 10 seconds");
    eprintln!("Paper PnL snapshot received: {:?}", response.message);

    let _ = handle.disconnect().await;
    let _ = plant.await_shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "SUBMITS a one-lot far-away limit order on Paper, then modifies and cancels it"]
async fn paper_trading_far_limit_order_modify_cancel_lifecycle() {
    dotenvy::dotenv().ok();
    assert_demo();
    assert!(
        std::env::var("RITHMIC_RUN_ORDER_LIFECYCLE_TEST")
            .is_ok_and(|value| value.eq_ignore_ascii_case("true")),
        "set RITHMIC_RUN_ORDER_LIFECYCLE_TEST=true to authorize this Paper order test"
    );
    let config = RithmicConfig::from_env(RithmicEnv::Demo).unwrap();
    let account = discover_account(&config).await;
    let symbol = std::env::var("RITHMIC_DTC_SYMBOL")
        .or_else(|_| std::env::var("RITHMIC_PROBE_SYMBOL"))
        .unwrap();
    let exchange = std::env::var("RITHMIC_DTC_EXCHANGE")
        .or_else(|_| std::env::var("RITHMIC_PROBE_EXCHANGE"))
        .unwrap();
    let bid = current_bid(&config, &symbol, &exchange).await;
    let safe_price = ((bid - 100.0) * 4.0).floor() / 4.0;
    assert!(safe_price > 0.0 && safe_price < bid - 90.0);

    let plant = RithmicOrderPlant::connect(&config, ConnectStrategy::Simple)
        .await
        .unwrap();
    let handle = plant.get_handle(&account);
    let mut login = LoginConfig::default();
    login.mac_addr = Some(vec![synthetic_mac()]);
    handle.login_with_config(login).await.unwrap();
    let mut updates = handle.subscription_receiver.resubscribe();
    let subscribed = handle.subscribe_order_updates().await.unwrap();
    assert!(
        subscribed.error.is_none(),
        "order subscription rejected: {subscribed:?}"
    );

    let tag = format!("rdtb-{}", std::process::id());
    let order = RithmicOrder::new()
        .symbol(&symbol)
        .exchange(&exchange)
        .quantity(1)
        .transaction_type(OrderSide::Buy)
        .price_type(OrderType::Limit)
        .price(safe_price)
        .user_tag(&tag)
        .manual_or_auto(ManualOrAutoEntry::Auto)
        .window_name("rithmic-dtc-bridge lifecycle test")
        .cancel_after_secs(30)
        .build()
        .unwrap();
    assert_clean(handle.place_order(order).await.unwrap(), "place");

    let basket_id = wait_for_order(&mut updates, &tag, |status, _| {
        matches!(status, "open" | "pending")
    })
    .await
    .expect("Paper order did not become pending/open");
    eprintln!("Paper limit accepted: basket_id={basket_id} price={safe_price}");

    let modified_price = safe_price - 0.25;
    let modification = RithmicModifyOrder::new()
        .id(&basket_id)
        .symbol(&symbol)
        .exchange(&exchange)
        .quantity(1)
        .price(modified_price)
        .price_type(OrderType::Limit)
        .manual_or_auto(ManualOrAutoEntry::Auto)
        .window_name("rithmic-dtc-bridge lifecycle test")
        .build()
        .unwrap();
    let modify_result = handle.modify_order(modification).await;
    if let Err(error) = &modify_result {
        let _ = handle
            .cancel_order(RithmicCancelOrder::new().id(&basket_id).build().unwrap())
            .await;
        panic!("modify request failed: {error}");
    }
    assert_clean(modify_result.unwrap(), "modify");
    let modified = wait_for_basket(&mut updates, &basket_id, |status, price| {
        matches!(status, "open" | "modified")
            && price.is_some_and(|price| (price - modified_price).abs() < 0.001)
    })
    .await;

    let cancel = RithmicCancelOrder::new()
        .id(&basket_id)
        .manual_or_auto(ManualOrAutoEntry::Auto)
        .window_name("rithmic-dtc-bridge lifecycle test")
        .build()
        .unwrap();
    assert_clean(handle.cancel_order(cancel).await.unwrap(), "cancel");
    wait_for_basket(&mut updates, &basket_id, |status, _| {
        matches!(status, "cancelled" | "canceled" | "complete")
    })
    .await
    .expect("Paper order cancellation was not confirmed");
    assert!(
        modified.is_some(),
        "Paper order modification was not confirmed"
    );
    eprintln!("Paper order modified to {modified_price} and cancellation confirmed");

    let _ = handle.disconnect().await;
    let _ = plant.await_shutdown().await;
}

async fn current_bid(config: &RithmicConfig, symbol: &str, exchange: &str) -> f64 {
    let plant = RithmicTickerPlant::connect(config, ConnectStrategy::Simple)
        .await
        .unwrap();
    let mut handle = plant.get_handle();
    let mut login = LoginConfig::default();
    login.mac_addr = Some(vec![synthetic_mac()]);
    handle.login_with_config(login).await.unwrap();
    let subscribed = handle.subscribe(symbol, exchange).await.unwrap();
    assert!(
        subscribed.error.is_none(),
        "ticker subscription rejected: {subscribed:?}"
    );
    let bid = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            let response = handle.subscription_receiver.recv().await.unwrap();
            if let RithmicMessage::BestBidOffer(quote) = response.message
                && quote.symbol.as_deref() == Some(symbol)
                && quote.exchange.as_deref() == Some(exchange)
                && let Some(bid) = quote.bid_price
            {
                break bid;
            }
        }
    })
    .await
    .expect("no ES best bid received in 20 seconds");
    let _ = handle.disconnect().await;
    let _ = plant.await_shutdown().await;
    bid
}

async fn wait_for_order<F>(
    updates: &mut rithmic_rs::SubscriptionFilter,
    tag: &str,
    predicate: F,
) -> Option<String>
where
    F: Fn(&str, Option<f64>) -> bool,
{
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let response = updates.recv().await.ok()?;
            let (found_tag, basket, status, price) = match response.message {
                RithmicMessage::RithmicOrderNotification(order) => {
                    (order.user_tag, order.basket_id, order.status, order.price)
                }
                RithmicMessage::ExchangeOrderNotification(order) => {
                    (order.user_tag, order.basket_id, order.status, order.price)
                }
                _ => continue,
            };
            if found_tag.as_deref() == Some(tag)
                && predicate(status.as_deref().unwrap_or(""), price)
            {
                return basket;
            }
        }
    })
    .await
    .ok()
    .flatten()
}

async fn wait_for_basket<F>(
    updates: &mut rithmic_rs::SubscriptionFilter,
    basket_id: &str,
    predicate: F,
) -> Option<()>
where
    F: Fn(&str, Option<f64>) -> bool,
{
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let response = updates.recv().await.ok()?;
            let (basket, status, price, notify_type) = match response.message {
                RithmicMessage::RithmicOrderNotification(order) => {
                    (order.basket_id, order.status, order.price, order.notify_type)
                }
                RithmicMessage::ExchangeOrderNotification(order) => {
                    (order.basket_id, order.status, order.price, order.notify_type)
                }
                _ => continue,
            };
            if basket.as_deref() == Some(basket_id) {
                eprintln!(
                    "Paper order update: basket={} status={} price={price:?} notify={notify_type:?}",
                    basket_id,
                    status.as_deref().unwrap_or("")
                );
                if predicate(status.as_deref().unwrap_or(""), price) {
                    return Some(());
                }
            }
        }
    })
    .await
    .ok()
    .flatten()
}

fn assert_clean(responses: Vec<rithmic_rs::RithmicResponse>, action: &str) {
    for response in responses {
        assert!(response.error.is_none(), "{action} rejected: {response:?}");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "safety cleanup for a specifically named Paper basket ID"]
async fn cleanup_named_paper_order() {
    dotenvy::dotenv().ok();
    assert_demo();
    let basket_id =
        std::env::var("RITHMIC_CLEANUP_BASKET_ID").expect("RITHMIC_CLEANUP_BASKET_ID is required");
    let config = RithmicConfig::from_env(RithmicEnv::Demo).unwrap();
    let account = discover_account(&config).await;
    let plant = RithmicOrderPlant::connect(&config, ConnectStrategy::Simple)
        .await
        .unwrap();
    let handle = plant.get_handle(&account);
    let mut login = LoginConfig::default();
    login.mac_addr = Some(vec![synthetic_mac()]);
    handle.login_with_config(login).await.unwrap();
    let cancel = RithmicCancelOrder::new().id(&basket_id).build().unwrap();
    let responses = handle.cancel_order(cancel).await.unwrap();
    for response in responses {
        eprintln!("cleanup response: {response:?}");
    }
    let _ = handle.disconnect().await;
    let _ = plant.await_shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "EXITS the configured instrument position on Paper; safety cleanup only"]
async fn cleanup_configured_paper_position() {
    dotenvy::dotenv().ok();
    assert_demo();
    assert!(
        std::env::var("RITHMIC_CONFIRM_POSITION_CLEANUP")
            .is_ok_and(|value| value.eq_ignore_ascii_case("true")),
        "set RITHMIC_CONFIRM_POSITION_CLEANUP=true to authorize Paper position cleanup"
    );
    let symbol = std::env::var("RITHMIC_DTC_SYMBOL")
        .or_else(|_| std::env::var("RITHMIC_PROBE_SYMBOL"))
        .expect("RITHMIC_DTC_SYMBOL or RITHMIC_PROBE_SYMBOL is required");
    let exchange = std::env::var("RITHMIC_DTC_EXCHANGE")
        .or_else(|_| std::env::var("RITHMIC_PROBE_EXCHANGE"))
        .expect("RITHMIC_DTC_EXCHANGE or RITHMIC_PROBE_EXCHANGE is required");
    let config = RithmicConfig::from_env(RithmicEnv::Demo).unwrap();
    let account = discover_account(&config).await;
    let plant = RithmicOrderPlant::connect(&config, ConnectStrategy::Simple)
        .await
        .unwrap();
    let handle = plant.get_handle(&account);
    let mut login = LoginConfig::default();
    login.mac_addr = Some(vec![synthetic_mac()]);
    handle.login_with_config(login).await.unwrap();
    let command = RithmicExitPosition::new()
        .symbol(&symbol)
        .exchange(&exchange)
        .manual_or_auto(ManualOrAutoEntry::Auto)
        .window_name("rithmic-dtc-bridge safety cleanup")
        .build()
        .unwrap();
    let responses = handle.exit_position(command).await.unwrap();
    assert_clean(responses, "exit position");
    eprintln!("Paper position exit accepted for {symbol}.{exchange}");
    let _ = handle.disconnect().await;
    let _ = plant.await_shutdown().await;
}

fn assert_demo() {
    assert_eq!(
        std::env::var("RITHMIC_ENV")
            .unwrap_or_else(|_| "demo".to_owned())
            .to_ascii_lowercase(),
        "demo",
        "live trading tests are restricted to Paper Trading"
    );
}

async fn discover_account(config: &RithmicConfig) -> RithmicAccount {
    let plant = RithmicOrderPlant::connect(config, ConnectStrategy::Simple)
        .await
        .unwrap();
    let handle = plant.get_handle(&RithmicAccount::new("", "", ""));
    let mut login = LoginConfig::default();
    login.mac_addr = Some(vec![synthetic_mac()]);
    handle.login_with_config(login).await.unwrap();
    let responses = handle.get_account_list().await.unwrap();
    let account = responses
        .into_iter()
        .find_map(|response| match response.message {
            RithmicMessage::ResponseAccountList(account) if response.error.is_none() => Some(
                RithmicAccount::new(account.fcm_id?, account.ib_id?, account.account_id?),
            ),
            _ => None,
        })
        .expect("no Paper trading account was returned");
    let _ = handle.disconnect().await;
    let _ = plant.await_shutdown().await;
    account
}
