use std::{collections::BTreeMap, collections::HashMap, error::Error, fmt};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Bid,
    Ask,
}

impl Side {
    pub fn dtc_value(self) -> u8 {
        match self {
            Self::Bid => 1,
            Self::Ask => 2,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateAction {
    New,
    Change,
    Delete,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderUpdate {
    pub action: UpdateAction,
    pub order_id: String,
    pub side: Side,
    pub price: f64,
    pub quantity: i32,
    pub priority: u64,
}

#[derive(Debug, Clone, PartialEq)]
struct Order {
    side: Side,
    price_ticks: i64,
    quantity: i64,
    priority: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Aggregate {
    quantity: i64,
    num_orders: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DepthLevel {
    pub side: Side,
    pub price: f64,
    pub quantity: f64,
    pub num_orders: u32,
    pub level: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LevelUpdateType {
    Delete = 2,
    Insert = 3,
    Update = 4,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LevelUpdate {
    pub side: Side,
    pub price: f64,
    pub quantity: f64,
    pub num_orders: u16,
    pub level: u16,
    pub update_type: LevelUpdateType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BookError {
    InvalidTickSize,
    InvalidPrice,
    InvalidQuantity(i32),
    EmptyOrderId,
    UnknownOrder(String),
    CrossedMarket {
        best_bid_ticks: i64,
        best_ask_ticks: i64,
    },
}

impl fmt::Display for BookError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTickSize => f.write_str("tick size must be finite and positive"),
            Self::InvalidPrice => f.write_str("order price must be finite and positive"),
            Self::InvalidQuantity(quantity) => {
                write!(f, "order quantity must be positive, received {quantity}")
            }
            Self::EmptyOrderId => f.write_str("order ID must not be empty"),
            Self::UnknownOrder(order_id) => write!(f, "unknown order ID {order_id}"),
            Self::CrossedMarket {
                best_bid_ticks,
                best_ask_ticks,
            } => write!(
                f,
                "crossed market: best bid tick {best_bid_ticks} >= best ask tick {best_ask_ticks}"
            ),
        }
    }
}

impl Error for BookError {}

#[derive(Debug, Clone)]
pub struct OrderBook {
    tick_size: f64,
    orders: HashMap<String, Order>,
    bids: BTreeMap<i64, Aggregate>,
    asks: BTreeMap<i64, Aggregate>,
    last_sequence: Option<u64>,
}

impl OrderBook {
    pub fn new(tick_size: f64) -> Result<Self, BookError> {
        if !tick_size.is_finite() || tick_size <= 0.0 {
            return Err(BookError::InvalidTickSize);
        }
        Ok(Self {
            tick_size,
            orders: HashMap::new(),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_sequence: None,
        })
    }

    pub fn reset_snapshot<I>(&mut self, sequence: Option<u64>, orders: I) -> Result<(), BookError>
    where
        I: IntoIterator<Item = OrderUpdate>,
    {
        self.orders.clear();
        self.bids.clear();
        self.asks.clear();
        self.last_sequence = sequence;
        for mut update in orders {
            update.action = UpdateAction::New;
            self.insert(update)?;
        }
        self.validate_not_crossed()
    }

    pub fn apply_batch(
        &mut self,
        sequence: Option<u64>,
        updates: impl IntoIterator<Item = OrderUpdate>,
        max_levels: usize,
    ) -> Result<Vec<LevelUpdate>, BookError> {
        if let (Some(previous), Some(received)) = (self.last_sequence, sequence) {
            // Rithmic may split one logical matching-engine event across several
            // DBO messages that share a sequence number. Only an older sequence is
            // stale; an equal sequence still carries updates that must be applied.
            if received < previous {
                return Ok(Vec::new());
            }
        }

        let old_bids = self.levels(Side::Bid, max_levels);
        let old_asks = self.levels(Side::Ask, max_levels);
        for update in updates {
            match update.action {
                UpdateAction::New => self.insert(update)?,
                UpdateAction::Change => {
                    // Rithmic can emit a full-state CHANGE for an order which was not
                    // present in the point-in-time DBO snapshot. Treat it as an upsert:
                    // if it is known, replace the old state; otherwise insert the complete
                    // state carried by this update.
                    if self.orders.contains_key(&update.order_id) {
                        self.remove(&update.order_id)?;
                    }
                    self.insert(update)?;
                }
                UpdateAction::Delete => {
                    // A DELETE for an order absent from the snapshot is already reflected
                    // in this local book. It is safe and idempotent to ignore it.
                    if self.orders.contains_key(&update.order_id) {
                        self.remove(&update.order_id)?;
                    }
                }
            }
        }
        if sequence.is_some() {
            self.last_sequence = sequence;
        }

        let new_bids = self.levels(Side::Bid, max_levels);
        let new_asks = self.levels(Side::Ask, max_levels);
        let mut changes = diff_levels(&old_bids, &new_bids);
        changes.extend(diff_levels(&old_asks, &new_asks));
        Ok(changes)
    }

    pub fn is_crossed(&self) -> bool {
        matches!(
            (self.bids.last_key_value(), self.asks.first_key_value()),
            (Some((&best_bid_ticks, _)), Some((&best_ask_ticks, _)))
                if best_bid_ticks > best_ask_ticks
        )
    }

    pub fn levels(&self, side: Side, max_levels: usize) -> Vec<DepthLevel> {
        let entries: Box<dyn Iterator<Item = (&i64, &Aggregate)> + '_> = match side {
            Side::Bid => Box::new(self.bids.iter().rev()),
            Side::Ask => Box::new(self.asks.iter()),
        };
        entries
            .take(max_levels)
            .enumerate()
            .map(|(index, (&price_ticks, aggregate))| DepthLevel {
                side,
                price: price_ticks as f64 * self.tick_size,
                quantity: aggregate.quantity as f64,
                num_orders: aggregate.num_orders,
                level: (index + 1).min(u16::MAX as usize) as u16,
            })
            .collect()
    }

    pub fn order_count(&self) -> usize {
        self.orders.len()
    }

    pub fn last_sequence(&self) -> Option<u64> {
        self.last_sequence
    }

    fn insert(&mut self, update: OrderUpdate) -> Result<(), BookError> {
        if update.order_id.is_empty() {
            return Err(BookError::EmptyOrderId);
        }
        if update.quantity <= 0 {
            return Err(BookError::InvalidQuantity(update.quantity));
        }
        if !update.price.is_finite() || update.price <= 0.0 {
            return Err(BookError::InvalidPrice);
        }
        if self.orders.contains_key(&update.order_id) {
            self.remove(&update.order_id)?;
        }
        let price_ticks = (update.price / self.tick_size).round() as i64;
        let order = Order {
            side: update.side,
            price_ticks,
            quantity: i64::from(update.quantity),
            priority: update.priority,
        };
        let aggregate = self.levels_mut(update.side).entry(price_ticks).or_default();
        aggregate.quantity += order.quantity;
        aggregate.num_orders = aggregate.num_orders.saturating_add(1);
        self.orders.insert(update.order_id, order);
        Ok(())
    }

    fn remove(&mut self, order_id: &str) -> Result<(), BookError> {
        let order = self
            .orders
            .remove(order_id)
            .ok_or_else(|| BookError::UnknownOrder(order_id.to_owned()))?;
        let levels = self.levels_mut(order.side);
        let remove_level = if let Some(aggregate) = levels.get_mut(&order.price_ticks) {
            aggregate.quantity -= order.quantity;
            aggregate.num_orders = aggregate.num_orders.saturating_sub(1);
            aggregate.quantity <= 0 || aggregate.num_orders == 0
        } else {
            true
        };
        if remove_level {
            levels.remove(&order.price_ticks);
        }
        Ok(())
    }

    fn levels_mut(&mut self, side: Side) -> &mut BTreeMap<i64, Aggregate> {
        match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        }
    }

    fn validate_not_crossed(&self) -> Result<(), BookError> {
        if let (Some((&best_bid_ticks, _)), Some((&best_ask_ticks, _))) =
            (self.bids.last_key_value(), self.asks.first_key_value())
        {
            // Rithmic can briefly publish a locked DBO market at the trade price.
            // A lock is not a crossed book and must not trigger a snapshot storm.
            if best_bid_ticks > best_ask_ticks {
                return Err(BookError::CrossedMarket {
                    best_bid_ticks,
                    best_ask_ticks,
                });
            }
        }
        Ok(())
    }
}

pub(crate) fn diff_levels(old: &[DepthLevel], new: &[DepthLevel]) -> Vec<LevelUpdate> {
    let mut working = old.to_vec();
    let mut changes = Vec::new();
    let mut index = 0;
    while index < new.len() {
        if index >= working.len() {
            let level = new[index].clone();
            changes.push(to_update_at(&level, LevelUpdateType::Insert, index));
            working.insert(index, level);
            index += 1;
            continue;
        }
        if working[index].price == new[index].price {
            if working[index].quantity != new[index].quantity
                || working[index].num_orders != new[index].num_orders
            {
                changes.push(to_update_at(&new[index], LevelUpdateType::Update, index));
                working[index] = new[index].clone();
            }
            index += 1;
            continue;
        }
        if working[index + 1..]
            .iter()
            .any(|level| level.price == new[index].price)
        {
            let removed = working.remove(index);
            changes.push(to_update_at(&removed, LevelUpdateType::Delete, index));
        } else {
            let level = new[index].clone();
            changes.push(to_update_at(&level, LevelUpdateType::Insert, index));
            working.insert(index, level);
            index += 1;
        }
    }
    while working.len() > new.len() {
        let removed = working.remove(new.len());
        changes.push(to_update_at(&removed, LevelUpdateType::Delete, new.len()));
    }
    changes
}

fn to_update_at(
    level: &DepthLevel,
    update_type: LevelUpdateType,
    current_index: usize,
) -> LevelUpdate {
    LevelUpdate {
        side: level.side,
        price: level.price,
        quantity: level.quantity,
        num_orders: level.num_orders.min(u16::MAX as u32) as u16,
        // DTC V2 applies each mutation immediately. After an earlier insert/delete
        // in the same batch, the cached snapshot level is stale; send the level's
        // position at the moment this particular mutation is applied.
        level: (current_index + 1).min(u16::MAX as usize) as u16,
        update_type,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order(action: UpdateAction, id: &str, side: Side, price: f64, quantity: i32) -> OrderUpdate {
        OrderUpdate {
            action,
            order_id: id.to_owned(),
            side,
            price,
            quantity,
            priority: 1,
        }
    }

    #[test]
    fn aggregates_orders_at_price_and_sorts_best_first() {
        let mut book = OrderBook::new(0.25).unwrap();
        book.reset_snapshot(
            Some(10),
            [
                order(UpdateAction::New, "b1", Side::Bid, 6500.0, 2),
                order(UpdateAction::New, "b2", Side::Bid, 6500.0, 3),
                order(UpdateAction::New, "b3", Side::Bid, 6499.75, 4),
                order(UpdateAction::New, "a1", Side::Ask, 6500.25, 5),
            ],
        )
        .unwrap();
        assert_eq!(book.order_count(), 4);
        let bids = book.levels(Side::Bid, 10);
        assert_eq!(
            (bids[0].price, bids[0].quantity, bids[0].num_orders),
            (6500.0, 5.0, 2)
        );
        assert_eq!(bids[1].price, 6499.75);
        assert_eq!(book.levels(Side::Ask, 10)[0].price, 6500.25);
    }

    #[test]
    fn order_move_generates_level_delete_and_insert() {
        let mut book = OrderBook::new(0.25).unwrap();
        book.reset_snapshot(
            Some(20),
            [
                order(UpdateAction::New, "b1", Side::Bid, 6500.0, 2),
                order(UpdateAction::New, "b2", Side::Bid, 6499.75, 3),
            ],
        )
        .unwrap();
        let changes = book
            .apply_batch(
                Some(21),
                [order(UpdateAction::Change, "b1", Side::Bid, 6499.5, 2)],
                10,
            )
            .unwrap();
        assert!(
            changes
                .iter()
                .any(|change| change.update_type == LevelUpdateType::Delete)
        );
        assert!(
            changes
                .iter()
                .any(|change| change.update_type == LevelUpdateType::Insert)
        );
        assert_eq!(book.levels(Side::Bid, 10)[0].price, 6499.75);
    }

    #[test]
    fn accepts_monotonic_non_contiguous_global_sequences() {
        let mut book = OrderBook::new(0.25).unwrap();
        book.reset_snapshot(
            Some(30),
            [order(UpdateAction::New, "b1", Side::Bid, 6500.0, 2)],
        )
        .unwrap();
        let changes = book
            .apply_batch(
                Some(32),
                [order(UpdateAction::Change, "b1", Side::Bid, 6500.0, 3)],
                10,
            )
            .unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(book.order_count(), 1);
        assert_eq!(book.levels(Side::Bid, 10)[0].quantity, 3.0);
    }

    #[test]
    fn applies_multiple_messages_with_the_same_sequence() {
        let mut book = OrderBook::new(0.25).unwrap();
        book.reset_snapshot(
            Some(30),
            [
                order(UpdateAction::New, "b1", Side::Bid, 6499.75, 2),
                order(UpdateAction::New, "a1", Side::Ask, 6500.0, 2),
            ],
        )
        .unwrap();

        book.apply_batch(
            Some(31),
            [order(UpdateAction::New, "b2", Side::Bid, 6500.25, 1)],
            10,
        )
        .unwrap();
        assert!(book.is_crossed());

        book.apply_batch(
            Some(31),
            [
                order(UpdateAction::Delete, "b2", Side::Bid, 6500.25, 1),
                order(UpdateAction::Delete, "a1", Side::Ask, 6500.0, 2),
                order(UpdateAction::New, "a2", Side::Ask, 6500.25, 2),
            ],
            10,
        )
        .unwrap();

        assert!(!book.is_crossed());
        assert_eq!(book.levels(Side::Bid, 1)[0].price, 6499.75);
        assert_eq!(book.levels(Side::Ask, 1)[0].price, 6500.25);
    }

    #[test]
    fn reconciles_full_state_change_for_order_missing_from_snapshot() {
        let mut book = OrderBook::new(0.25).unwrap();
        book.reset_snapshot(Some(40), []).unwrap();

        let changes = book
            .apply_batch(
                Some(41),
                [order(UpdateAction::Change, "late", Side::Ask, 6500.25, 7)],
                10,
            )
            .unwrap();

        assert_eq!(book.order_count(), 1);
        assert_eq!(book.levels(Side::Ask, 10)[0].quantity, 7.0);
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].update_type, LevelUpdateType::Insert);
    }

    #[test]
    fn ignores_delete_for_order_missing_from_snapshot() {
        let mut book = OrderBook::new(0.25).unwrap();
        book.reset_snapshot(Some(50), []).unwrap();

        let changes = book
            .apply_batch(
                Some(51),
                [order(
                    UpdateAction::Delete,
                    "already-gone",
                    Side::Bid,
                    6500.0,
                    1,
                )],
                10,
            )
            .unwrap();

        assert!(changes.is_empty());
        assert_eq!(book.order_count(), 0);
        assert_eq!(book.last_sequence(), Some(51));
    }

    #[test]
    fn consecutive_top_level_deletes_use_the_current_dtc_level() {
        let mut book = OrderBook::new(0.25).unwrap();
        book.reset_snapshot(
            Some(60),
            [
                order(UpdateAction::New, "b1", Side::Bid, 6500.0, 1),
                order(UpdateAction::New, "b2", Side::Bid, 6499.75, 1),
                order(UpdateAction::New, "b3", Side::Bid, 6499.5, 1),
            ],
        )
        .unwrap();

        let changes = book
            .apply_batch(
                Some(61),
                [
                    order(UpdateAction::Delete, "b1", Side::Bid, 6500.0, 1),
                    order(UpdateAction::Delete, "b2", Side::Bid, 6499.75, 1),
                ],
                10,
            )
            .unwrap();

        assert_eq!(changes.len(), 2);
        assert_eq!(changes[0].update_type, LevelUpdateType::Delete);
        assert_eq!((changes[0].price, changes[0].level), (6500.0, 1));
        assert_eq!(changes[1].update_type, LevelUpdateType::Delete);
        assert_eq!((changes[1].price, changes[1].level), (6499.75, 1));
    }

    #[test]
    fn accepts_a_locked_aggregated_book() {
        let mut book = OrderBook::new(0.25).unwrap();
        book.reset_snapshot(
            Some(70),
            [
                order(UpdateAction::New, "b1", Side::Bid, 6499.75, 1),
                order(UpdateAction::New, "a1", Side::Ask, 6500.0, 1),
            ],
        )
        .unwrap();

        let changes = book
            .apply_batch(
                Some(71),
                [order(UpdateAction::Change, "b1", Side::Bid, 6500.0, 1)],
                10,
            )
            .unwrap();

        assert!(!changes.is_empty());
        assert_eq!(book.levels(Side::Bid, 1)[0].price, 6500.0);
        assert_eq!(book.levels(Side::Ask, 1)[0].price, 6500.0);
    }

    #[test]
    fn detects_a_strictly_crossed_increment_without_rejecting_the_batch() {
        let mut book = OrderBook::new(0.25).unwrap();
        book.reset_snapshot(
            Some(70),
            [
                order(UpdateAction::New, "b1", Side::Bid, 6499.75, 1),
                order(UpdateAction::New, "a1", Side::Ask, 6500.0, 1),
            ],
        )
        .unwrap();

        let changes = book
            .apply_batch(
                Some(71),
                [order(UpdateAction::Change, "b1", Side::Bid, 6500.25, 1)],
                10,
            )
            .unwrap();

        assert!(!changes.is_empty());
        assert!(book.is_crossed());
    }

    #[test]
    fn rejects_a_strictly_crossed_snapshot() {
        let mut book = OrderBook::new(0.25).unwrap();
        let error = book
            .reset_snapshot(
                Some(70),
                [
                    order(UpdateAction::New, "b1", Side::Bid, 6500.25, 1),
                    order(UpdateAction::New, "a1", Side::Ask, 6500.0, 1),
                ],
            )
            .unwrap_err();

        assert_eq!(
            error,
            BookError::CrossedMarket {
                best_bid_ticks: 26_001,
                best_ask_ticks: 26_000,
            }
        );
    }
}
