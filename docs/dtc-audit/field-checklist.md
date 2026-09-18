# 已接入消息的字段逐项清单

来源：官方 DTCProtocol.h，偏移/大小由 g++ 实际编译 offsetof/sizeof 取得。逐字段“读取/生成”仅表示本地有处理；具体语义缺陷见主报告。未读取字段和输出默认值明确列出，不能把默认值当作已实现业务。Size/Type 统一处理；入站旧长度兼容问题见 A03。

## 6 ENCODING_REQUEST（16 字节）

已实现：验证长度及 DTC 标识；服务端可选择自己的编码。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `ProtocolVersion` | `int32_t` | 4/4 | `CURRENT_VERSION` | 不读取；回复服务器 v8/Binary，协议允许编码不同 |
| `Encoding` | `EncodingEnum` | 8/4 | `BINARY_ENCODING` | 不读取；回复服务器 v8/Binary，协议允许编码不同 |
| `ProtocolType` | `char[4]` | 12/4 | `"DTC"` | 读取（入站） |

## 7 ENCODING_RESPONSE（16 字节）

已实现：返回 Binary=0，符合只支持一种编码的规则。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `ProtocolVersion` | `int32_t` | 4/4 | `CURRENT_VERSION` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Encoding` | `EncodingEnum` | 8/4 | `BINARY_ENCODING` | 生成（正常/空/拒绝路径的差异见主报告） |
| `ProtocolType` | `char[4]` | 12/4 | `"DTC"` | 生成（正常/空/拒绝路径的差异见主报告） |

## 1 LOGON_REQUEST（284 字节）

部分：仅读取心跳；可选认证未启用；非法登录直接断开，见 A03/A14。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `ProtocolVersion` | `int32_t` | 4/4 | `CURRENT_VERSION` | 未读取/未使用 |
| `Username` | `char[USERNAME_PASSWORD_LENGTH]` | 8/32 | `{}` | 未读取/未使用 |
| `Password` | `char[USERNAME_PASSWORD_LENGTH]` | 40/32 | `{}` | 未读取/未使用 |
| `GeneralTextData` | `char[GENERAL_IDENTIFIER_LENGTH]` | 72/64 | `{}` | 未读取/未使用 |
| `Integer_1` | `int32_t` | 136/4 | `0` | 未读取/未使用 |
| `Integer_2` | `int32_t` | 140/4 | `0` | 未读取/未使用 |
| `HeartbeatIntervalInSeconds` | `int32_t` | 144/4 | `0` | 读取（入站） |
| `Unused1` | `int32_t` | 148/4 | `0` | 未读取/未使用 |
| `TradeAccount` | `char[TRADE_ACCOUNT_LENGTH]` | 152/32 | `{}` | 未读取/未使用 |
| `HardwareIdentifier` | `char[GENERAL_IDENTIFIER_LENGTH]` | 184/64 | `{}` | 未读取/未使用 |
| `ClientName` | `char[32]` | 248/32 | `{}` | 未读取/未使用 |
| `MarketDataTransmissionInterval` | `int32_t` | 280/4 | `0` | 未读取/未使用 |

## 2 LOGON_RESPONSE（256 字节）

已实现：v8、服务能力、分隔符；OCO/Bracket=0；没有历史成交专用能力位。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `ProtocolVersion` | `int32_t` | 4/4 | `CURRENT_VERSION` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Result` | `LogonStatusEnum` | 8/4 | `LOGON_SUCCESS` | 生成（正常/空/拒绝路径的差异见主报告） |
| `ResultText` | `char[TEXT_DESCRIPTION_LENGTH]` | 12/96 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `ReconnectAddress` | `char[64]` | 108/64 | `{}` | 未赋业务值：零/空串 |
| `Integer_1` | `int32_t` | 172/4 | `0` | 未赋业务值：零/空串 |
| `ServerName` | `char[60]` | 176/60 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `MarketDepthUpdatesBestBidAndAsk` | `uint8_t` | 236/1 | `0` | 未赋业务值：零/空串 |
| `TradingIsSupported` | `uint8_t` | 237/1 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `OCOOrdersSupported` | `uint8_t` | 238/1 | `0` | 未赋业务值：零/空串 |
| `OrderCancelReplaceSupported` | `uint8_t` | 239/1 | `1` | 生成（正常/空/拒绝路径的差异见主报告） |
| `SymbolExchangeDelimiter` | `char[SYMBOL_EXCHANGE_DELIMITER_LENGTH]` | 240/4 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `SecurityDefinitionsSupported` | `uint8_t` | 244/1 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `HistoricalPriceDataSupported` | `uint8_t` | 245/1 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `ResubscribeWhenMarketDataFeedAvailable` | `uint8_t` | 246/1 | `0` | 未赋业务值：零/空串 |
| `MarketDepthIsSupported` | `uint8_t` | 247/1 | `1` | 生成（正常/空/拒绝路径的差异见主报告） |
| `OneHistoricalPriceDataRequestPerConnection` | `uint8_t` | 248/1 | `0` | 未赋业务值：零/空串 |
| `BracketOrdersSupported` | `uint8_t` | 249/1 | `0` | 未赋业务值：零/空串 |
| `Unused_1` | `uint8_t` | 250/1 | `0` | 未赋业务值：零/空串 |
| `UsesMultiplePositionsPerSymbolAndTradeAccount` | `uint8_t` | 251/1 | `0` | 未赋业务值：零/空串 |
| `MarketDataSupported` | `uint8_t` | 252/1 | `1` | 生成（正常/空/拒绝路径的差异见主报告） |

## 5 LOGOFF（102 字节）

已实现接收退出及超时发送；其他协议错误并未统一发送 LOGOFF。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `Reason` | `char[TEXT_DESCRIPTION_LENGTH]` | 4/96 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `DoNotReconnect` | `uint8_t` | 100/1 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |

## 3 HEARTBEAT（16 字节）

部分：5–60 秒发送、两周期静默超时；历史专用连接策略见 A13。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `NumDroppedMessages` | `uint32_t` | 4/4 | `0` | 未赋业务值：零/空串 |
| `CurrentDateTime` | `t_DateTime` | 8/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |

## 100 MARKET_DATA_FEED_STATUS（8 字节）

已实现全局断线/恢复状态；未覆盖逐合约状态。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `Status` | `MarketDataFeedStatusEnum` | 4/4 | `MARKET_DATA_FEED_STATUS_UNSET` | 生成（正常/空/拒绝路径的差异见主报告） |

## 101 MARKET_DATA_REQUEST（96 字节）

部分：订阅/退订；缺 SNAPSHOT、SymbolID 双向唯一性，见 A06/A07。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `RequestAction` | `RequestActionEnum` | 4/4 | `SUBSCRIBE` | 读取（入站） |
| `SymbolID` | `uint32_t` | 8/4 | `0` | 读取（入站） |
| `Symbol` | `char[SYMBOL_LENGTH]` | 12/64 | `{}` | 读取（入站） |
| `Exchange` | `char[EXCHANGE_LENGTH]` | 76/16 | `{}` | 读取（入站） |
| `IntervalForSnapshotUpdatesInMilliseconds` | `uint32_t` | 92/4 | `0` | 未读取/未使用 |

## 102 MARKET_DEPTH_REQUEST（96 字节）

部分：订阅/退订、深度数量；缺 SNAPSHOT，见 A06。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `RequestAction` | `RequestActionEnum` | 4/4 | `SUBSCRIBE` | 读取（入站） |
| `SymbolID` | `uint32_t` | 8/4 | `0` | 读取（入站） |
| `Symbol` | `char[SYMBOL_LENGTH]` | 12/64 | `{}` | 读取（入站） |
| `Exchange` | `char[EXCHANGE_LENGTH]` | 76/16 | `{}` | 读取（入站） |
| `NumLevels` | `int32_t` | 92/4 | `0` | 读取（入站） |

## 103 MARKET_DATA_REJECT（104 字节）

已实现 SymbolID + RejectText。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `SymbolID` | `uint32_t` | 4/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `RejectText` | `char[TEXT_DESCRIPTION_LENGTH]` | 8/96 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |

## 104 MARKET_DATA_SNAPSHOT（144 字节）

部分：仅空初始快照；未知值哨兵正确；日统计缺失，见 A08。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `SymbolID` | `uint32_t` | 4/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `SessionSettlementPrice` | `double` | 8/8 | `DBL_MAX` | 未知值哨兵 |
| `SessionOpenPrice` | `double` | 16/8 | `DBL_MAX` | 未知值哨兵 |
| `SessionHighPrice` | `double` | 24/8 | `DBL_MAX` | 未知值哨兵 |
| `SessionLowPrice` | `double` | 32/8 | `DBL_MAX` | 未知值哨兵 |
| `SessionVolume` | `double` | 40/8 | `DBL_MAX` | 未知值哨兵 |
| `SessionNumTrades` | `uint32_t` | 48/4 | `UINT_MAX` | 未知值哨兵 |
| `OpenInterest` | `uint32_t` | 52/4 | `UINT_MAX` | 未知值哨兵 |
| `BidPrice` | `double` | 56/8 | `DBL_MAX` | 未知值哨兵 |
| `AskPrice` | `double` | 64/8 | `DBL_MAX` | 未知值哨兵 |
| `AskQuantity` | `double` | 72/8 | `DBL_MAX` | 未知值哨兵 |
| `BidQuantity` | `double` | 80/8 | `DBL_MAX` | 未知值哨兵 |
| `LastTradePrice` | `double` | 88/8 | `DBL_MAX` | 未知值哨兵 |
| `LastTradeVolume` | `double` | 96/8 | `DBL_MAX` | 未知值哨兵 |
| `LastTradeDateTime` | `t_DateTimeWithMilliseconds` | 104/8 | `0` | 零：时间/未知状态 |
| `BidAskDateTime` | `t_DateTimeWithMilliseconds` | 112/8 | `0` | 零：时间/未知状态 |
| `SessionSettlementDateTime` | `t_DateTime4Byte` | 120/4 | `0` | 零：时间/未知状态 |
| `TradingSessionDate` | `t_DateTime4Byte` | 124/4 | `0` | 零：时间/未知状态 |
| `TradingStatus` | `TradingStatusEnum` | 128/1 | `TRADING_STATUS_UNKNOWN` | 零：时间/未知状态 |
| `MarketDepthUpdateDateTime` | `t_DateTimeWithMilliseconds` | 136/8 | `0` | 零：时间/未知状态 |

## 122 MARKET_DEPTH_SNAPSHOT_LEVEL（56 字节）

已实现：首/尾批标志、空簿、档位、数量、订单数。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `SymbolID` | `uint32_t` | 4/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Side` | `AtBidOrAskEnum` | 8/2 | `BID_ASK_UNSET` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Price` | `double` | 16/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Quantity` | `double` | 24/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Level` | `uint16_t` | 32/2 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `IsFirstMessageInBatch` | `uint8_t` | 34/1 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `IsLastMessageInBatch` | `uint8_t` | 35/1 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `DateTime` | `t_DateTimeWithMilliseconds` | 40/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `NumOrders` | `uint32_t` | 48/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |

## 109 MARKET_DEPTH_UPDATE_LEVEL_V2（39 字节）

已实现：39 字节 pack(1)，毫秒时间、档位、数量、批次结束。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `SymbolID` | `uint32_t` | 4/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `DateTime` | `t_DateTimeWithMillisecondsInt` | 8/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Price` | `double` | 16/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Quantity` | `double` | 24/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `NumOrders` | `uint16_t` | 32/2 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Level` | `uint16_t` | 34/2 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Side` | `AtBidOrAskEnum8` | 36/1 | `BID_ASK_UNSET_8` | 生成（正常/空/拒绝路径的差异见主报告） |
| `UpdateType` | `MarketDepthUpdateTypeEnum` | 37/1 | `MARKET_DEPTH_UNSET` | 生成（正常/空/拒绝路径的差异见主报告） |
| `FinalUpdateInBatch` | `FinalUpdateInBatchEnum` | 38/1 | `FINAL_UPDATE_UNSET` | 生成（正常/空/拒绝路径的差异见主报告） |

## 121 MARKET_DEPTH_REJECT（104 字节）

已实现 SymbolID + RejectText。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `SymbolID` | `uint32_t` | 4/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `RejectText` | `char[TEXT_DESCRIPTION_LENGTH]` | 8/96 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |

## 147 MARKET_DATA_UPDATE_TRADE_V2（40 字节）

已实现核心成交；UnbundledTradeIndicator 恒 0，未传递拆单标志。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `SymbolID` | `uint32_t` | 4/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Price` | `double` | 8/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Volume` | `double` | 16/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `DateTime` | `t_DateTimeWithMicrosecondsInt` | 24/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `AtBidOrAsk` | `AtBidOrAskEnum8` | 32/1 | `BID_ASK_UNSET_8` | 生成（正常/空/拒绝路径的差异见主报告） |
| `UnbundledTradeIndicator` | `UnbundledTradeIndicatorEnum` | 33/1 | `UNBUNDLED_TRADE_NONE` | 未赋业务值：零/空串 |
| `TradeCondition` | `t_TradeCondition` | 34/1 | `TRADE_CONDITION_NONE` | 未赋业务值：零/空串 |

## 148 MARKET_DATA_UPDATE_BID_ASK_DOUBLE_WITH_MICROSECONDS（48 字节）

已实现：项目名称 MARKET_DATA_UPDATE_BID_ASK_V2 为官方 ID 148 的别名。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `SymbolID` | `uint32_t` | 4/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `BidPrice` | `double` | 8/8 | `DBL_MAX` | 生成（正常/空/拒绝路径的差异见主报告） |
| `BidQuantity` | `double` | 16/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `AskPrice` | `double` | 24/8 | `DBL_MAX` | 生成（正常/空/拒绝路径的差异见主报告） |
| `AskQuantity` | `double` | 32/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `DateTime` | `t_DateTimeWithMicrosecondsInt` | 40/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |

## 134 MARKET_DATA_UPDATE_LAST_TRADE_SNAPSHOT（32 字节）

已实现：最近成交快照，避免当新成交重复统计。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `SymbolID` | `uint32_t` | 4/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `LastTradePrice` | `double` | 8/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `LastTradeVolume` | `double` | 16/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `LastTradeDateTime` | `t_DateTimeWithMilliseconds` | 24/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |

## 208 SUBMIT_NEW_SINGLE_ORDER（304 字节）

部分：四种普通单；IsParentOrder 被忽略，价格校验硬编码，见 A05/A11。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `Symbol` | `char[SYMBOL_LENGTH]` | 4/64 | `{}` | 读取（入站） |
| `Exchange` | `char[EXCHANGE_LENGTH]` | 68/16 | `{}` | 读取（入站） |
| `TradeAccount` | `char[TRADE_ACCOUNT_LENGTH]` | 84/32 | `{}` | 读取（入站） |
| `ClientOrderID` | `char[ORDER_ID_LENGTH]` | 116/32 | `{}` | 读取（入站） |
| `OrderType` | `OrderTypeEnum` | 148/4 | `ORDER_TYPE_UNSET` | 读取（入站） |
| `BuySell` | `BuySellEnum` | 152/4 | `BUY_SELL_UNSET` | 读取（入站） |
| `Price1` | `double` | 160/8 | `0` | 读取（入站） |
| `Price2` | `double` | 168/8 | `0` | 读取（入站） |
| `Quantity` | `double` | 176/8 | `0` | 读取（入站） |
| `TimeInForce` | `TimeInForceEnum` | 184/4 | `TIF_UNSET` | 读取（入站） |
| `GoodTillDateTime` | `t_DateTime` | 192/8 | `0` | 未读取/未使用 |
| `IsAutomatedOrder` | `uint8_t` | 200/1 | `0` | 读取（入站） |
| `IsParentOrder` | `uint8_t` | 201/1 | `0` | 未读取/未使用 |
| `FreeFormText` | `char[ORDER_FREE_FORM_TEXT_LENGTH]` | 202/48 | `{}` | 未读取/未使用 |
| `OpenOrClose` | `OpenCloseTradeEnum` | 252/4 | `TRADE_UNSET` | 未读取/未使用 |
| `MaxShowQuantity` | `double` | 256/8 | `0` | 未读取/未使用 |
| `Price1AsString` | `char[PRICE_STRING_LENGTH]` | 264/16 | `{}` | 未读取/未使用 |
| `Price2AsString` | `char[PRICE_STRING_LENGTH]` | 280/16 | `{}` | 未读取/未使用 |
| `IntendedPositionQuantity` | `double` | 296/8 | `0` | 未读取/未使用 |

## 204 CANCEL_REPLACE_ORDER（192 字节）

部分：价格设置标志、总数量语义正确；拒绝状态错误，见 A04。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `ServerOrderID` | `char[ORDER_ID_LENGTH]` | 4/32 | `{}` | 读取（入站） |
| `ClientOrderID` | `char[ORDER_ID_LENGTH]` | 36/32 | `{}` | 读取（入站） |
| `Price1` | `double` | 72/8 | `0` | 读取（入站） |
| `Price2` | `double` | 80/8 | `0` | 读取（入站） |
| `Quantity` | `double` | 88/8 | `0` | 读取（入站） |
| `Price1IsSet` | `uint8_t` | 96/1 | `1` | 读取（入站） |
| `Price2IsSet` | `uint8_t` | 97/1 | `1` | 读取（入站） |
| `Unused` | `int32_t` | 100/4 | `0` | 未读取/未使用 |
| `TimeInForce` | `TimeInForceEnum` | 104/4 | `TIF_UNSET` | 读取（入站） |
| `GoodTillDateTime` | `t_DateTime` | 112/8 | `0` | 未读取/未使用 |
| `UpdatePrice1OffsetToParent` | `uint8_t` | 120/1 | `0` | 未读取/未使用 |
| `TradeAccount` | `char[TRADE_ACCOUNT_LENGTH]` | 121/32 | `{}` | 读取（入站） |
| `Price1AsString` | `char[PRICE_STRING_LENGTH]` | 153/16 | `{}` | 未读取/未使用 |
| `Price2AsString` | `char[PRICE_STRING_LENGTH]` | 169/16 | `{}` | 未读取/未使用 |

## 203 CANCEL_ORDER（100 字节）

部分：正常撤单校验；拒绝回报状态/关联错误，见 A02/A04。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `ServerOrderID` | `char[ORDER_ID_LENGTH]` | 4/32 | `{}` | 读取（入站） |
| `ClientOrderID` | `char[ORDER_ID_LENGTH]` | 36/32 | `{}` | 读取（入站） |
| `TradeAccount` | `char[TRADE_ACCOUNT_LENGTH]` | 68/32 | `{}` | 读取（入站） |

## 300 OPEN_ORDERS_REQUEST（76 字节）

已实现请求过滤和空结果；仅当前服务缓存，终态按 ID 查询范围待扩展。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `RequestID` | `int32_t` | 4/4 | `0` | 读取（入站） |
| `RequestAllOrders` | `int32_t` | 8/4 | `1` | 读取（入站） |
| `ServerOrderID` | `char[ORDER_ID_LENGTH]` | 12/32 | `{}` | 读取（入站） |
| `TradeAccount` | `char[TRADE_ACCOUNT_LENGTH]` | 44/32 | `{}` | 读取（入站） |

## 305 CURRENT_POSITIONS_REQUEST（40 字节）

已实现：按账户筛选。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `RequestID` | `int32_t` | 4/4 | `0` | 读取（入站） |
| `TradeAccount` | `char[TRADE_ACCOUNT_LENGTH]` | 8/32 | `{}` | 读取（入站） |

## 307 CURRENT_POSITIONS_REJECT（104 字节）

已实现：请求号与拒绝文本。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `RequestID` | `int32_t` | 4/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `RejectText` | `char[TEXT_DESCRIPTION_LENGTH]` | 8/96 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |

## 301 ORDER_UPDATE（720 字节）

部分：主体布局正确；拒单字段/状态错误，时间及附加字段未提供，见 A02/A04。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `RequestID` | `int32_t` | 4/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `TotalNumMessages` | `int32_t` | 8/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `MessageNumber` | `int32_t` | 12/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Symbol` | `char[SYMBOL_LENGTH]` | 16/64 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Exchange` | `char[EXCHANGE_LENGTH]` | 80/16 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `PreviousServerOrderID` | `char[ORDER_ID_LENGTH]` | 96/32 | `{}` | 未赋业务值：零/空串 |
| `ServerOrderID` | `char[ORDER_ID_LENGTH]` | 128/32 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `ClientOrderID` | `char[ORDER_ID_LENGTH]` | 160/32 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `ExchangeOrderID` | `char[ORDER_ID_LENGTH]` | 192/32 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `OrderStatus` | `OrderStatusEnum` | 224/4 | `ORDER_STATUS_UNSPECIFIED` | 生成（正常/空/拒绝路径的差异见主报告） |
| `OrderUpdateReason` | `OrderUpdateReasonEnum` | 228/4 | `ORDER_UPDATE_REASON_UNSET` | 生成（正常/空/拒绝路径的差异见主报告） |
| `OrderType` | `OrderTypeEnum` | 232/4 | `ORDER_TYPE_UNSET` | 生成（正常/空/拒绝路径的差异见主报告） |
| `BuySell` | `BuySellEnum` | 236/4 | `BUY_SELL_UNSET` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Price1` | `double` | 240/8 | `DBL_MAX` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Price2` | `double` | 248/8 | `DBL_MAX` | 生成（正常/空/拒绝路径的差异见主报告） |
| `TimeInForce` | `TimeInForceEnum` | 256/4 | `TIF_UNSET` | 生成（正常/空/拒绝路径的差异见主报告） |
| `GoodTillDateTime` | `t_DateTime` | 264/8 | `0` | 未赋业务值：零/空串 |
| `OrderQuantity` | `double` | 272/8 | `DBL_MAX` | 生成（正常/空/拒绝路径的差异见主报告） |
| `FilledQuantity` | `double` | 280/8 | `DBL_MAX` | 生成（正常/空/拒绝路径的差异见主报告） |
| `RemainingQuantity` | `double` | 288/8 | `DBL_MAX` | 生成（正常/空/拒绝路径的差异见主报告） |
| `AverageFillPrice` | `double` | 296/8 | `DBL_MAX` | 生成（正常/空/拒绝路径的差异见主报告） |
| `LastFillPrice` | `double` | 304/8 | `DBL_MAX` | 生成（正常/空/拒绝路径的差异见主报告） |
| `LastFillDateTime` | `t_DateTimeWithMillisecondsInt` | 312/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `LastFillQuantity` | `double` | 320/8 | `DBL_MAX` | 生成（正常/空/拒绝路径的差异见主报告） |
| `LastFillExecutionID` | `char[64]` | 328/64 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `TradeAccount` | `char[TRADE_ACCOUNT_LENGTH]` | 392/32 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `InfoText` | `char[TEXT_DESCRIPTION_LENGTH]` | 424/96 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `NoOrders` | `uint8_t` | 520/1 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `ParentServerOrderID` | `char[ORDER_ID_LENGTH]` | 521/32 | `{}` | 未赋业务值：零/空串 |
| `OCOLinkedOrderServerOrderID` | `char[ORDER_ID_LENGTH]` | 553/32 | `{}` | 未赋业务值：零/空串 |
| `OpenOrClose` | `OpenCloseTradeEnum` | 588/4 | `TRADE_UNSET` | 未赋业务值：零/空串 |
| `PreviousClientOrderID` | `char[ORDER_ID_LENGTH]` | 592/32 | `{}` | 未赋业务值：零/空串 |
| `FreeFormText` | `char[ORDER_FREE_FORM_TEXT_LENGTH]` | 624/48 | `{}` | 未赋业务值：零/空串 |
| `OrderReceivedDateTime` | `t_DateTime` | 672/8 | `0` | 未赋业务值：零/空串 |
| `LatestTransactionDateTime` | `t_DateTimeWithMilliseconds` | 680/8 | `0` | 未赋业务值：零/空串 |
| `Username` | `char[USERNAME_PASSWORD_LENGTH]` | 688/32 | `{}` | 未赋业务值：零/空串 |

## 302 OPEN_ORDERS_REJECT（104 字节）

已实现：请求号与拒绝文本。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `RequestID` | `int32_t` | 4/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `RejectText` | `char[TEXT_DESCRIPTION_LENGTH]` | 8/96 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |

## 306 POSITION_UPDATE（240 字节）

部分：数量、均价、浮盈、空持仓及主动更新；保证金等扩展字段未提供。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `RequestID` | `int32_t` | 4/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `TotalNumberMessages` | `int32_t` | 8/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `MessageNumber` | `int32_t` | 12/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Symbol` | `char[SYMBOL_LENGTH]` | 16/64 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Exchange` | `char[EXCHANGE_LENGTH]` | 80/16 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Quantity` | `double` | 96/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `AveragePrice` | `double` | 104/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `PositionIdentifier` | `char[ORDER_ID_LENGTH]` | 112/32 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `TradeAccount` | `char[TRADE_ACCOUNT_LENGTH]` | 144/32 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `NoPositions` | `uint8_t` | 176/1 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Unsolicited` | `uint8_t` | 177/1 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `MarginRequirement` | `double` | 184/8 | `0` | 未赋业务值：零/空串 |
| `EntryDateTime` | `DTC::t_DateTime4Byte` | 192/4 | `0` | 未赋业务值：零/空串 |
| `OpenProfitLoss` | `double` | 200/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `HighPriceDuringPosition` | `double` | 208/8 | `0` | 未赋业务值：零/空串 |
| `LowPriceDuringPosition` | `double` | 216/8 | `0` | 未赋业务值：零/空串 |
| `QuantityLimit` | `double` | 224/8 | `0` | 未赋业务值：零/空串 |
| `MaxPotentialPostionQuantity` | `double` | 232/8 | `0` | 未赋业务值：零/空串 |

## 400 TRADE_ACCOUNTS_REQUEST（8 字节）

部分：账户查询；失败借用余额拒绝类型、空列表无终止，见 A12。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `RequestID` | `int32_t` | 4/4 | `0` | 读取（入站） |

## 401 TRADE_ACCOUNT_RESPONSE（52 字节）

部分：非空单账户列表及编号；空列表无回包，见 A12。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `TotalNumberMessages` | `int32_t` | 4/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `MessageNumber` | `int32_t` | 8/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `TradeAccount` | `char[TRADE_ACCOUNT_LENGTH]` | 12/32 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `RequestID` | `int32_t` | 44/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `TradingIsDisabled` | `int32_t` | 48/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |

## 500 EXCHANGE_LIST_REQUEST（8 字节）

已实现：上游交易所列表；失败退回默认交易所。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `RequestID` | `int32_t` | 4/4 | `0` | 读取（入站） |

## 501 EXCHANGE_LIST_RESPONSE（76 字节）

已实现逐交易所回包、空列表及末包标志。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `RequestID` | `int32_t` | 4/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Exchange` | `char[EXCHANGE_LENGTH]` | 8/16 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `IsFinalMessage` | `uint8_t` | 24/1 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Description` | `char[EXCHANGE_DESCRIPTION_LENGTH]` | 25/48 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |

## 502 SYMBOLS_FOR_EXCHANGE_REQUEST（96 字节）

部分：只匹配默认合约，未枚举完整目录，也未维护定义订阅，见 A09。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `RequestID` | `int32_t` | 4/4 | `0` | 读取（入站） |
| `Exchange` | `char[EXCHANGE_LENGTH]` | 8/16 | `{}` | 读取（入站） |
| `SecurityType` | `SecurityTypeEnum` | 24/4 | `SECURITY_TYPE_UNSET` | 读取（入站） |
| `RequestAction` | `RequestActionEnum` | 28/4 | `SUBSCRIBE` | 读取（入站） |
| `Symbol` | `char[SYMBOL_LENGTH]` | 32/64 | `{}` | 读取（入站） |

## 503 UNDERLYING_SYMBOLS_FOR_EXCHANGE_REQUEST（28 字节）

部分：只返回默认 underlying，见 A09。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `RequestID` | `int32_t` | 4/4 | `0` | 读取（入站） |
| `Exchange` | `char[EXCHANGE_LENGTH]` | 8/16 | `{}` | 读取（入站） |
| `SecurityType` | `SecurityTypeEnum` | 24/4 | `SECURITY_TYPE_UNSET` | 读取（入站） |

## 504 SYMBOLS_FOR_UNDERLYING_REQUEST（60 字节）

部分：只返回默认到期合约，见 A09。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `RequestID` | `int32_t` | 4/4 | `0` | 读取（入站） |
| `UnderlyingSymbol` | `char[UNDERLYING_SYMBOL_LENGTH]` | 8/32 | `{}` | 读取（入站） |
| `Exchange` | `char[EXCHANGE_LENGTH]` | 40/16 | `{}` | 读取（入站） |
| `SecurityType` | `SecurityTypeEnum` | 56/4 | `SECURITY_TYPE_UNSET` | 读取（入站） |

## 508 SYMBOL_SEARCH_REQUEST（96 字节）

部分：动态搜索，但 SearchType 未传入上游且未统一过滤，见 A09。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `RequestID` | `int32_t` | 4/4 | `0` | 读取（入站） |
| `SearchText` | `char[SYMBOL_DESCRIPTION_LENGTH]` | 8/64 | `{}` | 读取（入站） |
| `Exchange` | `char[EXCHANGE_LENGTH]` | 72/16 | `{}` | 读取（入站） |
| `SecurityType` | `SecurityTypeEnum` | 88/4 | `SECURITY_TYPE_UNSET` | 读取（入站） |
| `SearchType` | `SearchTypeEnum` | 92/4 | `SEARCH_TYPE_UNSET` | 读取（入站） |

## 506 SECURITY_DEFINITION_FOR_SYMBOL_REQUEST（88 字节）

已实现：默认/动态合约解析、失败拒绝。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `RequestID` | `int32_t` | 4/4 | `0` | 读取（入站） |
| `Symbol` | `char[SYMBOL_LENGTH]` | 8/64 | `{}` | 读取（入站） |
| `Exchange` | `char[EXCHANGE_LENGTH]` | 72/16 | `{}` | 读取（入站） |

## 507 SECURITY_DEFINITION_RESPONSE（432 字节）

部分：基础期货元数据；到期日/保证金等缺失；空结果默认值错误，见 A10。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `RequestID` | `int32_t` | 4/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Symbol` | `char[SYMBOL_LENGTH]` | 8/64 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Exchange` | `char[EXCHANGE_LENGTH]` | 72/16 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `SecurityType` | `SecurityTypeEnum` | 88/4 | `SECURITY_TYPE_UNSET` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Description` | `char[SYMBOL_DESCRIPTION_LENGTH]` | 92/64 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `MinPriceIncrement` | `float` | 156/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `PriceDisplayFormat` | `PriceDisplayFormatEnum` | 160/4 | `PRICE_DISPLAY_FORMAT_UNSET` | 正常响应来自 instrument；空结果误为 0，应为 -1（A10） |
| `CurrencyValuePerIncrement` | `float` | 164/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `IsFinalMessage` | `uint8_t` | 168/1 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `FloatToIntPriceMultiplier` | `float` | 172/4 | `1.0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `IntToFloatPriceDivisor` | `float` | 176/4 | `1.0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `UnderlyingSymbol` | `char[UNDERLYING_SYMBOL_LENGTH]` | 180/32 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `UpdatesBidAskOnly` | `uint8_t` | 212/1 | `0` | 未赋业务值：零/空串 |
| `StrikePrice` | `float` | 216/4 | `0` | 未赋业务值：零/空串 |
| `PutOrCall` | `PutCallEnum` | 220/1 | `PC_UNSET` | 未赋业务值：零/空串 |
| `ShortInterest` | `uint32_t` | 224/4 | `0` | 未赋业务值：零/空串 |
| `SecurityExpirationDate` | `t_DateTime4Byte` | 228/4 | `0` | 未赋业务值：零/空串 |
| `BuyRolloverInterest` | `float` | 232/4 | `0` | 未赋业务值：零/空串 |
| `SellRolloverInterest` | `float` | 236/4 | `0` | 未赋业务值：零/空串 |
| `EarningsPerShare` | `float` | 240/4 | `0` | 未赋业务值：零/空串 |
| `SharesOutstanding` | `uint32_t` | 244/4 | `0` | 未赋业务值：零/空串 |
| `IntToFloatQuantityDivisor` | `float` | 248/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `HasMarketDepthData` | `uint8_t` | 252/1 | `1` | 生成（正常/空/拒绝路径的差异见主报告） |
| `DisplayPriceMultiplier` | `float` | 256/4 | `1.0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `ExchangeSymbol` | `char[SYMBOL_LENGTH]` | 260/64 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `InitialMarginRequirement` | `float` | 324/4 | `0` | 未赋业务值：零/空串 |
| `MaintenanceMarginRequirement` | `float` | 328/4 | `0` | 未赋业务值：零/空串 |
| `Currency` | `char[CURRENCY_CODE_LENGTH]` | 332/8 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `ContractSize` | `float` | 340/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `OpenInterest` | `uint32_t` | 344/4 | `0` | 未赋业务值：零/空串 |
| `RolloverDate` | `t_DateTime4Byte` | 348/4 | `0` | 未赋业务值：零/空串 |
| `IsDelayed` | `uint8_t` | 352/1 | `0` | 未赋业务值：零/空串 |
| `SecurityIdentifier` | `int64_t` | 360/8 | `0` | 未赋业务值：零/空串 |
| `ProductIdentifier` | `char[GENERAL_IDENTIFIER_LENGTH]` | 368/64 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |

## 509 SECURITY_DEFINITION_REJECT（104 字节）

已实现：请求号与拒绝文本。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `RequestID` | `int32_t` | 4/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `RejectText` | `char[TEXT_DESCRIPTION_LENGTH]` | 8/96 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |

## 601 ACCOUNT_BALANCE_REQUEST（40 字节）

已实现：单账户/空账户请求；未知账户拒绝。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `RequestID` | `int32_t` | 4/4 | `0` | 读取（入站） |
| `TradeAccount` | `char[TRADE_ACCOUNT_LENGTH]` | 8/32 | `{}` | 读取（入站） |

## 602 ACCOUNT_BALANCE_REJECT（104 字节）

已实现余额拒绝；也被错误用于账户列表失败，见 A12。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `RequestID` | `int32_t` | 4/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `RejectText` | `char[TEXT_DESCRIPTION_LENGTH]` | 8/96 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |

## 600 ACCOUNT_BALANCE_UPDATE（416 字节）

部分：现金、可用资金、币种、盈亏；保证金/风险限额等默认 0。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `RequestID` | `int32_t` | 4/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `CashBalance` | `double` | 8/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `BalanceAvailableForNewPositions` | `double` | 16/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `AccountCurrency` | `char[8]` | 24/8 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `TradeAccount` | `char[TRADE_ACCOUNT_LENGTH]` | 32/32 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `SecuritiesValue` | `double` | 64/8 | `0` | 未赋业务值：零/空串 |
| `MarginRequirement` | `double` | 72/8 | `0` | 未赋业务值：零/空串 |
| `TotalNumberMessages` | `int32_t` | 80/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `MessageNumber` | `int32_t` | 84/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `NoAccountBalances` | `uint8_t` | 88/1 | `0` | 未赋业务值：零/空串 |
| `Unsolicited` | `uint8_t` | 89/1 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `OpenPositionsProfitLoss` | `double` | 96/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `DailyProfitLoss` | `double` | 104/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `InfoText` | `char[TEXT_DESCRIPTION_LENGTH]` | 112/96 | `{}` | 未赋业务值：零/空串 |
| `TransactionIdentifier` | `uint64_t` | 208/8 | `0` | 未赋业务值：零/空串 |
| `DailyNetLossLimit` | `double` | 216/8 | `0` | 未赋业务值：零/空串 |
| `TrailingAccountValueToLimitPositions` | `double` | 224/8 | `0` | 未赋业务值：零/空串 |
| `DailyNetLossLimitReached` | `uint8_t` | 232/1 | `0` | 未赋业务值：零/空串 |
| `IsUnderRequiredMargin` | `uint8_t` | 233/1 | `0` | 未赋业务值：零/空串 |
| `ClosePositionsAtEndOfDay` | `uint8_t` | 234/1 | `0` | 未赋业务值：零/空串 |
| `TradingIsDisabled` | `uint8_t` | 235/1 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Description` | `char[TEXT_DESCRIPTION_LENGTH]` | 236/96 | `{}` | 未赋业务值：零/空串 |
| `IsUnderRequiredAccountValue` | `uint8_t` | 332/1 | `0` | 未赋业务值：零/空串 |
| `TransactionDateTime` | `t_DateTimeWithMicrosecondsInt` | 336/8 | `0` | 未赋业务值：零/空串 |
| `MarginRequirementFull` | `double` | 344/8 | `0` | 未赋业务值：零/空串 |
| `MarginRequirementFullPositionsOnly` | `double` | 352/8 | `0` | 未赋业务值：零/空串 |
| `PeakMarginRequirement` | `double` | 360/8 | `0` | 未赋业务值：零/空串 |
| `IntroducingBroker` | `char[TRADE_ACCOUNT_LENGTH]` | 368/32 | `{}` | 未赋业务值：零/空串 |
| `OpenPositionsProfitLossBasedOnSettlementPrice` | `double` | 400/8 | `0.0` | 未赋业务值：零/空串 |
| `LastOrderActivityDateTime` | `t_DateTimeWithMicrosecondsInt` | 408/8 | `0` | 未赋业务值：零/空串 |

## 800 HISTORICAL_PRICE_DATA_REQUEST（128 字节）

部分：Tick/秒/分钟/日/周；MaxDays 与 Start 同时设置语义错误，见 A03/A15。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `RequestID` | `int32_t` | 4/4 | `0` | 读取（入站） |
| `Symbol` | `char[SYMBOL_LENGTH]` | 8/64 | `{}` | 读取（入站） |
| `Exchange` | `char[EXCHANGE_LENGTH]` | 72/16 | `{}` | 读取（入站） |
| `RecordInterval` | `HistoricalDataIntervalEnum` | 88/4 | `INTERVAL_TICK` | 读取（入站） |
| `StartDateTime` | `t_DateTime` | 96/8 | `0` | 读取（入站） |
| `EndDateTime` | `t_DateTime` | 104/8 | `0` | 读取（入站） |
| `MaxDaysToReturn` | `uint32_t` | 112/4 | `0` | 读取（入站） |
| `UseZLibCompression` | `uint8_t` | 116/1 | `0` | 未读取/未使用 |
| `RequestDividendAdjustedStockData` | `uint8_t` | 117/1 | `0` | 未读取/未使用 |
| `Integer_1` | `uint16_t` | 118/2 | `0` | 未读取/未使用 |
| `UseZLibNGCompression` | `uint8_t` | 120/1 | `0` | 未读取/未使用 |

## 801 HISTORICAL_PRICE_DATA_RESPONSE_HEADER（24 字节）

已实现：请求号、周期、无记录、压缩=0；ZLib/NG 未启用属于可选。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `RequestID` | `int32_t` | 4/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `RecordInterval` | `HistoricalDataIntervalEnum` | 8/4 | `INTERVAL_TICK` | 生成（正常/空/拒绝路径的差异见主报告） |
| `UseZLibCompression` | `uint8_t` | 12/1 | `0` | 未赋业务值：零/空串 |
| `NoRecordsToReturn` | `uint8_t` | 13/1 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `IntToFloatPriceDivisor` | `float` | 16/4 | `0` | 未赋业务值：零/空串 |
| `UseZLibNGCompression` | `uint8_t` | 20/1 | `0` | 未赋业务值：零/空串 |

## 802 HISTORICAL_PRICE_DATA_REJECT（108 字节）

已实现：拒绝文本；原因码统一 GENERAL_REJECT，未细分。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `RequestID` | `int32_t` | 4/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `RejectText` | `char[TEXT_DESCRIPTION_LENGTH]` | 8/96 | `{}` | 生成（正常/空/拒绝路径的差异见主报告） |
| `RejectReasonCode` | `HistoricalPriceDataRejectReasonCodeEnum` | 104/2 | `HPDR_UNSET` | 生成（正常/空/拒绝路径的差异见主报告） |
| `RetryTimeInSeconds` | `uint16_t` | 106/2 | `0` | 未赋业务值：零/空串 |

## 803 HISTORICAL_PRICE_DATA_RECORD_RESPONSE（88 字节）

部分：OHLCV/买卖量/末条；只提供 NumTrades，没有独立日线 OI；上游日周边界待实盘数据验证。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `RequestID` | `int32_t` | 4/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `StartDateTime` | `t_DateTimeWithMicrosecondsInt` | 8/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `OpenPrice` | `double` | 16/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `HighPrice` | `double` | 24/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `LowPrice` | `double` | 32/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `LastPrice` | `double` | 40/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Volume` | `double` | 48/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `OpenInterest` | `uint32_t` | 56/4 | `0` | 共用偏移 56，固定写 NumTrades；未提供 OI |
| `NumTrades` | `uint32_t` | 56/4 | `union alias` | 共用偏移 56，固定写 NumTrades；未提供 OI |
| `BidVolume` | `double` | 64/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `AskVolume` | `double` | 72/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `IsFinalRecord` | `uint8_t` | 80/1 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |

## 804 HISTORICAL_PRICE_DATA_TICK_RECORD_RESPONSE（48 字节）

已实现：小数秒、成交价量、方向、末条；上游时间顺序按块排序。

| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |
|---|---|---|---|---|
| `Size` | `uint16_t` | 0/2 | `sizeof(*this)` | 帧头读取/生成 |
| `Type` | `uint16_t` | 2/2 | `MESSAGE_TYPE` | 帧头读取/生成 |
| `RequestID` | `int32_t` | 4/4 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `DateTime` | `t_DateTimeWithMilliseconds` | 8/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `AtBidOrAsk` | `AtBidOrAskEnum` | 16/2 | `BID_ASK_UNSET` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Price` | `double` | 24/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `Volume` | `double` | 32/8 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
| `IsFinalRecord` | `uint8_t` | 40/1 | `0` | 生成（正常/空/拒绝路径的差异见主报告） |
