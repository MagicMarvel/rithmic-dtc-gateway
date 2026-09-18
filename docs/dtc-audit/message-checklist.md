# DTC 消息逐项清单（97 项）

按官方头文件顺序列出；“已实现”是该消息路径存在并经过静态审阅，不代表所有上游环境已认证。45 个 ID 有实现，52 个未实现；不能把 45/97 当作合规率。未实现的整条消息，其所有字段也未接入。

| ID | 官方消息 | 核对结果 |
|---|---|---|
| 1 | `LOGON_REQUEST` | 部分：仅读取心跳；可选认证未启用；非法登录直接断开，见 A03/A14。 |
| 2 | `LOGON_RESPONSE` | 已实现：v8、服务能力、分隔符；OCO/Bracket=0；没有历史成交专用能力位。 |
| 3 | `HEARTBEAT` | 部分：5–60 秒发送、两周期静默超时；历史专用连接策略见 A13。 |
| 5 | `LOGOFF` | 已实现接收退出及超时发送；其他协议错误并未统一发送 LOGOFF。 |
| 6 | `ENCODING_REQUEST` | 已实现：验证长度及 DTC 标识；服务端可选择自己的编码。 |
| 7 | `ENCODING_RESPONSE` | 已实现：返回 Binary=0，符合只支持一种编码的规则。 |
| 101 | `MARKET_DATA_REQUEST` | 部分：订阅/退订；缺 SNAPSHOT、SymbolID 双向唯一性，见 A06/A07。 |
| 103 | `MARKET_DATA_REJECT` | 已实现 SymbolID + RejectText。 |
| 104 | `MARKET_DATA_SNAPSHOT` | 部分：仅空初始快照；未知值哨兵正确；日统计缺失，见 A08。 |
| 107 | `MARKET_DATA_UPDATE_TRADE` | 旧变体未实现；官方标注 To be removed，已有新版替代，不列为必补项。 |
| 112 | `MARKET_DATA_UPDATE_TRADE_COMPACT` | 旧变体未实现；官方标注 To be removed，已有新版替代，不列为必补项。 |
| 134 | `MARKET_DATA_UPDATE_LAST_TRADE_SNAPSHOT` | 已实现：最近成交快照，避免当新成交重复统计。 |
| 137 | `MARKET_DATA_UPDATE_TRADE_WITH_UNBUNDLED_INDICATOR` | 旧变体未实现；官方标注 To be removed，已有新版替代，不列为必补项。 |
| 146 | `MARKET_DATA_UPDATE_TRADE_WITH_UNBUNDLED_INDICATOR_2` | 旧变体未实现；官方标注 To be removed，已有新版替代，不列为必补项。 |
| 147 | `MARKET_DATA_UPDATE_TRADE_V2` | 已实现核心成交；UnbundledTradeIndicator 恒 0，未传递拆单标志。 |
| 108 | `MARKET_DATA_UPDATE_BID_ASK` | 旧变体未实现；官方标注 To be removed，已有新版替代，不列为必补项。 |
| 117 | `MARKET_DATA_UPDATE_BID_ASK_COMPACT` | 旧变体未实现；官方标注 To be removed，已有新版替代，不列为必补项。 |
| 144 | `MARKET_DATA_UPDATE_BID_ASK_FLOAT_WITH_MICROSECONDS` | 旧变体未实现；官方标注 To be removed，已有新版替代，不列为必补项。 |
| 148 | `MARKET_DATA_UPDATE_BID_ASK_DOUBLE_WITH_MICROSECONDS` | 已实现：项目名称 MARKET_DATA_UPDATE_BID_ASK_V2 为官方 ID 148 的别名。 |
| 120 | `MARKET_DATA_UPDATE_SESSION_OPEN` | 未实现：行情日统计/逐合约状态信息，见 A08。 |
| 114 | `MARKET_DATA_UPDATE_SESSION_HIGH` | 未实现：行情日统计/逐合约状态信息，见 A08。 |
| 115 | `MARKET_DATA_UPDATE_SESSION_LOW` | 未实现：行情日统计/逐合约状态信息，见 A08。 |
| 113 | `MARKET_DATA_UPDATE_SESSION_VOLUME` | 未实现：行情日统计/逐合约状态信息，见 A08。 |
| 124 | `MARKET_DATA_UPDATE_OPEN_INTEREST` | 未实现：行情日统计/逐合约状态信息，见 A08。 |
| 119 | `MARKET_DATA_UPDATE_SESSION_SETTLEMENT` | 未实现：行情日统计/逐合约状态信息，见 A08。 |
| 135 | `MARKET_DATA_UPDATE_SESSION_NUM_TRADES` | 未实现：行情日统计/逐合约状态信息，见 A08。 |
| 136 | `MARKET_DATA_UPDATE_TRADING_SESSION_DATE` | 未实现：行情日统计/逐合约状态信息，见 A08。 |
| 102 | `MARKET_DEPTH_REQUEST` | 部分：订阅/退订、深度数量；缺 SNAPSHOT，见 A06。 |
| 121 | `MARKET_DEPTH_REJECT` | 已实现 SymbolID + RejectText。 |
| 122 | `MARKET_DEPTH_SNAPSHOT_LEVEL` | 已实现：首/尾批标志、空簿、档位、数量、订单数。 |
| 145 | `MARKET_DEPTH_SNAPSHOT_LEVEL_FLOAT` | 旧变体未实现；官方标注 To be removed，已有新版替代，不列为必补项。 |
| 109 | `MARKET_DEPTH_UPDATE_LEVEL_V2` | 已实现：39 字节 pack(1)，毫秒时间、档位、数量、批次结束。 |
| 140 | `MARKET_DEPTH_UPDATE_LEVEL_FLOAT_WITH_MILLISECONDS` | 旧变体未实现；官方标注 To be removed，已有新版替代，不列为必补项。 |
| 100 | `MARKET_DATA_FEED_STATUS` | 已实现全局断线/恢复状态；未覆盖逐合约状态。 |
| 116 | `MARKET_DATA_FEED_SYMBOL_STATUS` | 未实现：行情日统计/逐合约状态信息，见 A08。 |
| 138 | `TRADING_SYMBOL_STATUS` | 未实现：行情日统计/逐合约状态信息，见 A08。 |
| 150 | `MARKET_ORDERS_REQUEST` | 未实现：对外 MBO；当前仅输出聚合 L2，属于明确范围外。 |
| 151 | `MARKET_ORDERS_REJECT` | 未实现：对外 MBO；当前仅输出聚合 L2，属于明确范围外。 |
| 152 | `MARKET_ORDERS_ADD` | 未实现：对外 MBO；当前仅输出聚合 L2，属于明确范围外。 |
| 153 | `MARKET_ORDERS_MODIFY` | 未实现：对外 MBO；当前仅输出聚合 L2，属于明确范围外。 |
| 154 | `MARKET_ORDERS_REMOVE` | 未实现：对外 MBO；当前仅输出聚合 L2，属于明确范围外。 |
| 155 | `MARKET_ORDERS_SNAPSHOT_MESSAGE_BOUNDARY` | 未实现：对外 MBO；当前仅输出聚合 L2，属于明确范围外。 |
| 208 | `SUBMIT_NEW_SINGLE_ORDER` | 部分：四种普通单；IsParentOrder 被忽略，价格校验硬编码，见 A05/A11。 |
| 201 | `SUBMIT_NEW_OCO_ORDER` | 未实现：OCO/平仓命令。OCO/Bracket 能力=0；收到此类型当前静默忽略。 |
| 209 | `SUBMIT_FLATTEN_POSITION_ORDER` | 未实现：OCO/平仓命令。OCO/Bracket 能力=0；收到此类型当前静默忽略。 |
| 210 | `FLATTEN_POSITIONS_FOR_TRADE_ACCOUNT` | 未实现：OCO/平仓命令。OCO/Bracket 能力=0；收到此类型当前静默忽略。 |
| 203 | `CANCEL_ORDER` | 部分：正常撤单校验；拒绝回报状态/关联错误，见 A02/A04。 |
| 204 | `CANCEL_REPLACE_ORDER` | 部分：价格设置标志、总数量语义正确；拒绝状态错误，见 A04。 |
| 300 | `OPEN_ORDERS_REQUEST` | 已实现请求过滤和空结果；仅当前服务缓存，终态按 ID 查询范围待扩展。 |
| 302 | `OPEN_ORDERS_REJECT` | 已实现：请求号与拒绝文本。 |
| 301 | `ORDER_UPDATE` | 部分：主体布局正确；拒单字段/状态错误，时间及附加字段未提供，见 A02/A04。 |
| 303 | `HISTORICAL_ORDER_FILLS_REQUEST` | 未实现：历史成交或更正成交；收到请求当前静默忽略。 |
| 308 | `HISTORICAL_ORDER_FILLS_REJECT` | 未实现：历史成交或更正成交；收到请求当前静默忽略。 |
| 304 | `HISTORICAL_ORDER_FILL_RESPONSE` | 未实现：历史成交或更正成交；收到请求当前静默忽略。 |
| 305 | `CURRENT_POSITIONS_REQUEST` | 已实现：按账户筛选。 |
| 307 | `CURRENT_POSITIONS_REJECT` | 已实现：请求号与拒绝文本。 |
| 306 | `POSITION_UPDATE` | 部分：数量、均价、浮盈、空持仓及主动更新；保证金等扩展字段未提供。 |
| 309 | `ADD_CORRECTING_ORDER_FILL` | 未实现：历史成交或更正成交；收到请求当前静默忽略。 |
| 310 | `CORRECTING_ORDER_FILL_RESPONSE` | 未实现：历史成交或更正成交；收到请求当前静默忽略。 |
| 400 | `TRADE_ACCOUNTS_REQUEST` | 部分：账户查询；失败借用余额拒绝类型、空列表无终止，见 A12。 |
| 401 | `TRADE_ACCOUNT_RESPONSE` | 部分：非空单账户列表及编号；空列表无回包，见 A12。 |
| 500 | `EXCHANGE_LIST_REQUEST` | 已实现：上游交易所列表；失败退回默认交易所。 |
| 501 | `EXCHANGE_LIST_RESPONSE` | 已实现逐交易所回包、空列表及末包标志。 |
| 502 | `SYMBOLS_FOR_EXCHANGE_REQUEST` | 部分：只匹配默认合约，未枚举完整目录，也未维护定义订阅，见 A09。 |
| 503 | `UNDERLYING_SYMBOLS_FOR_EXCHANGE_REQUEST` | 部分：只返回默认 underlying，见 A09。 |
| 504 | `SYMBOLS_FOR_UNDERLYING_REQUEST` | 部分：只返回默认到期合约，见 A09。 |
| 506 | `SECURITY_DEFINITION_FOR_SYMBOL_REQUEST` | 已实现：默认/动态合约解析、失败拒绝。 |
| 507 | `SECURITY_DEFINITION_RESPONSE` | 部分：基础期货元数据；到期日/保证金等缺失；空结果默认值错误，见 A10。 |
| 510 | `SECURITY_DEFINITION_RESPONSE_V2` | 新版证券定义未实现；旧版 507 仍在官方头文件中，不等于协议错误。 |
| 508 | `SYMBOL_SEARCH_REQUEST` | 部分：动态搜索，但 SearchType 未传入上游且未统一过滤，见 A09。 |
| 509 | `SECURITY_DEFINITION_REJECT` | 已实现：请求号与拒绝文本。 |
| 601 | `ACCOUNT_BALANCE_REQUEST` | 已实现：单账户/空账户请求；未知账户拒绝。 |
| 602 | `ACCOUNT_BALANCE_REJECT` | 已实现余额拒绝；也被错误用于账户列表失败，见 A12。 |
| 600 | `ACCOUNT_BALANCE_UPDATE` | 部分：现金、可用资金、币种、盈亏；保证金/风险限额等默认 0。 |
| 607 | `ACCOUNT_BALANCE_ADJUSTMENT` | 未实现：历史余额/余额调整；不影响基础余额查询。 |
| 608 | `ACCOUNT_BALANCE_ADJUSTMENT_REJECT` | 未实现：历史余额/余额调整；不影响基础余额查询。 |
| 609 | `ACCOUNT_BALANCE_ADJUSTMENT_COMPLETE` | 未实现：历史余额/余额调整；不影响基础余额查询。 |
| 603 | `HISTORICAL_ACCOUNT_BALANCES_REQUEST` | 未实现：历史余额/余额调整；不影响基础余额查询。 |
| 604 | `HISTORICAL_ACCOUNT_BALANCES_REJECT` | 未实现：历史余额/余额调整；不影响基础余额查询。 |
| 606 | `HISTORICAL_ACCOUNT_BALANCE_RESPONSE` | 未实现：历史余额/余额调整；不影响基础余额查询。 |
| 700 | `USER_MESSAGE` | 未实现：用户消息/日志/告警/日志簿；当前错误只写服务端日志。 |
| 701 | `GENERAL_LOG_MESSAGE` | 未实现：用户消息/日志/告警/日志簿；当前错误只写服务端日志。 |
| 702 | `ALERT_MESSAGE` | 未实现：用户消息/日志/告警/日志簿；当前错误只写服务端日志。 |
| 703 | `JOURNAL_ENTRY_ADD` | 未实现：用户消息/日志/告警/日志簿；当前错误只写服务端日志。 |
| 704 | `JOURNAL_ENTRIES_REQUEST` | 未实现：用户消息/日志/告警/日志簿；当前错误只写服务端日志。 |
| 705 | `JOURNAL_ENTRIES_REJECT` | 未实现：用户消息/日志/告警/日志簿；当前错误只写服务端日志。 |
| 706 | `JOURNAL_ENTRY_RESPONSE` | 未实现：用户消息/日志/告警/日志簿；当前错误只写服务端日志。 |
| 800 | `HISTORICAL_PRICE_DATA_REQUEST` | 部分：Tick/秒/分钟/日/周；MaxDays 与 Start 同时设置语义错误，见 A03/A15。 |
| 801 | `HISTORICAL_PRICE_DATA_RESPONSE_HEADER` | 已实现：请求号、周期、无记录、压缩=0；ZLib/NG 未启用属于可选。 |
| 802 | `HISTORICAL_PRICE_DATA_REJECT` | 已实现：拒绝文本；原因码统一 GENERAL_REJECT，未细分。 |
| 803 | `HISTORICAL_PRICE_DATA_RECORD_RESPONSE` | 部分：OHLCV/买卖量/末条；只提供 NumTrades，没有独立日线 OI；上游日周边界待实盘数据验证。 |
| 804 | `HISTORICAL_PRICE_DATA_TICK_RECORD_RESPONSE` | 已实现：小数秒、成交价量、方向、末条；上游时间顺序按块排序。 |
| 807 | `HISTORICAL_PRICE_DATA_RESPONSE_TRAILER` | 可选未实现：已有 IsFinalRecord/NoRecordsToReturn 结束方式，不是必补项。 |
| 900 | `HISTORICAL_MARKET_DEPTH_DATA_REQUEST` | 未实现：历史市场深度；实时 L2 不等于历史深度回填。 |
| 901 | `HISTORICAL_MARKET_DEPTH_DATA_RESPONSE_HEADER` | 未实现：历史市场深度；实时 L2 不等于历史深度回填。 |
| 902 | `HISTORICAL_MARKET_DEPTH_DATA_REJECT` | 未实现：历史市场深度；实时 L2 不等于历史深度回填。 |
| 903 | `HISTORICAL_MARKET_DEPTH_DATA_RECORD_RESPONSE` | 未实现：历史市场深度；实时 L2 不等于历史深度回填。 |
