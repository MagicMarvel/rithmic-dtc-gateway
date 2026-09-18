# DTC v8 逐项协议审计

> 本文及 message/field/size 清单保留**修复前**的审计快照、计数和行号，不代表修复后的当前状态。2026-09-04 的代码修复、验证和剩余限制见 [修复记录](fixes.md)。原来的 3 个复现测试现已转为默认运行的回归测试。

审计日期：2026-09-04。对象：当前工作区 `src/dtc.rs`、`src/rithmic_feed.rs`、`src/history_feed.rs`、`src/trading_feed.rs`、`src/order_book.rs` 与现有测试。

结论：当前实现是可用的 DTC 功能子集，但仍有协议语义缺陷，不能认定为完整合规。官方头文件共定义 **97 个消息 ID**，项目接入 **45 个**，未接入 **52 个**。未接入包含旧变体和可选功能，不能直接当作 52 个 bug。已接入的 **442 个字段**已逐个列出读取、生成或未使用状态；字段处理存在不代表上游数据已验证。

完整清单：

- [97 个消息逐项对照](message-checklist.md)：按官方头文件顺序，每项独立列出状态。
- [45 个已接入消息、442 个字段逐项对照](field-checklist.md)：官方类型、偏移、字节数、默认值和项目处理。
- [独立 C++ ABI 输出](official-layout.tsv)：96 个官方结构的编译器结果；ID 210 在本次头文件只有常量，没有对应结构定义。
- [45 个消息大小对照](size-comparison.tsv)：全部相符；LOGON_REQUEST 使用正常完整布局 284 字节作比较，当前 parser 的最短接受长度为 148。
- [机器可读清单](inventory.json)、[ABI 生成脚本](build_inventory.py)、[审阅清单生成脚本](generate_checklist.py)。

## 基准及方法

以 [官方协议流程](https://www.sierrachart.com/index.php?page=doc/DTCMessageDocumentation.php)、[消息总目录](https://www.sierrachart.com/index.php?page=doc/DTCMessages_All.php) 和 [官方 DTCProtocol.h](https://www.sierrachart.com/DTC_Files/DTCProtocol.h) 为准。头文件仍为 CURRENT_VERSION=8；官网下载目录标注头文件更新于 2026-08-23，消息总目录页落后于头文件，因此把两者合并核对。旧 INT 消息已在官方流程中标明停用，不计入最新 97 个有效常量。

下载头文件 SHA-256：`5E445B6C7B2316A50EF3D04ECDE9EE3CFB0573D83A0619D5A164618B26474A76`。原文件保存在本目录，未改写；`procedures.html` 和 `messages.html` 为下载时的流程和总目录快照。

用 MinGW g++17 编译官方结构的 `sizeof/offsetof`，独立于 Rust 现有测试中手写的预期偏移。对已实现的消息大小和已使用字段偏移进行核对，未发现大小/主要字段位置不一致；包括容易出错的 pack(1) 深度增量 39 字节、ORDER_UPDATE 720 字节、账户余额 416 字节。148 在项目中的命名虽为 BID_ASK_V2，但 ID 和官方结构对应，不能算消息缺失。

本次新增审计材料和 3 个默认忽略的离线回归用例，未修改运行逻辑、配置或凭据，未连接 Rithmic、未下单。既有测试通过不等于 Sierra/Rithmic 的全部场景通过。

## 优先修复的具体问题

### A01 · P1 · 接收半包会被心跳/行情事件取消而丢失（已复现）

位置：`src/dtc.rs:701`、`src/dtc.rs:1344`。会话在 `tokio::select!` 内反复创建 `read_frame`；该函数的 header、body 都是局部缓冲，通过 `read_exact` 消费 socket。当另一分支先完成时，已消费的一部分数据不会放回 socket。下一轮将余下数据当成新帧头。

离线发送 506 请求的前 2 字节，等服务端心跳后再发送剩余数据，完整请求不再得到响应。影响不仅是心跳，任何行情/交易/历史输出分支都能触发。应使用跨轮保存的帧缓冲，或独立持续运行的 reader task。依据：[Handling Network Data](https://www.sierrachart.com/index.php?page=doc/DTCMessageDocumentation.php#HandlingNetworkData)。

### A02 · P1 · 禁用交易的拒绝回报丢失关联信息（已复现）

位置：`src/dtc.rs:1475`、`src/dtc.rs:1332`、`src/dtc.rs:1734`。没有 trading service 时，在解析具体请求前统一拒绝。提交、撤单、改单最终都变成空 ClientOrderID、空 TradeAccount 的 NEW_ORDER_REJECTED；撤单/改单也没有对应 reason=9/10。

离线 CANCEL_ORDER 的 ClientOrderID=`client`，收到回报对应 6 字节全为零。客户端无法把失败关联到操作。应先解析可用关联字段，再按原请求类型返回错误。依据：[ORDER_UPDATE](https://www.sierrachart.com/index.php?page=doc/DTCMessages_TradingRelatedMessages.php#Messages-ORDER_UPDATE)。

### A03 · P2 · 强制完整最新长度，缺少旧版尾字段默认处理（已复现）

位置：`src/dtc.rs:876`、`src/dtc.rs:1719` 及各请求的 require_size。协议要求按收到的 Size 读取现有字段，不存在的新增字段使用默认值。目前历史请求强制 128 字节，旧的 120 字节布局包含全部基础字段，只缺可选 UseZLibNGCompression 尾部，仍被关闭连接。类似风险存在于新增尾字段的订单、发现请求。

离线 120 字节历史请求得到 EOF；合理响应应进入业务层并在无历史服务时返回带 RequestID 的 802。应按必需字段下界校验，尾字段逐个安全读取。依据：[Versioning](https://www.sierrachart.com/index.php?page=doc/DTCMessageDocumentation.php#Versioning)。

### A04 · P1 · 已存在订单的撤单/改单失败被报告为订单已拒绝

位置：`src/dtc.rs:1240`、`src/dtc.rs:1680`。`order_action_rejection` 无条件设置 OrderStatus=9。官方要求：已知订单的操作失败应携带订单当前状态；不知道状态时使用 UNSPECIFIED；不存在的 ServerOrderID 应留空。当前失败回报还会照抄无效 ServerOrderID。新单拒绝也没有填写官方要求的 Symbol/Exchange。

例如一笔仍 Working 的 Limit 单因改单数量非法被拒，回报却把订单标成 Rejected，可能导致客户端错误理解仍在工作的订单。修复时需把现有订单快照、原请求 Symbol/Exchange 传给回报构造器，区分“操作失败”和“订单终态”。依据：[ORDER_UPDATE 状态及拒绝规则](https://www.sierrachart.com/index.php?page=doc/DTCMessages_TradingRelatedMessages.php#Messages-ORDER_UPDATE)。静态确认，未进行上游交易复现。

### A05 · P1 · IsParentOrder=1 被当普通独立单提交

位置：`src/dtc.rs:1743`。只读 IsAutomatedOrder，忽略偏移 201 的 IsParentOrder。虽然 BracketOrdersSupported=0，但收到 208 父单时没有拒绝，仍走普通提交路径；后续 201 OCO 消息又被忽略。

不要求为了合规实现 bracket；需要明确拒绝这种不支持的组合语义，避免只提交父单。官方对该标志要求等待后续 OCO 成组处理。依据：[SUBMIT_NEW_SINGLE_ORDER](https://www.sierrachart.com/index.php?page=doc/DTCMessages_OrderEntryModificationMessages.php#Messages-SUBMIT_NEW_SINGLE_ORDER)。此为异常客户端输入路径，未用真实订单复现。

### A06 · P2 · RequestAction=SNAPSHOT(3) 缺失

位置：`src/dtc.rs:1771`、`src/dtc.rs:1837`。行情和深度都只接受 SUBSCRIBE/UNSUBSCRIBE，SNAPSHOT 返回拒绝。应提供单次快照并且不留下持续订阅。依据：[Market Data Messages](https://www.sierrachart.com/index.php?page=doc/DTCMessages_MarketDataMessages.php)。这是已支持消息中的功能缺口，和只实现一种输出变体不同。

### A07 · P2 · 缺少 Symbol/Exchange → SymbolID 的唯一性检查

位置：`src/rithmic_feed.rs:307`。只校验 symbol_id 是否存在，未校验同一 Symbol/Exchange 是否已经绑定其他 ID。官方要求已有订阅的合约改用另一 SymbolID 时拒绝。当前可能同时接受两份订阅，退掉其中一份又直接取消共享上游行情，另一份受影响。应维护双向映射并处理上下游订阅生命周期。依据：[MARKET_DATA_REQUEST::SymbolID](https://www.sierrachart.com/index.php?page=doc/DTCMessages_MarketDataMessages.php#Messages-MARKET_DATA_REQUEST)。

### A08 · P2 · 日统计行情没有接入，初始行情快照始终为空

位置：`src/dtc.rs:167`、`src/dtc.rs:2429`。快照 DBL_MAX/UINT_MAX 哨兵正确，但不带缓存的最新报价/成交；随后只靠 Trade/BBO 消息补齐。Session Open/High/Low/Volume、OpenInterest、Settlement、NumTrades、TradingSessionDate 没有增量消息或另一种真实快照替代。休市/无新数据时可能长时间没有完整当前状态。逐合约 feed status 和 trading status 也未提供。

这是行情完整性缺口，不意味着所有八种统计消息都必须单独实现：也可按协议用合适的快照更新；必须保证所需数据有来源和更新路径。依据：[行情快照及统计字段](https://www.sierrachart.com/index.php?page=doc/DTCMessages_MarketDataMessages.php#Messages-MARKET_DATA_SNAPSHOT)。

### A09 · P2 · 合约发现链路只实现了一部分

位置：`src/dtc.rs:2007`、`src/dtc.rs:2030`、`src/dtc.rs:2047`、`src/dtc.rs:2091`；`src/rithmic_feed.rs:548`。

502/503/504 只匹配默认 instrument，并不使用已发现目录。因此即使 506 可以解析 NQ，按交易所/underlying 枚举仍只可能返回默认 ES。508 的 SearchType 只影响默认合约 fallback；动态上游搜索未接收这个参数，结果也没有再按该类型过滤。搜索错误用空结果吞掉，且结果有截断上限。502 没有维护定义订阅/退订状态。

应把查询接到统一目录，对支持范围外/失败明确拒绝或记录限制，按请求过滤并正确标记末包。依据：[Symbol Discovery](https://www.sierrachart.com/index.php?page=doc/DTCMessages_SymbolDiscoverySecurityDefinitionsMessages.php)。

### A10 · P2 · 空证券定义未按官方默认值初始化；期货元数据不全

位置：`src/dtc.rs:2309`、`src/dtc.rs:2324`。空结果全零，PriceDisplayFormat 因此为 0（零位小数），而官方默认是 -1（未设置）；FloatToIntPriceMultiplier、IntToFloatPriceDivisor、DisplayPriceMultiplier 等默认 1 也变成 0。官方空搜索结果要求除 RequestID/IsFinalMessage 外保持默认值。

正常定义缺 SecurityExpirationDate、RolloverDate、Initial/MaintenanceMargin、OpenInterest、IsDelayed 的上游映射；HasMarketDepthData 固定 1，没有按实际服务/合约能力设置。缺元数据本身属于功能范围，但 README 把 rollover 全归为 Sierra 私有能力不准确：DTC 有基础到期/换月日期，Sierra 特有规则才是另一层。依据：[证券定义规范](https://www.sierrachart.com/index.php?page=doc/DTCMessages_SymbolDiscoverySecurityDefinitionsMessages.php) 及本目录头文件默认值。

### A11 · P2 · 合约定义 tick size 与下单价格验证不一致

位置：`src/dtc.rs:1621`、`src/trading_feed.rs:958`。动态合约已解析出各自 MinPriceIncrement，但交易只取 symbol/exchange，校验一律用 0.25。对其他 tick size 的合约可能错误拒绝合法价格，或放过不合法价格；与已公布证券定义不一致。

应使用目标合约元数据，或在交易入口限制为已验证的 0.25-tick 合约。README 已提示该限制，但入口未实际限制品种。另：负价格被本地规则拒绝，属于明确实现限制，不能由通用 DTC price 类型推导为禁止。依据：[Order Price Formatting](https://www.sierrachart.com/index.php?page=doc/DTCMessages_OrderEntryModificationMessages.php#PriceFormattingNotes)。

### A12 · P2 · 账户列表错误/空列表响应不完整

位置：`src/dtc.rs:1494`。TRADE_ACCOUNTS_REQUEST 出错发送 ACCOUNT_BALANCE_REJECT（602），不是账户列表响应；空 accounts 生成零条消息，客户端无法获知结束。正常单账户路径和编号正确。应明确空列表结束表示，失败通过适当消息/会话状态说明，避免使用不相关的余额拒绝。当前生产配置通常预选一个账户，空列表场景需专项测试。依据：[Account List Messages](https://www.sierrachart.com/index.php?page=doc/DTCMessages_AccountListMessages.php)。

### A13 · P2 · 历史专用连接仍强制客户端心跳

位置：`src/dtc.rs:637`、`src/dtc.rs:843`。历史连接与普通连接共用接收静默超时；即使服务端持续输出历史，客户端按官方建议不发 heartbeat，也会在 2×5–60 秒后被强制关闭。

应区分历史传输中活动和真正空闲。当前压缩=0，混合发送 heartbeat 不会破坏压缩流；问题是单向下载可能被误判超时。依据：[Historical Price Data 流程](https://www.sierrachart.com/index.php?page=doc/DTCMessageDocumentation.php#HistoricalPriceData)。此为静态确认的兼容风险，未模拟完整长下载。

### A14 · P2 · 慢上游请求阻塞整个 DTC 会话；登录错误缺少结果回报

位置：`src/dtc.rs:601`、`src/dtc.rs:715`、`src/dtc.rs:807`。发现/订阅/交易 handler 在 select 分支内等待上游，因此在慢查询期间不能发送心跳，也不能读取 LOGOFF；登录后目录预加载同样发生在 select 启动前。历史工作已拆出，其他路径尚未拆出。

非法心跳值等登录错误直接返回 SessionError，没有 LOGON_RESPONSE 的失败 Result/ResultText。应让会话 I/O 持续工作，上游操作异步回包，登录参数拒绝有可解释结果。没有规定必须实现重定向或所有认证方式。依据：[Connection and Logon / Heartbeat](https://www.sierrachart.com/index.php?page=doc/DTCMessages_AuthenticationConnectionMonitoringMessages.php)。

### A15 · P2 · MaxDaysToReturn 与 StartDateTime 同时设置时，天数上限失效

位置：`src/history_feed.rs:210`。当 start_time>0 时直接使用它，max_days 完全忽略；Tick 分块随后还把 max_days 清零。例：Start=30 天前，End=当前，MaxDays=2，实际查询仍从 30 天前开始。官方 MaxDays 是从 End/最新数据向前计算的最大天数，不是仅在 Start 未提供时的默认值。

应取 Start 与 End−MaxDays 的较晚边界，并保持起止区间合法。README 所称 MaxDays 会被转发到 Rithmic 也不准确：当前本地算区间后调用 load_ticks_all/load_time_bars_all。依据：[HISTORICAL_PRICE_DATA_REQUEST](https://www.sierrachart.com/index.php?page=doc/DTCMessages_HistoricalPriceDataMessages.php#Messages-HISTORICAL_PRICE_DATA_REQUEST)。

## 按官方流程顺序的核对

| 官方项目 | 结论 |
|---|---|
| Client/Server 支持范围 | DTC 允许子集；45/97 不是合规率，需结合能力位和请求行为。 |
| Message Structure | 2 字节 Size + 2 字节 Type、小端、各结构对齐/大小符合当前头文件。 |
| Get/Set Functions | Rust 可不采用 C++ 方法；缺少尾字段默认处理见 A03。 |
| Handling Network Data | 完整连续帧可读；分包跨 select 事件失败，A01。 |
| Versioning：固定字符串 Binary | 能跳过未知类型、接收更长帧；旧短帧兼容不足，A03。 |
| Versioning：VLS | 未支持；可选编码，不要求全部支持。 |
| Versioning：GPB | 未支持；可选编码。 |
| Breaking Changes | 使用当前 8 与当前核心消息；不承担已经停用 INT 变体。 |
| Header namespaces | Rust 自己的常量/编码，无需采用 C++ 命名空间。 |
| Symbols | 支持 SYMBOL-EXCHANGE / SYMBOL.EXCHANGE；纯符号无 Exchange 的动态解析有限。 |
| Multiple Connections | listener 为每连接建服务客户端；支持多个 DTC 会话，上游账户并发限制另计。 |
| Basic Server Procedures | 服务能力按服务是否存在设置；慢操作/错误退出待改，A14。 |
| Connection and Logon | Binary 默认、可跳过编码协商、先登录；可选认证未用，失败回包不足。 |
| Encoding Request Sequence | 返回服务器 Binary，即使请求其他编码也合法；仅登录前处理协商。 |
| Logging Off | 收到 LOGOFF 退出；超时发送 LOGOFF；一般协议错误直接 EOF。 |
| Market Data | Trade/BBO/L2/FeedStatus 有实现；快照请求、日统计、唯一 ID 缺陷见 A06–A08。 |
| Market Data Price Format | 普通价格、DisplayPriceMultiplier=1；证券 tick 与交易校验不一致，A11。 |
| Trade Order Procedures | 普通单生命周期存在；ClientOrderID 校验存在；拒绝语义错误，A02/A04。 |
| Bracket Order Procedures | 能力=0；不支持合理，但 IsParentOrder 不应降级成普通单，A05。 |
| Integer Trading Messages | 官方已停用，不补。 |
| Automatic Trading Data Updates | Order/Position/Balance 广播存在；上游重连及长时间丢消息未做本次集成验证。 |
| Unset Message Fields | 行情 DBL_MAX/UINT_MAX 和订单价格量哨兵存在；空证券定义默认值错误，A10。 |
| Historical Price Data | 分块、时间排序、请求关联、末条、空结果、连续请求存在；A03/A13/A15 待修。 |
| SymbolID / RequestID | 回应回填 RequestID；SymbolID 缺少合约反向唯一性，A07。RequestID 的唯一分配主要是客户端责任。 |
| Nonstandard Messages | 未定义自有 10000+ 消息；未知类型静默跳过符合向前兼容原则。 |
| Transport / Security | 本地普通 TCP、没有 TLS。官方总览对用于交易的连接要求 TLS；当前属于本地 Paper 工具的明确偏离，不应声称满足完整安全部署要求。 |

对未知消息静默跳过本身不能一概判为 bug，官方 Versioning 允许这样做；但已识别请求的拒绝必须携带正确关联，受支持消息中的关键语义不能静默降级。

## 编码、枚举与数据语义

| 项目 | 实现与限制 |
|---|---|
| EncodingEnum | Binary(0)；VLS/JSON/Compact JSON/GPB 未实现，可选。 |
| LogonStatusEnum | 只构造 SUCCESS，缺 ERROR/NO_RECONNECT 失败说明；重定向是可选。 |
| RequestActionEnum | 1/2；缺 3，A06。 |
| OrderStatusEnum | 当前映射覆盖通常生命周期；操作拒绝错误地固定 REJECTED，A04。 |
| OrderUpdateReasonEnum | 1–10 的主要原因有映射；关闭交易错误使用 8，A02。 |
| AtBidOrAskEnum / Enum8 | 0/1/2 分类；快照成交不当作新成交；历史无法区分时保留 0。 |
| UnbundledTradeIndicatorEnum | Trade V2 留 0，无拆单首/尾标记来源。 |
| MarketDepthUpdateTypeEnum | Insert/Update=1，Delete=2。 |
| FinalUpdateInBatchEnum | Final=1，非末=2；不是 bool，当前写法正确。 |
| MessageSetBoundaryEnum | MBO 未实现。 |
| OrderTypeEnum | 1–4；MIT/LIT 显式拒绝，允许服务器限定支持范围。 |
| TimeInForceEnum | Day/GTC/IOC/FOK；GTD/AON 不支持并拒绝；Unset 本地归为 Day；改单改变 TIF 被拒绝。 |
| BuySellEnum | 1/2；非法值拒绝。 |
| OpenCloseTradeEnum | 未解析/转发，当前按上游默认处理；需明确限制。 |
| PartialFillHandlingEnum | OCO/Bracket 未支持。 |
| MarketDataFeedStatusEnum | 1/2；缺逐合约状态。 |
| TradingStatusEnum | 保持 UNKNOWN；无停牌/开闭市状态更新。 |
| PriceDisplayFormatEnum | 普通期货来自元数据；空定义错误默认为 0；其他显示格式需合约样本验证。 |
| SecurityTypeEnum | 默认/期货；股票/外汇/期权等不在产品范围。 |
| PutCallEnum | 未提供；期货期权未支持。 |
| SearchTypeEnum | fallback 解析，动态结果未贯彻，A09。 |
| HistoricalDataIntervalEnum | Tick、1–59 秒、整分钟、整日、整周；其他间隔拒绝。 |
| HistoricalPriceDataRejectReasonCodeEnum | 全部用 GENERAL_REJECT_ERROR，RetryTime=0。 |
| float/double、整数、固定字符串 | 小端读写，输出字符串预留 NUL；输入按 UTF-8 解析，非 UTF-8 字节会失败，若需国际字符应明确编码约定。 |
| 时间单位 | Trade/BBO V2=整数微秒；Depth V2=整数毫秒；Heartbeat=整数秒；Tick history/LastTradeSnapshot=double 秒；Bar Start=整数微秒（最新头文件允许）；未发现把这些混用。 |
| 历史起止/交易日 | 本地按 Unix UTC；上游 bar.marker 减 interval 的日/周会话边界仍需真实样本核对，未判定必错。 |

## 不必当作协议 bug 的未实现项

OCO、Bracket、外部 MBO、历史深度、历史成交、历史余额、账户余额调整、更正成交、日志簿、新证券定义 V2 都是当前缺少的功能，逐消息见清单。旧 Trade/BBO/Depth 浮点或 compact 变体有当前替代，可不补。807 trailer 有 IsFinalRecord 等现有结束方案，可不补。ZLib/NG 请求可由服务器选择不采用，响应压缩标志为 0 即可。

注意当前 LOGON_RESPONSE 结构没有“HistoricalOrderFillsSupported”“HistoricalMarketDepthSupported”“MBO Supported”等单独位。README 的“不 advertise”不等于通过专门能力位通知客户端；应明确记录服务支持范围和不支持请求的行为。

历史结束空块的潜在问题没有列成当前 bug：`stream_historical_response` 自身无法在“已发 header 后最后空块”补终止，但 `history_feed.rs:162` 保留最后一个非空块的现有调用约定避免了该路径；后续重构需保留约定或增加 trailer。

## 验证记录与后续顺序

`cargo test --lib`：49 passed。随后完整普通测试 `cargo test --target-dir target/dtc-audit`：53 passed、19 ignored（16 个既有联网测试及 3 个本次回归）；`cargo fmt --check` 通过。新增离线回归位于 `tests/dtc_protocol_audit.rs`，默认 ignore，显式运行：

```powershell
cargo test --target-dir target/dtc-audit --test dtc_protocol_audit -- --ignored --nocapture
```

结果：3 failed，分别是 A01 超时、A02 ID 全零、A03 EOF；失败是按正确协议行为编写的已知缺陷验证，修复后应取消 ignore。单独 target-dir 避开默认 target 下正在占用的 dtc_server.exe；没有停止或替换运行中的服务。

建议修复顺序：先 A01 接收安全，再 A02/A04/A05 订单语义，再 A03/A13/A15 历史兼容；然后补 A06–A12 的行情与发现细节。当前请求是协议对照，因此这些修复尚未实施。
