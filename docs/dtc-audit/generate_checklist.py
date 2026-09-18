"""Render reviewed message coverage and field handling against the official ABI."""
from pathlib import Path
import csv, json, re
root = Path(__file__).resolve().parent
data = json.loads((root/'inventory.json').read_text())
abi = list(csv.DictReader((root/'official-layout.tsv').open(encoding='utf-8'), delimiter='\t'))
layouts = {(r['struct'], r['field']): r for r in abi}
notes = {
1:'部分：仅读取心跳；可选认证未启用；非法登录直接断开，见 A03/A14。',
2:'已实现：v8、服务能力、分隔符；OCO/Bracket=0；没有历史成交专用能力位。',
3:'部分：5–60 秒发送、两周期静默超时；历史专用连接策略见 A13。',
5:'已实现接收退出及超时发送；其他协议错误并未统一发送 LOGOFF。',
6:'已实现：验证长度及 DTC 标识；服务端可选择自己的编码。',7:'已实现：返回 Binary=0，符合只支持一种编码的规则。',
100:'已实现全局断线/恢复状态；未覆盖逐合约状态。',
101:'部分：订阅/退订；缺 SNAPSHOT、SymbolID 双向唯一性，见 A06/A07。',
102:'部分：订阅/退订、深度数量；缺 SNAPSHOT，见 A06。',
103:'已实现 SymbolID + RejectText。',104:'部分：仅空初始快照；未知值哨兵正确；日统计缺失，见 A08。',
109:'已实现：39 字节 pack(1)，毫秒时间、档位、数量、批次结束。',
121:'已实现 SymbolID + RejectText。',122:'已实现：首/尾批标志、空簿、档位、数量、订单数。',
134:'已实现：最近成交快照，避免当新成交重复统计。',
147:'已实现核心成交；UnbundledTradeIndicator 恒 0，未传递拆单标志。',
148:'已实现：项目名称 MARKET_DATA_UPDATE_BID_ASK_V2 为官方 ID 148 的别名。',
203:'部分：正常撤单校验；拒绝回报状态/关联错误，见 A02/A04。',
204:'部分：价格设置标志、总数量语义正确；拒绝状态错误，见 A04。',
208:'部分：四种普通单；IsParentOrder 被忽略，价格校验硬编码，见 A05/A11。',
300:'已实现请求过滤和空结果；仅当前服务缓存，终态按 ID 查询范围待扩展。',
301:'部分：主体布局正确；拒单字段/状态错误，时间及附加字段未提供，见 A02/A04。',
302:'已实现：请求号与拒绝文本。',305:'已实现：按账户筛选。',
306:'部分：数量、均价、浮盈、空持仓及主动更新；保证金等扩展字段未提供。',
307:'已实现：请求号与拒绝文本。',400:'部分：账户查询；失败借用余额拒绝类型、空列表无终止，见 A12。',
401:'部分：非空单账户列表及编号；空列表无回包，见 A12。',
500:'已实现：上游交易所列表；失败退回默认交易所。',501:'已实现逐交易所回包、空列表及末包标志。',
502:'部分：只匹配默认合约，未枚举完整目录，也未维护定义订阅，见 A09。',
503:'部分：只返回默认 underlying，见 A09。',504:'部分：只返回默认到期合约，见 A09。',
506:'已实现：默认/动态合约解析、失败拒绝。',
507:'部分：基础期货元数据；到期日/保证金等缺失；空结果默认值错误，见 A10。',
508:'部分：动态搜索，但 SearchType 未传入上游且未统一过滤，见 A09。',
509:'已实现：请求号与拒绝文本。',
600:'部分：现金、可用资金、币种、盈亏；保证金/风险限额等默认 0。',
601:'已实现：单账户/空账户请求；未知账户拒绝。',602:'已实现余额拒绝；也被错误用于账户列表失败，见 A12。',
800:'部分：Tick/秒/分钟/日/周；MaxDays 与 Start 同时设置语义错误，见 A03/A15。',
801:'已实现：请求号、周期、无记录、压缩=0；ZLib/NG 未启用属于可选。',
802:'已实现：拒绝文本；原因码统一 GENERAL_REJECT，未细分。',
803:'部分：OHLCV/买卖量/末条；只提供 NumTrades，没有独立日线 OI；上游日周边界待实盘数据验证。',
804:'已实现：小数秒、成交价量、方向、末条；上游时间顺序按块排序。',
}
used = {
1:'HeartbeatIntervalInSeconds',2:'ProtocolVersion Result ResultText ServerName TradingIsSupported OrderCancelReplaceSupported SymbolExchangeDelimiter SecurityDefinitionsSupported HistoricalPriceDataSupported MarketDepthIsSupported MarketDataSupported',
3:'CurrentDateTime',5:'Reason DoNotReconnect',6:'ProtocolType',7:'ProtocolVersion Encoding ProtocolType',100:'Status',
101:'RequestAction SymbolID Symbol Exchange',102:'RequestAction SymbolID Symbol Exchange NumLevels',103:'SymbolID RejectText',
104:'SymbolID SessionSettlementPrice SessionOpenPrice SessionHighPrice SessionLowPrice SessionVolume SessionNumTrades OpenInterest BidPrice AskPrice AskQuantity BidQuantity LastTradePrice LastTradeVolume',
109:'SymbolID DateTime Price Quantity NumOrders Level Side UpdateType FinalUpdateInBatch',
121:'SymbolID RejectText',122:'SymbolID Side Price Quantity Level IsFirstMessageInBatch IsLastMessageInBatch DateTime NumOrders',
134:'SymbolID LastTradePrice LastTradeVolume LastTradeDateTime',147:'SymbolID Price Volume DateTime AtBidOrAsk',148:'SymbolID BidPrice BidQuantity AskPrice AskQuantity DateTime',
203:'ServerOrderID ClientOrderID TradeAccount',204:'ServerOrderID ClientOrderID Price1 Price2 Quantity Price1IsSet Price2IsSet TimeInForce TradeAccount',
208:'Symbol Exchange TradeAccount ClientOrderID OrderType BuySell Price1 Price2 Quantity TimeInForce IsAutomatedOrder',
300:'RequestID RequestAllOrders ServerOrderID TradeAccount',
301:'RequestID TotalNumMessages MessageNumber Symbol Exchange ServerOrderID ClientOrderID ExchangeOrderID OrderStatus OrderUpdateReason OrderType BuySell Price1 Price2 TimeInForce OrderQuantity FilledQuantity RemainingQuantity AverageFillPrice LastFillPrice LastFillDateTime LastFillQuantity LastFillExecutionID TradeAccount InfoText NoOrders',
302:'RequestID RejectText',305:'RequestID TradeAccount',306:'RequestID TotalNumberMessages MessageNumber Symbol Exchange Quantity AveragePrice PositionIdentifier TradeAccount NoPositions Unsolicited OpenProfitLoss',
307:'RequestID RejectText',400:'RequestID',401:'TotalNumberMessages MessageNumber TradeAccount RequestID TradingIsDisabled',500:'RequestID',501:'RequestID Exchange IsFinalMessage Description',
502:'RequestID Exchange SecurityType RequestAction Symbol',503:'RequestID Exchange SecurityType',504:'RequestID UnderlyingSymbol Exchange SecurityType',506:'RequestID Symbol Exchange',
507:'RequestID Symbol Exchange SecurityType Description MinPriceIncrement PriceDisplayFormat CurrencyValuePerIncrement IsFinalMessage FloatToIntPriceMultiplier IntToFloatPriceDivisor UnderlyingSymbol IntToFloatQuantityDivisor HasMarketDepthData DisplayPriceMultiplier ExchangeSymbol Currency ContractSize ProductIdentifier',
508:'RequestID SearchText Exchange SecurityType SearchType',509:'RequestID RejectText',
600:'RequestID CashBalance BalanceAvailableForNewPositions AccountCurrency TradeAccount TotalNumberMessages MessageNumber Unsolicited OpenPositionsProfitLoss DailyProfitLoss TradingIsDisabled',601:'RequestID TradeAccount',602:'RequestID RejectText',
800:'RequestID Symbol Exchange RecordInterval StartDateTime EndDateTime MaxDaysToReturn',801:'RequestID RecordInterval NoRecordsToReturn',802:'RequestID RejectText RejectReasonCode',
803:'RequestID StartDateTime OpenPrice HighPrice LowPrice LastPrice Volume OpenInterest NumTrades BidVolume AskVolume IsFinalRecord',804:'RequestID DateTime AtBidOrAsk Price Volume IsFinalRecord'
}
incoming = {1,6,101,102,203,204,208,300,305,400,500,502,503,504,506,508,601,800}
by_name = {m['name']: m for m in data['messages']}
rows = ['# DTC 消息逐项清单（97 项）','', '按官方头文件顺序列出；“已实现”是该消息路径存在并经过静态审阅，不代表所有上游环境已认证。45 个 ID 有实现，52 个未实现；不能把 45/97 当作合规率。未实现的整条消息，其所有字段也未接入。', '', '| ID | 官方消息 | 核对结果 |', '|---|---|---|']
for m in data['messages']:
    i = m['id']
    note = notes.get(i)
    if not note:
        if m['deprecated']: note = '旧变体未实现；官方标注 To be removed，已有新版替代，不列为必补项。'
        elif 150 <= i <= 155: note = '未实现：对外 MBO；当前仅输出聚合 L2，属于明确范围外。'
        elif i in [201,209,210]: note = '未实现：OCO/平仓命令。OCO/Bracket 能力=0；收到此类型当前静默忽略。'
        elif 303 <= i <= 310: note = '未实现：历史成交或更正成交；收到请求当前静默忽略。'
        elif 603 <= i <= 609: note = '未实现：历史余额/余额调整；不影响基础余额查询。'
        elif 700 <= i <= 706: note = '未实现：用户消息/日志/告警/日志簿；当前错误只写服务端日志。'
        elif i >= 900: note = '未实现：历史市场深度；实时 L2 不等于历史深度回填。'
        elif i == 807: note = '可选未实现：已有 IsFinalRecord/NoRecordsToReturn 结束方式，不是必补项。'
        elif i == 510: note = '新版证券定义未实现；旧版 507 仍在官方头文件中，不等于协议错误。'
        else: note = '未实现：行情日统计/逐合约状态信息，见 A08。'
    rows.append(f"| {i} | `{m['name']}` | {note} |")
(root/'message-checklist.md').write_text('\n'.join(rows)+'\n',encoding='utf-8')
rows = ['# 已接入消息的字段逐项清单','', '来源：官方 DTCProtocol.h，偏移/大小由 g++ 实际编译 offsetof/sizeof 取得。逐字段“读取/生成”仅表示本地有处理；具体语义缺陷见主报告。未读取字段和输出默认值明确列出，不能把默认值当作已实现业务。Size/Type 统一处理；入站旧长度兼容问题见 A03。', '']
for st in data['structs']:
    m = by_name[st['message']]
    i = m['id']
    if i not in used: continue
    size = layouts[(st['name'],'__SIZE__')]['size']
    rows += [f"## {i} {st['message']}（{size} 字节）", '', notes[i], '', '| 字段 | 类型 | 偏移/字节 | 官方默认 | 本地处理 |','|---|---|---|---|---|']
    for f in st['fields']:
        name = f['name']; a=layouts[(st['name'],name)]
        if name in ['Size','Type']: status='帧头读取/生成'
        elif name in used[i].split(): status='读取（入站）' if i in incoming else '生成（正常/空/拒绝路径的差异见主报告）'
        else: status='未读取/未使用' if i in incoming else '未赋业务值：零/空串'
        if i==104 and name not in ['Size','Type','SymbolID']: status='未知值哨兵' if name in used[i].split() else '零：时间/未知状态'
        if i==803 and name in ['OpenInterest','NumTrades']: status='共用偏移 56，固定写 NumTrades；未提供 OI'
        if i==507 and name=='PriceDisplayFormat': status='正常响应来自 instrument；空结果误为 0，应为 -1（A10）'
        if i==6 and name in ['ProtocolVersion','Encoding']: status='不读取；回复服务器 v8/Binary，协议允许编码不同'
        rows.append(f"| `{name}` | `{f['type']}{f['array']}` | {a['offset']}/{a['size']} | `{f['default']}` | {status} |")
    rows.append('')
(root/'field-checklist.md').write_text('\n'.join(rows),encoding='utf-8')
print('message-checklist.md and field-checklist.md generated')
