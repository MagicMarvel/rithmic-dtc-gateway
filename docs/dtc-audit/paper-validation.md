# Paper 账户联调记录

2026-09-04，用户授权后使用工作区现有 Paper 配置，串行执行账户和行情只读测试。没有提交、修改或撤销订单，也没有清理既有仓位。使用 `target/dtc-audit` 的当前源码测试程序。

## 历史权限问题

首次直连历史测试能登录但没有取得记录。检查测试发现它原先忽略了响应中的 error，导致权限拒绝被报告为“近期没有成交”。已补充错误断言，并新增默认忽略的 `paper_history_permission_matrix` 诊断测试。

两个独立新登录会话使用完全相同的时间范围逐项查询：

| 请求 | 窗口 | 第一个会话 | 第二个会话 |
|---|---|---|---|
| ESU6.CME Tick | 最近 10 分钟，结束时间为测试开始前 60 秒 | `[13] permission denied` | 同样拒绝 |
| ESU6.CME 1 分钟线 | 同上 | `[13] permission denied` | 同样拒绝 |
| NQU6.CME 日线 | 最近 7 天 | `[13] permission denied` | 同样拒绝 |
| GCZ6.COMEX Tick | 最近 10 分钟 | `[13] permission denied` | 同样拒绝 |

此矩阵直接调用 Rithmic History Plant API，不经过 DTC 编解码；8 次请求全部失败、0 条数据。因此本次拒绝能在 DTC 之外复现，缩短窗口及重新登录均未恢复。不能据此确定 Rithmic 拒绝的内部原因，也不能证明账户永远没有历史权限。

## 其他只读验证

- `paper_trading_es_flows_through_dtc_wire`：通过，验证证券定义、初始快照、带方向分类的 Trade V2 和 BBO V2。
- `paper_trading_es_dbo_flows_as_aggregated_dtc_l2`：通过，验证真实 DBO 聚合为 DTC L2 快照和更新。
- `paper_trading_pnl_plant_accepts_account_snapshot`：通过，包含账户发现及 PnL 快照。
- `paper_trading_order_plant_discovers_account_and_cme_route`：首次在 Order Plant 登录阶段返回 `[13] permission denied`；紧接着 PnL 测试内的账户发现成功，说明该次登录拒绝没有持续阻断所有后续登录。
- 再次单独运行账户/路由测试：通过。最终 4 类只读验证通过；历史矩阵仍为 8 次拒绝。没有执行下单生命周期测试或完整动态目录测试，不能将本结果解释为全部账户集成测试通过。

建议向账户提供方核查历史回放权限、环境/服务端授权及会话限制，提供这 8 次短窗口拒绝和测试时间。当前证据不足以将权限拒绝统一当作网络故障并循环重连。
