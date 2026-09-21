# 多标签页公平租约锁

一个零构建浏览器示例，用 BroadcastChannel、IndexedDB 和 Web Worker 实现多标签页对同一资源的互斥访问。

## 运行

```bash
npm test
npm start
```

打开 `http://localhost:5173/index.html`，点击“打开竞争标签页”或直接访问 `http://localhost:5173/concurrency.html` 运行 4 路自动验收。

不要直接用 `file://` 打开；模块 Worker 需要 HTTP 源。

## 协议

- **互斥**：IndexedDB 的 `resources` 保存持有者、fencing token、租约到期时间和续约次数。
- **FIFO 公平性**：`requests` 保存等待请求，自动递增 `seq`；只有最小的存活队首可以获得锁。
- **自动释放**：持有者崩溃、Worker 被终止或页面冻结后，其租约到期，其他 Worker 在同一个 IndexedDB 事务内清除旧持有者并授予队首。
- **续约**：持有者在约 40% 租约时间自动续约；续约是条件写入，必须同时匹配持有者和请求序号，过期后拒绝续约。
- **死锁避免**：等待不占用资源；锁有 TTL；等待请求也有心跳 TTL 和可选等待超时，崩溃等待者会被清除。
- **时钟漂移**：BroadcastChannel 执行简化 NTP ping/pong，估计对端时钟 offset 和 RTT；判断对端租约时加入 `RTT/2 + 速率漂移裕量`，未知时钟使用保守 5 秒保护。
- **实时可视化**：每个 Worker 每 250ms 从 IndexedDB 生成快照，UI 显示锁状态、FIFO 队列、剩余租约、漂移、RTT、持有者和事件日志。

## 关键文件

- `src/engine.js`：平台无关的租约状态机和公平调度纯函数。
- `src/lock-worker.js`：BroadcastChannel 心跳、时钟同步、IndexedDB 条件事务、自动续约和超时维护。
- `src/app.js`：主仪表盘和 Worker 通信。
- `src/concurrency.js` 与 `concurrency.html`：4 个独立页面的自动并发验收台。
- `test/engine.test.js`：FIFO、TTL、漂移裕量、释放和续约失败的 Node 单测。

## 验收映射

- 并发请求下无死锁：非阻塞 FIFO + TTL + 自动清除崩溃等待者。
- 锁可自动释放：租约到期后其他 Worker 原子切换给队首。
- 等待队列公平：IndexedDB 自增序号决定队首，后来者不能抢占。
- 租约超时准确：持有者以自身时钟续约；观察者使用时钟偏移和不确定性保守判断。
- 可视化实时：250ms 快照 + 状态变化广播 + 100ms 倒计时平滑刷新。
