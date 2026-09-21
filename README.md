# 多标签页公平租约锁

这是一个零前端依赖的浏览器并发控制演示。多个标签页竞争同一个 IndexedDB 资源，使用显式租约、条件续约、单调本地时钟、保守跨页时钟同步和 FIFO 等待队列，避免死锁、饥饿与失效持有者继续写入。

## 运行

```bash
npm start
```

然后打开 `http://localhost:5173`，复制 URL 到至少 3 个浏览器标签页。Worker 使用 ES Module，请通过 HTTP 服务访问，不要直接双击 `index.html`。

运行纯状态机测试：

```bash
npm test
```

## 验收路径

1. 在标签页 A 点击“请求锁”，状态变为持有者，租约环形进度实时倒计时。
2. 在 B、C 中依次请求锁，界面显示同一 FIFO 队列，B 为 `NEXT`，C 为 `P2`。
3. A 点击“释放锁”，B 在下一个 200 ms tick 获得锁；C 不会插队，随后的新请求排在 C 后。
4. 持有者 A 点击“冻结本标签页”：Worker 不再心跳、续约或广播；租约到期后其他标签页的事务原子清除 owner 并提升队首。
5. 点击“模拟下一次续约失败”：一次条件续约被跳过，后续 tick 自动重试；若持续失败直到真实 TTL，锁自动释放。
6. 修改各标签页的模拟时钟偏移；标签页之间通过 BroadcastChannel ping/pong 估计偏移和 RTT，远端租约显示带误差的安全剩余时间。
7. 持有者点击“带令牌写入”，资源记录单调递增的 fencing token；失去锁后的条件写入会被拒绝。

## 核心机制

- **原子提交**：每个标签页的 Worker 将 IndexedDB 操作串行化；获取、过期清理、队首提升在同一个 readwrite 事务中完成。
- **租约 TTL**：持有者获得逻辑过期时间，同时 Worker 用 `performance.now()` 保存不可被模拟偏移影响的本地物理 deadline。
- **自动续约**：默认在剩余时间小于 2/3 TTL 时条件续约；续约必须同时匹配 tab、request 和 fencing token。
- **自动释放**：冻结、崩溃或持续续约失败时，其他存活标签页依据保守估计的远端时钟清除 owner；本地持有者也由单调时钟兜底。
- **时钟漂移**：BroadcastChannel 交换双方逻辑时间，按 Cristian 风格计算偏移和 `RTT / 2` 不确定度；没有新鲜样本时使用 1500 ms 最大保护边界。
- **公平队列**：IndexedDB 保存单调 ticket。锁空闲时只允许当前队首提升；新请求不能绕过已有等待者。
- **死锁避免**：本模型只有一个锁资源，因此没有循环等待；owner 和 waiter 都是带 TTL 的租约，任何标签页死亡后队列都会被回收。
- **实时可视化**：Worker 每 200 ms 执行协调，每 500 ms 推送快照；提交后立即广播 `state-changed`。

## 文件结构

- `src/lock-engine.js`：纯 JS 锁状态机和公平性规则，可用 Node 直接测试。
- `src/clock-sync.js`：跨标签页时钟偏移、RTT 和保守误差估计。
- `src/lease-worker.js`：IndexedDB、BroadcastChannel、定时续约、队列推进和故障注入。
- `src/app.js` / `src/styles.css`：实时控制台和可视化。
- `test/lock-engine.test.js`：租约、条件释放、FIFO 与死亡队首回收测试。

## 边界说明

浏览器后台标签页可能节流定时器。租约本身不依赖可见标签页的前台刷新，但极低电耗模式下故障转移会变慢；页面恢复后会立即重新读取 IndexedDB 并校正角色。
