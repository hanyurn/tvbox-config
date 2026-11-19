# tvbox-config

每天自动抓取、测活、合并 TVBox / 影视仓的配置，供电视直接引用。

## 它能做什么

- 每天定时跑一次（UTC 21:00 = 北京时间 05:00），也可手动触发
- 拉取配置里的各路单仓，按 TVBox 的 jar 规则合并站点：JS 站源只保留主 spider 那一组，采集站源全并
- 从单仓/多仓配置里收集直播条目，逐条测活（HTTP 可用 + 是 m3u/txt + 频道数 ≥ 3）
- **每三天从公网高清源池拉新一次**：拉下来先测活检测（是直播表 + 频道数够 + 记录延迟），通过的才入库；没到三天就沿用上次的清单，不会天天乱换
- 高清源单独用 `hd_state.json` 记台账，和单仓直播一起参与最终测活；排序时高清源优先
- 直播源用 `lives_state.json` 记台账：**连续两次测不通才淘汰**，避免上游一时抖动把好源删掉
- 单张表频道数超过 3000 的（几十万频道那种巨表）直接跳过，否则电视加载会卡死
- 结果原子写入 `agg_true.json`；如果所有源都拉不到，保留上一版，不写坏文件

### 关于“每三天拉新高清源”

- 定时任务仍然每天跑，但脚本用 `hd_state.json` 里的 `last_fetch` 判断间隔：**距上次拉新满 3 天才真正去公网拉新+检测**，其余天沿用现有清单。
- 这么做的 cron 好处：不受“每月 1 号重置”影响，且 GitHub 定时任务偶尔延迟/漏跑也不会错乱。
- 想立刻拉新一次：在 GitHub 仓库 Actions 里手动触发 `daily-aggregate`，勾选「强制立即拉新一次公网高清源」；或本地跑 `python merge_true.py --hd-refresh`。

## 电视上怎么填

配置地址填下面任意一个（把 `你的账号/仓库名` 换成实际值）：

```
https://cdn.jsdelivr.net/gh/你的账号/仓库名@main/agg_true.json
https://raw.githubusercontent.com/你的账号/仓库名/main/agg_true.json
```

jsDelivr 那个更适合国内，有 CDN 加速。第一次设置后，每天开电视/点「刷新」就会拿到当天那版。

> 电视端不会自己定时拉，只在你打开应用或点刷新时取一次；所以“每天更新”的效果 = 每天开机看到当天结果。

## 文件说明

| 文件 | 用途 |
| --- | --- |
| `merge_true.py` | 主脚本：抓取 + 合并 + 高清源拉新检测 + 测活 + 台账 |
| `.github/workflows/daily.yml` | 每日定时任务（含手动强制拉新高源开关） |
| `agg_true.json` | 生成结果，电视读这个（自动更新，别手改） |
| `lives_state.json` | 直播源台账（失败计数、频道数、延迟），自动更新 |
| `hd_state.json` | 公网高清源台账：上次拉新日期、通过的源清单、最近检测结果，自动更新 |
| `last_run.txt` | 最近一次的运行摘要，方便看有没有出问题 |

## 本地手动跑

```bash
python merge_true.py                # 完整流程（高清源到期则拉新）
python merge_true.py --hd-refresh   # 强制立即拉新+检测一次公网高清源
python merge_true.py --no-hd        # 跳过高清源环节
python merge_true.py --no-live      # 只合站点，跳过直播（快）
python merge_true.py --max-live 40  # 直播最多保留 40 条
```

## 想加/换线路

改 `merge_true.py` 顶部的 `SOURCES`：

```python
SOURCES = [
    ("饭太硬", "https://fty.tvdog.top"),
    ...
]
```

单仓（含 `sites`）会贡献站点；多仓（含 `urls`）只用来收直播条目。加完之后不用改别的。

想加公网高清源，改 `HD_SOURCES`（每三天拉新时会自动检测，不通过的自动丢弃）：

```python
HD_SOURCES = [
    ("范明明IPv4", "https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/ipv4.m3u"),
    ...
]
```

## 想调阈值

- `HD_INTERVAL_DAYS = 3`：高清源拉新间隔，默认每三天
- `HD_MIN_CHANNELS = 3`：高清源最少频道数，低于此视为无效
- `MAX_CHANNELS = 3000`：巨表上限，调小更省电视性能
- `MIN_CHANNELS = 3`：少于几个频道算无效表
- `FAIL_LIMIT = 2`：连续失败几次才淘汰
- `--max-live`：最终保留条数

## 说明

仓库是公开的，配置内容（含第三方线路地址）会公网可见。本仓库不含任何影视内容，只做配置的抓取、去重、测活与合并。
