#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日合并 + 直播源维护（含公网高清源每三天拉新 + 检测）

做四件事：
  1. 拉取各路单仓配置，按 TVBox 的 jar 依赖规则合并（站点/解析/flags）
  2. 每三天从公网高清源池拉新一次：拉下来后逐条测活检测，通过才写入 hd_state.json；
     不到三天则沿用上次清单（用状态文件跨天持久，不依赖 cron 的日期缺陷）
  3. 把高清源并入直播集合，和单仓收集来的直播一起逐条测活
  4. 用 lives_state.json 记账，做“连败两次才淘汰”的缓慢清理，避免上游抖动误删

用法：
  python merge_true.py                 # 完整跑：合并 + 高清拉新(如到期) + 测活
  python merge_true.py --hd-refresh    # 强制立即拉新一次高清源（无视三天间隔）
  python merge_true.py --no-hd         # 跳过高清源环节（本地快速验证）
  python merge_true.py --no-live       # 只合并站点，不动直播（快速验证）
  python merge_true.py --max-live 40   # 限制直播条目数量上限

输出：agg_true.json（给电视用）、lives_state.json（直播台账）、
      hd_state.json（高清源台账）、last_run.txt（摘要）
"""
import argparse
import concurrent.futures as cf
import datetime
import json
import os
import re
import ssl
import sys
import time
import urllib.parse
import urllib.request

UA = "okhttp/3.12.0"
OUT = "agg_true.json"
STATE = "lives_state.json"
HD_STATE = "hd_state.json"
LOG = "last_run.txt"

# 源清单：单仓优先（能贡献站点），多仓只用来收直播条目
SOURCES = [
    ("饭太硬", "https://fty.tvdog.top"),
    ("高速APP", "https://dash.tvdog.top"),
    ("科技长青", "https://13413.kstore.vip/tv/changqing.json"),
    ("资源线路", "https://tv.seekvpn.online"),
    ("挺好分享", "http://ztha.top/TVBox/GYCK.json"),
    ("小盒子", "http://xhztv.top/dc"),
]

# 公网高清直播源候选池（只收公网可直连的，排除移动/电信/联通内网组播源）
# 想加新源直接往这里加 (名称, m3u/txt 地址) 即可，拉新时会自动检测、通过才入库
HD_SOURCES = [
    ("范明明IPv4", "https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/ipv4.m3u"),
    ("范明明国内镜像", "https://live.fanmingming.cn/tv/m3u/ipv4.m3u"),
    ("apTV官方", "https://gh.aptv.app/https://raw.githubusercontent.com/Kimentanm/aptv/master/m3u/iptv.m3u"),
    ("YueChan高清", "https://testingcf.jsdelivr.net/gh/YueChan/Live@main/IPTV.m3u"),
    ("suxuang IPv4", "https://gh.aptv.app/https://raw.githubusercontent.com/suxuang/myIPTV/main/ipv4.m3u"),
    ("IPTV4聚合", "https://live.zbds.top/tv/iptv4.m3u"),
    ("Ftindy 4K8K", "https://raw.githubusercontent.com/Ftindy/IPTV-URL/main/IPTV.m3u"),
    ("iptv-org CN", "https://iptv-org.github.io/iptv/countries/cn.m3u"),
    ("YanG集合", "https://iptv.yang-1989.xyz/playlist.m3u"),
    ("GNTV", "https://raw.githubusercontent.com/YueChan/Live/main/GNTV.m3u"),
]

HD_INTERVAL_DAYS = 3        # 高清源拉新间隔（天）
HD_MIN_CHANNELS = 3         # 高清源最少频道数，低于此视为无效

MAX_CHANNELS = 3000      # 单张表超过这个频道数就不要（电视加载会卡死）
MIN_CHANNELS = 3         # 少于这个算无效表
FAIL_LIMIT = 2           # 连续失败达到几次才淘汰
TIMEOUT = 12

CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE


# ---------- 网络 ----------

def fix_url(url):
    """中文域名转 punycode，urllib 才认"""
    try:
        p = urllib.parse.urlsplit(url)
        host = p.hostname or ""
        if any(ord(c) > 127 for c in host):
            newhost = host.encode("idna").decode("ascii")
            netloc = newhost + (":%d" % p.port if p.port else "")
            return urllib.parse.urlunsplit((p.scheme, netloc, p.path, p.query, p.fragment))
    except Exception:
        pass
    return url


def fetch(url, timeout=TIMEOUT):
    url = fix_url(url)
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "*/*"})
    with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
        raw = r.read()
    for enc in ("utf-8", "gbk"):
        try:
            return raw.decode(enc)
        except Exception:
            continue
    return raw.decode("utf-8", "ignore")


def fetch_json(url, timeout=TIMEOUT):
    return json.loads(fetch(url, timeout).lstrip("\ufeff"))


def abs_url(base, u):
    if not isinstance(u, str) or not u:
        return u
    if u.startswith(("http://", "https://", "file://")):
        return u
    return urllib.parse.urljoin(fix_url(base) + "/", u)


def looks_live(text):
    t = text.lstrip("\ufeff \r\n\t")
    return t.startswith("#EXTM3U") or t.startswith("#EXTINF") or "#genre#" in t[:20000]


# ---------- 站点合并 ----------

def is_js(site):
    return str(site.get("type", "")) == "3" or str(site.get("api", "")).startswith("csp_")


def merge_sites(cfgs):
    primary = max(cfgs, key=lambda n: sum(1 for s in (cfgs[n][1].get("sites") or []) if is_js(s)))
    p_url, p_cfg = cfgs[primary]

    merged = {"spider": abs_url(p_url, p_cfg.get("spider") or ""),
              "sites": [], "parses": [], "flags": []}
    seen_site, seen_parse, seen_flag = set(), set(), set()
    dropped = {}

    for name, (url, cfg) in cfgs.items():
        for s in cfg.get("sites") or []:
            key = s.get("key") or s.get("name")
            if not key or key in seen_site:
                continue
            if is_js(s) and name != primary:
                dropped[name] = dropped.get(name, 0) + 1
                continue
            seen_site.add(key)
            site = dict(s)
            if site.get("ext"):
                site["ext"] = abs_url(url, site["ext"])
            merged["sites"].append(site)

        for p in cfg.get("parses") or []:
            key = p.get("name")
            if not key or key in seen_parse:
                continue
            seen_parse.add(key)
            parse = dict(p)
            if parse.get("url"):
                parse["url"] = abs_url(url, parse["url"])
            merged["parses"].append(parse)

        for f in cfg.get("flags") or []:
            if f and f not in seen_flag:
                seen_flag.add(f)
                merged["flags"].append(f)

    return merged, primary, dropped


# ---------- 直播收集 ----------

def collect_lives(url, name, depth=1, budget=None):
    """从单仓/多仓配置里挖出所有直播条目，返回 [(label, entry_dict, abs_url)]"""
    found = []

    try:
        txt = fetch(url)
    except Exception as e:
        return found, "拉取失败：%s" % str(e)[:40]

    if looks_live(txt):
        return [(name, {"name": name}, url)], "纯直播表"

    try:
        cfg = json.loads(txt.lstrip("\ufeff"))
    except Exception:
        return found, "非 JSON"

    inner = cfg.get("urls")
    if isinstance(inner, list) and inner and not cfg.get("sites"):
        if budget is not None:
            budget[0] -= 1
            if budget[0] <= 0:
                return found, "多仓展开预算耗尽"
        urls = [u if isinstance(u, str) else (u.get("url") if isinstance(u, dict) else None)
                for u in inner][:80]
        subs = []
        for u in [x for x in urls if x]:
            sub, _ = collect_lives(abs_url(url, u), name, depth + 1, budget)
            subs.extend(sub)
        return subs, "多仓 %d 条" % len(subs)

    for l in cfg.get("lives") or []:
        lu = l.get("url") or l.get("api")
        if not lu:
            continue
        entry = dict(l)
        entry["url"] = abs_url(url, lu)
        found.append((entry.get("name") or name, entry, entry["url"]))
    return found, "单仓 %d 条" % len(found)


def probe(url):
    """测活：返回 (是否可用, 频道数, 延迟ms)"""
    t0 = time.time()
    try:
        text = fetch(url, timeout=10)
        ms = int((time.time() - t0) * 1000)
        ch = len(re.findall(r"#EXTINF", text)) or len(re.findall(r"^[^#\s].*,http", text, re.M))
        ok = ("#EXTM3U" in text or "#EXTINF" in text or "genre" in text.lower()) and ch >= MIN_CHANNELS
        return ok, ch, ms
    except Exception:
        pass
    return False, 0, int((time.time() - t0) * 1000)


# ---------- 公网高清源：每三天拉新 + 检测 ----------

def load_hd_state():
    st = {"last_fetch": "", "sources": [], "last_check": ""}
    if os.path.exists(HD_STATE):
        try:
            data = json.load(open(HD_STATE, encoding="utf-8"))
            if isinstance(data, dict):
                st.update(data)
        except Exception:
            pass
    return st


def days_since(date_str):
    """距某个 YYYY-MM-DD 日期的天数；空值/无效值视为很久以前"""
    if not date_str:
        return 9999
    try:
        d = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
        return (datetime.date.today() - d).days
    except Exception:
        return 9999


def check_hd_source(name, url):
    """检测单个公网高清源：拉下来 -> 是不是直播表 -> 频道数够不够 -> 延迟。
    返回 (是否通过, 记录字典)。这一步就是“拉新后检测”，不通过的不入库。"""
    t0 = time.time()
    rec = {"name": name, "url": url, "channels": 0, "ms": 0, "last_check": ""}
    try:
        text = fetch(url, timeout=TIMEOUT)
        rec["ms"] = int((time.time() - t0) * 1000)
        ch = len(re.findall(r"#EXTINF", text)) or len(re.findall(r"^[^#\s].*,http", text, re.M))
        ok = ("#EXTM3U" in text or "#EXTINF" in text or "genre" in text.lower()) and ch >= HD_MIN_CHANNELS
        rec["channels"] = ch
        rec["last_check"] = time.strftime("%Y-%m-%d")
        return ok, rec
    except Exception as e:
        rec["error"] = str(e)[:80]
        return False, rec


def refresh_hd_sources(force=False):
    """如距上次拉新已满 HD_INTERVAL_DAYS 天（或 force），拉新公网高清源并检测。
    通过的写入 hd_state.json 并返回 (state, note)；不到三天则沿用旧清单。"""
    st = load_hd_state()
    elapsed = days_since(st.get("last_fetch"))
    if not force and elapsed < HD_INTERVAL_DAYS:
        return st, "高清源：距上次拉新 %d 天，未满 %d 天，沿用既有 %d 条" % (
            elapsed, HD_INTERVAL_DAYS, len(st.get("sources", [])))

    print("  公网高清源：到期，开始拉新并检测 %d 个候选..." % len(HD_SOURCES))
    results = []
    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        fut = {ex.submit(check_hd_source, name, url): (name, url) for name, url in HD_SOURCES}
        for f in cf.as_completed(fut):
            name, url = fut[f]
            try:
                results.append(f.result())
            except Exception as e:
                results.append((False, {"name": name, "url": url, "error": str(e)[:80]}))

    good = [rec for ok, rec in results if ok]
    # 频道多的排前面，同样多则按延迟
    good.sort(key=lambda r: (-r.get("channels", 0), r.get("ms", 0)))

    st["last_fetch"] = time.strftime("%Y-%m-%d")
    st["sources"] = good
    st["last_check"] = "测试 %d / 通过 %d" % (len(results), len(good))
    tmp = HD_STATE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2, sort_keys=True)
    os.replace(tmp, HD_STATE)

    passed = "、".join("%s(%d台)" % (r["name"], r.get("channels", 0)) for r in good) or "无"
    note = "高清源拉新：测试 %d / 通过 %d：%s" % (len(results), len(good), passed)
    return st, note


# ---------- 主流程 ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-live", action="store_true", help="跳过直播抓取与测活")
    ap.add_argument("--no-hd", action="store_true", help="跳过公网高清源的拉新与并入")
    ap.add_argument("--hd-refresh", action="store_true", help="强制立即拉新一次高清源（无视三天间隔）")
    ap.add_argument("--max-live", type=int, default=30, help="最终保留的直播条目上限")
    args = ap.parse_args()

    # 1) 拉单仓
    cfgs, failed = {}, []
    for name, url in SOURCES:
        try:
            cfg = fetch_json(url)
            if not isinstance(cfg, dict):
                failed.append("%s 非对象" % name)
                continue
            if "sites" in cfg:
                cfgs[name] = (url, cfg)
        except Exception as e:
            failed.append("%s %s" % (name, str(e)[:36]))

    if not cfgs:
        print("所有单仓都拉不到，保留上一版配置")
        return 1

    merged, primary, dropped = merge_sites(cfgs)

    # 2) 直播（单仓收集 + 公网高清源）
    live_entries = []
    live_note = "已跳过"
    hd_note = "已跳过"
    if not args.no_live:
        budget = [120]           # 多仓展开次数上限，防止炸开
        raw, notes = [], []
        for name, url in SOURCES:
            subs, note = collect_lives(url, name, budget=budget)
            raw.extend(subs)
            notes.append("%s %s" % (name, note))

        by_url = {}
        for label, entry, u in raw:
            if u not in by_url:
                by_url[u] = entry
        print("  直播收集：" + "；".join(notes))

        # 2a) 公网高清源（每三天拉新 + 检测），通过的并入待检测集合
        hd_state = {"sources": []}
        if not args.no_hd:
            hd_state, hd_note = refresh_hd_sources(force=args.hd_refresh)
            added = 0
            for rec in hd_state.get("sources", []):
                u = rec.get("url")
                if not u:
                    continue
                if u not in by_url:
                    by_url[u] = {"name": rec.get("name") or "高清直播", "url": u}
                    added += 1
            hd_note += "；并入 %d 条" % added
            print("  " + hd_note)

        print("  去重后 %d 条，开始测活..." % len(by_url))

        rows = []
        with cf.ThreadPoolExecutor(max_workers=16) as ex:
            fut = {ex.submit(probe, u): u for u in by_url}
            for f in cf.as_completed(fut):
                u = fut[f]
                ok, ch, ms = f.result()
                rows.append((u, ok, ch, ms))

        # 3) 台账
        state = {}
        if os.path.exists(STATE):
            try:
                state = json.load(open(STATE, encoding="utf-8"))
            except Exception:
                state = {}

        today = time.strftime("%Y-%m-%d")
        hd_urls = {r.get("url") for r in hd_state.get("sources", [])}
        usable = []
        for u, ok, ch, ms in rows:
            rec = state.get(u, {"fail": 0, "ok": 0})
            if ok and ch > MAX_CHANNELS:      # 巨表不进配置，也不记账
                state[u] = rec
                continue
            if ok:
                rec["fail"] = 0
                rec["ok"] = rec.get("ok", 0) + 1
                rec["last_ok"] = today
                rec["channels"] = ch
                rec["ms"] = ms
            else:
                rec["fail"] = rec.get("fail", 0) + 1
            state[u] = rec

            entry = dict(by_url[u])
            entry["_ch"] = ch if ok else rec.get("channels", 0)
            entry["_ms"] = ms
            entry["_good"] = ok
            entry["_fail"] = rec["fail"]
            entry["_ever"] = rec.get("ok", 0) > 0     # 历史上至少通过过一次
            entry["_hd"] = u in hd_urls               # 来自公网高清源池
            usable.append(entry)

        # 保留规则：这次通过的直接留；这次没过但以前通过过的，容许再观察一次
        #（从没通过过的条目不留，避免首跑把一堆空壳塞进电视）
        keep = [e for e in usable if e["_good"] or (e["_ever"] and e["_fail"] < FAIL_LIMIT)]
        keep.sort(key=lambda e: (not e["_good"], e["_fail"] >= 1,
                                 not e["_hd"],
                                 -(e["_ch"] if 0 < e["_ch"] <= 800 else 800), e["_ms"]))
        keep = keep[:args.max_live]

        names = {}
        hd_kept = 0
        for e in keep:
            is_hd = e.pop("_hd", False)
            for k in ("_ch", "_ms", "_good", "_fail", "_ever"):
                e.pop(k, None)
            if is_hd:
                hd_kept += 1
            n = (e.get("name") or "直播").strip()
            if n in names:
                names[n] += 1
                n = "%s(%d)" % (n, names[n])
            names[n] = names.get(n, 1)
            e["name"] = n
        live_entries = keep

        good_n = sum(1 for r in rows if r[1])
        live_note = "可用 %d / 收集 %d，保留 %d（其中高清源 %d）" % (
            good_n, len(by_url), len(live_entries), hd_kept)
        json.dump(state, open(STATE, "w", encoding="utf-8"),
                  ensure_ascii=False, indent=2, sort_keys=True)

    merged["lives"] = live_entries
    for k in list(merged):
        if not merged[k]:
            del merged[k]

    tmp = OUT + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    os.replace(tmp, OUT)

    summary = "[%s UTC] 站源 %d（JS %d）/ 直播 %d / 解析 %d / spider=%s\n直播：%s\n%s" % (
        time.strftime("%Y-%m-%d %H:%M"), len(merged["sites"]),
        sum(1 for s in merged["sites"] if is_js(s)),
        len(merged.get("lives", [])), len(merged.get("parses", [])), primary, live_note, hd_note)
    if failed:
        summary += "\n拉取失败：" + "、".join(failed)
    if dropped:
        summary += "\n因 jar 不同丢弃：" + "、".join("%s %d" % (k, v) for k, v in dropped.items())
    print(summary)
    with open(LOG, "w", encoding="utf-8") as f:
        f.write(summary + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
