#!/usr/bin/env python3
"""Annotator capture browser — annotator types the URL each launch.

Each launch: prompt for a URL, open it, capture every media asset the page
loads. To keep the browser snappy we do NOT pull bytes through Chrome's CDP
(await resp.body() slows the browser); instead we record asset URLs as the page
loads and a background thread re-fetches them with requests. Stored under
sessions/<seq>_<host>/. Closing the browser ends that site. No UI injected.

Test (self-drive, headless):
    CAP_HEADLESS=1 CAP_AUTOTEST=1 CAP_URL=https://midu.design/ \
        python3 capture_browser.py --store /tmp/cap
Real use:
    python3 capture_browser.py --store ~/asset_capture
"""
import argparse
import asyncio
import hashlib
import json
import os
import queue
import re
import socket
import sys
import threading
from urllib.parse import urlparse

import requests

MEDIA_EXT = {
    "image": {"jpg", "jpeg", "png", "webp", "avif", "gif", "svg", "bmp", "ico"},
    "video": {"mp4", "webm", "mov", "m4v", "ogv"},
    "audio": {"mp3", "wav", "ogg", "m4a", "aac", "flac"},
    "model": {"glb", "gltf", "drc", "ktx2", "usdz", "fbx", "obj", "ply"},
    "lottie": {"lottie"},
}
MAX_BYTES = 60 * 1024 * 1024

BLOCK_HOSTS = ("google", "gstatic", "googleapis", "doubleclick", "recaptcha",
               "facebook", "instagram", "twitter", "x.com", "tiktok", "linkedin",
               "paypal", "alipay", "mail.", "accounts.", "login.", "analytics",
               "sentry", "hotjar", "segment")

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")


def ext_of(url):
    m = re.search(r"\.([a-z0-9]{2,5})(?:[?#]|$)", urlparse(url).path.lower())
    return m.group(1) if m else ""


def classify(url, ct):
    ct = (ct or "").lower()
    e = ext_of(url)
    if url.endswith(".m3u8") or "mpegurl" in ct:
        return "video_hls"
    if ct.startswith("image/") or e in MEDIA_EXT["image"]:
        return "image"
    if ct.startswith("video/") or e in MEDIA_EXT["video"]:
        return "video"
    if ct.startswith("audio/") or e in MEDIA_EXT["audio"]:
        return "audio"
    if ct.startswith("model/") or e in MEDIA_EXT["model"]:
        return "model"
    if e == "lottie" or ("lottie" in url.lower() and e == "json"):
        return "lottie"
    return None


class Capture:
    """Records asset URLs from the browser (cheap) and downloads them in a
    background thread (keeps the browser responsive)."""

    def __init__(self, sess_dir, site):
        self.site = site
        self.assets = os.path.join(sess_dir, "assets")
        self.manifest = os.path.join(sess_dir, "manifest.json")
        os.makedirs(self.assets, exist_ok=True)
        self.seen_url = set()
        self.seen_sha = set()
        self.count = 0
        self.bytes = 0
        self._q = queue.Queue()
        self._mf = open(self.manifest, "a", encoding="utf-8")
        self._sess = requests.Session()
        self._sess.headers.update({"User-Agent": UA})
        self._thread = threading.Thread(target=self._worker, daemon=True)
        self._thread.start()

    def _worker(self):
        while True:
            item = self._q.get()
            if item is None:
                break
            url, host, ct, kind = item
            try:
                r = self._sess.get(url, timeout=(10, 20),
                                   headers={"Referer": self.site})
                if r.status_code != 200:
                    continue
                body = r.content
                if not body or len(body) > MAX_BYTES:
                    continue
                sha = hashlib.sha256(body).hexdigest()
                e = ext_of(url) or (ct.split("/")[-1].split(";")[0] or "bin")
                if sha not in self.seen_sha:
                    self.seen_sha.add(sha)
                    with open(os.path.join(self.assets, "%s.%s" % (sha, e[:5])), "wb") as f:
                        f.write(body)
                self.count += 1
                self.bytes += len(body)
                self._mf.write(json.dumps({
                    "site": self.site, "url": url, "host": host,
                    "type": kind, "sha": sha, "bytes": len(body),
                }, ensure_ascii=False) + "\n")
                self._mf.flush()
            except Exception:
                pass

    async def on_response(self, resp):
        try:
            url = resp.url
            if url.startswith("data:") or url.startswith("blob:"):
                return
            host = urlparse(url).netloc.lower()
            if any(b in host for b in BLOCK_HOSTS):
                return
            ct = resp.headers.get("content-type", "")
            kind = classify(url, ct)
            if not kind or kind == "video_hls":
                return
            if url in self.seen_url:
                return
            self.seen_url.add(url)
            self._q.put((url, host, ct, kind))   # background fetch, no CDP body
        except Exception:
            pass

    def stop(self, timeout=120):
        self._q.put(None)
        self._thread.join(timeout=timeout)


def default_store():
    if getattr(sys, "frozen", False):
        host = socket.gethostname().split(".")[0]
        return os.path.join(os.path.expanduser("~/Desktop"), "motion_capture_%s" % host)
    return os.path.expanduser("~/asset_capture")


def next_seq(store):
    d = os.path.join(store, "sessions")
    os.makedirs(d, exist_ok=True)
    nums = []
    for name in os.listdir(d):
        base = name.split("_", 1)[0]
        if base.isdigit():
            nums.append(int(base))
    return (max(nums) + 1) if nums else 1


async def run(url, store, headless, autotest):
    from playwright.async_api import async_playwright
    host = urlparse(url).netloc.replace(":", "_").replace("/", "_") or "site"
    seq = next_seq(store)
    sess = os.path.join(store, "sessions", "%05d_%s" % (seq, host))
    os.makedirs(os.path.join(sess, "assets"), exist_ok=True)

    print("=" * 56, file=sys.stderr)
    print(" 第 %d 条" % seq, file=sys.stderr)
    print(" 正在打开: %s" % url, file=sys.stderr)
    print(" 素材存到: %s" % sess, file=sys.stderr)
    print(" 看完直接关掉浏览器窗口即可。", file=sys.stderr)
    print("=" * 56, file=sys.stderr)

    cap = Capture(sess, url)
    closed = asyncio.Event()

    async def launch_ctx(p):
        profile = os.path.join(store, "_profile")
        last = None
        for ch in ("chrome", "msedge", None):
            try:
                kw = dict(
                    headless=headless, viewport={"width": 1440, "height": 900},
                    accept_downloads=True,
                    ignore_default_args=["--enable-automation"],
                    args=["--disable-blink-features=AutomationControlled",
                          "--no-sandbox", "--disable-dev-shm-usage"],
                )
                if ch:
                    kw["channel"] = ch
                ctx = await p.chromium.launch_persistent_context(profile, **kw)
                print("[cap] browser: %s" % (ch or "bundled chromium"), file=sys.stderr)
                return ctx
            except Exception as e:
                last = e
        raise RuntimeError("没有可用浏览器,请先装 Chrome 或 Edge (%s)" % str(last)[:80])

    async with async_playwright() as p:
        ctx = await launch_ctx(p)
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()

        def on_page_close():
            if closed.is_set():
                return
            loop = asyncio.get_event_loop()
            loop.call_later(0.4, lambda: closed.set() if not ctx.pages else None)

        def attach(p):
            p.on("response", lambda r: asyncio.create_task(cap.on_response(r)))
            p.on("close", on_page_close)

        attach(page)
        ctx.on("page", attach)
        ctx.on("close", lambda: closed.set())
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=45000)
        except Exception as e:
            print("[cap] 打开站点失败: %s" % str(e)[:90], file=sys.stderr)

        if autotest:
            await asyncio.sleep(4)
            await page.close()
        else:
            await closed.wait()
        try:
            await ctx.close()
        except Exception:
            pass

    print("[cap] 正在保存剩余素材…", file=sys.stderr)
    cap.stop()
    print("[cap] 本条完成:采集 %d 个素材, %.1fMB" % (cap.count, cap.bytes / 1e6), file=sys.stderr)
    print("[cap] 素材已保存到: %s" % sess, file=sys.stderr)
    print("[cap] 在桌面「motion_capture」文件夹里,每条一个子文件夹。", file=sys.stderr)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", default=None)
    ap.add_argument("--url", default=None, help="skip the prompt, use this URL (testing)")
    args = ap.parse_args()
    headless = os.environ.get("CAP_HEADLESS") == "1"
    autotest = os.environ.get("CAP_AUTOTEST") == "1"

    store = args.store or default_store()
    os.makedirs(store, exist_ok=True)

    if args.url:
        url = args.url.strip()
    elif autotest:
        url = os.environ.get("CAP_URL", "").strip()
    else:
        try:
            url = input("请输入要采集的网站地址(例如 www.example.com),然后回车: ").strip()
        except EOFError:
            url = ""
    if not url:
        print("未输入网址,退出。", file=sys.stderr)
        sys.exit(0)
    if not re.match(r"^https?://", url, re.I):
        url = "http://" + url
    asyncio.run(run(url, store, headless, autotest))


if __name__ == "__main__":
    main()
