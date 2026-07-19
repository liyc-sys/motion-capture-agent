#!/usr/bin/env python3
"""Annotator capture browser — annotator types the URL each launch.

Browse-time is ZERO-download: we only record asset URLs as the page loads, so
the annotator's bandwidth is untouched (no lag on click / no frozen video
backgrounds). After the browser window is closed, all recorded assets are
downloaded with a visible progress counter, and a DONE.txt flag is written into
the session folder — a session without DONE.txt means the annotator closed the
terminal early and the capture is incomplete.

Layout per session:  sessions/<seq>_<host>/assets/*  manifest.json  DONE.txt

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
import re
import socket
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
DL_DEADLINE = 60          # per-asset wall clock cap in the post-close download
DL_WORKERS = 6

BLOCK_HOSTS = ("google", "gstatic", "googleapis", "doubleclick", "recaptcha",
               "facebook", "instagram", "twitter", "x.com", "tiktok", "linkedin",
               "paypal", "alipay", "mail.", "accounts.", "login.", "analytics",
               "sentry", "hotjar", "segment")

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

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
    """Browse-time: record URLs only (zero bandwidth). After close: download."""

    def __init__(self, sess_dir, site):
        self.site = site
        self.sess_dir = sess_dir
        self.assets = os.path.join(sess_dir, "assets")
        self.manifest = os.path.join(sess_dir, "manifest.json")
        os.makedirs(self.assets, exist_ok=True)
        self.pending = []
        self.seen_url = set()
        self.seen_sha = set()
        self.count = 0
        self.bytes = 0

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
            self.pending.append((url, host, ct, kind))
        except Exception:
            pass

    def _fetch(self, sess, item):
        url = item[0]
        try:
            r = sess.get(url, timeout=(10, 25), stream=True,
                         headers={"Referer": self.site})
            if r.status_code != 200:
                return None
            cl = int(r.headers.get("content-length") or 0)
            if cl and cl > MAX_BYTES:
                return None
            h = hashlib.sha256()
            chunks = []
            total = 0
            t0 = time.time()
            for chunk in r.iter_content(65536):
                if time.time() - t0 > DL_DEADLINE:
                    r.close()
                    return None
                total += len(chunk)
                if total > MAX_BYTES:
                    r.close()
                    return None
                h.update(chunk)
                chunks.append(chunk)
            return (item, h.hexdigest(), b"".join(chunks))
        except Exception:
            return None

    def download_all(self):
        total = len(self.pending)
        if not total:
            self._write_done()
            return
        print("", file=sys.stderr)
        print("⚠️⚠️⚠️  正在下载素材(共 %d 个),请不要关闭这个窗口!  ⚠️⚠️⚠️" % total, file=sys.stderr)
        print("      下载完成后会明确提示,并自动写入完成标志。", file=sys.stderr)
        sess = requests.Session()
        sess.headers.update({"User-Agent": UA})
        done = 0
        with ThreadPoolExecutor(max_workers=DL_WORKERS) as ex:
            futs = [ex.submit(self._fetch, sess, it) for it in self.pending]
            with open(self.manifest, "a", encoding="utf-8") as mf:
                for fut in as_completed(futs):
                    done += 1
                    res = fut.result()
                    if res:
                        (url, host, ct, kind), sha, body = res
                        e = ext_of(url) or (ct.split("/")[-1].split(";")[0] or "bin")
                        if sha not in self.seen_sha:
                            self.seen_sha.add(sha)
                            fp = os.path.join(self.assets, "%s.%s" % (sha, e[:5]))
                            if not os.path.exists(fp):
                                with open(fp, "wb") as f:
                                    f.write(body)
                        self.count += 1
                        self.bytes += len(body)
                        mf.write(json.dumps({
                            "site": self.site, "url": url, "host": host,
                            "type": kind, "sha": sha, "bytes": len(body),
                        }, ensure_ascii=False) + "\n")
                        mf.flush()
                    if done % 10 == 0 or done == total:
                        print("[cap] 下载进度 %d/%d …(请勿关闭窗口)" % (done, total), file=sys.stderr)
        self._write_done()

    def _write_done(self):
        with open(os.path.join(self.sess_dir, "DONE.txt"), "w", encoding="utf-8") as f:
            f.write("下载完成\n站点: %s\n素材: %d 个, %.1f MB\n完成时间: %s\n"
                    % (self.site, self.count, self.bytes / 1e6,
                       time.strftime("%Y-%m-%d %H:%M:%S")))


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
    print(" 浏览时不占网速;关掉浏览器后才开始下载素材。", file=sys.stderr)
    print("=" * 56, file=sys.stderr)

    cap = Capture(sess, url)
    closed = asyncio.Event()

    async def launch_ctx(p):
        # profile lives in the system temp dir, NOT in the store — so annotators
        # never accidentally send back browser cache/cookies
        profile = os.path.join(tempfile.gettempdir(), "motion_capture_profile")
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

    cap.download_all()
    print("", file=sys.stderr)
    print("✅ 本条完成:采集 %d 个素材, %.1fMB" % (cap.count, cap.bytes / 1e6), file=sys.stderr)
    print("✅ 素材已保存到: %s" % sess, file=sys.stderr)
    print("✅ 已写入完成标志 DONE.txt,现在可以关闭窗口了。", file=sys.stderr)


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
    if sys.platform == "win32" and not autotest:
        try:
            input("\n[cap] 按回车键关闭窗口...")
        except EOFError:
            pass


if __name__ == "__main__":
    main()
