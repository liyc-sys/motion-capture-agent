#!/usr/bin/env python3
"""Annotator capture browser (v1 core).

A headed Chromium the annotator browses in. Every media asset the page actually
loads is captured from the browser's own network layer (real bytes, no cert / no
proxy / no re-fetch), content-addressed and deduped into a local store, with a
manifest attributing each asset to the site it came from. A floating panel acts
as the work queue (site N/total, Next button, live captured count). A background
thread mirrors new assets to a central target (local copy is always kept).

Test (no window, drives itself through the queue):
    CAP_HEADLESS=1 CAP_AUTOTEST=1 python3 capture_browser.py --queue q.txt --store ~/asset_capture
Real use (annotator window):
    python3 capture_browser.py --queue q.txt --store ~/asset_capture
"""
import argparse
import asyncio
import hashlib
import json
import os
import re
import sys
import time
from urllib.parse import urlparse

MEDIA_EXT = {
    "image": {"jpg", "jpeg", "png", "webp", "avif", "gif", "svg", "bmp", "ico"},
    "video": {"mp4", "webm", "mov", "m4v", "ogv"},
    "audio": {"mp3", "wav", "ogg", "m4a", "aac", "flac"},
    "model": {"glb", "gltf", "drc", "ktx2", "usdz", "fbx", "obj", "ply"},
    "lottie": {"lottie"},
}
MAX_BYTES = 60 * 1024 * 1024  # human-paced, so allow bigger than the batch's 30MB

# never store media requested from these hosts (privacy: analytics/social/auth/pay)
BLOCK_HOSTS = ("google", "gstatic", "googleapis", "doubleclick", "recaptcha",
               "facebook", "instagram", "twitter", "x.com", "tiktok", "linkedin",
               "paypal", "alipay", "mail.", "accounts.", "login.", "analytics",
               "sentry", "hotjar", "segment")


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


PANEL_JS = r"""
() => {
  if (window.__capPanel) return;
  const d = document.createElement('div');
  d.id = '__capPanel'; window.__capPanel = d;
  d.style.cssText = 'position:fixed;z-index:2147483647;right:14px;bottom:14px;'
    + 'background:#111;color:#eee;font:12px/1.4 -apple-system,sans-serif;'
    + 'padding:10px 12px;border-radius:10px;box-shadow:0 4px 16px rgba(0,0,0,.4);'
    + 'min-width:180px;opacity:.92';
  d.innerHTML = '<div id="__capInfo">采集中…</div>'
    + '<div style="margin-top:6px;display:flex;gap:6px">'
    + '<button id="__capNext" style="flex:1;padding:5px;border:0;border-radius:6px;'
    + 'background:#3b82f6;color:#fff;cursor:pointer">下一个 ▶</button></div>';
  document.body.appendChild(d);
  d.querySelector('#__capNext').onclick = () => window.__capNext && window.__capNext();
}
"""


class Capture:
    def __init__(self, store, upload_target):
        self.assets = os.path.join(store, "assets")
        self.manifest = os.path.join(store, "manifest.jsonl")
        os.makedirs(self.assets, exist_ok=True)
        self.seen_url = set()
        self.seen_sha = set()
        self.count = 0
        self.bytes = 0
        self.current_site = None
        self._mf = open(self.manifest, "a", encoding="utf-8")

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
            cl = int(resp.headers.get("content-length") or 0)
            if cl and cl > MAX_BYTES:
                return
            body = await resp.body()
            if not body or len(body) > MAX_BYTES:
                return
            sha = hashlib.sha256(body).hexdigest()
            e = ext_of(url) or (ct.split("/")[-1].split(";")[0] or "bin")
            if sha not in self.seen_sha:
                self.seen_sha.add(sha)
                fp = os.path.join(self.assets, "%s.%s" % (sha, e[:5]))
                if not os.path.exists(fp):
                    with open(fp, "wb") as f:
                        f.write(body)
            self.count += 1
            self.bytes += len(body)
            self._mf.write(json.dumps({
                "site": self.current_site, "url": url, "host": host,
                "type": kind, "sha": sha, "bytes": len(body),
            }, ensure_ascii=False) + "\n")
            self._mf.flush()
        except Exception:
            pass


async def run(queue, store, headless, autotest, upload_target):
    from playwright.async_api import async_playwright
    urls = [l.strip() for l in open(queue, encoding="utf-8") if l.strip()]
    progress_path = os.path.join(store, "progress.json")
    done = set()
    if os.path.exists(progress_path):
        try:
            done = set(json.load(open(progress_path)).get("done", []))
        except Exception:
            pass
    todo = [u for u in urls if u not in done]
    print("[cap] queue %d | remaining %d" % (len(urls), len(todo)), file=sys.stderr)

    cap = Capture(store, upload_target)
    idx = {"i": 0}

    async def launch_ctx(p):
        profile = os.path.join(store, "_profile")
        last = None
        for ch in ("chrome", "msedge", None):
            try:
                kw = dict(headless=headless, viewport={"width": 1440, "height": 900},
                          accept_downloads=True, args=["--disable-dev-shm-usage"])
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
        page.on("response", lambda r: asyncio.create_task(cap.on_response(r)))

        async def goto_idx():
            if idx["i"] >= len(todo):
                print("[cap] queue done", file=sys.stderr)
                return False
            cap.current_site = todo[idx["i"]]
            try:
                await page.goto(cap.current_site, wait_until="domcontentloaded", timeout=45000)
            except Exception as e:
                print("[cap] goto %s: %s" % (cap.current_site, str(e)[:70]), file=sys.stderr)
            return True

        async def next_site():
            done.add(todo[idx["i"]])
            json.dump({"done": sorted(done)}, open(progress_path, "w"))
            idx["i"] += 1
            await goto_idx()
            await refresh_panel()

        async def refresh_panel():
            try:
                await page.evaluate(PANEL_JS)
                info = "站 %d/%d · 已采 %d 个 · %.1fMB" % (
                    idx["i"] + 1, len(todo), cap.count, cap.bytes / 1e6)
                await page.evaluate("(t)=>{const e=document.getElementById('__capInfo');if(e)e.textContent=t;}", info)
            except Exception:
                pass

        await ctx.expose_binding("__capNext", lambda source: asyncio.create_task(next_site()))
        await goto_idx()
        await refresh_panel()

        if autotest:
            # self-drive: scroll each site, advance, prove capture works
            for _ in range(min(2, len(todo))):
                for _s in range(6):
                    try:
                        await page.mouse.wheel(0, 4000)
                    except Exception:
                        break
                    await asyncio.sleep(0.6)
                await asyncio.sleep(2)
                await next_site()
            print("[cap] AUTOTEST captured %d assets, %.1fMB" % (cap.count, cap.bytes / 1e6), file=sys.stderr)
            await ctx.close()
            return

        # real use: keep the window open; refresh panel periodically
        while True:
            await asyncio.sleep(3)
            await refresh_panel()


def default_store():
    import socket
    if getattr(sys, "frozen", False):
        host = socket.gethostname().split(".")[0]
        return os.path.join(os.path.expanduser("~/Desktop"), "motion_capture_%s" % host)
    return os.path.expanduser("~/asset_capture")


def resolve_queue(arg):
    if arg and os.path.exists(arg):
        return arg
    exedir = os.path.dirname(sys.executable if getattr(sys, "frozen", False)
                             else os.path.abspath(__file__))
    cands = [os.path.join(exedir, "queue.txt")]
    if hasattr(sys, "_MEIPASS"):
        cands.append(os.path.join(sys._MEIPASS, "queue.txt"))
    for c in cands:
        if os.path.exists(c):
            return c
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queue", default=None)
    ap.add_argument("--store", default=None)
    ap.add_argument("--upload-target", default=os.environ.get("CAP_UPLOAD", ""))
    args = ap.parse_args()
    headless = os.environ.get("CAP_HEADLESS") == "1"
    autotest = os.environ.get("CAP_AUTOTEST") == "1"

    store = args.store or default_store()
    os.makedirs(store, exist_ok=True)
    queue = resolve_queue(args.queue)
    if not queue:
        print("找不到 queue.txt(要看的网站清单),请把 queue.txt 放到程序同一个文件夹。", file=sys.stderr)
        sys.exit(2)

    print("=" * 56, file=sys.stderr)
    print(" 素材采集器已启动", file=sys.stderr)
    print(" 采集文件存放在: %s" % store, file=sys.stderr)
    print(" 完成后把这个文件夹整个压缩发回即可。", file=sys.stderr)
    print("=" * 56, file=sys.stderr)
    asyncio.run(run(queue, store, headless, autotest, args.upload_target))


if __name__ == "__main__":
    main()



if __name__ == "__main__":
    main()
