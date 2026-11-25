#!/usr/bin/env python3
"""
LAN Chat - group chat for everyone on the same Wi-Fi / network. No internet needed.

Python 3.7+, standard library only (nothing to pip install).

    python3 lan_chat.py              start on port 8000 (tries the next ports if busy)
    python3 lan_chat.py 5000         start on port 5000
    python3 lan_chat.py --save       also keep history in chat_history.json (text only)

On Windows use "python" or "py" instead of "python3".
Then open the "Everyone else" address it prints on any phone or computer on the network.
"""

import argparse
import json
import os
import queue
import re
import shutil
import socket
import ssl
import subprocess
import sys
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

parser = argparse.ArgumentParser(description="Offline LAN chat")
parser.add_argument("port", nargs="?", type=int, default=8000)
parser.add_argument("--save", action="store_true", help="keep text history in chat_history.json")
parser.add_argument("--http", action="store_true",
                    help="plain HTTP (text and images only; browsers block mic and camera on http)")
ARGS = parser.parse_args()

HERE = os.path.dirname(os.path.abspath(__file__))
HISTORY_FILE = os.path.join(HERE, "chat_history.json")
MAX_HISTORY = 200
MAX_MEDIA_KEPT = 20           # older images / voice notes are dropped from memory to keep RAM small
MAX_NAME = 24
MAX_TEXT = 2000
MAX_MEDIA_CHARS = 1_500_000
MAX_BODY = 1_700_000
MAX_SIGNAL_CHARS = 100_000
LEAVE_GRACE = 6               # seconds before "X left" (hides brief Wi-Fi blips)
IMAGE_RE = re.compile(r"^data:image/(png|jpeg|webp|gif);base64,[A-Za-z0-9+/=]+$")
AUDIO_RE = re.compile(r"^data:audio/(webm|ogg|mp4|mpeg|wav|aac|x-m4a)(;codecs=[A-Za-z0-9.,\-]+)?;base64,[A-Za-z0-9+/=]+$")
CERT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lanchat-cert.pem")
KEY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lanchat-key.pem")

lock = threading.RLock()
history = deque(maxlen=MAX_HISTORY)
clients = []        # (queue, name) per open browser connection
presence = {}       # name -> {client_id: open_connection_count}
call_members = set()  # names currently in the video call
dms = {}              # (name_a, name_b) sorted -> deque of private messages (never saved to disk)
name_owner = {}       # name -> client id that last used it, so a new person can't read old private chats
next_id = 1
PORT = ARGS.port


# ------------------------------------------------------------------ helpers

def clean_name(raw):
    name = " ".join((raw or "").split())[:MAX_NAME]
    return name or "Anonymous"


def sse(event, data):
    return "event: %s\ndata: %s\n\n" % (event, json.dumps(data))


def push(payload):
    for q, _name in clients:  # call with lock held
        q.put(payload)


def push_to(name, payload):
    for q, nm in clients:  # call with lock held
        if nm == name:
            q.put(payload)


def push_call():
    with lock:
        push(sse("call", sorted(call_members, key=str.lower)))


def online_names():
    with lock:
        names = [n for n, users in presence.items() if sum(users.values()) > 0]
    return sorted(names, key=str.lower)


def push_presence():
    with lock:
        push(sse("presence", online_names()))


def save_history():
    if not ARGS.save:
        return
    rows = []
    for m in history:
        row = dict(m)
        if row.get("image"):
            row["image"] = None
            row["text"] = row.get("text") or "(image)"
        if row.get("audio"):
            row["audio"] = None
            row["text"] = row.get("text") or "(voice message)"
        rows.append(row)
    try:
        tmp = HISTORY_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(rows, f)
        os.replace(tmp, HISTORY_FILE)
    except OSError:
        pass


def load_history():
    global next_id
    if not ARGS.save or not os.path.exists(HISTORY_FILE):
        return
    try:
        with open(HISTORY_FILE, encoding="utf-8") as f:
            for m in json.load(f)[-MAX_HISTORY:]:
                history.append(m)
                next_id = max(next_id, int(m.get("id", 0)) + 1)
    except (OSError, ValueError, TypeError):
        pass


def dm_key(a, b):
    return tuple(sorted((a, b)))


def trim_media(dq):
    with_media = [m for m in dq if m.get("image") or m.get("audio")]
    for old in with_media[:-MAX_MEDIA_KEPT]:
        old["image"] = None
        old["audio"] = None
        old["text"] = old.get("text") or "(media removed to save memory)"


def dm_snapshot(name):
    """All private conversations this person is part of. Call with lock held."""
    out = {}
    for (a, b), dq in dms.items():
        if name == a:
            out[b] = list(dq)
        elif name == b:
            out[a] = list(dq)
    return out


def purge_dms(name):
    """Forget private chats involving a name. Call with lock held."""
    for key in [k for k in dms if name in k]:
        del dms[key]


def is_online(name):
    with lock:
        return sum(presence.get(name, {}).values()) > 0


def add_message(kind, name, text="", image=None, audio=None, to=None):
    global next_id
    with lock:
        msg = {"id": next_id, "type": kind, "name": name, "text": text,
               "image": image, "audio": audio, "to": to, "ts": int(time.time() * 1000)}
        next_id += 1
        if to:
            # private: only the two people involved ever receive this
            dq = dms.setdefault(dm_key(name, to), deque(maxlen=MAX_HISTORY))
            dq.append(msg)
            trim_media(dq)
            payload = sse("dm", msg)
            push_to(name, payload)
            push_to(to, payload)
            return
        history.append(msg)
        trim_media(history)
        push(sse("msg", msg))
        save_history()


def is_member(name, cid):
    with lock:
        return cid in presence.get(name, {})


def lan_ips():
    ips = []
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("10.255.255.255", 1))  # nothing is actually sent
        ips.append(s.getsockname()[0])
    except OSError:
        pass
    finally:
        s.close()
    try:
        for ip in socket.gethostbyname_ex(socket.gethostname())[2]:
            if ip not in ips:
                ips.append(ip)
    except OSError:
        pass
    ips = [ip for ip in ips if not ip.startswith("127.") and not ip.startswith("169.254.")]
    return ips or ["127.0.0.1"]


def leave_call(name):
    with lock:
        if name not in call_members:
            return
        call_members.discard(name)
        now_empty = not call_members
    push_call()
    if now_empty:
        add_message("sys", "", "The call ended")


def finalize_leave(name):
    with lock:
        users = presence.get(name)
        if users is None or sum(users.values()) > 0:
            return
        del presence[name]
    leave_call(name)
    add_message("sys", "", "%s left" % name)
    push_presence()


def find_openssl():
    """openssl on PATH, or the copy that ships inside Git for Windows."""
    found = shutil.which("openssl")
    if found:
        return found
    roots = [os.environ.get("ProgramFiles"), os.environ.get("ProgramFiles(x86)"),
             os.path.join(os.environ.get("LOCALAPPDATA", ""), "Programs")]
    for root in roots:
        if not root:
            continue
        for sub in (("Git", "usr", "bin", "openssl.exe"), ("Git", "mingw64", "bin", "openssl.exe")):
            path = os.path.join(root, *sub)
            if os.path.exists(path):
                return path
    return None


def _cert_with_openssl(ips):
    exe = find_openssl()
    if not exe:
        return False
    cnf = os.path.join(os.path.dirname(CERT_FILE), "lanchat-openssl.cnf")
    lines = ["[req]", "distinguished_name=dn", "x509_extensions=v3", "prompt=no",
             "[dn]", "CN=LAN Chat", "[v3]", "subjectAltName=@alt", "[alt]", "DNS.1=localhost", "IP.1=127.0.0.1"]
    for i, ip in enumerate(ips, start=2):
        lines.append("IP.%d=%s" % (i, ip))
    with open(cnf, "w") as f:
        f.write("\n".join(lines) + "\n")
    try:
        subprocess.run(
            [exe, "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "825",
             "-keyout", KEY_FILE, "-out", CERT_FILE, "-config", cnf],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60)
        return True
    except (OSError, subprocess.SubprocessError):
        return False
    finally:
        try:
            os.remove(cnf)
        except OSError:
            pass


def _cert_with_cryptography(ips):
    try:
        import datetime
        import ipaddress
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID
    except ImportError:
        return False
    try:
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "LAN Chat")])
        san = [x509.DNSName("localhost")] + [
            x509.IPAddress(ipaddress.ip_address(ip)) for ip in ["127.0.0.1"] + list(ips)]
        now = datetime.datetime.utcnow()
        cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
                .public_key(key.public_key()).serial_number(x509.random_serial_number())
                .not_valid_before(now - datetime.timedelta(days=1))
                .not_valid_after(now + datetime.timedelta(days=825))
                .add_extension(x509.SubjectAlternativeName(san), critical=False)
                .sign(key, hashes.SHA256()))
        with open(KEY_FILE, "wb") as f:
            f.write(key.private_bytes(serialization.Encoding.PEM,
                                      serialization.PrivateFormat.TraditionalOpenSSL,
                                      serialization.NoEncryption()))
        with open(CERT_FILE, "wb") as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))
        return True
    except (ValueError, OSError):
        return False


def make_ssl_context(ips):
    """Returns an SSL context using a self-signed certificate, or None if one can't be made.
    Browsers only allow the microphone and camera on https pages (or localhost)."""
    if not (os.path.exists(CERT_FILE) and os.path.exists(KEY_FILE)):
        if not (_cert_with_openssl(ips) or _cert_with_cryptography(ips)):
            return None
    try:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(CERT_FILE, KEY_FILE)
        return ctx
    except (ssl.SSLError, OSError):
        return None


# ------------------------------------------------------------------ web page

PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>LAN Chat</title>
<style>
  :root {
    --bg: #edf0f3; --panel: #ffffff; --ink: #17212c; --muted: #637082;
    --line: #d9dfe6; --mine: #2b4bff; --mine-ink: #ffffff; --theirs: #ffffff;
    --live: #12a150; --warn: #e0a100; --focus: #2b4bff; --shade: rgba(16,24,36,.55);
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #10151c; --panel: #18202a; --ink: #e7ecf2; --muted: #8d9bad;
      --line: #263140; --mine: #4c68ff; --theirs: #1f2935; --focus: #8aa0ff;
    }
  }
  [hidden] { display: none !important; }
  * { box-sizing: border-box; }
  html, body { height: 100%; margin: 0; }
  body {
    font-family: "Avenir Next", "Segoe UI Variable", "Segoe UI", system-ui, sans-serif;
    background: var(--bg); color: var(--ink); font-size: 15px;
  }
  :focus-visible { outline: 2px solid var(--focus); outline-offset: 2px; }
  button { font: inherit; cursor: pointer; }
  .app { height: 100%; display: flex; flex-direction: column; }

  header {
    background: var(--panel); border-bottom: 1px solid var(--line);
    padding: 10px 16px; padding-top: max(10px, env(safe-area-inset-top));
    display: flex; align-items: center; gap: 12px;
  }
  .title { flex: 1; min-width: 0; }
  .title h1 { margin: 0; font-size: 19px; font-weight: 700; letter-spacing: -0.01em; }
  .title p { margin: 1px 0 0; font-size: 12.5px; color: var(--muted); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .tools { display: flex; align-items: center; gap: 8px; }
  .ghost {
    background: transparent; color: var(--ink); border: 1px solid var(--line);
    border-radius: 999px; padding: 6px 12px; font-size: 13px;
  }
  .ghost:hover { background: var(--bg); }
  #status { font-size: 12.5px; color: var(--muted); display: flex; align-items: center; gap: 6px; white-space: nowrap; }
  #dot { width: 9px; height: 9px; border-radius: 50%; background: var(--warn); }
  #dot.on { background: var(--live); }

  .body { flex: 1; display: flex; min-height: 0; }
  aside {
    width: 210px; background: var(--panel); border-right: 1px solid var(--line);
    padding: 14px 12px; overflow-y: auto;
  }
  aside h2 { margin: 0 0 10px; font-size: 13px; font-weight: 600; color: var(--muted); }
  #userlist { list-style: none; margin: 0; padding: 0; }
  #userlist li { padding: 1px 0; }
  .av {
    flex: none; width: 32px; height: 32px; border-radius: 50%; color: #fff;
    display: flex; align-items: center; justify-content: center; font-weight: 700; font-size: 14px;
  }
  .av.sm { width: 26px; height: 26px; font-size: 12px; }

  main { flex: 1; display: flex; flex-direction: column; min-width: 0; position: relative; }
  #log { flex: 1; overflow-y: auto; padding: 16px 16px 8px; display: flex; flex-direction: column; gap: 10px; }
  #empty { margin: auto; text-align: center; color: var(--muted); max-width: 280px; line-height: 1.45; }

  .row { display: flex; align-items: flex-end; gap: 8px; max-width: 78%; }
  .row.mine { align-self: flex-end; flex-direction: row-reverse; }
  .row.cont { margin-top: -7px; }
  .spacer { flex: none; width: 32px; }
  .col { display: flex; flex-direction: column; min-width: 0; }
  .row.mine .col { align-items: flex-end; }
  .meta { font-size: 12px; color: var(--muted); margin: 0 4px 3px; }
  .meta b { font-weight: 600; }
  .bubble {
    background: var(--theirs); border: 1px solid var(--line);
    padding: 8px 13px; border-radius: 18px; border-bottom-left-radius: 5px;
    white-space: pre-wrap; overflow-wrap: anywhere; line-height: 1.38;
  }
  .row.mine .bubble {
    background: var(--mine); color: var(--mine-ink); border-color: transparent;
    border-bottom-left-radius: 18px; border-bottom-right-radius: 5px;
  }
  .bubble a { color: inherit; text-decoration: underline; }
  .bubble img { display: block; max-width: min(320px, 100%); max-height: 300px; border-radius: 10px; cursor: zoom-in; margin: 2px 0; }
  .bubble img + span { display: block; margin-top: 6px; }
  .sys { align-self: center; font-size: 12.5px; color: var(--muted); }

  #typing { min-height: 22px; padding: 0 18px; font-size: 12.5px; color: var(--muted); }
  #preview { padding: 6px 16px 0; display: flex; align-items: center; gap: 10px; }
  #preview img { height: 64px; border-radius: 8px; border: 1px solid var(--line); }

  form#composer {
    display: flex; align-items: flex-end; gap: 8px; padding: 10px 12px;
    padding-bottom: max(10px, env(safe-area-inset-bottom));
    background: var(--panel); border-top: 1px solid var(--line);
  }
  #text {
    flex: 1; resize: none; max-height: 120px; padding: 9px 14px; font: inherit; line-height: 1.35;
    border: 1px solid var(--line); border-radius: 20px; background: var(--bg); color: var(--ink);
  }
  #sendBtn, #recSend {
    border: 0; background: var(--mine); color: #fff; font-weight: 600;
    padding: 9px 20px; border-radius: 999px;
  }
  button:disabled, #text:disabled { opacity: .5; cursor: default; }

  #emoji {
    position: absolute; left: 12px; bottom: 66px; background: var(--panel);
    border: 1px solid var(--line); border-radius: 14px; padding: 8px;
    display: grid; grid-template-columns: repeat(8, 1fr); gap: 2px;
    box-shadow: 0 6px 24px rgba(0,0,0,.18);
  }
  #emoji button { background: none; border: 0; font-size: 22px; padding: 4px; border-radius: 8px; }
  #emoji button:hover { background: var(--bg); }

  .overlay {
    position: fixed; inset: 0; background: var(--shade); z-index: 10;
    display: flex; align-items: center; justify-content: center; padding: 20px;
  }
  .card {
    background: var(--panel); border-radius: 18px; padding: 24px; width: 100%; max-width: 380px;
    display: flex; flex-direction: column; gap: 12px;
  }
  .card h2 { margin: 0; font-size: 21px; }
  .card p { margin: 0; color: var(--muted); line-height: 1.4; }
  .card input {
    padding: 11px 14px; border-radius: 12px; border: 1px solid var(--line);
    background: var(--bg); color: var(--ink); font: inherit;
  }
  .card .go { border: 0; background: var(--mine); color: #fff; font-weight: 600; padding: 11px; border-radius: 12px; }
  .err { color: #d6336c; min-height: 1.2em; font-size: 13.5px; }
  .url { display: flex; align-items: center; gap: 8px; }
  .url code { flex: 1; padding: 8px 10px; background: var(--bg); border-radius: 10px; font-size: 14px; overflow-wrap: anywhere; }
  #lightbox { cursor: zoom-out; }
  #lightbox img { max-width: 100%; max-height: 100%; border-radius: 8px; }
  #toast {
    position: fixed; left: 50%; bottom: 84px; transform: translateX(-50%); z-index: 20;
    background: var(--ink); color: var(--bg); padding: 9px 16px; border-radius: 999px; font-size: 13.5px;
  }

  #callBanner {
    background: var(--mine); color: #fff; padding: 8px 16px;
    display: flex; align-items: center; gap: 12px; font-size: 14px;
  }
  #callBanner span { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  #callBanner button { background: #fff; color: #1a2bb3; border: 0; border-radius: 999px; padding: 5px 16px; font-weight: 600; }
  .ghost.live { border-color: var(--live); color: var(--live); font-weight: 600; }

  .bubble audio { display: block; width: 250px; max-width: 100%; height: 40px; }
  .bubble .vlabel { display: block; font-size: 12px; opacity: .75; margin-bottom: 4px; }
  #recBar { flex: 1; display: none; align-items: center; gap: 12px; }
  form.recording > :not(#recBar) { display: none !important; }
  form.recording #recBar { display: flex; }
  #recDot { width: 10px; height: 10px; border-radius: 50%; background: #e5383b; animation: pulse 1s infinite; }
  @keyframes pulse { 50% { opacity: .25; } }
  @media (prefers-reduced-motion: reduce) { #recDot { animation: none; } }
  #recTime { flex: 1; font-variant-numeric: tabular-nums; }

  #callView { position: fixed; inset: 0; z-index: 15; background: #0b0f14; color: #fff; display: flex; flex-direction: column; }
  #callGrid {
    flex: 1; min-height: 0; padding: 12px; display: grid; gap: 8px;
    grid-template-columns: repeat(auto-fit, minmax(min(100%, 280px), 1fr)); grid-auto-rows: minmax(0, 1fr);
  }
  .tile { position: relative; background: #18202a; border-radius: 14px; overflow: hidden; min-height: 0; display: flex; align-items: center; justify-content: center; }
  .tile .av { width: 84px; height: 84px; font-size: 36px; }
  .tile video { position: absolute; inset: 0; width: 100%; height: 100%; object-fit: cover; background: #18202a; }
  .tile video.mirror { transform: scaleX(-1); }
  .tile .tname { position: absolute; left: 10px; bottom: 8px; background: rgba(0,0,0,.55); padding: 3px 10px; border-radius: 999px; font-size: 13px; }
  #callBar { display: flex; flex-wrap: wrap; justify-content: center; gap: 10px; padding: 12px; padding-bottom: max(12px, env(safe-area-inset-bottom)); }
  #callBar button { border: 0; border-radius: 999px; padding: 11px 20px; font-weight: 600; background: #2a3442; color: #fff; }
  #callBar button.off { background: #fff; color: #17212c; }
  #callBar #hang { background: #e5383b; color: #fff; }

  .person {
    display: flex; align-items: center; gap: 10px; width: 100%; padding: 6px 8px; border: 0;
    border-radius: 10px; background: none; color: var(--ink); text-align: left;
  }
  .person:hover { background: var(--bg); }
  .person.active { background: var(--bg); font-weight: 600; }
  .person.off, .chip.off { opacity: .6; }
  .person .pn { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .badge { background: var(--mine); color: #fff; border-radius: 999px; font-size: 11px; font-weight: 700; padding: 1px 7px; }
  .me-note { margin-top: 12px; padding: 0 8px; font-size: 12.5px; color: var(--muted); }
  .link { cursor: pointer; }
  #chips { display: none; gap: 8px; padding: 8px 12px; overflow-x: auto; background: var(--panel); border-bottom: 1px solid var(--line); }
  .chip { flex: none; border: 1px solid var(--line); background: var(--panel); color: var(--ink); border-radius: 999px; padding: 6px 12px; font-size: 13.5px; }
  .chip.active { background: var(--mine); color: #fff; border-color: transparent; }
  .chip .badge { margin-left: 6px; background: #e5383b; }
  #convoHead { display: flex; align-items: center; gap: 12px; padding: 10px 16px; background: var(--panel); border-bottom: 1px solid var(--line); }
  #convoHead > div { flex: 1; min-width: 0; }
  #convoHead small { display: block; color: var(--muted); font-size: 12.5px; }

  @media (max-width: 700px) {
    aside { display: none; }
    #chips { display: flex; }
    .row { max-width: 92%; }
    .ghost.hide-sm { display: none; }
  }
</style>
</head>
<body>
<div class="app">
  <header>
    <div class="title">
      <h1>LAN Chat</h1>
      <p id="where"></p>
    </div>
    <div class="tools">
      <span id="status"><span id="dot"></span><span id="statusText">Connecting</span></span>
      <button class="ghost" id="callBtn" type="button">Call</button>
      <button class="ghost" id="invite" type="button">Invite</button>
      <button class="ghost hide-sm" id="mute" type="button">Sound on</button>
    </div>
  </header>
  <div id="callBanner" hidden><span id="callWho"></span><button id="callJoin" type="button">Join</button></div>
  <div class="body">
    <aside>
      <h2 id="count">Here now</h2>
      <ul id="userlist"></ul>
    </aside>
    <main id="main">
      <nav id="chips" aria-label="Conversations"></nav>
      <div id="convoHead" hidden>
        <div><b id="convoName"></b><small id="convoNote"></small></div>
        <button class="ghost" id="convoBack" type="button">Back to everyone</button>
      </div>
      <div id="log"><div id="empty">No messages yet. Say hello, or press Invite to bring others in.</div></div>
      <div id="typing"></div>
      <div id="preview" hidden><img id="previewImg" alt="Image to send"><button class="ghost" id="dropImg" type="button">Remove</button></div>
      <form id="composer" autocomplete="off">
        <div id="recBar">
          <span id="recDot"></span><span id="recTime">0:00</span>
          <button class="ghost" id="recCancel" type="button">Cancel</button>
          <button id="recSend" type="button">Send voice</button>
        </div>
        <button class="ghost" id="voiceBtn" type="button">Voice</button>
        <button class="ghost" id="photoBtn" type="button">Photo</button>
        <button class="ghost" id="emojiBtn" type="button">Emoji</button>
        <textarea id="text" rows="1" maxlength="2000" placeholder="Write a message" disabled></textarea>
        <button id="sendBtn" disabled>Send</button>
        <input type="file" id="file" accept="image/*" hidden>
      </form>
      <div id="emoji" hidden></div>
    </main>
  </div>
</div>

<div class="overlay" id="join">
  <form class="card" id="joinForm">
    <h2>Join the chat</h2>
    <p>Everyone on this network can see what you write. Pick a name so they know it's you.</p>
    <input id="nameInput" placeholder="Your name" maxlength="24" required autofocus>
    <div class="err" id="joinErr"></div>
    <button class="go">Join chat</button>
  </form>
</div>

<div class="overlay" id="inviteBox" hidden>
  <div class="card">
    <h2>Invite people</h2>
    <p>Anyone on the same Wi-Fi or network can open one of these addresses in a browser.</p>
    <div id="urls"></div>
    <p>If a device can't open it, allow Python through the firewall on the computer running the chat.</p>
    <button class="ghost" id="closeInvite" type="button">Close</button>
  </div>
</div>

<div class="overlay" id="lightbox" hidden><img id="lightImg" alt=""></div>

<div id="callView" hidden>
  <div id="callGrid"></div>
  <div id="callBar">
    <button id="micBtn" type="button">Mute</button>
    <button id="camBtn" type="button">Camera off</button>
    <button id="backBtn" type="button">Back to chat</button>
    <button id="hang" type="button">Leave call</button>
  </div>
</div>
<div id="toast" hidden></div>

<script>
(function () {
  "use strict";
  var $ = function (id) { return document.getElementById(id); };
  var enc = encodeURIComponent;
  var me = "", cid = "", es = null, muted = false, unread = 0;
  var lastTyping = 0, pendingImage = null, lastMsg = null, actx = null;
  var typers = {};
  var pub = [], convos = {}, current = "", unreadBy = {}, online = [], recTo = "";
  var baseTitle = document.title;
  var log = $("log"), text = $("text"), sendBtn = $("sendBtn");

  function store(k, v) {
    try {
      if (v === undefined) return localStorage.getItem(k);
      localStorage.setItem(k, v);
    } catch (e) { return null; }
  }
  cid = store("lanchat-cid") || (Math.random().toString(36).slice(2) + Date.now().toString(36));
  store("lanchat-cid", cid);
  muted = store("lanchat-muted") === "1";
  $("mute").textContent = muted ? "Sound off" : "Sound on";
  $("where").textContent = "On the network at " + location.host;
  $("nameInput").value = store("lanchat-name") || "";

  function el(tag, cls, txt) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (txt !== undefined) e.textContent = txt;
    return e;
  }
  function hue(n) { var h = 0; for (var i = 0; i < n.length; i++) h = (h * 31 + n.charCodeAt(i)) >>> 0; return h % 360; }
  function avatar(n, small) {
    var a = el("div", "av" + (small ? " sm" : ""), (Array.from(n)[0] || "?").toUpperCase());
    a.style.background = "hsl(" + hue(n) + " 52% 40%)";
    return a;
  }
  function fmt(ts) { return new Date(ts).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" }); }
  function toast(msg) {
    var t = $("toast"); t.textContent = msg; t.hidden = false;
    clearTimeout(toast.h); toast.h = setTimeout(function () { t.hidden = true; }, 3200);
  }

  // ---------- join
  $("joinForm").addEventListener("submit", function (e) {
    e.preventDefault();
    var n = $("nameInput").value.trim();
    if (!n) return;
    $("joinErr").textContent = "";
    try { actx = actx || new (window.AudioContext || window.webkitAudioContext)(); if (actx.resume) actx.resume(); } catch (x) {}
    fetch("/check?name=" + enc(n) + "&cid=" + enc(cid))
      .then(function (r) { return r.json(); })
      .then(function (res) {
        if (!res.ok) { $("joinErr").textContent = "Someone here already uses that name. Pick another."; return; }
        me = res.name; store("lanchat-name", me);
        $("join").hidden = true;
        connect();
        text.focus();
      })
      .catch(function () { $("joinErr").textContent = "Can't reach the chat server. Check that it is still running."; });
  });

  // ---------- connection
  function setOnline(on) {
    $("dot").className = on ? "on" : "";
    $("statusText").textContent = on ? "Connected" : "Reconnecting";
    text.disabled = !on; sendBtn.disabled = !on;
  }
  function connect() {
    if (es) es.close();
    es = new EventSource("/events?name=" + enc(me) + "&cid=" + enc(cid));
    es.onopen = function () { setOnline(true); };
    es.onerror = function () {
      setOnline(false);
      if (es.readyState === 2) setTimeout(connect, 2500);
    };
    es.addEventListener("history", function (e) {
      pub = JSON.parse(e.data);
      if (current === "") renderLog();
    });
    es.addEventListener("msg", function (e) {
      var m = JSON.parse(e.data);
      pub.push(m);
      if (current === "") {
        var stick = log.scrollHeight - log.scrollTop - log.clientHeight < 120 || m.name === me;
        var empty = $("empty"); if (empty) empty.remove();
        render(m);
        if (stick) log.scrollTop = log.scrollHeight;
      } else if (m.type === "msg" && m.name !== me) {
        unreadBy[""] = (unreadBy[""] || 0) + 1;
        renderPeople();
      }
      if (m.type === "msg" && m.name !== me) { clearTyper("pub:" + m.name); ping(false); }
    });
    es.addEventListener("dmhistory", function (e) {
      convos = JSON.parse(e.data);
      renderPeople();
      if (current !== "") renderLog();
    });
    es.addEventListener("dm", function (e) {
      var m = JSON.parse(e.data), key = m.name === me ? m.to : m.name;
      (convos[key] = convos[key] || []).push(m);
      if (m.name !== me) clearTyper("dm:" + key);
      if (current === key) {
        var stick = log.scrollHeight - log.scrollTop - log.clientHeight < 120 || m.name === me;
        var empty = $("empty"); if (empty) empty.remove();
        render(m);
        if (stick) log.scrollTop = log.scrollHeight;
        if (m.name !== me) ping(false);
      } else if (m.name !== me) {
        unreadBy[key] = (unreadBy[key] || 0) + 1;
        ping(true);
      }
      renderPeople();
    });
    es.addEventListener("presence", function (e) {
      online = JSON.parse(e.data);
      $("count").textContent = online.length + " here now";
      renderPeople();
      updateHead();
    });
    es.addEventListener("typing", function (e) {
      var t = JSON.parse(e.data);
      if (t.name === me) return;
      var key = (t.dm ? "dm:" : "pub:") + t.name;
      clearTimeout(typers[key]);
      typers[key] = setTimeout(function () { clearTyper(key); }, 3500);
      showTyping();
    });
    es.addEventListener("call", function (e) {
      callMembers = JSON.parse(e.data);
      if (inCall) {
        if (callMembers.indexOf(me) < 0) { leaveCall(); toast("You were disconnected from the call."); return; }
        Object.keys(pcs).forEach(function (n) { if (callMembers.indexOf(n) < 0) closePeer(n); });
      }
      updateCallUI();
    });
    es.addEventListener("signal", function (e) {
      var s = JSON.parse(e.data);
      onSignal(s.from, s.data);
    });
  }
  function clearTyper(key) { if (key in typers) { clearTimeout(typers[key]); delete typers[key]; showTyping(); } }
  function showTyping() {
    var names = [];
    Object.keys(typers).forEach(function (k) {
      if (current === "" && k.indexOf("pub:") === 0) names.push(k.slice(4));
      else if (current !== "" && k === "dm:" + current) names.push(current);
    });
    var t = "";
    if (names.length === 1) t = names[0] + " is typing...";
    else if (names.length === 2) t = names[0] + " and " + names[1] + " are typing...";
    else if (names.length > 2) t = "Several people are typing...";
    $("typing").textContent = t;
  }
  function ping(force) {   // force: play the sound even if this tab is in front (message in another chat)
    if (document.hidden) { unread++; document.title = "(" + unread + ") " + baseTitle; beep(); }
    else if (force) beep();
  }

  // ---------- people and private chats
  function badgeFor(key) {
    var c = unreadBy[key] || 0;
    return c ? el("span", "badge", c > 99 ? "99+" : String(c)) : null;
  }
  function renderPeople() {
    var ul = $("userlist"), chips = $("chips");
    ul.textContent = ""; chips.textContent = "";
    var others = online.filter(function (n) { return n !== me; });
    var away = Object.keys(convos).filter(function (n) { return n !== me && online.indexOf(n) < 0; });
    if (current && others.indexOf(current) < 0 && away.indexOf(current) < 0) away.push(current);
    function entry(key, label, isOff) {
      var li = el("li"), b = el("button", "person" + (current === key ? " active" : "") + (isOff ? " off" : ""));
      b.type = "button";
      var a;
      if (key === "") { a = el("div", "av sm", "#"); a.style.background = "var(--muted)"; }
      else a = avatar(key, true);
      b.appendChild(a);
      b.appendChild(el("span", "pn", label + (isOff ? " (offline)" : "")));
      var bd = badgeFor(key); if (bd) b.appendChild(bd);
      b.addEventListener("click", function () { openChat(key); });
      li.appendChild(b); ul.appendChild(li);

      var c = el("button", "chip" + (current === key ? " active" : "") + (isOff ? " off" : ""), label);
      c.type = "button";
      var bd2 = badgeFor(key); if (bd2) c.appendChild(bd2);
      c.addEventListener("click", function () { openChat(key); });
      chips.appendChild(c);
    }
    entry("", "Everyone", false);
    others.forEach(function (n) { entry(n, n, false); });
    away.forEach(function (n) { entry(n, n, true); });
    ul.appendChild(el("li", "me-note", "You are " + me));
  }
  function updateHead() {
    $("convoHead").hidden = current === "";
    text.placeholder = current ? "Message " + current : "Write a message";
    if (!current) return;
    $("convoName").textContent = "Private chat with " + current;
    $("convoNote").textContent = online.indexOf(current) >= 0
      ? "Only you and " + current + " can see this chat."
      : current + " is offline. You can write again when they're back.";
  }
  function renderLog() {
    log.textContent = ""; lastMsg = null;
    var list = current ? (convos[current] || []) : pub;
    if (!list.length) {
      var hint = el("div", "", current ? "No messages with " + current + " yet. Say hi." : "No messages yet. Say hello, or press Invite to bring others in.");
      hint.id = "empty";
      log.appendChild(hint);
    }
    list.forEach(function (m) { render(m); });
    log.scrollTop = log.scrollHeight;
    updateHead(); showTyping();
  }
  function openChat(key) {
    if (key === me) return;
    current = key; unreadBy[key] = 0;
    renderLog(); renderPeople();
    if (!window.matchMedia("(max-width: 700px)").matches) text.focus();
  }
  function makeLink(node, name) {
    node.classList.add("link");
    node.title = "Message " + name + " privately";
    node.addEventListener("click", function () { openChat(name); });
  }
  $("convoBack").addEventListener("click", function () { openChat(""); });

  // ---------- rendering
  function linkify(parent, s) {
    var re = /(https?:\/\/[^\s<]+)/g, last = 0, m;
    while ((m = re.exec(s))) {
      if (m.index > last) parent.appendChild(document.createTextNode(s.slice(last, m.index)));
      var a = el("a", "", m[1]);
      a.href = m[1]; a.target = "_blank"; a.rel = "noopener noreferrer";
      parent.appendChild(a);
      last = m.index + m[1].length;
    }
    if (last < s.length) parent.appendChild(document.createTextNode(s.slice(last)));
  }
  function render(m) {
    if (m.type === "sys") { log.appendChild(el("div", "sys", m.text)); lastMsg = null; return; }
    var mine = m.name === me;
    var cont = !!lastMsg && lastMsg.name === m.name && (m.ts - lastMsg.ts) < 180000;
    lastMsg = m;
    var row = el("div", "row" + (mine ? " mine" : "") + (cont ? " cont" : ""));
    if (!mine) {
      if (cont) row.appendChild(el("div", "spacer"));
      else { var av = avatar(m.name); makeLink(av, m.name); row.appendChild(av); }
    }
    var col = el("div", "col");
    if (!cont) {
      var meta = el("div", "meta");
      var who = el("b", "", mine ? "You" : m.name);
      if (!mine) makeLink(who, m.name);
      meta.appendChild(who);
      meta.appendChild(document.createTextNode("  " + fmt(m.ts)));
      col.appendChild(meta);
    }
    var b = el("div", "bubble"); b.title = fmt(m.ts);
    if (m.image) {
      var img = el("img"); img.src = m.image; img.alt = "Shared image";
      img.addEventListener("click", function () { $("lightImg").src = m.image; $("lightbox").hidden = false; });
      b.appendChild(img);
    }
    if (m.audio) {
      var au = document.createElement("audio");
      au.controls = true; au.preload = "metadata"; au.src = m.audio;
      b.appendChild(el("span", "vlabel", "Voice message"));
      b.appendChild(au);
    }
    if (m.text) { var s = el("span"); linkify(s, m.text); b.appendChild(s); }
    col.appendChild(b); row.appendChild(col);
    log.appendChild(row);
  }
  $("lightbox").addEventListener("click", function () { this.hidden = true; });

  // ---------- sending
  function post(path, body) {
    return fetch(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
  }
  function send() {
    var t = text.value.trim();
    if (!t && !pendingImage) return;
    var img = pendingImage;
    text.value = ""; grow(); clearPreview();
    post("/send", { name: me, cid: cid, to: current || undefined, text: t, image: img })
      .then(function (r) { if (!r.ok) throw new Error(r.status); })
      .catch(function (err) {
        text.value = t; pendingImage = img; showPreview();
        toast(err && err.message === "404" ? current + " is offline right now." : "Message not sent. Check your connection.");
      });
    text.focus();
  }
  $("composer").addEventListener("submit", function (e) { e.preventDefault(); send(); });
  text.addEventListener("keydown", function (e) {
    if (e.key === "Enter" && !e.shiftKey && !e.isComposing) { e.preventDefault(); send(); }
  });
  function grow() { text.style.height = "auto"; text.style.height = Math.min(text.scrollHeight, 120) + "px"; }
  text.addEventListener("input", function () {
    grow();
    var now = Date.now();
    if (text.value && now - lastTyping > 2000) { lastTyping = now; post("/typing", { name: me, cid: cid, to: current || undefined }).catch(function () {}); }
  });

  // ---------- images
  function prepare(file) {
    return new Promise(function (resolve, reject) {
      if (!file || file.type.indexOf("image/") !== 0) return reject(new Error("not an image"));
      var url = URL.createObjectURL(file), img = new Image();
      img.onload = function () {
        var scale = Math.min(1, 1280 / Math.max(img.width, img.height));
        var w = Math.round(img.width * scale), h = Math.round(img.height * scale);
        var c = document.createElement("canvas"); c.width = w; c.height = h;
        var ctx = c.getContext("2d"); ctx.fillStyle = "#fff"; ctx.fillRect(0, 0, w, h); ctx.drawImage(img, 0, 0, w, h);
        URL.revokeObjectURL(url);
        var q = 0.82, out = c.toDataURL("image/jpeg", q);
        while (out.length > 1000000 && q > 0.4) { q -= 0.12; out = c.toDataURL("image/jpeg", q); }
        out.length > 1400000 ? reject(new Error("too large")) : resolve(out);
      };
      img.onerror = function () { URL.revokeObjectURL(url); reject(new Error("bad image")); };
      img.src = url;
    });
  }
  function takeImage(file) {
    prepare(file).then(function (d) { pendingImage = d; showPreview(); text.focus(); })
      .catch(function () { toast("That image couldn't be added. Try a smaller one."); });
  }
  function showPreview() { if (pendingImage) { $("previewImg").src = pendingImage; $("preview").hidden = false; } }
  function clearPreview() { pendingImage = null; $("preview").hidden = true; $("previewImg").removeAttribute("src"); }
  $("photoBtn").addEventListener("click", function () { $("file").click(); });
  $("file").addEventListener("change", function () { if (this.files[0]) takeImage(this.files[0]); this.value = ""; });
  $("dropImg").addEventListener("click", clearPreview);
  text.addEventListener("paste", function (e) {
    var items = (e.clipboardData && e.clipboardData.items) || [];
    for (var i = 0; i < items.length; i++) {
      if (items[i].kind === "file" && items[i].type.indexOf("image/") === 0) { e.preventDefault(); takeImage(items[i].getAsFile()); return; }
    }
  });
  $("main").addEventListener("dragover", function (e) { e.preventDefault(); });
  $("main").addEventListener("drop", function (e) {
    e.preventDefault();
    if (e.dataTransfer.files && e.dataTransfer.files[0]) takeImage(e.dataTransfer.files[0]);
  });

  // ---------- emoji
  ["😀","😂","😊","😍","😎","🤔","😅","😭","👍","👏","🙏","🔥","🎉","❤️","💯","✅"].forEach(function (c) {
    var b = el("button", "", c); b.type = "button";
    b.addEventListener("click", function () {
      var s = text.selectionStart || text.value.length, en = text.selectionEnd || s;
      text.value = text.value.slice(0, s) + c + text.value.slice(en);
      text.focus(); text.selectionStart = text.selectionEnd = s + c.length;
      $("emoji").hidden = true;
    });
    $("emoji").appendChild(b);
  });
  $("emojiBtn").addEventListener("click", function () { $("emoji").hidden = !$("emoji").hidden; });

  // ---------- invite
  function copy(str, btn) {
    function done() { btn.textContent = "Copied"; setTimeout(function () { btn.textContent = "Copy"; }, 1500); }
    if (navigator.clipboard && window.isSecureContext) { navigator.clipboard.writeText(str).then(done); return; }
    var t = document.createElement("textarea"); t.value = str; document.body.appendChild(t); t.select();
    try { document.execCommand("copy"); done(); } catch (e) { toast("Select the address and copy it."); }
    t.remove();
  }
  $("invite").addEventListener("click", function () {
    fetch("/info").then(function (r) { return r.json(); }).then(function (info) {
      var box = $("urls"); box.textContent = "";
      info.urls.forEach(function (u) {
        var row = el("div", "url"); row.appendChild(el("code", "", u));
        var b = el("button", "ghost", "Copy"); b.type = "button";
        b.addEventListener("click", function () { copy(u, b); });
        row.appendChild(b); box.appendChild(row);
      });
      $("inviteBox").hidden = false;
    }).catch(function () { toast("Couldn't load the addresses."); });
  });
  $("closeInvite").addEventListener("click", function () { $("inviteBox").hidden = true; });

  // ---------- mic / camera availability
  var mediaOK = !!(window.isSecureContext && navigator.mediaDevices && navigator.mediaDevices.getUserMedia);
  function needSecure() {
    toast(location.protocol === "http:" && location.hostname !== "localhost"
      ? "Voice and video need https. Restart the chat without --http."
      : "This browser can't use the microphone or camera on this page.");
  }

  // ---------- video call (peer to peer over the LAN, everyone connects to everyone)
  var RTC = window.RTCPeerConnection;
  var inCall = false, localStream = null, pcs = {}, pendingIce = {}, tiles = {};
  var callMembers = [], sigChain = Promise.resolve(), micOn = true, camOn = true;

  function signal(to, data) {
    sigChain = sigChain.then(function () {
      return post("/signal", { name: me, cid: cid, to: to, data: data });
    }).catch(function () {});
  }
  function updateCallUI() {
    var btn = $("callBtn");
    btn.textContent = inCall ? "Return to call" : (callMembers.length ? "Join call" : "Call");
    btn.className = "ghost" + (callMembers.length ? " live" : "");
    $("callBanner").hidden = !callMembers.length;
    $("callWho").textContent = "Call in progress: " + callMembers.map(function (n) { return n === me ? "You" : n; }).join(", ");
    $("callJoin").textContent = inCall ? "Return" : "Join";
  }
  function refreshBar() {
    var hasCam = !!localStream && localStream.getVideoTracks().length > 0;
    $("micBtn").textContent = micOn ? "Mute" : "Unmute";
    $("micBtn").className = micOn ? "" : "off";
    $("camBtn").disabled = !hasCam;
    $("camBtn").textContent = camOn ? "Camera off" : "Camera on";
    $("camBtn").className = camOn ? "" : "off";
  }
  function addTile(name, stream, local) {
    var t = tiles[name];
    if (!t) {
      var tile = el("div", "tile");
      tile.appendChild(avatar(name));
      var v = document.createElement("video");
      v.autoplay = true; v.playsInline = true; v.setAttribute("playsinline", "");
      if (local) { v.muted = true; v.className = "mirror"; }
      tile.appendChild(v);
      tile.appendChild(el("span", "tname", local ? "You" : name));
      $("callGrid").appendChild(tile);
      t = tiles[name] = { tile: tile, video: v };
    }
    if (t.video.srcObject !== stream) t.video.srcObject = stream;
    t.video.style.display = stream.getVideoTracks().length ? "" : "none";
    var p = t.video.play(); if (p && p.catch) p.catch(function () {});
  }
  function closePeer(name, keepIce) {
    var pc = pcs[name];
    if (pc) {
      pc.onicecandidate = null; pc.ontrack = null; pc.onconnectionstatechange = null;
      try { pc.close(); } catch (e) {}
      delete pcs[name];
    }
    if (!keepIce) delete pendingIce[name];
    var t = tiles[name];
    if (t && name !== me) { t.tile.remove(); delete tiles[name]; }
  }
  function makePeer(name, initiator) {
    var pc = new RTC({ iceServers: [] });   // no STUN/TURN: devices find each other on the LAN
    pcs[name] = pc;
    localStream.getTracks().forEach(function (t) { pc.addTrack(t, localStream); });
    pc.onicecandidate = function (e) { if (e.candidate) signal(name, { candidate: e.candidate }); };
    pc.ontrack = function (e) { addTile(name, e.streams[0] || new MediaStream([e.track]), false); };
    pc.onconnectionstatechange = function () {
      if (pc.connectionState === "failed") toast("Couldn't connect to " + name + " for the call.");
    };
    if (initiator) {
      pc.createOffer()
        .then(function (o) { return pc.setLocalDescription(o); })
        .then(function () { signal(name, { sdp: pc.localDescription }); })
        .catch(function () {});
    }
    return pc;
  }
  function flushIce(name) {
    var p = pcs[name], q = pendingIce[name] || [];
    pendingIce[name] = [];
    if (p) q.forEach(function (c) { p.addIceCandidate(c).catch(function () {}); });
  }
  function onSignal(from, data) {
    if (!inCall || !localStream || !data) return;
    if (data.sdp) {
      if (data.sdp.type === "offer") {
        closePeer(from, true);
        var pc = makePeer(from, false);
        pc.setRemoteDescription(data.sdp)
          .then(function () { flushIce(from); return pc.createAnswer(); })
          .then(function (a) { return pc.setLocalDescription(a); })
          .then(function () { signal(from, { sdp: pc.localDescription }); })
          .catch(function () {});
      } else if (pcs[from]) {
        pcs[from].setRemoteDescription(data.sdp).then(function () { flushIce(from); }).catch(function () {});
      }
    } else if (data.candidate) {
      var p = pcs[from];
      if (p && p.remoteDescription) p.addIceCandidate(data.candidate).catch(function () {});
      else (pendingIce[from] = pendingIce[from] || []).push(data.candidate);
    }
  }
  function openCall() {
    if (inCall) { $("callView").hidden = false; return; }
    if (!mediaOK || !RTC) { needSecure(); return; }
    navigator.mediaDevices.getUserMedia({ audio: true, video: { width: { ideal: 640 }, height: { ideal: 480 } } })
      .catch(function () { return navigator.mediaDevices.getUserMedia({ audio: true, video: false }); })
      .then(function (stream) {
        localStream = stream; inCall = true; micOn = true; camOn = stream.getVideoTracks().length > 0;
        $("callView").hidden = false;
        refreshBar();
        addTile(me, stream, true);
        return post("/call/join", { name: me, cid: cid }).then(function (r) { return r.json(); });
      })
      .then(function (res) {
        res.members.forEach(function (n) { makePeer(n, true); });
        updateCallUI();
      })
      .catch(function () {
        leaveCall();
        toast("Couldn't start the call. Allow the camera and microphone, then try again.");
      });
  }
  function leaveCall() {
    var was = inCall;
    inCall = false;
    Object.keys(pcs).forEach(function (n) { closePeer(n); });
    Object.keys(tiles).forEach(function (n) { tiles[n].tile.remove(); });
    tiles = {}; pendingIce = {};
    if (localStream) { localStream.getTracks().forEach(function (t) { t.stop(); }); localStream = null; }
    $("callView").hidden = true;
    if (was) post("/call/leave", { name: me, cid: cid }).catch(function () {});
    updateCallUI();
  }
  $("callBtn").addEventListener("click", openCall);
  $("callJoin").addEventListener("click", openCall);
  $("hang").addEventListener("click", leaveCall);
  $("backBtn").addEventListener("click", function () { $("callView").hidden = true; });
  $("micBtn").addEventListener("click", function () {
    if (!localStream) return;
    micOn = !micOn;
    localStream.getAudioTracks().forEach(function (t) { t.enabled = micOn; });
    refreshBar();
  });
  $("camBtn").addEventListener("click", function () {
    if (!localStream) return;
    camOn = !camOn;
    localStream.getVideoTracks().forEach(function (t) { t.enabled = camOn; });
    refreshBar();
  });
  window.addEventListener("pagehide", function () {
    if (inCall && navigator.sendBeacon) {
      navigator.sendBeacon("/call/leave", new Blob([JSON.stringify({ name: me, cid: cid })], { type: "application/json" }));
    }
  });

  // ---------- voice messages
  var rec = null, recChunks = [], recTick = null, recStart = 0, recKeep = true;
  function fmtDur(ms) { var s = Math.floor(ms / 1000); return Math.floor(s / 60) + ":" + ("0" + (s % 60)).slice(-2); }
  function startRec() {
    if (!mediaOK || !window.MediaRecorder) { needSecure(); return; }
    navigator.mediaDevices.getUserMedia({ audio: true }).then(function (stream) {
      var types = ["audio/webm;codecs=opus", "audio/webm", "audio/ogg;codecs=opus", "audio/mp4"], type = "";
      for (var i = 0; i < types.length; i++) { if (MediaRecorder.isTypeSupported(types[i])) { type = types[i]; break; } }
      var opts = { audioBitsPerSecond: 32000 };
      if (type) opts.mimeType = type;
      recChunks = []; recKeep = true; recTo = current;
      rec = new MediaRecorder(stream, opts);
      rec.ondataavailable = function (e) { if (e.data && e.data.size) recChunks.push(e.data); };
      rec.onstop = function () {
        stream.getTracks().forEach(function (t) { t.stop(); });
        clearInterval(recTick);
        $("composer").classList.remove("recording");
        if (!recKeep || !recChunks.length) return;
        if (Date.now() - recStart < 600) { toast("Too short. Hold on a little longer."); return; }
        var fr = new FileReader();
        fr.onload = function () {
          var d = fr.result;
          if (d.length > 1400000) { toast("That voice message is too long."); return; }
          post("/send", { name: me, cid: cid, to: recTo || undefined, text: "", audio: d })
            .then(function (r) { if (!r.ok) throw new Error(r.status); })
            .catch(function () { toast("Voice message not sent."); });
        };
        fr.readAsDataURL(new Blob(recChunks, { type: rec.mimeType || type || "audio/webm" }));
      };
      rec.start();
      recStart = Date.now();
      $("recTime").textContent = "0:00";
      $("composer").classList.add("recording");
      recTick = setInterval(function () {
        var ms = Date.now() - recStart;
        $("recTime").textContent = fmtDur(ms);
        if (ms >= 120000) stopRec(true);
      }, 250);
    }).catch(function () { toast("Microphone blocked. Allow it in the browser's site settings."); });
  }
  function stopRec(keep) { recKeep = keep; if (rec && rec.state !== "inactive") rec.stop(); }
  $("voiceBtn").addEventListener("click", startRec);
  $("recSend").addEventListener("click", function () { stopRec(true); });
  $("recCancel").addEventListener("click", function () { stopRec(false); });

  // ---------- sound + unread
  function beep() {
    if (muted || !actx) return;
    try {
      var o = actx.createOscillator(), g = actx.createGain(), t = actx.currentTime;
      o.type = "sine"; o.frequency.value = 880;
      g.gain.setValueAtTime(0.0001, t); g.gain.exponentialRampToValueAtTime(0.15, t + 0.02); g.gain.exponentialRampToValueAtTime(0.0001, t + 0.25);
      o.connect(g); g.connect(actx.destination); o.start(t); o.stop(t + 0.26);
    } catch (e) {}
  }
  $("mute").addEventListener("click", function () {
    muted = !muted; store("lanchat-muted", muted ? "1" : "0");
    this.textContent = muted ? "Sound off" : "Sound on";
  });
  document.addEventListener("visibilitychange", function () {
    if (!document.hidden) { unread = 0; document.title = baseTitle; }
  });
  document.addEventListener("keydown", function (e) {
    if (e.key === "Escape") { $("lightbox").hidden = true; $("inviteBox").hidden = true; $("emoji").hidden = true; }
  });
})();
</script>
</body>
</html>
"""


# ------------------------------------------------------------------ server

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass

    def send_json(self, code, obj=None):
        body = json.dumps(obj if obj is not None else {}).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def read_json(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = -1
        if length < 0 or length > MAX_BODY:
            self.close_connection = True
            self.send_json(413, {"error": "too large"})
            return None
        try:
            data = json.loads(self.rfile.read(length).decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            self.send_json(400, {"error": "bad json"})
            return None
        if not isinstance(data, dict):
            self.send_json(400, {"error": "bad json"})
            return None
        return data

    def do_GET(self):
        url = urlparse(self.path)
        qs = parse_qs(url.query)
        first = lambda key: (qs.get(key, [""])[0])

        if url.path == "/":
            body = PAGE.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
        elif url.path == "/events":
            self.stream(clean_name(first("name")), first("cid")[:64])
        elif url.path == "/check":
            name, cid = clean_name(first("name")), first("cid")[:64]
            with lock:
                users = presence.get(name)
                ok = not users or cid in users
            self.send_json(200, {"ok": ok, "name": name})
        elif url.path == "/info":
            self.send_json(200, {"urls": ["http://%s:%d" % (ip, PORT) for ip in lan_ips()]})
        else:
            self.send_error(404)

    def do_POST(self):
        path = urlparse(self.path).path
        if path not in ("/send", "/typing", "/call/join", "/call/leave", "/signal"):
            self.send_error(404)
            return
        data = self.read_json()
        if data is None:
            return
        name, cid = clean_name(data.get("name")), str(data.get("cid", ""))[:64]
        if not is_member(name, cid):
            self.send_json(403, {"error": "not joined"})
            return

        if path == "/typing":
            to = data.get("to")
            with lock:
                if to:
                    push_to(clean_name(to), sse("typing", {"name": name, "dm": True}))
                else:
                    push(sse("typing", {"name": name, "dm": False}))
            self.send_json(200)
            return

        if path == "/call/join":
            with lock:
                started = not call_members
                already = name in call_members
                call_members.add(name)
                others = sorted(n for n in call_members if n != name)
            push_call()
            if started and not already:
                add_message("sys", "", "%s started a call" % name)
            self.send_json(200, {"members": others})
            return

        if path == "/call/leave":
            leave_call(name)
            self.send_json(200)
            return

        if path == "/signal":
            to = clean_name(data.get("to"))
            payload = data.get("data")
            if not isinstance(payload, dict) or len(json.dumps(payload)) > MAX_SIGNAL_CHARS:
                self.send_json(400, {"error": "bad signal"})
                return
            with lock:
                ok = name in call_members and to in call_members and to != name
                if ok:
                    push_to(to, sse("signal", {"from": name, "data": payload}))
            self.send_json(200 if ok else 409)
            return

        text = str(data.get("text") or "").strip()[:MAX_TEXT]
        image, audio = data.get("image"), data.get("audio")
        if image is not None:
            if not (isinstance(image, str) and len(image) <= MAX_MEDIA_CHARS and IMAGE_RE.match(image)):
                self.send_json(400, {"error": "bad image"})
                return
        if audio is not None:
            if not (isinstance(audio, str) and len(audio) <= MAX_MEDIA_CHARS and AUDIO_RE.match(audio)):
                self.send_json(400, {"error": "bad audio"})
                return
        to = data.get("to")
        if to:
            to = clean_name(to)
            if to == name:
                self.send_json(400, {"error": "can't message yourself"})
                return
            if not is_online(to):
                self.send_json(404, {"error": "offline"})
                return
        if text or image or audio:
            add_message("msg", name, text, image, audio, to or None)
        self.send_json(200)

    # ---- live stream (Server-Sent Events)
    def stream(self, name, cid):
        q = queue.Queue()
        registered = False
        with lock:
            users = presence.get(name)
            if users and cid not in users:
                taken = True
            else:
                taken = False
                first = users is None
                if first:
                    if name_owner.get(name, cid) != cid:
                        purge_dms(name)      # someone new is using this name: drop its old private chats
                    name_owner[name] = cid
                users = presence.setdefault(name, {})
                users[cid] = users.get(cid, 0) + 1
                clients.append((q, name))
                registered = True
                snapshot = list(history)
                call_now = sorted(call_members, key=str.lower)
                dm_now = dm_snapshot(name)
        if taken:
            self.send_json(409, {"error": "name in use"})
            return

        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()
            self.wfile.write(("retry: 2000\n\n" + sse("history", snapshot)
                              + sse("call", call_now) + sse("dmhistory", dm_now)).encode("utf-8"))
            self.wfile.flush()

            if first:
                add_message("sys", "", "%s joined" % name)
            push_presence()

            while True:
                try:
                    payload = q.get(timeout=5)
                except queue.Empty:
                    payload = ": ping\n\n"
                self.wfile.write(payload.encode("utf-8"))
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            pass
        finally:
            if registered:
                with lock:
                    if (q, name) in clients:
                        clients.remove((q, name))
                    users = presence.get(name)
                    if users is not None and cid in users:
                        users[cid] = max(0, users[cid] - 1)
                    idle = users is not None and sum(users.values()) == 0
                push_presence()
                if idle:
                    t = threading.Timer(LEAVE_GRACE, finalize_leave, args=(name,))
                    t.daemon = True
                    t.start()


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    ssl_context = None   # set in main() when serving https

    def finish_request(self, request, client_address):
        """Runs in the connection's own thread. Handles https, and bounces plain
        http visitors to the https address instead of showing them garbage."""
        if self.ssl_context is None:
            return super().finish_request(request, client_address)
        try:
            request.settimeout(10)
            first = request.recv(1, socket.MSG_PEEK)
            if not first:
                return
            if first != b"\x16":          # not a TLS handshake: someone typed http://
                self._redirect_to_https(request)
                return
            tls = self.ssl_context.wrap_socket(request, server_side=True)
            tls.settimeout(None)
        except (OSError, ssl.SSLError):
            return
        try:
            self.RequestHandlerClass(tls, client_address, self)
        finally:
            try:
                tls.close()
            except OSError:
                pass

    @staticmethod
    def _redirect_to_https(conn):
        try:
            head = conn.recv(4096).decode("latin-1", "replace")
            m = re.search(r"^Host:\s*([A-Za-z0-9.:\-\[\]]+)", head, re.I | re.M)
            host = m.group(1) if m else "localhost:%d" % PORT
            conn.sendall(("HTTP/1.1 301 Moved Permanently\r\nLocation: https://%s/\r\n"
                          "Content-Length: 0\r\nConnection: close\r\n\r\n" % host).encode("latin-1"))
        except OSError:
            pass


def main():
    global PORT
    httpd = None
    for port in range(ARGS.port, ARGS.port + 20):
        try:
            httpd = Server(("0.0.0.0", port), Handler)
            PORT = port
            break
        except OSError:
            continue
    if httpd is None:
        print("Could not open any port from %d to %d. Close other programs or pick another port."
              % (ARGS.port, ARGS.port + 19))
        sys.exit(1)

    load_history()
    ips = lan_ips()

    scheme = "http"
    if not ARGS.http:
        ctx = make_ssl_context(ips)
        if ctx is not None:
            httpd.ssl_context = ctx
            scheme = "https"

    print("\n  LAN Chat is running. No internet needed.\n")
    print("  On this computer : %s://localhost:%d" % (scheme, PORT))
    for ip in ips:
        print("  Everyone else    : %s://%s:%d" % (scheme, ip, PORT))
    if len(ips) > 1:
        print("\n  Several network addresses found. Use the one that starts like the")
        print("  Wi-Fi address on your phone (for example both begin with 192.168.).")

    if scheme == "https":
        print("\n  Voice messages and video calls are ON.")
        print("  First visit on each device: the browser shows a security warning because the")
        print("  certificate is self-made. Choose Advanced, then Proceed / Visit this website.")
        print("  This is safe here; the connection never leaves your network.")
    elif ARGS.http:
        print("\n  Running in plain http mode: text and images only (no voice or video).")
    else:
        print("\n  Voice and video are OFF: couldn't create a certificate for https.")
        print("  Fix: install OpenSSL (Windows: install Git for Windows, it includes it),")
        print("  or run once with internet:  pip install cryptography   then start again.")

    print("\n  If other devices can't open it, allow Python through this computer's firewall.")
    if ARGS.save:
        print("  Saving text history to %s" % HISTORY_FILE)
    print("  Press Ctrl+C to stop.\n")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
