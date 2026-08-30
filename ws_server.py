import asyncio
import websockets
import json
import uuid
import struct
import gzip
import base64
import threading
import time
import ssl
import wave
import os

# ── 豆包 API 参数 ──
DOUBAO_WS_URL = "wss://openspeech.bytedance.com/api/v3/sauc/bigmodel"
RESOURCE_ID = "volc.bigasr.sauc.duration"


def pack_request(msg_type, payload_bytes, seq, is_last=False,
                 serialization=0b0001, compression=0b0001):
    """Seed 协议 V1 打包"""
    if is_last:
        flags = 0b0011  # NEG_WITH_SEQUENCE
        seq = -seq
    else:
        flags = 0b0001  # POS_SEQUENCE

    byte0 = (0b0001 << 4) | 1  # V1, header_size=1
    byte1 = (msg_type << 4) | flags
    byte2 = (serialization << 4) | compression
    byte3 = 0x00
    header = bytes([byte0, byte1, byte2, byte3])

    if compression == 1:
        payload_bytes = gzip.compress(payload_bytes)

    return header + struct.pack('>i', seq) + struct.pack('>I', len(payload_bytes)) + payload_bytes


def parse_response(data):
    """Seed 协议 V1 响应解析"""
    if len(data) < 4:
        return None
    hs = data[0] & 0x0F
    mt = (data[1] >> 4) & 0x0F
    fl = data[1] & 0x0F
    sm = (data[2] >> 4) & 0x0F
    mc = data[2] & 0x0F

    payload = data[hs * 4:]
    seq = None

    if fl & 0x01:
        seq = struct.unpack('>i', payload[:4])[0]
        payload = payload[4:]

    if mt == 0b1001:  # SERVER_FULL_RESPONSE
        psize = struct.unpack('>I', payload[:4])[0]
        payload = payload[4:]
    elif mt == 0b1111:  # ERROR
        code = struct.unpack('>i', payload[:4])[0]
        psize = struct.unpack('>I', payload[4:8])[0]
        payload = payload[8:]
    else:
        code = 0
        psize = 0

    if mc == 1:  # GZIP
        try:
            payload = gzip.decompress(payload)
        except:
            pass

    data = None
    if sm == 0b0001 and payload:  # JSON
        try:
            data = json.loads(payload.decode())
        except:
            pass

    return {"type": mt, "seq": seq, "data": data,
            "error_code": 0 if mt != 0b1111 else code}


def extract_text(data):
    """只返回最后一个 utterance 的文字；空 utterance 不 fallback 到历史"""
    if not isinstance(data, dict):
        return ""
    result = data.get("result", {})
    if isinstance(result, dict):
        utterances = result.get("utterances", [])
        if utterances:
            t = utterances[-1].get("text", "") or ""
            return t  # 可能为空字符串，主调方需判断
    return ""


async def handle_client(browser_ws):
    print(f"[WS] 浏览器连接: {browser_ws.remote_address}")

    try:
        first = await asyncio.wait_for(browser_ws.recv(), timeout=10)
        cfg = json.loads(first)
    except Exception as e:
        await browser_ws.send(json.dumps({"error": str(e)}))
        return

    app_id = cfg.get("app_id", "").strip() or cfg.get("key_id", "").strip()
    access_token = cfg.get("access_token", "").strip() or cfg.get("secret", "").strip()
    if not app_id or not access_token:
        await browser_ws.send(json.dumps({"error": "缺少凭证"}))
        return

    import websocket as ws_client
    loop = asyncio.get_event_loop()
    send_queue = asyncio.Queue()
    audio_buf = bytearray()
    latest_text = ""

    async def send_worker():
        while True:
            m = await send_queue.get()
            if m is None:
                break
            try:
                await browser_ws.send(m)
            except:
                pass

    send_task = asyncio.create_task(send_worker())

    def queue_send(m):
        try:
            loop.call_soon_threadsafe(send_queue.put_nowait, m)
        except:
            pass

    async def run_doubao_session(audio_queue):
        """Connect to Doubao, handshake, forward audio, yield results. Returns when session ends."""
        nonlocal latest_text
        request_id = str(uuid.uuid4())
        connect_id = str(uuid.uuid4())
        ready_evt = threading.Event()
        err_evt = threading.Event()
        doubao_seq = 1

        def on_open(ws_app):
            nonlocal doubao_seq
            print("[豆包] on_open, 发送 Full Client Request")
            init = {
                "user": {"uid": "user_" + connect_id[:8]},
                "audio": {"format": "pcm", "rate": 16000, "bits": 16, "channel": 1},
                "request": {
                    "model_name": "bigmodel",
                    "enable_itn": True,
                    "enable_punc": True,
                    "enable_ddc": True,
                    "show_utterances": True,
                    "enable_nonstream": False,
                }
            }
            payload = json.dumps(init, ensure_ascii=False).encode()
            pkg = pack_request(0b0001, payload, doubao_seq)
            ws_app.send(pkg, opcode=ws_client.ABNF.OPCODE_BINARY)
            print(f"[豆包] init sent seq={doubao_seq} size={len(pkg)}B")
            doubao_seq += 1

        def on_msg(ws_app, message):
            nonlocal latest_text
            try:
                r = parse_response(message)
                if not r:
                    return

                if r["type"] == 0b1111:
                    err = r.get("data") or f"error_code={r.get('error_code')}"
                    print(f"[豆包] ❌ {err}")
                    queue_send(json.dumps({"error": str(err)[:200]}))
                    err_evt.set()
                    return

                data = r.get("data")
                if data:
                    text = extract_text(data)
                    if text and text != latest_text:
                        latest_text = text
                        print(f"[豆包] 🎤 {text[:120]}")
                        queue_send(json.dumps({"text": text}))

                if not ready_evt.is_set():
                    ready_evt.set()
                    print(f"[豆包] 握手完成 (seq={r.get('seq')})")

            except Exception as e:
                print(f"[豆包] on_msg异常: {e}")

        def on_err(ws_app, error):
            print(f"[豆包] on_error: {error}")
            err_evt.set()

        def on_close(ws_app, code, reason):
            print(f"[豆包] on_close: code={code}")
            err_evt.set()

        headers = [
            f"X-Api-App-Key: {app_id}",
            f"X-Api-Access-Key: {access_token}",
            f"X-Api-Resource-Id: {RESOURCE_ID}",
            f"X-Api-Request-Id: {request_id}",
            f"X-Api-Connect-Id: {connect_id}",
        ]

        doubao_ws = ws_client.WebSocketApp(
            DOUBAO_WS_URL, header=headers,
            on_open=on_open, on_message=on_msg, on_error=on_err, on_close=on_close,
        )

        def run():
            doubao_ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

        t = threading.Thread(target=run, daemon=True)
        t.start()

        # 等待握手
        try:
            await asyncio.wait_for(loop.run_in_executor(None, ready_evt.wait), timeout=10)
        except asyncio.TimeoutError:
            queue_send(json.dumps({"error": "豆包连接超时"}))
            doubao_ws.close()
            t.join(timeout=3)
            return False  # failed to connect

        print("[豆包] 会话已建立")

        MAX_SESSION_DURATION = 600  # 10 minutes max per session
        session_start = time.time()
        is_rotation = False

        # Heartbeat task: send silence every 280s to prevent session timeout
        async def heartbeat():
            silence = b'\x00\x00' * 160  # 10ms silence at 16kHz 16bit
            while not err_evt.is_set():
                await asyncio.sleep(280)
                if err_evt.is_set():
                    break
                try:
                    if doubao_ws.sock and doubao_ws.sock.connected:
                        nonlocal doubao_seq
                        pkg = pack_request(0b0010, silence, doubao_seq)
                        doubao_ws.send(pkg, opcode=ws_client.ABNF.OPCODE_BINARY)
                        doubao_seq += 1
                        print("[豆包] 心跳")
                except Exception as e:
                    print(f"[豆包] 心跳异常: {e}")
                    break

        heartbeat_task = asyncio.create_task(heartbeat())

        # Forward audio from queue to Doubao
        try:
            while True:
                # Session rotation: prevent context slowdown after long sessions
                elapsed = time.time() - session_start
                if elapsed > MAX_SESSION_DURATION:
                    print(f"[豆包] 会话已运行 {elapsed:.0f}s，达到 {MAX_SESSION_DURATION}s 上限，启动轮换")
                    is_rotation = True
                    pkg = pack_request(0b0010, b'', doubao_seq, is_last=True)
                    if doubao_ws.sock and doubao_ws.sock.connected:
                        doubao_ws.send(pkg, opcode=ws_client.ABNF.OPCODE_BINARY)
                    break

                try:
                    item = await asyncio.wait_for(audio_queue.get(), timeout=30)
                except asyncio.TimeoutError:
                    if err_evt.is_set():
                        break
                    continue

                if item is None:  # stop signal
                    # Send end-of-stream to Doubao
                    pkg = pack_request(0b0010, b'', doubao_seq, is_last=True)
                    if doubao_ws.sock and doubao_ws.sock.connected and not err_evt.is_set():
                        doubao_ws.send(pkg, opcode=ws_client.ABNF.OPCODE_BINARY)
                        print("[豆包] 结束包已发")
                    break

                pcm = item
                audio_buf.extend(pcm)
                if doubao_ws.sock and doubao_ws.sock.connected and not err_evt.is_set():
                    pkg = pack_request(0b0010, pcm, doubao_seq)
                    doubao_ws.send(pkg, opcode=ws_client.ABNF.OPCODE_BINARY)
                    doubao_seq += 1

                if err_evt.is_set():
                    print("[豆包] 会话出错，退出转发")
                    break

        except Exception as e:
            print(f"[豆包] 转发异常: {e}")

        heartbeat_task.cancel()
        try:
            await heartbeat_task
        except asyncio.CancelledError:
            pass

        # Wait for final results only if session was healthy
        if not err_evt.is_set():
            try:
                await asyncio.wait_for(loop.run_in_executor(None, err_evt.wait), timeout=3)
            except asyncio.TimeoutError:
                pass

        try:
            doubao_ws.close()
        except:
            pass
        t.join(timeout=3)

        if is_rotation:
            return True  # Clean rotation, start new session
        return not err_evt.is_set()  # False if error

    # ── Run Doubao session, forward browser audio ──
    await browser_ws.send(json.dumps({"status": "connected"}))
    print("[WS] 流式转发已启动")

    audio_queue = asyncio.Queue()
    session_running = True
    browser_end = False

    async def session_loop():
        nonlocal session_running
        session_num = 0
        while not browser_end:
            session_num += 1
            session_running = True
            ok = await run_doubao_session(audio_queue)
            session_running = False
            if browser_end:
                break
            if not ok:
                # Error - let browser handle reconnection
                print(f"[豆包] 会话 #{session_num} 异常结束")
                break
            # Clean rotation - start new session immediately
            print(f"[WS] 会话 #{session_num} 轮换，开启新豆包会话...")

    session_task = asyncio.create_task(session_loop())

    async def browser_recv_loop():
        """Receive audio from browser, feed to Doubao session."""
        nonlocal browser_end
        try:
            while True:
                try:
                    msg = await asyncio.wait_for(browser_ws.recv(), timeout=5)
                except asyncio.TimeoutError:
                    if session_task.done():
                        break
                    continue

                data = json.loads(msg)
                audio_b64 = data.get("audio", "")
                is_last = data.get("last", False)

                if audio_b64:
                    pcm = base64.b64decode(audio_b64)
                    audio_buf.extend(pcm)
                    if session_running:
                        await audio_queue.put(pcm)

                if is_last:
                    await audio_queue.put(None)
                    print("[WS] 浏览器请求停止")
                    browser_end = True
                    break

        except Exception as e:
            print(f"[WS] 接收异常: {e}")

    recv_task = asyncio.create_task(browser_recv_loop())

    # Wait for either session end or browser end
    done, pending = await asyncio.wait(
        [session_task, recv_task],
        return_when=asyncio.FIRST_COMPLETED
    )

    # If session died first, stop receiving and close browser connection immediately
    if session_task in done:
        ok = session_task.result()
        if not ok:
            print("[豆包] 会话异常结束，立即通知浏览器重连")
        recv_task.cancel()
        try:
            await recv_task
        except asyncio.CancelledError:
            pass
    else:
        # Browser disconnected first (user stopped STT)
        await audio_queue.put(None)
        try:
            await asyncio.wait_for(session_task, timeout=10)
        except asyncio.TimeoutError:
            session_task.cancel()

    # ── 保存 WAV ──
    if len(audio_buf) > 0:
        wav_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "recordings")
        os.makedirs(wav_dir, exist_ok=True)
        wav_path = os.path.join(wav_dir, f"record_{int(time.time())}.wav")
        with wave.open(wav_path, 'wb') as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(16000)
            wf.writeframes(bytes(audio_buf))
        print(f"[WAV] {wav_path}")

    if not latest_text:
        queue_send(json.dumps({"error": "未获取到识别结果"}))

    send_queue.put_nowait(None)
    await send_task
    print("[豆包] 结束")


async def main():
    async with websockets.serve(handle_client, "127.0.0.1", 5001):
        print("WebSocket 服务器已启动 -> ws://127.0.0.1:5001/ws/stt")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
