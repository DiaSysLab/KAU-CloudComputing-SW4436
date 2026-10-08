"""HPA 실습 웹 서버: 요청마다 약 20ms의 스레드 CPU 시간 사용."""
import json
import os
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SLOTS = threading.BoundedSemaphore(2)
POD = socket.gethostname()
VERSION = os.environ.get("APP_VERSION", "v1")


def work():
    # 기다리는 시간도 클라이언트의 응답 시간에 포함됩니다.
    if not SLOTS.acquire(timeout=1.0):
        return 503, {"pod": POD, "version": VERSION, "result": "busy"}
    try:
        deadline = time.thread_time() + 0.020
        value = 1
        while time.thread_time() < deadline:
            value = (value * 1664525 + 1013904223) & 0xFFFFFFFF
        return 200, {"pod": POD, "version": VERSION, "result": "ok"}
    finally:
        SLOTS.release()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/healthz":
            status, result = 200, {"result": "ready"}
        elif self.path == "/":
            status, result = work()
        else:
            status, result = 404, {"result": "not found"}
        body = (json.dumps(result) + "\n").encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, *args):
        pass  # 부하 실험 중 요청 로그가 쌓이지 않게 합니다.


class Server(ThreadingHTTPServer):
    request_queue_size = 128
    daemon_threads = True


if __name__ == "__main__":
    print(f"pod={POD} version={VERSION} slots=2 cpu_work=20ms", flush=True)
    Server(("0.0.0.0", 8000), Handler).serve_forever()
