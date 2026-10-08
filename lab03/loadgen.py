"""표준 라이브러리만 사용하는 속도 조절 부하 생성기."""
import argparse
import csv
import json
import math
import queue
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor


def request(url):
    started = time.monotonic()
    try:
        req = urllib.request.Request(url, headers={"Connection": "close"})
        with urllib.request.urlopen(req, timeout=3) as response:
            status, body = response.status, response.read()
    except urllib.error.HTTPError as error:
        status, body = error.code, error.read()
        error.close()
    except Exception:
        return 0, (time.monotonic() - started) * 1000, "network-error"
    elapsed = (time.monotonic() - started) * 1000
    try:
        pod = json.loads(body).get("pod", "unknown")
    except (ValueError, AttributeError):
        pod = "unknown"
    return status, elapsed, pod


def percentile95(values):
    return sorted(values)[math.ceil(len(values) * 0.95) - 1] if values else 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://web")
    parser.add_argument("--rps", type=float, default=5)
    parser.add_argument("--duration", type=float, default=0,
                        help="0이면 q 입력까지 실행")
    parser.add_argument("--csv", default="/tmp/results.csv")
    args = parser.parse_args()
    if not 1 <= args.rps <= 50 or args.duration < 0:
        parser.error("rps는 1~50, duration은 0 이상이어야 합니다")

    controls, results = queue.Queue(), queue.Queue()

    def read_input():
        for line in sys.stdin:
            controls.put(line.strip())

    threading.Thread(target=read_input, daemon=True).start()
    pool = ThreadPoolExecutor(max_workers=40)
    inflight = 0
    sent = skipped = 0
    completed = []
    started = last_report = next_send = time.monotonic()
    rate = args.rps
    stop = False
    fields = ["elapsed_s", "target_rps", "sent_rps", "success_rps",
              "completed", "error_pct", "success_avg_ms", "success_p95_ms",
              "skipped", "inflight", "status_counts", "success_by_pod"]
    csv_file = open(args.csv, "w", newline="", buffering=1)
    writer = csv.DictWriter(csv_file, fieldnames=fields)
    writer.writeheader()

    def report(now):
        nonlocal sent, skipped, completed, last_report
        elapsed = max(now - last_report, 0.001)
        good = [(latency, pod) for status, latency, pod in completed if status == 200]
        times = [latency for latency, _ in good]
        errors = len(completed) - len(good)
        row = dict(elapsed_s=round(now-started, 1), target_rps=rate,
                   sent_rps=round(sent/elapsed, 2), success_rps=round(len(good)/elapsed, 2),
                   completed=len(completed),
                   error_pct=round(100*errors/len(completed), 1) if completed else 0,
                   success_avg_ms=round(sum(times)/len(times), 1) if times else 0,
                   success_p95_ms=round(percentile95(times), 1), skipped=skipped,
                   inflight=inflight,
                   status_counts=json.dumps(dict(Counter(str(s) for s, _, _ in completed))),
                   success_by_pod=json.dumps(dict(Counter(p for _, p in good))))
        writer.writerow(row)
        print(f"t={row['elapsed_s']:6.1f}s target={rate:4.1f}/s "
              f"sent={row['sent_rps']:5.1f}/s ok={row['success_rps']:5.1f}/s "
              f"error={row['error_pct']:5.1f}% avg={row['success_avg_ms']:6.1f}ms "
              f"p95={row['success_p95_ms']:6.1f}ms skip={skipped} active={inflight}", flush=True)
        print("  status=" + row["status_counts"] + " pods=" + row["success_by_pod"], flush=True)
        sent = skipped = 0
        completed = []
        last_report = now

    def collect():
        nonlocal inflight
        while True:
            try:
                completed.append(results.get_nowait())
                inflight -= 1
            except queue.Empty:
                break

    print("입력: 1~50 숫자로 초당 요청 수 변경 / q로 종료", flush=True)
    try:
        while not stop:
            now = time.monotonic()
            collect()
            while not controls.empty():
                command = controls.get_nowait()
                if command.lower() == "q":
                    stop = True
                    break
                try:
                    new_rate = float(command)
                    if not 1 <= new_rate <= 50:
                        raise ValueError
                    rate, next_send = new_rate, now
                    print(f"target -> {rate:g} req/s", flush=True)
                except ValueError:
                    print("1~50 사이 숫자 또는 q를 입력하세요.", flush=True)
            if stop or (args.duration and now-started >= args.duration):
                break
            if now >= next_send:
                # 지연된 요청을 한꺼번에 몰아 보내지 않습니다.
                due = int((now-next_send)*rate) + 1
                skipped += due-1
                next_send += due/rate
                if inflight < 40:
                    future = pool.submit(request, args.url)
                    future.add_done_callback(lambda f: results.put(f.result()))
                    sent += 1
                    inflight += 1
                else:
                    skipped += 1
            if now-last_report >= 5:
                report(now)
            time.sleep(0.005)
    except KeyboardInterrupt:
        pass
    finally:
        print("새 요청을 중지하고 진행 중 요청을 기다립니다.", flush=True)
        pool.shutdown(wait=True)
        collect()
        report(time.monotonic())
        csv_file.close()
        print(f"CSV: {args.csv}", flush=True)


if __name__ == "__main__":
    main()
