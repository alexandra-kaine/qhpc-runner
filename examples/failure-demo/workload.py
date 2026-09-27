import argparse
import os
import sys
import time

p = argparse.ArgumentParser()
p.add_argument("--mode", choices=["success", "error", "timeout", "oom"], required=True)
p.add_argument("--sleep", type=int, default=90)
p.add_argument("--alloc-mb", type=int, default=256)
args = p.parse_args()

print(f"mode={args.mode}", flush=True)

if args.mode == "success":
    print("success", flush=True)
    raise SystemExit(0)

if args.mode == "error":
    print("intentional application error", file=sys.stderr, flush=True)
    raise SystemExit(7)

if args.mode == "timeout":
    print(f"sleeping for {args.sleep}s", flush=True)
    time.sleep(args.sleep)
    raise SystemExit(0)

if args.mode == "oom":
    print(f"allocating about {args.alloc_mb} MB", flush=True)
    blocks = []
    for _ in range(args.alloc_mb):
        blocks.append(bytearray(1024 * 1024))
        time.sleep(0.005)
    print("allocation completed", flush=True)
