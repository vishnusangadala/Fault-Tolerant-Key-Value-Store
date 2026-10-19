#!/usr/bin/env python3
"""Measure client-observed latency; report failures separately, never fabricate gains."""
import argparse, concurrent.futures, json, socket, time, math
p=argparse.ArgumentParser();p.add_argument('--port',type=int,required=True);p.add_argument('--requests',type=int,default=1000);p.add_argument('--clients',type=int,default=1);p.add_argument('--value-bytes',type=int,default=128);a=p.parse_args()
assert a.requests>0 and a.clients>0 and 0<=a.value_bytes<=16384

def run(i):
    start=time.perf_counter_ns()
    try:
        with socket.create_connection(('127.0.0.1',a.port),timeout=5) as s:
            s.settimeout(5)
            s.sendall((json.dumps({'type':'put','key':f'bench-{i%100}','value':'x'*a.value_bytes})+'\n').encode())
            ok=json.loads(s.makefile('rb').readline()).get('ok',False)
    except (OSError,ValueError):ok=False
    return (time.perf_counter_ns()-start)/1e6,ok
start=time.perf_counter()
with concurrent.futures.ThreadPoolExecutor(max_workers=a.clients) as pool:results=list(pool.map(run,range(a.requests)))
elapsed=time.perf_counter()-start
lat=sorted(ms for ms,ok in results if ok)
def pct(q):return lat[max(0,math.ceil(q*len(lat))-1)] if lat else None
print(json.dumps({'requests':a.requests,'clients':a.clients,'value_bytes':a.value_bytes,'successes':len(lat),'failures':a.requests-len(lat),'elapsed_seconds':elapsed,'successful_ops_per_second':len(lat)/elapsed,'p50_ms':pct(.50),'p95_ms':pct(.95),'p99_ms':pct(.99),'max_ms':lat[-1] if lat else None},indent=2))
