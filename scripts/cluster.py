#!/usr/bin/env python3
"""Run a local three-process cluster until Ctrl-C; retain data across runs."""
import argparse, json, pathlib, signal, subprocess, time
p=argparse.ArgumentParser();p.add_argument('--binary',default='build/raftkv');p.add_argument('--dir',default='.cluster');p.add_argument('--base-port',type=int,default=7100);a=p.parse_args()
root=pathlib.Path(a.dir).resolve();root.mkdir(parents=True,exist_ok=True)
children=[]
try:
    for i in range(3):
        cfg=root/f'node{i}.json'
        cfg.write_text(json.dumps({'id':i,'port':a.base_port+i,'data_dir':str(root/f'db{i}'),'peers':[{'id':j,'port':a.base_port+j} for j in range(3) if j!=i]},indent=2))
        children.append(subprocess.Popen([a.binary,'serve',str(cfg)]))
    print('Cluster started. Use client status on ports',*[a.base_port+i for i in range(3)],flush=True)
    while all(c.poll() is None for c in children):time.sleep(.2)
    raise SystemExit('A node exited; stopping cluster.')
except KeyboardInterrupt:pass
finally:
    for c in children:
        if c.poll() is None:c.terminate()
    for c in children:c.wait()
