#!/usr/bin/env python3
"""Targeted persistent-vote, log freshness, and matched-prefix commit checks."""
import json, pathlib, socket, subprocess, sys, tempfile, time
binary=str(pathlib.Path(sys.argv[1]).resolve())

def call(port,j):
    with socket.create_connection(('127.0.0.1',port),timeout=3) as s:
        s.sendall((json.dumps(j)+'\n').encode())
        return json.loads(s.makefile('rb').readline())

with tempfile.TemporaryDirectory() as tmp:
    root=pathlib.Path(tmp)
    sockets=[]
    for _ in range(3):
        s=socket.socket();s.bind(('127.0.0.1',0));sockets.append(s)
    ports=[s.getsockname()[1] for s in sockets]
    for s in sockets:s.close()
    cfg=root/'config.json'
    cfg.write_text(json.dumps({'id':0,'port':ports[0],'data_dir':str(root/'data'),'peers':[{'id':i,'port':ports[i]} for i in (1,2)]}))
    proc=None
    def start():
        global proc
        proc=subprocess.Popen([binary,'serve',str(cfg)],stdout=subprocess.DEVNULL)
        end=time.monotonic()+5
        while time.monotonic()<end:
            try:return call(ports[0],{'type':'status'})
            except OSError:time.sleep(.02)
        raise AssertionError('startup timeout')
    def stop():
        proc.kill();proc.wait()
    def vote(term,candidate,last=0,last_term=0):
        return call(ports[0],{'type':'vote','term':term,'id':candidate,'last':last,'last_term':last_term})
    def append(term,prev,prev_term,entries,commit):
        return call(ports[0],{'type':'append','term':term,'id':1,'prev':prev,'prev_term':prev_term,'entries':entries,'commit':commit})
    try:
        start();assert vote(100,1)['yes'];stop();s=start();assert s['term']==100
        assert not vote(100,2)['yes'];assert vote(100,1)['yes']
        assert append(101,0,0,[{'term':101,'op':'put','key':'a','value':'b'}],0)['yes']
        # A heartbeat matching only the sentinel cannot commit an unmatched suffix.
        assert append(101,0,0,[],1)['yes']
        assert call(ports[0],{'type':'status'})['commit']==0
        assert not vote(102,2,100,100)['yes'] # index cannot compensate for older last term
        assert not vote(102,2,0,101)['yes'] # same term requires sufficient index
        assert vote(102,1,1,101)['yes']
        assert not append(101,1,101,[],1)['yes'] # reject stale leader
        assert not append(103,1,99,[],1)['yes'] # reject predecessor term mismatch
        assert append(103,1,101,[],1)['yes']
        stop();s=start();assert s['term']==103 and s['commit']==1 and s['last']==1
        print('PASS durable single vote, log freshness, stale leader, predecessor matching, bounded commit')
    finally:
        if proc is not None and proc.poll() is None:stop()
