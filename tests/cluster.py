#!/usr/bin/env python3
"""Real processes, TCP, RocksDB, crash recovery and directed partition scenarios."""
import json, os, pathlib, random, socket, subprocess, sys, tempfile, time
BINARY = str(pathlib.Path(sys.argv[1]).resolve())

def call(port, request):
    with socket.create_connection(('127.0.0.1', port), timeout=3) as sock:
        sock.settimeout(4)
        sock.sendall((json.dumps(request)+'\n').encode())
        return json.loads(sock.makefile('rb').readline())

def eventually(fn, seconds=12):
    end = time.monotonic()+seconds
    while time.monotonic()<end:
        try:
            value=fn()
            if value: return value
        except (OSError, ValueError, AssertionError): pass
        time.sleep(.08)
    raise AssertionError('condition did not become true')

with tempfile.TemporaryDirectory(prefix='raftkv-') as tmp:
    root=pathlib.Path(tmp)
    reservations=[]
    for _ in range(3):
        s=socket.socket(); s.bind(('127.0.0.1',0)); reservations.append(s)
    ports=[s.getsockname()[1] for s in reservations]
    for s in reservations: s.close()
    procs={}; logs=[]
    for i in range(3):
        (root/f'{i}.json').write_text(json.dumps({'id':i,'port':ports[i], 'data_dir':str(root/f'data{i}'), 'peers':[{'id':j,'port':ports[j]} for j in range(3) if j!=i]}))
    def start(i):
        f=open(root/f'{i}.log','ab');logs.append(f)
        procs[i]=subprocess.Popen([BINARY,'serve',str(root/f'{i}.json')],stdout=f,stderr=f)
    def stop(i):
        p=procs.pop(i);p.kill();p.wait(timeout=5)
    def status(i):return call(ports[i],{'type':'status'})
    def find_leader(allowed=None):
        for i in list(procs) if allowed is None else allowed:
            if status(i)['role']=='leader':return i+1
        return 0
    def leader(allowed=None):return eventually(lambda:find_leader(allowed))-1
    def op(i,kind,key='key',value=None):
        req={'type':kind,'key':key}
        if value is not None:req['value']=value
        return call(ports[i],req)
    try:
        for i in range(3):start(i)
        l=leader();print('PASS election',flush=True)
        assert op(l,'put',value='initial')['ok']
        assert op(l,'get')['value']=='initial'
        assert op((l+1)%3,'get')['error']=='not_leader'
        assert op(l,'put',key='',value='a\n\x00☃')['ok']
        assert op(l,'get',key='')['value']=='a\n\x00☃'
        # Invalid requests must leave the process alive.
        assert call(ports[l],{'type':'put'})['error']=='invalid_request'
        assert call(ports[l],{'type':'bogus'})['error']=='invalid_request'
        print('PASS CRUD, binary strings, follower rejection, malformed requests',flush=True)
        # Isolate the active leader. Its reads and writes must fail without quorum.
        call(ports[l],{'type':'isolate','enabled':True})
        assert not op(l,'put',key='orphan',value='uncommitted')['ok']
        assert not op(l,'get')['ok']
        others=[i for i in procs if i!=l]; nl=leader(others)
        assert op(nl,'put',value='majority')['ok']
        call(ports[l],{'type':'isolate','enabled':False})
        eventually(lambda:status(l)['role']=='follower')
        eventually(lambda:status(l)['commit']==status(nl)['commit'])
        assert op(nl,'get',key='orphan')['found'] is False
        print('PASS partition safety, majority progress, conflicting suffix repair',flush=True)
        stop(nl); nl2=leader();assert op(nl2,'get')['value']=='majority'
        assert op(nl2,'put',value='after-crash')['ok'];start(nl)
        eventually(lambda:status(nl)['commit']==status(nl2)['commit'])
        print('PASS SIGKILL leader failover and catch-up',flush=True)
        # Kill all nodes, reopen persisted term/vote/log/commit.
        for i in list(procs):stop(i)
        for i in range(3):start(i)
        l=leader();assert op(l,'get')['value']=='after-crash'
        assert op(l,'del')['ok'];assert not op(l,'get')['found']
        print('PASS full cluster durable restart and deletion',flush=True)
        lagging=next(i for i in procs if i!=l);stop(lagging)
        for n in range(40):assert op(l,'put',key=f'batch-{n}',value=str(n))['ok']
        start(lagging)
        eventually(lambda:status(lagging)['commit']==status(l)['commit'])
        assert op(l,'get',key='batch-39')['value']=='39'
        print('PASS follower catch-up across bounded replication batches',flush=True)
        # Quorum loss: no successful reads/writes; later majority can recover.
        for i in list(procs):
            if i!=l:stop(i)
        assert not op(l,'put',value='uncertain')['ok']
        assert not op(l,'get')['ok']
        for i in range(3):
            if i not in procs:start(i)
        l=leader();eventually(lambda:op(l,'put',value='restored').get('ok'))
        assert op(l,'get')['value']=='restored'
        print('PASS quorum loss and recovery',flush=True)
    except BaseException:
        for f in root.glob('*.log'):print(f.name, f.read_text())
        raise
    finally:
        for i in list(procs):stop(i)
        for f in logs:f.close()
