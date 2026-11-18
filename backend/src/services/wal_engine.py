"""Quiescent-checkpoint WAL teaching model with strict write locks, transactional rename/create, and redo/undo."""
import copy,hashlib,json,re

NAME_RE=r'[A-Za-z0-9_-]{1,50}'
MAX_POINTS=30
MAX_ABS=1000000000

def _name_ok(value):return isinstance(value,str) and re.fullmatch(NAME_RE,value) is not None
def _value_ok(value):return not isinstance(value,bool) and isinstance(value,int) and abs(value)<=MAX_ABS

class _State:
    # Points carry values under stable internal ids; names are a committed namespace over those ids.
    def __init__(self,initial):
        self.points={};self.names={};self.counter=0
        for name,value in initial.items():
            self.counter+=1;pid=f'P{self.counter}';self.points[pid]=value;self.names[name]=pid
        self.active={};self.used=set();self.point_locks={};self.name_locks={}
        self.trace=[];self.writes=[];self.winners=[];self.aborted=[];self.effects=[]
        self.checkpoint_lsn=0;self.durable=self.snapshot()
    def snapshot(self):return {'points':copy.deepcopy(self.points),'names':copy.deepcopy(self.names)}
    def release(self,tx):
        self.point_locks={k:v for k,v in self.point_locks.items() if v!=tx}
        self.name_locks={k:v for k,v in self.name_locks.items() if v!=tx}
        del self.active[tx]

def _resolve(st,tx,name):
    # A transaction keeps the point its first resolution bound, so it can cross a later rename or name reuse.
    ctx=st.active[tx]
    if name in ctx['bindings']:return ctx['bindings'][name]
    pid=st.names.get(name)
    for c in ctx['creates']:
        if c['name']==name:pid=c['pid']
    for r in ctx['renames']:
        if r['new']==name:pid=r['pid']
    if pid is not None:ctx['bindings'][name]=pid
    return pid

def _begin(st,lsn,tx):
    if tx in st.used:raise ValueError(f'LSN {lsn}: 事务标识不得重复')
    st.used.add(tx);st.active[tx]={'writes':[],'bindings':{},'renames':[],'creates':[]}

def _write(st,lsn,tx,key,value):
    ctx=st.active[tx]
    pid=_resolve(st,tx,key)
    if pid is None:raise ValueError(f'LSN {lsn}: 写入名称 {key} 不存在于当前账本')
    holder=st.point_locks.get(pid)
    if holder is not None and holder!=tx:raise ValueError(f'LSN {lsn}: 违反严格写锁，活动事务持有同一采样点')
    st.point_locks[pid]=tx
    row={'lsn':lsn,'tx':tx,'key':key,'pid':pid,'before':st.points[pid],'after':value}
    ctx['writes'].append(row);st.writes.append(row);st.points[pid]=value
    st.trace.append(dict(row,phase='redo'))

def _create(st,lsn,tx,key,value):
    ctx=st.active[tx]
    if not _name_ok(key):raise ValueError(f'LSN {lsn}: 新点名称需为字母数字下划线短横线，不超过50字符')
    if value is None:value=0
    if not _value_ok(value):raise ValueError(f'LSN {lsn}: 数值超限')
    if key in st.names:raise ValueError(f'LSN {lsn}: 名称 {key} 已被占用')
    if any(c['name']==key for c in ctx['creates']) or any(r['new']==key for r in ctx['renames']):
        raise ValueError(f'LSN {lsn}: 名称 {key} 已被本事务占用')
    holder=st.name_locks.get(key)
    if holder is not None and holder!=tx:raise ValueError(f'LSN {lsn}: 名称 {key} 被活动事务占用，等待其提交或中止')
    if len(st.points)>=MAX_POINTS:raise ValueError(f'LSN {lsn}: 采样点总数超过{MAX_POINTS}个')
    st.name_locks[key]=tx;st.counter+=1;pid=f'P{st.counter}'
    st.points[pid]=value;ctx['creates'].append({'lsn':lsn,'name':key,'pid':pid})

def _rename(st,lsn,tx,old,new):
    ctx=st.active[tx]
    if not _name_ok(old) or not _name_ok(new):raise ValueError(f'LSN {lsn}: 名称需为字母数字下划线短横线，不超过50字符')
    if old==new:raise ValueError(f'LSN {lsn}: 新旧名称相同')
    pid=_resolve(st,tx,old)
    if pid is None:raise ValueError(f'LSN {lsn}: 改名源名称 {old} 不存在')
    if new in st.names:raise ValueError(f'LSN {lsn}: 名称 {new} 已被占用')
    if any(c['name']==new for c in ctx['creates']) or any(r['new']==new for r in ctx['renames']):
        raise ValueError(f'LSN {lsn}: 名称 {new} 已被本事务占用')
    for n in (old,new):
        holder=st.name_locks.get(n)
        if holder is not None and holder!=tx:raise ValueError(f'LSN {lsn}: 名称 {n} 被活动事务占用，等待其提交或中止')
    st.name_locks[old]=tx;st.name_locks[new]=tx
    ctx['renames'].append({'lsn':lsn,'old':old,'new':new,'pid':pid})

def _commit(st,lsn,tx):
    # Name changes take effect only here: pending creates/renames publish into the committed namespace.
    ctx=st.active[tx]
    for c in ctx['creates']:
        if c['name'] in st.names:raise ValueError(f'LSN {lsn}: 名称 {c["name"]} 已被占用')
        st.names[c['name']]=c['pid'];value=st.points[c['pid']]
        st.effects.append({'op':'create','name':c['name'],'pid':c['pid'],'value':value})
        st.trace.append({'lsn':lsn,'op_lsn':c['lsn'],'tx':tx,'phase':'redo_create','key':c['name'],'pid':c['pid'],'after':value})
    for r in ctx['renames']:
        if r['new'] in st.names:raise ValueError(f'LSN {lsn}: 名称 {r["new"]} 已被占用')
        for n,p in list(st.names.items()):
            if p==r['pid']:del st.names[n]
        st.names[r['new']]=r['pid']
        st.effects.append({'op':'rename','new':r['new'],'pid':r['pid']})
        st.trace.append({'lsn':lsn,'op_lsn':r['lsn'],'tx':tx,'phase':'redo_rename','key':r['old'],'name':r['new'],'pid':r['pid']})
    st.winners.append(tx);st.release(tx)

def _abort(st,lsn,tx):
    # Pending name changes never touched the committed namespace; discard them and undo writes.
    ctx=st.active[tx]
    for row in reversed(ctx['writes']):
        st.points[row['pid']]=row['before'];st.trace.append(dict(row,phase='redo_abort_undo'))
    for c in reversed(ctx['creates']):
        st.trace.append({'lsn':lsn,'op_lsn':c['lsn'],'tx':tx,'phase':'redo_abort_discard','kind':'create','key':c['name'],'pid':c['pid']})
        del st.points[c['pid']]
    for r in reversed(ctx['renames']):
        st.trace.append({'lsn':lsn,'op_lsn':r['lsn'],'tx':tx,'phase':'redo_abort_discard','kind':'rename','key':r['old'],'name':r['new'],'pid':r['pid']})
    st.aborted.append(tx);st.release(tx)

def _execute(st,records):
    for lsn,r in enumerate(records,1):
        op=r.get('op');tx=r.get('tx')
        if op=='checkpoint':
            if st.active:raise ValueError(f'LSN {lsn}: 仅支持无活动事务的静止检查点')
            st.checkpoint_lsn=lsn;st.durable=st.snapshot()
            st.trace=[];st.writes=[];st.winners=[];st.aborted=[];st.effects=[]
            continue
        if not tx:raise ValueError(f'LSN {lsn}: 需要事务标识')
        if op=='begin':_begin(st,lsn,tx)
        elif op=='write':
            if tx not in st.active:raise ValueError(f'LSN {lsn}: 写入事务尚未begin')
            if not _name_ok(r.get('key')):raise ValueError(f'LSN {lsn}: 写入名称需为字母数字下划线短横线')
            if not _value_ok(r.get('value')):raise ValueError(f'LSN {lsn}: 数值超限')
            _write(st,lsn,tx,r['key'],r['value'])
        elif op=='create':
            if tx not in st.active:raise ValueError(f'LSN {lsn}: 开设新点的事务尚未begin')
            _create(st,lsn,tx,r.get('key'),r.get('value'))
        elif op=='rename':
            if tx not in st.active:raise ValueError(f'LSN {lsn}: 改名事务尚未begin')
            _rename(st,lsn,tx,r.get('key'),r.get('name'))
        elif op in ('commit','abort'):
            if tx not in st.active:raise ValueError(f'LSN {lsn}: 事务未开启或已结束')
            _commit(st,lsn,tx) if op=='commit' else _abort(st,lsn,tx)
        else:raise ValueError(f'LSN {lsn}: 未知操作')

def validate(initial,records):
    if not initial or len(initial)>MAX_POINTS or not records or len(records)>200:raise ValueError('需1—30个键及1—200条日志')
    for key,value in initial.items():
        if not _name_ok(key) or not _value_ok(value):raise ValueError('初始键为字母数字下划线短横线，值为绝对值不超过10亿的整数')
    _execute(_State(initial),records)
    return True

def recover(initial,records,crash_after):
    validate(initial,records)
    if isinstance(crash_after,bool) or not isinstance(crash_after,int) or not 0<=crash_after<=len(records):raise ValueError('崩溃点超出日志范围')
    st=_State(initial);_execute(st,records[:crash_after])
    # Simulated crash durable pages are exactly the last quiescent checkpoint.
    # Recovery repeats history after that point, then undoes all unfinished writes and discards pending name changes.
    before_undo=st.snapshot();losers=sorted(st.active)
    undo=sorted([w for tx in losers for w in st.active[tx]['writes']],key=lambda w:w['lsn'],reverse=True)
    for row in undo:
        st.points[row['pid']]=row['before'];st.trace.append(dict(row,phase='undo_loser'))
    discards=sorted([(c['lsn'],tx,'create',c) for tx in losers for c in st.active[tx]['creates']]
                    +[(r['lsn'],tx,'rename',r) for tx in losers for r in st.active[tx]['renames']],key=lambda x:x[0],reverse=True)
    for _,tx,kind,item in discards:
        entry={'lsn':item['lsn'],'tx':tx,'phase':'undo_loser_discard','kind':kind,'pid':item['pid']}
        if kind=='create':entry['key']=item['name'];del st.points[item['pid']]
        else:entry['key']=item['old'];entry['name']=item['new']
        st.trace.append(entry)
    # Independent winner-only oracle from durable checkpoint: committed writes plus committed name changes.
    op=copy.deepcopy(st.durable['points']);on=copy.deepcopy(st.durable['names'])
    for e in st.effects:
        if e['op']=='create':on[e['name']]=e['pid'];op[e['pid']]=e['value']
        else:
            for n,p in list(on.items()):
                if p==e['pid']:del on[n]
            on[e['new']]=e['pid']
    for row in st.writes:
        if row['tx'] in st.winners:op[row['pid']]=row['after']
    def named(points,names):return {n:points[p] for n,p in names.items()}
    pids=sorted(set(st.durable['points'])|set(before_undo['points'])|set(st.points)|set(op),key=lambda p:int(p[1:]))
    points=[{'pid':p,'checkpoint_name':next((n for n,q in st.durable['names'].items() if q==p),None),
             'name':next((n for n,q in st.names.items() if q==p),None),
             'checkpoint':st.durable['points'].get(p),'after_redo':before_undo['points'].get(p),
             'recovered':st.points.get(p),'oracle':op.get(p)} for p in pids]
    recovered=named(st.points,st.names)
    checksum=hashlib.sha256(json.dumps(recovered,sort_keys=True).encode()).hexdigest()
    source=json.dumps({'initial':initial,'records':records,'crash_after':crash_after},sort_keys=True)
    return {'simulation':True,'model_version':'strict-wal-quiescent-v2','crash_after':crash_after,'checkpoint_lsn':st.checkpoint_lsn,
            'durable_checkpoint':named(st.durable['points'],st.durable['names']),'after_redo':named(before_undo['points'],before_undo['names']),
            'recovered':recovered,'oracle':named(op,on),'invariant_passed':st.points==op and st.names==on,
            'committed':st.winners,'aborted':st.aborted,'losers':losers,
            'redo_count':len([t for t in st.trace if t['phase']=='redo']),'undo_count':len(undo),'discard_count':len(discards),
            'names':{'checkpoint':st.durable['names'],'recovered':st.names},'points':points,
            'trace':st.trace,'result_sha256':checksum,'input_sha256':hashlib.sha256(source.encode()).hexdigest(),
            'boundary':'Python WAL教学模型；严格写锁、静止检查点、整数键值、日志前缀持久。改名与开设新点随事务提交生效，未提交的名称变动随撤销一并丢弃。没有注入真实PostgreSQL崩溃，未执行数据库物理恢复。'}
