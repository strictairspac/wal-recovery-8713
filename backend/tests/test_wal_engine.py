"""Engine drills for the WAL teaching model: classic logs, transactional rename/create, crash recovery."""
import sys,pathlib
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parent.parent))
from src.services.wal_engine import validate,recover

INITIAL={'sample_A':100,'sample_B':50}
CLASSIC=[{'op':'begin','tx':'T1'},{'op':'write','tx':'T1','key':'sample_A','value':75},{'op':'commit','tx':'T1'},{'op':'checkpoint'},
 {'op':'begin','tx':'T2'},{'op':'write','tx':'T2','key':'sample_B','value':40},{'op':'begin','tx':'T3'},
 {'op':'write','tx':'T3','key':'sample_A','value':60},{'op':'commit','tx':'T3'},
 {'op':'write','tx':'T2','key':'sample_B','value':35},{'op':'commit','tx':'T2'}]
RENAME=[{'op':'begin','tx':'T1'},{'op':'write','tx':'T1','key':'sample_A','value':75},{'op':'commit','tx':'T1'},{'op':'checkpoint'},
 {'op':'begin','tx':'T2'},{'op':'write','tx':'T2','key':'sample_A','value':80},
 {'op':'begin','tx':'T3'},{'op':'rename','tx':'T3','key':'sample_A','name':'sample_A_old'},{'op':'commit','tx':'T3'},
 {'op':'begin','tx':'T4'},{'op':'create','tx':'T4','key':'sample_A','value':500},{'op':'commit','tx':'T4'},
 {'op':'write','tx':'T2','key':'sample_A','value':90},{'op':'commit','tx':'T2'},
 {'op':'begin','tx':'T5'},{'op':'write','tx':'T5','key':'sample_A','value':600},{'op':'commit','tx':'T5'},
 {'op':'begin','tx':'T6'},{'op':'rename','tx':'T6','key':'sample_B','name':'sample_B_old'},
 {'op':'write','tx':'T6','key':'sample_B','value':55},{'op':'create','tx':'T6','key':'sample_C','value':10}]

def expect_error(records,fragment,initial=INITIAL):
    try:validate(initial,records)
    except ValueError as exc:
        assert fragment in str(exc),f'期望错误包含 {fragment!r}，实际 {exc}'
        return
    raise AssertionError(f'期望拒绝：{fragment!r}')

def run(records,crash_after,initial=INITIAL):
    result=recover(initial,records,crash_after)
    assert result['invariant_passed'],f"oracle不一致: {result['recovered']} != {result['oracle']}"
    return result

# 1. 历史日志兼容：经典晚班日志结果与v1一致（A60/B50），模型升级为v2
r=run(CLASSIC,10)
assert r['model_version']=='strict-wal-quiescent-v2'
assert r['recovered']=={'sample_A':60,'sample_B':50}==r['oracle']
assert r['checkpoint_lsn']==4 and r['losers']==['T2'] and r['undo_count']==2
assert r['durable_checkpoint']=={'sample_A':75,'sample_B':50}
assert [p['pid'] for p in r['points']]==['P1','P2']
r=run(CLASSIC,11)
assert r['recovered']=={'sample_A':60,'sample_B':35} and r['losers']==[]
r=run(CLASSIC,0)
assert r['recovered']==INITIAL and r['checkpoint_lsn']==0

# 2. 改名随提交生效；新事务写旧名（已改走）拒绝
expect_error([{'op':'begin','tx':'T1'},{'op':'rename','tx':'T1','key':'sample_A','name':'sample_B'}],'已被占用')
expect_error([{'op':'begin','tx':'T1'},{'op':'rename','tx':'T1','key':'sample_A','name':'sample_A_old'},{'op':'commit','tx':'T1'},
 {'op':'begin','tx':'T2'},{'op':'write','tx':'T2','key':'sample_A','value':1}],'不存在于当前账本')
expect_error([{'op':'begin','tx':'T1'},{'op':'rename','tx':'T1','key':'ghost','name':'sample_C'}],'源名称 ghost 不存在')
expect_error([{'op':'begin','tx':'T1'},{'op':'rename','tx':'T1','key':'sample_A','name':'sample_A'}],'新旧名称相同')

# 3. 开设新点：撞名拒绝、活动事务占名拒绝、提交后可见
expect_error([{'op':'begin','tx':'T1'},{'op':'create','tx':'T1','key':'sample_A','value':1}],'已被占用')
expect_error([{'op':'begin','tx':'T1'},{'op':'rename','tx':'T1','key':'sample_A','name':'sample_C'},
 {'op':'begin','tx':'T2'},{'op':'create','tx':'T2','key':'sample_C','value':1}],'被活动事务占用')
expect_error([{'op':'begin','tx':'T1'},{'op':'rename','tx':'T1','key':'sample_A','name':'sample_C'},
 {'op':'begin','tx':'T2'},{'op':'rename','tx':'T2','key':'sample_A','name':'sample_D'}],'被活动事务占用')
expect_error([{'op':'begin','tx':'T1'},{'op':'rename','tx':'T1','key':'sample_A','name':'sample_C'},
 {'op':'begin','tx':'T2'},{'op':'rename','tx':'T2','key':'sample_B','name':'sample_C'}],'被活动事务占用')
r=run([{'op':'begin','tx':'T1'},{'op':'create','tx':'T1','key':'sample_C'},{'op':'write','tx':'T1','key':'sample_C','value':7},{'op':'commit','tx':'T1'}],4)
assert r['recovered']=={'sample_A':100,'sample_B':50,'sample_C':7}

# 4. 跨改名续写旧名归原点；复用名由新事务写落到新点；不串账
r=run(RENAME,14)
assert r['recovered']=={'sample_A_old':90,'sample_B':50,'sample_A':500}
by_id={p['pid']:p for p in r['points']}
assert by_id['P1']['checkpoint_name']=='sample_A' and by_id['P1']['name']=='sample_A_old'
assert by_id['P1']['checkpoint']==75 and by_id['P1']['recovered']==90
assert by_id['P3']['checkpoint'] is None and by_id['P3']['name']=='sample_A' and by_id['P3']['recovered']==500
assert r['names']['checkpoint']['sample_A']!=r['names']['recovered']['sample_A']

# 5. 断电撤净未提交的改名、开设和写入；跨检查点分清同名两点
r=run(RENAME,21)
assert r['recovered']=={'sample_A_old':90,'sample_B':50,'sample_A':600}==r['oracle']
assert r['losers']==['T6'] and r['undo_count']==1 and r['discard_count']==2
assert 'sample_B_old' not in r['recovered'] and 'sample_C' not in r['recovered']
by_id={p['pid']:p for p in r['points']}
assert by_id['P2']['after_redo']==55 and by_id['P2']['recovered']==50
assert by_id['P4']['after_redo']==10 and by_id['P4']['recovered'] is None
phases=[t['phase'] for t in r['trace']]
assert 'redo_rename' in phases and 'redo_create' in phases
assert phases[-3:]==['undo_loser','undo_loser_discard','undo_loser_discard']
assert r['trace'][-2]['kind']=='create' and r['trace'][-1]['kind']=='rename'
r=run(RENAME,9)
assert r['recovered']=={'sample_A_old':75,'sample_B':50} and r['losers']==['T2']

# 6. 中止同样丢弃名称变动并回滚写入
r=run([{'op':'begin','tx':'T1'},{'op':'rename','tx':'T1','key':'sample_A','name':'sample_A_old'},
 {'op':'create','tx':'T1','key':'sample_C','value':9},{'op':'write','tx':'T1','key':'sample_A','value':1},{'op':'abort','tx':'T1'}],5)
assert r['recovered']==INITIAL and r['aborted']==['T1']
assert [t['phase'] for t in r['trace']].count('redo_abort_discard')==2

# 7. 严格写锁按采样点生效：改名后新名写仍撞原点锁
expect_error([{'op':'begin','tx':'T1'},{'op':'write','tx':'T1','key':'sample_A','value':1},
 {'op':'begin','tx':'T2'},{'op':'rename','tx':'T2','key':'sample_A','name':'sample_A_old'},{'op':'commit','tx':'T2'},
 {'op':'begin','tx':'T3'},{'op':'write','tx':'T3','key':'sample_A_old','value':2}],'严格写锁')

# 8. 检查点仍需静止；名称锁在提交/中止后释放
expect_error([{'op':'begin','tx':'T1'},{'op':'rename','tx':'T1','key':'sample_A','name':'sample_C'},{'op':'checkpoint'}],'静止检查点')
r=run([{'op':'begin','tx':'T1'},{'op':'rename','tx':'T1','key':'sample_A','name':'sample_C'},{'op':'abort','tx':'T1'},
 {'op':'begin','tx':'T2'},{'op':'rename','tx':'T2','key':'sample_A','name':'sample_C'},{'op':'commit','tx':'T2'}],6)
assert r['recovered']=={'sample_C':100,'sample_B':50}

# 9. 同事务链式改名与自见新名写入
r=run([{'op':'begin','tx':'T1'},{'op':'rename','tx':'T1','key':'sample_A','name':'tmp'},{'op':'rename','tx':'T1','key':'tmp','name':'sample_C'},
 {'op':'write','tx':'T1','key':'sample_C','value':8},{'op':'commit','tx':'T1'}],5)
assert r['recovered']=={'sample_C':8,'sample_B':50}

# 10. 既有校验保持：事务复用、未知操作、崩溃点超限、数值超限、点位上限
expect_error([{'op':'begin','tx':'T1'},{'op':'commit','tx':'T1'},{'op':'begin','tx':'T1'}],'不得重复')
expect_error([{'op':'begin','tx':'T1'},{'op':'freeze','tx':'T1'}],'未知操作')
expect_error([{'op':'begin','tx':'T1'},{'op':'write','tx':'T1','key':'sample_A','value':10**10}],'数值超限')
expect_error([{'op':'begin','tx':'T1'}]+[{'op':'create','tx':'T1','key':f'k{i}'} for i in range(30)],'超过30')
try:recover(INITIAL,CLASSIC,12)
except ValueError as exc:assert '崩溃点超出日志范围' in str(exc)
else:raise AssertionError('期望拒绝崩溃点超限')

# 11. 种子日志均可校验（不依赖数据库包，直接解析字面量）
import ast
seed_tree=ast.parse((pathlib.Path(__file__).resolve().parent.parent/'scripts'/'seed_demo.py').read_text())
seed_vars={t.targets[0].id:ast.literal_eval(t.value) for t in seed_tree.body if isinstance(t,ast.Assign) and isinstance(t.targets[0],ast.Name) and t.targets[0].id in ('RECORDS','RENAME_RECORDS')}
assert validate(INITIAL,seed_vars['RECORDS']) and validate(INITIAL,seed_vars['RENAME_RECORDS'])
print('全部引擎演练通过')
