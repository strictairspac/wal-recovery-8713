import os
from sqlalchemy import select
from src.config.database import Base,engine,Session
from src.models.entities import User,LogSet,CrashCase
from src.utils.security import hash_password
RECORDS=[{'op':'begin','tx':'T1'},{'op':'write','tx':'T1','key':'sample_A','value':75},{'op':'commit','tx':'T1'},{'op':'checkpoint'},{'op':'begin','tx':'T2'},{'op':'write','tx':'T2','key':'sample_B','value':40},{'op':'begin','tx':'T3'},{'op':'write','tx':'T3','key':'sample_A','value':60},{'op':'commit','tx':'T3'},{'op':'write','tx':'T2','key':'sample_B','value':35},{'op':'commit','tx':'T2'}]
# T2在旧名sample_A上写过后跨过改名续写仍归原点；T5写复用名落到新点；T6未提交的改名/开设/写入被断电撤净。
RENAME_RECORDS=[{'op':'begin','tx':'T1'},{'op':'write','tx':'T1','key':'sample_A','value':75},{'op':'commit','tx':'T1'},{'op':'checkpoint'},
 {'op':'begin','tx':'T2'},{'op':'write','tx':'T2','key':'sample_A','value':80},
 {'op':'begin','tx':'T3'},{'op':'rename','tx':'T3','key':'sample_A','name':'sample_A_old'},{'op':'commit','tx':'T3'},
 {'op':'begin','tx':'T4'},{'op':'create','tx':'T4','key':'sample_A','value':500},{'op':'commit','tx':'T4'},
 {'op':'write','tx':'T2','key':'sample_A','value':90},{'op':'commit','tx':'T2'},
 {'op':'begin','tx':'T5'},{'op':'write','tx':'T5','key':'sample_A','value':600},{'op':'commit','tx':'T5'},
 {'op':'begin','tx':'T6'},{'op':'rename','tx':'T6','key':'sample_B','name':'sample_B_old'},{'op':'write','tx':'T6','key':'sample_B','value':55},{'op':'create','tx':'T6','key':'sample_C','value':10}]
def seed():
    Base.metadata.create_all(engine)
    if os.getenv('SEED_DEMO','true').lower()!='true':return
    with Session() as db:
        for role in ('admin','operator','viewer'):
            email=role+'@haili.local'
            if not db.scalar(select(User).where(User.email==email)):db.add(User(email=email,role=role,password_hash=hash_password('Recovery2026!')))
        logs=db.scalar(select(LogSet).where(LogSet.name=='海砾采样账本-晚班'))
        if logs is None:
            logs=LogSet(name='海砾采样账本-晚班',description='A初始100份、B初始50份，T1提交后检查点，T2未提交，T3已提交',initial={'sample_A':100,'sample_B':50},records=RECORDS);db.add(logs);db.flush()
        for name,point in [('断电点10-撤销T2',10),('完整提交点11',11),('初始点0',0)]:
            if not db.scalar(select(CrashCase).where(CrashCase.name==name)):db.add(CrashCase(name=name,log_set_id=logs.id,crash_after=point))
        renamed=db.scalar(select(LogSet).where(LogSet.name=='海砾采样点改名复用-演练'))
        if renamed is None:
            renamed=LogSet(name='海砾采样点改名复用-演练',description='检查点后改名sample_A→sample_A_old并开设同名新点；T2跨改名续写旧名归原点，T5写复用名落新点，T6未提交名称变动待撤',initial={'sample_A':100,'sample_B':50},records=RENAME_RECORDS);db.add(renamed);db.flush()
        for name,point in [('改名复用-断电点21',21),('改名复用-断电点14',14),('改名复用-断电点9',9)]:
            if not db.scalar(select(CrashCase).where(CrashCase.name==name)):db.add(CrashCase(name=name,log_set_id=renamed.id,crash_after=point))
        db.commit()
if __name__=='__main__':seed()
