import {table} from '../components/common/table.js';
import {badge} from '../components/common/status.js';
import {esc} from '../utils/html.js';
import {download} from '../api/client.js';
import {message} from '../hooks/action.js';
const PHASES={redo:'重做写入',redo_abort_undo:'回滚中止写入',undo_loser:'撤销未提交写入',redo_create:'提交开设新点',redo_rename:'提交改名',redo_abort_discard:'中止丢弃名称变动',undo_loser_discard:'断电丢弃名称变动'};
const cell=(m,k)=>m&&k in m?m[k]:'—';
function change(t){
 if(t.phase==='redo_create')return `新点 ${esc(t.key)} = ${t.after}`;
 if(t.phase==='redo_rename')return `${esc(t.key)} → ${esc(t.name)}`;
 if(t.phase==='redo_abort_discard'||t.phase==='undo_loser_discard')return t.kind==='create'?`撤销开设 ${esc(t.key)}`:`撤销改名 ${esc(t.key)} → ${esc(t.name)}`;
 return t.phase==='undo_loser'||t.phase==='redo_abort_undo'?`${t.after} → ${t.before}`:`${t.before} → ${t.after}`;
}
export function detailPage(job){if(!job)return '<section class="empty"><h2>还没有恢复记录</h2><p>先选择故障场景并提交恢复任务，再逐条查看日志重做与撤销。</p><a href="#runs">进入恢复队列 →</a></section>';const r=job.result;
 const names=r?[...new Set(['durable_checkpoint','after_redo','recovered','oracle'].flatMap(f=>Object.keys(r[f]||{})))]:[];
 return `<div class="section-title"><h2>${esc(job.label)} <small>#${job.id}</small></h2>${badge(job.status)}</div>${r?`<section class="verdict"><div><p>恢复账本核对</p><h2>${r.invariant_passed?'提交结果一致':'恢复失败'}</h2></div><p>检查点LSN ${r.checkpoint_lsn??'—'} · 崩溃点 ${r.crash_after??'—'} · 模型 ${esc(r.model_version||'strict-wal-quiescent-v1')}<br>重做 ${r.redo_count??0} 次 / 撤销 ${r.undo_count??0} 次${r.discard_count?` / 丢弃名称变动 ${r.discard_count} 次`:''}<br>${r.error?esc(r.error):'未提交事务 '+esc((r.losers||[]).join(', ')||'无')}</p></section>${r.points?`<h2>采样点核对（内部标识区分跨检查点的同名点）</h2>${table(['点标识','检查点名称','当前名称','检查点值','重做后','恢复后','独立提交结果'],r.points.map(p=>[esc(p.pid),esc(p.checkpoint_name??'—'),esc(p.name??'—'),p.checkpoint??'—',p.after_redo??'—',p.recovered??'—',p.oracle??'—']))}`:''}${table(['账本键','检查点值','重做后','恢复后','独立提交结果'],names.map(k=>[esc(k),cell(r.durable_checkpoint,k),cell(r.after_redo,k),cell(r.recovered,k),cell(r.oracle,k)]))}<h2>日志恢复轨迹</h2>${table(['LSN','事务','阶段','键/名称','变化'],(r.trace||[]).map(t=>[t.lsn,esc(t.tx),esc(PHASES[t.phase]||t.phase),esc(t.key??'—'),change(t)]))}<p class="checksum">输入摘要 ${esc(r.input_sha256||'无')}<br>结果摘要 ${esc(r.result_sha256||'无')}</p>`:'<p class="notice">后台恢复计算中，请刷新状态。</p>'}<div class="toolbar"><button id="export">导出日志与恢复证据</button><button class="secondary" id="refresh">刷新状态</button></div><details><summary>查看冻结输入</summary><pre>${esc(JSON.stringify(job.snapshot,null,2))}</pre></details>`;}
export function bindDetail(job,render){document.querySelector('#refresh')?.addEventListener('click',render);document.querySelector('#export')?.addEventListener('click',()=>download(job.id).catch(e=>message(e.message,true)));}
