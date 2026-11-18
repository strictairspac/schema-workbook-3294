import {table} from '../components/common/table.js';
import {badge,storageBadge} from '../components/common/badge.js';
import {storagePanel} from '../components/common/storage.js';
import {esc,date} from '../utils/html.js';
import {session} from '../hooks/session.js';

// 任务行：执行状态 + 在线存储状态；失败/待发布可重试；已清退说明原因。
function reportRow(r,relations){
  const relCode=relations.find(x=>x.id===r.relation_id)?.code||'';
  const canRetry=r.storage_state==='pending'&&r.status==='failed'&&session.user.role!=='viewer';
  const action=r.storage_state==='evicted'
    ? '<span class="muted-text">正文已清退</span>'
    : `<a href="#evidence/${r.id}">查看 →</a>`;
  const retryBtn=canRetry?`<button class="small retry-btn" data-retry="${r.id}">恢复发布 ↻</button>`:'';
  return [
    `<strong>${esc(r.name)}</strong><small>${date(r.created_at)}</small>`,
    esc(relCode),
    `${badge(r.status)}${storageBadge(r.storage_state)}`,
    `<span class="size-text">${r.body_size?r.body_size+' B':'—'}</span>${r.storage_note?`<small class="note-text">${esc(r.storage_note)}</small>`:''}`,
    `${action}${retryBtn}`,
  ];
}

export function reportsPage(items,relations,overview){
  const writable=session.user.role!=='viewer';
  return `<div class="split">
    <section>
      ${storagePanel(overview)}
      <div class="section-top"><h2>分解任务记录</h2><span>PYTHON / EXACT ALGORITHM</span></div>
      ${table(['任务 / 时间','草案','执行 / 存储状态','容量占用与说明','报告'],
        items.map(r=>reportRow(r,relations)))}
      <div class="protocol">
        <h3>四步校验协议</h3>
        <ol>
          <li>计算属性闭包，精确枚举所有候选键</li>
          <li>剔除多余属性与冗余依赖，得到最小覆盖</li>
          <li>合成3NF关系，必要时补入候选键关系</li>
          <li>投影依赖验证3NF与依赖保持，追赶法判定无损</li>
        </ol>
      </div>
    </section>
    <section class="form-panel">
      <span class="eyebrow">SYNTHESIZE & VERIFY</span>
      <h2>生成分解报告</h2>
      <form id="report-form">
        <label>任务名称<input name="name" value="shift-20261117" required></label>
        <label>选择业务草案<select name="relation_id">${relations.map(r=>`<option value="${r.id}">${esc(r.code)} · ${esc(r.title)}</option>`).join('')}</select></label>
        <button class="primary" ${writable?'':'disabled'}>执行3NF合成 ↗</button>
      </form>
      <p class="help">入队时冻结关系和依赖快照，并预留并发发布额度与在线存储容量。空间不足会先清退最早的旧报告（正在下载与每草案最后一份除外）；发布失败可恢复原发布，重试不重复扣占用。</p>
    </section>
  </div>`;
}
