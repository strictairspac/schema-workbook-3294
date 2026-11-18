import {esc} from '../../utils/html.js';
// 人类可读字节数
export function formatBytes(n){
  if(n==null)return '—';
  if(n<1024)return n+' B';
  if(n<1024*1024)return (n/1024).toFixed(1)+' KB';
  return (n/1024/1024).toFixed(2)+' MB';
}
// 在线存储总容量与并发发布额度面板
export function storagePanel(ov){
  if(!ov)return '';
  const capPct=Math.min(100,ov.used_percent);
  const slotPct=ov.publish_slots?Math.round(100*ov.used_slots/ov.publish_slots):0;
  const capCls=capPct>=90?'meter danger':capPct>=70?'meter warn':'meter';
  return `<div class="storage-panel">
    <div class="section-top"><h2>在线存储与发布额度</h2><span>QUOTA / EVICTION</span></div>
    <div class="quota-row">
      <div class="quota-label"><span>正文总容量</span><strong>${formatBytes(ov.used_bytes)} / ${formatBytes(ov.limit_bytes)}</strong></div>
      <div class="${capCls}"><i style="width:${capPct}%"></i></div>
      <small>可用 ${formatBytes(ov.free_bytes)} · 已用 ${capPct}%</small>
    </div>
    <div class="quota-row">
      <div class="quota-label"><span>并发发布额度</span><strong>${ov.used_slots} / ${ov.publish_slots} 个在途</strong></div>
      <div class="meter slots"><i style="width:${slotPct}%"></i></div>
      <small>空闲额度 ${ov.free_slots} 个</small>
    </div>
    <div class="state-legend">
      <span><b class="dot pending"></b>待发布 ${ov.reports_by_state?.pending??0}</span>
      <span><b class="dot online"></b>可下载 ${ov.reports_by_state?.online??0}</span>
      <span><b class="dot evicted"></b>已清退 ${ov.reports_by_state?.evicted??0}</span>
    </div>
    <p class="help">空间不足时优先清退最早的旧报告；正在下载的报告不动，每个草案至少保留一份可下载。腾不出空间时会在任务上说明原因。</p>
  </div>`;
}
