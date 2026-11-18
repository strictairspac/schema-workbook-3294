import {RUN_STATES,STORAGE_STATES} from '../../constants/enums.js';
import {esc} from '../../utils/html.js';
// 任务执行状态徽章（queued/running/completed/failed）
export const badge=value=>`<span class="badge ${esc(value)}">${esc(RUN_STATES[value]||value)}</span>`;
// 报告正文在线存储状态徽章（pending/online/evicted）
export const storageBadge=value=>`<span class="badge storage ${esc(value)}" title="${esc(STORAGE_STATES[value]||value)}">${esc(STORAGE_STATES[value]||value)}</span>`;
