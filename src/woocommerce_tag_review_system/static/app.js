async function getJson(url) {
  const res = await fetch(url);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || 'Request failed');
  return data;
}
async function postSimple(url) {
  const res = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' } });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || data.message || 'Request failed');
  return data;
}
function fmtDuration(seconds) {
  seconds = Math.max(0, Math.floor(seconds || 0));
  const h = Math.floor(seconds / 3600), m = Math.floor((seconds % 3600) / 60), s = seconds % 60;
  if (h) return `${h}h ${m}m ${s}s`;
  if (m) return `${m}m ${s}s`;
  return `${s}s`;
}
function elapsed(startedAt, endedAt) {
  if (!startedAt) return 0;
  const start = Date.parse(startedAt), end = endedAt ? Date.parse(endedAt) : Date.now();
  if (Number.isNaN(start) || Number.isNaN(end)) return 0;
  return Math.max(0, Math.floor((end - start) / 1000));
}
function refreshDurationCells(root=document) {
  root.querySelectorAll('[data-started-at]').forEach(el => {
    const startedAt = el.dataset.startedAt;
    if (!startedAt) return;
    el.textContent = fmtDuration(elapsed(startedAt, el.dataset.endedAt || ''));
  });
}

async function renderRun(runId) {
  const run = await getJson(`/api/runs/${runId}`);
  document.querySelector('h1').textContent = `Run ${run.run_id}`;
  document.getElementById('run-total-tags').textContent = run.total_tags;
  document.getElementById('run-completed-tags').textContent = run.completed_tags;
  document.getElementById('run-failed-tags').textContent = run.failed_tags;
  document.getElementById('run-actionable-pages').textContent = run.actionable_pages_count;
  document.getElementById('run-browser-state').textContent = run.browser_state;
  const dur = document.getElementById('run-duration');
  dur.dataset.startedAt = run.started_at || '';
  dur.dataset.endedAt = run.ended_at || '';
  dur.textContent = fmtDuration(run.duration_seconds || 0);
  const logs = document.getElementById('run-logs');
  logs.innerHTML = (run.logs || []).map(log => `<div class="log-line">[${log.timestamp}] ${log.level} — ${log.message}</div>`).join('') || '<div class="log-line">No logs yet.</div>';
  const body = document.getElementById('run-tags-body');
  body.innerHTML = (run.tags || []).map(tag => `
    <tr>
      <td><a href="/runs/${run.run_id}/tags/${tag.tag_id}">${tag.tag_title}</a></td>
      <td><span class="badge">${tag.status}</span></td>
      <td data-started-at="${tag.started_at || ''}" data-ended-at="${tag.ended_at || ''}">${fmtDuration(tag.duration_seconds || 0)}</td>
      <td><div class="page-links">${(tag.pages || []).map(page => `<a class="page-pill" href="/runs/${run.run_id}/tags/${tag.tag_id}/pages/${page.page_number}">${page.page_number}</a>`).join('') || '<span class="muted">—</span>'}</div></td>
      <td><div class="page-links">${(tag.open_page_tabs || []).map(p => `<a class="page-pill" href="/runs/${run.run_id}/tags/${tag.tag_id}/pages/${p}">${p}</a>`).join('') || '<span class="muted">—</span>'}</div></td>
      <td>${tag.keep_count || 0}</td><td>${tag.mark_count || 0}</td><td>${tag.review_count || 0}</td><td>${tag.actionable_count || 0}</td><td>${tag.review_only_count || 0}</td><td>${tag.clean_count || 0}</td><td>${tag.error_summary || '—'}</td>
      <td>${!['queued','running'].includes(tag.status) ? `<button class="secondary rerun-tag-btn" data-run-id="${run.run_id}" data-tag-id="${tag.tag_id}">Rerun</button>` : '<span class="muted">—</span>'}</td>
    </tr>`).join('') || '<tr><td colspan="13">No tag results yet.</td></tr>';
  attachRunActions();
  refreshDurationCells(body);
}
function attachRunActions() {
  document.querySelectorAll('.rerun-tag-btn').forEach(btn => {
    btn.onclick = async () => {
      try {
        const data = await postSimple(`/api/runs/${btn.dataset.runId}/tags/${btn.dataset.tagId}/rerun`);
        window.location.href = data.redirect_url;
      } catch (err) { alert(err.message); }
    };
  });
}
const rerunFailedRunBtn = document.getElementById('rerun-failed-run-btn');
if (rerunFailedRunBtn) rerunFailedRunBtn.onclick = async () => { try { const data = await postSimple(`/api/runs/${rerunFailedRunBtn.dataset.runId}/rerun-failed`); window.location.href = data.redirect_url; } catch (err) { alert(err.message); } };
const focusBtn = document.getElementById('focus-page-btn');
if (focusBtn) focusBtn.onclick = async () => { const msg = document.getElementById('focus-message'); try { const data = await postSimple(`/api/runs/${focusBtn.dataset.runId}/tags/${focusBtn.dataset.tagId}/pages/${focusBtn.dataset.pageNumber}/focus`); msg.textContent = data.message || 'Live tab focused.'; msg.classList.remove('hidden'); } catch (err) { msg.textContent = err.message; msg.classList.remove('hidden'); } };
refreshDurationCells();
setInterval(() => refreshDurationCells(), 1000);
const pageType = document.body.dataset.page;
if (pageType === 'run-detail') {
  const runId = document.body.dataset.runId;
  renderRun(runId).catch(() => {});
  setInterval(() => renderRun(runId).catch(() => {}), 3000);
}
