async function getJson(url) {
  const res = await fetch(url);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || 'Request failed');
  return data;
}
async function postJson(url, payload) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload || {}),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || 'Request failed');
  return data;
}
function escapeHtml(v) {
  return String(v ?? '').replace(/[&<>"]/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[m]));
}
function payload() {
  return {
    category_url: document.getElementById('extract-category-url').value,
    website_label: document.getElementById('extract-website-label').value,
    login_url: document.getElementById('extract-login-url').value,
    base_site_url: document.getElementById('extract-base-site-url').value,
    username: document.getElementById('extract-username').value,
    password: document.getElementById('extract-password').value,
    use_api: document.getElementById('extract-use-api').checked,
    api_consumer_key: document.getElementById('extract-api-consumer-key').value,
    api_consumer_secret: document.getElementById('extract-api-consumer-secret').value,
    api_base_url: document.getElementById('extract-api-base-url').value,
    api_per_page: Number(document.getElementById('extract-api-per-page').value || 100),
    request_timeout_seconds: Number(document.getElementById('extract-timeout').value || 30),
  };
}
function attachSecretToggles() {
  document.querySelectorAll('.secret-toggle').forEach(btn => {
    btn.addEventListener('click', () => {
      const input = document.getElementById(btn.dataset.target);
      if (!input) return;
      input.type = input.type === 'password' ? 'text' : 'password';
      btn.textContent = input.type === 'password' ? 'Show' : 'Hide';
    });
  });
}
let latestExtractionId = null;
function renderRows(items) {
  const tbody = document.getElementById('extractions-body');
  if (!items.length) {
    tbody.innerHTML = '<tr><td colspan="9">No extractions yet.</td></tr>';
    return;
  }
  tbody.innerHTML = items.map(item => `
    <tr>
      <td><code>${escapeHtml(item.extraction_id)}</code></td>
      <td class="wrap-cell">${escapeHtml(item.website_label || item.base_site_url || '—')}</td>
      <td><span class="badge">${escapeHtml(item.status || '-')}</span></td>
      <td>${escapeHtml(item.mode || '-')}</td>
      <td>${escapeHtml(item.tags_found || 0)}</td>
      <td class="wrap-cell">${escapeHtml(item.progress_message || '-')}</td>
      <td>${escapeHtml(item.created_at || '-')}</td>
      <td>${escapeHtml(item.ended_at || '-')}</td>
      <td>${item.download_path ? `<a href="${escapeHtml(item.download_path)}">Download</a>` : '—'}</td>
    </tr>`).join('');
}
async function refreshExtractions() {
  const items = await getJson('/api/extractions');
  renderRows(items);
  const running = items.find(x => x.status === 'running' || x.status === 'queued');
  const live = document.getElementById('extract-live-status');
  if (running) {
    latestExtractionId = running.extraction_id;
    live.textContent = `${running.website_label || running.base_site_url || 'Extraction'} — ${running.progress_message || running.status}`;
  } else if (items[0]) {
    live.textContent = `${items[0].website_label || items[0].base_site_url || 'Latest extraction'} — ${items[0].progress_message || items[0].status}`;
  } else {
    live.textContent = 'No extraction has been started yet.';
  }
}

document.getElementById('extract-data-btn')?.addEventListener('click', async () => {
  const err = document.getElementById('extract-form-error');
  err.classList.add('hidden');
  err.textContent = '';
  try {
    const res = await postJson('/api/extractions/start', payload());
    latestExtractionId = res.extraction_id;
    document.getElementById('extract-live-status').textContent = 'Extraction queued...';
    await refreshExtractions();
  } catch (e) {
    err.textContent = e.message;
    err.classList.remove('hidden');
  }
});

attachSecretToggles();
refreshExtractions().catch(() => {});
setInterval(() => refreshExtractions().catch(() => {}), 3000);
