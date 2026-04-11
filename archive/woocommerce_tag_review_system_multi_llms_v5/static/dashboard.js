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
    body: JSON.stringify(payload || {})
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || data.message || 'Request failed');
  return data;
}
function fmtDuration(seconds) {
  seconds = Math.max(0, Math.floor(seconds || 0));
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = seconds % 60;
  if (h) return `${h}h ${m}m ${s}s`;
  if (m) return `${m}m ${s}s`;
  return `${s}s`;
}
function elapsed(startedAt, endedAt) {
  if (!startedAt) return 0;
  const start = Date.parse(startedAt);
  const end = endedAt ? Date.parse(endedAt) : Date.now();
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
function providerKeys() {
  return Array.from(document.querySelectorAll('.provider-card')).map(card => card.dataset.provider);
}
function currentProviderOrder() {
  return Array.from(document.querySelectorAll('.priority-item')).map(el => el.dataset.provider);
}
function selectedModelValue(key) {
  const select = document.querySelector(`.provider-model-select[data-provider="${key}"]`);
  const custom = document.querySelector(`.provider-model-custom[data-provider="${key}"]`);
  if (!select) return custom?.value || '';
  if (select.value === '__custom__') return custom?.value || '';
  return select.value || custom?.value || '';
}
function syncCustomVisibility() {
  document.querySelectorAll('.provider-model-select').forEach(select => {
    const key = select.dataset.provider;
    const wrap = document.querySelector(`.provider-custom-model-wrap[data-provider="${key}"]`);
    if (!wrap) return;
    wrap.classList.toggle('hidden', select.value !== '__custom__');
  });
}
function settingsPayload() {
  const providers = {};
  providerKeys().forEach(key => {
    providers[key] = {
      enabled: document.querySelector(`.provider-enabled[data-provider="${key}"]`)?.checked || false,
      api_key: document.querySelector(`.provider-api-key[data-provider="${key}"]`)?.value || '',
      model: selectedModelValue(key),
    };
    const base = document.querySelector(`.provider-base-url[data-provider="${key}"]`); if (base) providers[key].base_url = base.value || '';
    const version = document.querySelector(`.provider-api-version[data-provider="${key}"]`); if (version) providers[key].api_version = version.value || '';
    const org = document.querySelector(`.provider-organization[data-provider="${key}"]`); if (org) providers[key].organization = org.value || '';
  });
  return {
    site: {
      login_url: document.getElementById('site-login-url')?.value || '',
      base_url: document.getElementById('site-base-url')?.value || '',
    },
    credentials: {
      username: document.getElementById('credentials-username')?.value || '',
      password: document.getElementById('credentials-password')?.value || '',
    },
    classifier: {
      strictness: document.getElementById('strictness')?.value || 'balanced',
    },
    runtime: {
      preserve_all_pages_open: document.getElementById('preserve-all-pages-open')?.checked || false,
    },
    woo_api: {
      enabled: document.getElementById('woo-api-enabled')?.checked || false,
      consumer_key: document.getElementById('woo-api-consumer-key')?.value || '',
      consumer_secret: document.getElementById('woo-api-consumer-secret')?.value || '',
      base_api_url: document.getElementById('woo-api-base-url')?.value || '',
      per_page: Number(document.getElementById('woo-api-per-page')?.value || 100),
      request_timeout_seconds: Number(document.getElementById('woo-api-timeout')?.value || 30),
    },
    llm: {
      enabled: document.getElementById('llm-enabled')?.checked || false,
      routing: {
        timeout_seconds: Number(document.getElementById('llm-timeout-seconds')?.value || 8),
        provider_order: currentProviderOrder(),
      },
      providers,
    }
  };
}
function attachSecretToggles() {
  document.querySelectorAll('.secret-toggle').forEach(btn => {
    btn.addEventListener('click', () => {
      const targetId = btn.dataset.target;
      const provider = btn.dataset.provider;
      const input = targetId ? document.getElementById(targetId) : document.querySelector(`.provider-api-key[data-provider="${provider}"]`);
      if (!input) return;
      input.type = input.type === 'password' ? 'text' : 'password';
      btn.textContent = input.type === 'password' ? 'Show' : 'Hide';
    });
  });
}
function attachPriorityControls() {
  document.querySelectorAll('.priority-up').forEach(btn => btn.onclick = () => {
    const item = btn.closest('.priority-item');
    if (item?.previousElementSibling) item.parentNode.insertBefore(item, item.previousElementSibling);
  });
  document.querySelectorAll('.priority-down').forEach(btn => btn.onclick = () => {
    const item = btn.closest('.priority-item');
    if (item?.nextElementSibling) item.parentNode.insertBefore(item.nextElementSibling, item);
  });
}
async function refreshRecentRuns() {
  const tbody = document.getElementById('recent-runs-body');
  if (!tbody) return;
  const runs = await getJson('/api/runs');
  if (!runs.length) {
    tbody.innerHTML = '<tr><td colspan="7">No runs yet.</td></tr>';
    return;
  }
  tbody.innerHTML = runs.map(run => `
    <tr>
      <td><code>${run.run_id}</code></td>
      <td><span class="badge">${run.status}</span></td>
      <td>${run.total_tags || 0}</td>
      <td>${run.actionable_pages_count || 0}</td>
      <td>${run.started_at || run.created_at || '-'}</td>
      <td data-started-at="${run.started_at || ''}" data-ended-at="${run.ended_at || ''}">${fmtDuration(run.duration_seconds || 0)}</td>
      <td><a href="/runs/${run.run_id}">View run</a></td>
    </tr>`).join('');
  refreshDurationCells(tbody);
}

document.querySelectorAll('.provider-model-select').forEach(select => {
  select.addEventListener('change', syncCustomVisibility);
});
syncCustomVisibility();
attachPriorityControls();

const startBtn = document.getElementById('start-run-btn');
const closeBrowserBtn = document.getElementById('close-browser-btn');
const errorBox = document.getElementById('form-error');
const saveSettingsBtn = document.getElementById('save-settings-btn');
const settingsMessage = document.getElementById('settings-message');

if (saveSettingsBtn) {
  saveSettingsBtn.addEventListener('click', async () => {
    try {
      await postJson('/api/settings', settingsPayload());
      settingsMessage.textContent = 'Settings saved.';
      settingsMessage.classList.remove('hidden');
    } catch (err) {
      settingsMessage.textContent = err.message;
      settingsMessage.classList.remove('hidden');
    }
  });
}
if (startBtn) {
  startBtn.addEventListener('click', async () => {
    errorBox.classList.add('hidden');
    errorBox.textContent = '';
    try {
      await postJson('/api/settings', settingsPayload());
      const payload = {
        strictness: document.getElementById('strictness').value,
        max_pages: Number(document.getElementById('max-pages').value || 10),
        dry_run: document.getElementById('dry-run').checked,
        tags_text: document.getElementById('tags-text').value,
      };
      const data = await postJson('/api/runs/start', payload);
      window.location.href = data.redirect_url;
    } catch (err) {
      errorBox.textContent = err.message;
      errorBox.classList.remove('hidden');
    }
  });
}
if (closeBrowserBtn) {
  closeBrowserBtn.addEventListener('click', async () => {
    try {
      await postJson('/api/browser/close', {});
      alert('Browser closed.');
    } catch (err) {
      alert(err.message);
    }
  });
}
attachSecretToggles();
refreshDurationCells();
setInterval(() => refreshDurationCells(), 1000);
setInterval(() => refreshRecentRuns().catch(() => {}), 3000);
refreshRecentRuns().catch(() => {});
