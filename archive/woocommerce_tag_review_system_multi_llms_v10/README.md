# WooCommerce Tag-Cleaning Review System

A local, non-destructive WooCommerce review tool that logs into WordPress admin, scans product tag pages, uses an LLM-only judgment layer to classify rows into **keep**, **mark**, or **review**, optionally ticks strong **mark** checkboxes, preserves only actionable tabs, and gives you a Flask dashboard for review and reruns.

## What it does

- Logs into WordPress admin once and reuses the same browser session.
- Processes one or more tag URLs in a single run.
- Handles pagination page-by-page.
- Uses an LLM provider for judgment only; no rule-engine fallback classifier is used.
- Supports Gemini, GitHub Models, or a hybrid chain that tries GitHub Models first and Gemini last.
- Supports a tiered GitHub Models chain: best model → cheaper fallback → fastest fallback.
- Supports per-tier request timeouts so a slow model can be skipped automatically.
- Never clicks final destructive bulk apply actions.
- Optionally ticks row checkboxes for high-confidence **mark** results only.
- Preserves only actionable tabs by default.
- Shows run, tag, page, and failure summaries in a local dashboard.
- Lets you focus the already-open live Playwright tab from the dashboard while the browser session is alive.
- Supports rerunning one failed tag or all failed tags from a run.

## Safety notes

- This tool never deletes products.
- This tool never removes tags automatically.
- This tool never clicks WooCommerce or ACP final bulk apply actions.
- The human operator remains in control of the final destructive step.

## Project layout

```text
woocommerce_tag_review_system/
├── app.py
├── bot.py
├── config.example.json
├── requirements.txt
├── README.md
├── classifier/
├── managers/
├── utils/
├── templates/
├── static/
├── data/
└── runs/
```

## Setup

### 1) Create a virtual environment

#### Windows PowerShell
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

#### macOS / Linux
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 2) Install dependencies
```bash
pip install -r requirements.txt
```

### 3) Install Playwright browsers
```bash
python -m playwright install chromium
```

### 4) Create your config
Copy `config.example.json` to `config.json` and fill in:
- WordPress login URL
- site base URL
- WordPress admin username/password
- LLM API key (Gemini key or GitHub token with `models: read`)
- any custom selectors if your admin UI differs

### 5) Run the app
```bash
python app.py
```
Open `http://127.0.0.1:5000`

## How to start a scan

On the landing page:
1. Set strictness, max pages, and dry run.
2. Paste one or more lines as:
   ```text
   Tag Title | https://example.com/wp-admin/edit.php?post_type=product&product_tag=foo
   Another Tag | https://example.com/wp-admin/edit.php?post_type=product&product_tag=bar
   ```
3. Click **Start run**.

## What preserved page links mean

On the run detail page, the page numbers shown under each tag are only the pages whose tabs remain preserved/open for human review. By default, those are actionable pages where at least one strong mark existed and checkboxing likely happened.

## Page-specific assessment view

Clicking a preserved page number opens a lightweight page assessment screen that renders stored results only. It does not call the LLM again. Reasons are intentionally omitted on that page to keep the screen practical and lighter.

## Go to opened tab

The page view has a **Go to opened tab** button. It works only while the same Playwright browser session is still alive. If the browser was closed or the app restarted, the system cannot recover previously open live tabs.

## Failure summaries

Failures are classified on a best-effort basis into categories such as:
- selector issue
- timeout
- ACP row missing
- login/session issue
- pagination issue
- LLM/API issue
- unexpected error

These appear at page and tag level to make reruns faster.

## Reruns

- **Rerun** beside a failed tag: starts a new run using the same title, URL, strictness, max pages, and dry-run values.
- **Rerun failed tags** on a run page: starts a new run containing only failed tags from that run.

## Known limitations

- Focusing a live tab depends on an active browser session.
- WordPress admin layouts differ; selectors may need adjustment in `config.json`.
- ACP readiness is best-effort and based on configured selectors.
- This tool intentionally does not perform the final destructive action.


## Diagnostics patch included

This build includes row-level diagnostics for each processed page. After a run, open a preserved page detail screen to inspect:
- DOM row count vs assessed row count vs skipped row count
- whether a title selector was found on each row
- whether an LLM was called or cache was used
- parse failures and API failures
- checkbox outcome for each row
- skip reasons and row text preview

The same detail is saved to each page JSON file under `row_diagnostics` and page-level `logs`.


## Diagnostic stability patch

This build snapshots real product rows before any checkboxing begins, then applies checkbox actions in a second phase by stable row ID. This prevents ACP helper-row insertion from shifting the row list mid-scan and causing skipped or duplicated products.


## GitHub Models support

This build can use GitHub Models through the official REST inference endpoint. GitHub documents the non-org endpoint as `POST /inference/chat/completions` at `https://models.github.ai/inference/chat/completions`, and the org-attributed endpoint as `POST /orgs/{org}/inference/chat/completions`. Requests authenticate with a bearer token and the recommended `X-GitHub-Api-Version` header. GitHub also documents free, rate-limited access for all accounts, with higher limits once paid usage is enabled.

### Recommended config switch

Set `llm.provider` to `hybrid` to use GitHub Models tiers first and Gemini as a final fallback. You can still use `github_models` only or `gemini` only if you prefer.

Example strategy already included in `config.example.json`:
- best: `openai/gpt-4.1`
- cheaper: `meta/Llama-4-Maverick-17B-128E-Instruct-FP8`
- fastest: `openai/gpt-4.1-nano`

The system will:
1. try the first model
2. time out after the configured seconds if it hangs
3. fall through to the next model
4. cache successful decisions so repeated prompts do not call the API again

### Notes

- Use a GitHub token that can access GitHub Models. For fine-grained tokens, GitHub documents the `models: read` permission requirement.
- If you set `llm.organization`, requests are sent to the organization-attributed endpoint instead of the personal endpoint.
- Page detail diagnostics now show the provider, model, and tier used for each assessed row.


## Hybrid LLM configuration

Recommended setup:

- `llm.provider`: `hybrid`
- `llm.github_models.api_key`: your GitHub token
- `llm.gemini.api_key`: your Gemini key
- `llm.tiered_strategy.enabled`: `true`
- `llm.tiered_strategy.models`: ordered GitHub Models chain
- `llm.final_fallback.provider`: `gemini`

In hybrid mode the system tries the GitHub Models tiers in order. If they all fail, it falls back to Gemini before returning an API failure. Cached decisions still short-circuit the whole chain on repeated prompts.
