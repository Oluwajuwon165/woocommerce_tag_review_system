from pathlib import Path
from flask import Flask, jsonify, render_template, request, send_file, url_for

from bot import AppServices
from utils.time_utils import elapsed_seconds, format_duration

PROJECT_ROOT = Path(__file__).resolve().parent
services = AppServices(PROJECT_ROOT)
app = Flask(__name__)


def enrich_timing(data):
    if isinstance(data, dict):
        if data.get('started_at'):
            data['duration_seconds'] = int(data.get('duration_seconds') or elapsed_seconds(data.get('started_at'), data.get('ended_at')))
            data['duration_human'] = format_duration(data['duration_seconds'])
        for key in ('tags', 'pages'):
            if isinstance(data.get(key), list):
                data[key] = [enrich_timing(dict(item)) for item in data[key]]
    return data


def _get_run_or_404(run_id: str):
    run = services.persistence.load_run(run_id)
    return enrich_timing(run) if run else None


def _get_tag_or_404(run_id: str, tag_id: str):
    tag = services.persistence.load_tag(run_id, tag_id)
    return enrich_timing(tag) if tag else None


@app.get('/')
def index():
    runs = [enrich_timing(dict(r)) for r in services.persistence.list_runs()]
    settings = services.settings_payload()
    return render_template('index.html', recent_runs=runs, default_strictness=settings['classifier'].get('strictness', 'balanced'), settings=settings, provider_catalog=services.classifier.get_provider_catalog())


@app.get('/runs/<run_id>')
def run_detail(run_id: str):
    run = _get_run_or_404(run_id)
    if not run:
        return 'Run not found', 404
    return render_template('run_detail.html', run=run)


@app.get('/runs/<run_id>/tags/<tag_id>')
def tag_detail(run_id: str, tag_id: str):
    tag = _get_tag_or_404(run_id, tag_id)
    if not tag:
        return 'Tag not found', 404
    return render_template('tag_detail.html', tag=tag, run_id=run_id)


@app.get('/runs/<run_id>/tags/<tag_id>/pages/<int:page_number>')
def page_detail(run_id: str, tag_id: str, page_number: int):
    tag = _get_tag_or_404(run_id, tag_id)
    if not tag:
        return 'Tag not found', 404
    page = next((p for p in tag.get('pages', []) if int(p.get('page_number', 0)) == page_number), None)
    if not page:
        return 'Page not found', 404
    return render_template('page_detail.html', run_id=run_id, tag=tag, page=page)


@app.get('/api/runs')
def api_runs():
    return jsonify([enrich_timing(dict(r)) for r in services.persistence.list_runs()])


@app.get('/extract-tags')
def extract_tags_page():
    settings = services.settings_payload()
    extractions = [enrich_timing(dict(x)) for x in services.tag_extractor.list_extractions()]
    return render_template('extract_tags.html', settings=settings, extractions=extractions)


@app.get('/api/extractions')
def api_extractions():
    return jsonify([enrich_timing(dict(x)) for x in services.tag_extractor.list_extractions()])


@app.post('/api/extractions/start')
def api_extractions_start():
    payload = request.get_json(force=True, silent=True) or {}
    category_url = str(payload.get('category_url') or '').strip()
    login_url = str(payload.get('login_url') or '').strip()
    base_site_url = str(payload.get('base_site_url') or '').strip()
    username = str(payload.get('username') or '').strip()
    password = str(payload.get('password') or '')
    use_api = bool(payload.get('use_api'))
    if not category_url:
        return jsonify({'error': 'Tag category URL is required.'}), 400
    if not base_site_url:
        return jsonify({'error': 'Base site URL is required.'}), 400
    if use_api:
        if not str(payload.get('api_consumer_key') or '').strip():
            return jsonify({'error': 'Consumer key is required when WooCommerce API mode is enabled.'}), 400
        if not str(payload.get('api_consumer_secret') or '').strip():
            return jsonify({'error': 'Consumer secret is required when WooCommerce API mode is enabled.'}), 400
    else:
        if not login_url:
            return jsonify({'error': 'WordPress login URL is required for browser extraction mode.'}), 400
        if not username:
            return jsonify({'error': 'Username is required for browser extraction mode.'}), 400
        if not password:
            return jsonify({'error': 'Password is required for browser extraction mode.'}), 400
    extraction = services.tag_extractor.new_extraction(payload)
    services.run_manager.start_extraction(extraction)
    return jsonify({'ok': True, 'extraction_id': extraction['extraction_id']})


@app.get('/api/extractions/<extraction_id>')
def api_extraction_detail(extraction_id: str):
    extraction = services.tag_extractor.load_extraction(extraction_id)
    if not extraction:
        return jsonify({'error': 'Extraction not found'}), 404
    return jsonify(enrich_timing(extraction))


@app.get('/api/extractions/<extraction_id>/download')
def api_extraction_download(extraction_id: str):
    extraction = services.tag_extractor.load_extraction(extraction_id)
    if not extraction:
        return 'Extraction not found', 404
    file_path = extraction.get('file_path')
    if not file_path or not Path(file_path).exists():
        return 'File not found', 404
    return send_file(file_path, as_attachment=True, download_name=extraction.get('file_name') or Path(file_path).name)


@app.get('/api/runs/<run_id>')
def api_run_detail(run_id: str):
    run = _get_run_or_404(run_id)
    if not run:
        return jsonify({'error': 'Run not found'}), 404
    return jsonify(run)


@app.get('/api/runs/<run_id>/tags/<tag_id>')
def api_tag_detail(run_id: str, tag_id: str):
    tag = _get_tag_or_404(run_id, tag_id)
    if not tag:
        return jsonify({'error': 'Tag not found'}), 404
    return jsonify(tag)


@app.get('/api/settings')
def api_settings():
    return jsonify(services.settings_payload())


@app.post('/api/settings')
def api_settings_save():
    payload = request.get_json(force=True, silent=True) or {}
    settings = services.save_settings(payload)
    return jsonify({'ok': True, 'settings': settings})


@app.post('/api/runs/start')
def api_runs_start():
    payload = request.get_json(force=True, silent=True) or {}
    strictness = payload.get('strictness', services.config['classifier'].get('strictness', 'balanced'))
    max_pages = int(payload.get('max_pages', 10))
    dry_run = bool(payload.get('dry_run', False))
    tag_lines = payload.get('tags_text', '')
    run_mode = payload.get('run_mode', 'full_tag')
    execution_mode = payload.get('execution_mode', 'full_review')
    if strictness not in {'loose', 'balanced', 'strict'}:
        return jsonify({'error': 'strictness is invalid'}), 400
    if max_pages <= 0:
        return jsonify({'error': 'max_pages must be positive'}), 400
    config_errors = services.runtime_validation_errors()
    if config_errors:
        return jsonify({'error': 'Config validation failed: ' + ' | '.join(config_errors)}), 400
    try:
        tags = services.run_manager.parse_tag_lines(tag_lines, strictness, max_pages, dry_run, run_mode, execution_mode)
        run = services.run_manager.start_run(tags)
        return jsonify({'ok': True, 'run_id': run['run_id'], 'redirect_url': url_for('run_detail', run_id=run['run_id'])})
    except Exception as exc:
        return jsonify({'error': str(exc)}), 400


@app.post('/api/browser/close')
def api_close_browser():
    services.run_manager.call_in_worker(services.browser.close_browser)
    services.run_manager.call_in_replay_worker(services.replay_browser.close_browser, wait=True)
    return jsonify({'ok': True, 'browser_state': services.browser.browser_state(), 'replay_browser_state': services.replay_browser.browser_state()})


@app.post('/api/runs/<run_id>/rerun-failed')
def api_rerun_failed(run_id: str):
    run = _get_run_or_404(run_id)
    if not run:
        return jsonify({'error': 'Run not found'}), 404
    try:
        new_run = services.run_manager.rerun_failed_tags(run)
        return jsonify({'ok': True, 'run_id': new_run['run_id'], 'redirect_url': url_for('run_detail', run_id=new_run['run_id'])})
    except Exception as exc:
        return jsonify({'error': str(exc)}), 400


@app.post('/api/runs/<run_id>/tags/<tag_id>/rerun')
def api_rerun_single_tag(run_id: str, tag_id: str):
    tag = _get_tag_or_404(run_id, tag_id)
    if not tag:
        return jsonify({'error': 'Tag not found'}), 404
    try:
        new_run = services.run_manager.rerun_single_tag(tag)
        return jsonify({'ok': True, 'run_id': new_run['run_id'], 'redirect_url': url_for('run_detail', run_id=new_run['run_id'])})
    except Exception as exc:
        return jsonify({'error': str(exc)}), 400



@app.post('/api/runs/<run_id>/tags/<tag_id>/pages/<int:page_number>/replay-selected')
def api_replay_selected_page(run_id: str, tag_id: str, page_number: int):
    tag = _get_tag_or_404(run_id, tag_id)
    if not tag:
        return jsonify({'error': 'Tag not found'}), 404
    page = next((p for p in tag.get('pages', []) if int(p.get('page_number', 0)) == page_number), None)
    if not page:
        return jsonify({'error': 'Page not found'}), 404
    payload = request.get_json(force=True, silent=True) or {}
    selected_products = payload.get('selected_products') or []
    try:
        result = services.run_manager.call_in_replay_worker(services.replay_page_manager.replay_selected_assessments, run_id, tag, page, selected_products)
        return jsonify(result)
    except Exception as exc:
        return jsonify({'error': str(exc)}), 400


@app.post('/api/runs/<run_id>/tags/<tag_id>/pages/<int:page_number>/focus')
def api_focus_page(run_id: str, tag_id: str, page_number: int):
    tag = _get_tag_or_404(run_id, tag_id)
    if not tag:
        return jsonify({'error': 'Tag not found'}), 404
    page = next((p for p in tag.get('pages', []) if int(p.get('page_number', 0)) == page_number), None)
    if not page:
        return jsonify({'error': 'Page not found'}), 404
    token = page.get('tab_focus_token')
    ok = services.run_manager.call_in_worker(services.browser.focus_tab, token) if token else False
    if not ok:
        return jsonify({'ok': False, 'message': 'Live tab is no longer available in this browser session.'}), 400
    return jsonify({'ok': True, 'message': 'Live tab focused.'})


if __name__ == '__main__':
    app.run(debug=True, port=5000, threaded=True)
