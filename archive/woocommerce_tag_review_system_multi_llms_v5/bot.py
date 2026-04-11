from pathlib import Path
from typing import Any, Dict

from classifier.cache import DecisionCache
from classifier.llm_only_classifier import DEFAULT_PROVIDERS, PROVIDER_ORDER, LLMOnlyClassifier
from managers.browser_manager import BrowserManager
from managers.page_manager import PageManager
from managers.persistence_manager import PersistenceManager
from managers.run_manager import RunManager
from managers.woocommerce_api_manager import WooCommerceAPIManager
from managers.tag_manager import TagManager
from utils.json_utils import load_json, save_json
from utils.selectors import REQUIRED_BULK_KEYS, REQUIRED_SELECTOR_KEYS


class AppServices:
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.config_path = project_root / 'config.json'
        self.example_config_path = project_root / 'config.example.json'
        self.config = self.load_config()
        self.persistence = PersistenceManager(project_root)
        self.browser = BrowserManager(self.config)
        self.cache = DecisionCache(project_root / 'data' / 'decision_cache.json')
        self.classifier = LLMOnlyClassifier(self.config, self.cache)
        self.woo_api = WooCommerceAPIManager(self.config)
        self.page_manager = PageManager(self.config, self.classifier, self.browser, self.persistence)
        self.tag_manager = TagManager(self.config, self.browser, self.page_manager, self.persistence, self.woo_api)
        self.run_manager = RunManager(self.config, self.browser, self.tag_manager, self.persistence)

    def load_config(self) -> Dict[str, Any]:
        if not self.config_path.exists():
            config = load_json(self.example_config_path, {})
            save_json(self.config_path, config)
            return config
        return load_json(self.config_path, {})

    def reload_runtime(self) -> None:
        self.config = self.load_config()
        self.browser.config = self.config
        self.classifier.refresh_config(self.config)
        self.woo_api.refresh_config(self.config)
        self.page_manager.config = self.config
        self.tag_manager.config = self.config
        self.run_manager.config = self.config

    def settings_payload(self) -> Dict[str, Any]:
        llm = self.config.setdefault('llm', {})
        woo_api = self.config.setdefault('woo_api', {})
        woo_api.setdefault('enabled', False)
        woo_api.setdefault('consumer_key', '')
        woo_api.setdefault('consumer_secret', '')
        woo_api.setdefault('base_api_url', '')
        woo_api.setdefault('per_page', 100)
        woo_api.setdefault('request_timeout_seconds', 30)
        providers = llm.setdefault('providers', {})
        for key, defaults in DEFAULT_PROVIDERS.items():
            providers.setdefault(key, dict(defaults))
        llm.setdefault('routing', {}).setdefault('provider_order', list(PROVIDER_ORDER))
        return self.config

    def save_settings(self, incoming: Dict[str, Any]) -> Dict[str, Any]:
        config = self.load_config()
        site = config.setdefault('site', {})
        creds = config.setdefault('credentials', {})
        classifier = config.setdefault('classifier', {})
        runtime = config.setdefault('runtime', {})
        llm = config.setdefault('llm', {})
        providers = llm.setdefault('providers', {})
        incoming_site = incoming.get('site') or {}
        incoming_creds = incoming.get('credentials') or {}
        incoming_classifier = incoming.get('classifier') or {}
        incoming_runtime = incoming.get('runtime') or {}
        incoming_llm = incoming.get('llm') or {}
        incoming_woo = incoming.get('woo_api') or {}
        for field in ('login_url', 'base_url'):
            if field in incoming_site:
                site[field] = incoming_site.get(field, '')
        for field in ('username', 'password'):
            if field in incoming_creds:
                creds[field] = incoming_creds.get(field, '')
        for field in ('strictness',):
            if field in incoming_classifier:
                classifier[field] = incoming_classifier[field]
        for field in ('keep_browser_open', 'preserve_review_only_pages', 'preserve_all_pages_open'):
            if field in incoming_runtime:
                runtime[field] = bool(incoming_runtime[field])
        woo_api = config.setdefault('woo_api', {})
        for field in ('enabled',):
            if field in incoming_woo:
                woo_api[field] = bool(incoming_woo[field])
        for field in ('consumer_key', 'consumer_secret', 'base_api_url'):
            if field in incoming_woo:
                woo_api[field] = incoming_woo.get(field, '')
        for field in ('per_page', 'request_timeout_seconds'):
            if field in incoming_woo:
                woo_api[field] = incoming_woo.get(field)

        llm['enabled'] = bool(incoming_llm.get('enabled', True))
        routing = llm.setdefault('routing', {})
        incoming_routing = incoming_llm.get('routing') or {}
        routing['timeout_seconds'] = float(incoming_routing.get('timeout_seconds') or routing.get('timeout_seconds') or 8)
        requested_order = incoming_routing.get('provider_order') or routing.get('provider_order') or list(PROVIDER_ORDER)
        routing['provider_order'] = [key for key in requested_order if key in PROVIDER_ORDER] + [key for key in PROVIDER_ORDER if key not in requested_order]
        for key, defaults in DEFAULT_PROVIDERS.items():
            providers.setdefault(key, dict(defaults))
            updated = incoming_llm.get('providers', {}).get(key) or {}
            for fld in ('enabled', 'api_key', 'base_url', 'model', 'organization', 'api_version'):
                if fld in updated:
                    providers[key][fld] = bool(updated[fld]) if fld == 'enabled' else updated[fld]
        save_json(self.config_path, config)
        self.reload_runtime()
        return self.settings_payload()

    def runtime_validation_errors(self) -> list[str]:
        errors = []
        config = self.config
        if not config.get('site', {}).get('login_url'):
            errors.append('site.login_url is required.')
        if not config.get('site', {}).get('base_url'):
            errors.append('site.base_url is required.')
        if not config.get('credentials', {}).get('username'):
            errors.append('credentials.username is required.')
        if not config.get('credentials', {}).get('password'):
            errors.append('credentials.password is required.')
        strictness = config.get('classifier', {}).get('strictness', 'balanced')
        if strictness not in {'loose', 'balanced', 'strict'}:
            errors.append('classifier.strictness must be loose, balanced, or strict.')
        selectors = config.get('selectors', {})
        bulk = config.get('bulk_edit', {})
        missing_selectors = [key for key in REQUIRED_SELECTOR_KEYS if key not in selectors]
        missing_bulk = [key for key in REQUIRED_BULK_KEYS if key not in bulk]
        if missing_selectors:
            errors.append(f'Missing selectors: {missing_selectors}')
        if missing_bulk:
            errors.append(f'Missing bulk_edit selectors: {missing_bulk}')
        if config.get('woo_api', {}).get('enabled'):
            if not str(config.get('woo_api', {}).get('consumer_key') or '').strip():
                errors.append('woo_api.consumer_key is required when WooCommerce API mode is enabled.')
            if not str(config.get('woo_api', {}).get('consumer_secret') or '').strip():
                errors.append('woo_api.consumer_secret is required when WooCommerce API mode is enabled.')
        if config.get('llm', {}).get('enabled'):
            providers = self.classifier.get_provider_catalog().get('providers', {})
            enabled_with_keys = [k for k, v in providers.items() if v.get('enabled') and str(v.get('api_key') or '').strip()]
            if not enabled_with_keys:
                errors.append('At least one enabled LLM provider with an API key is required.')
        return errors
