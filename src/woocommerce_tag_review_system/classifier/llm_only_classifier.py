from typing import Any, Dict, List

import requests

from classifier.cache import DecisionCache
from classifier.llm_judge import (
    AnthropicJudge,
    CohereJudge,
    GeminiJudge,
    GitHubModelsJudge,
    OpenAICompatibleJudge,
)


PROVIDER_ORDER = [
    'openai', 'anthropic', 'gemini', 'github_models', 'groq', 'together',
    'xai', 'deepseek', 'openrouter', 'mistral', 'perplexity', 'cohere'
]

DEFAULT_PROVIDERS: Dict[str, Dict[str, Any]] = {
    'openai': {'label': 'OpenAI', 'enabled': False, 'api_key': '', 'base_url': 'https://api.openai.com/v1', 'model': 'gpt-4.1-mini'},
    'anthropic': {'label': 'Anthropic', 'enabled': False, 'api_key': '', 'base_url': 'https://api.anthropic.com/v1', 'api_version': '2023-06-01', 'model': 'claude-sonnet-4-5'},
    'gemini': {'label': 'Google Gemini', 'enabled': True, 'api_key': '', 'model': 'gemini-2.5-flash'},
    'github_models': {'label': 'GitHub Models', 'enabled': False, 'api_key': '', 'base_url': 'https://models.github.ai', 'api_version': '2026-03-10', 'organization': '', 'model': 'openai/gpt-4.1'},
    'groq': {'label': 'Groq', 'enabled': False, 'api_key': '', 'base_url': 'https://api.groq.com/openai/v1', 'model': 'llama-3.3-70b-versatile'},
    'together': {'label': 'Together AI', 'enabled': False, 'api_key': '', 'base_url': 'https://api.together.xyz/v1', 'model': 'meta-llama/Meta-Llama-3.1-70B-Instruct-Turbo'},
    'xai': {'label': 'xAI', 'enabled': False, 'api_key': '', 'base_url': 'https://api.x.ai/v1', 'model': 'grok-3-mini'},
    'deepseek': {'label': 'DeepSeek', 'enabled': False, 'api_key': '', 'base_url': 'https://api.deepseek.com/v1', 'model': 'deepseek-chat'},
    'openrouter': {'label': 'OpenRouter', 'enabled': False, 'api_key': '', 'base_url': 'https://openrouter.ai/api/v1', 'model': 'openai/gpt-4.1-mini'},
    'mistral': {'label': 'Mistral', 'enabled': False, 'api_key': '', 'base_url': 'https://api.mistral.ai/v1', 'model': 'mistral-small-latest'},
    'perplexity': {'label': 'Perplexity', 'enabled': False, 'api_key': '', 'base_url': 'https://api.perplexity.ai', 'model': 'sonar'},
    'cohere': {'label': 'Cohere', 'enabled': False, 'api_key': '', 'base_url': 'https://api.cohere.com/v2', 'model': 'command-a-03-2025'},
}

MODEL_CATALOG: Dict[str, List[str]] = {
    'openai': ['gpt-4.1', 'gpt-4.1-mini', 'gpt-4.1-nano', 'gpt-4o', 'gpt-4o-mini'],
    'anthropic': ['claude-opus-4-1', 'claude-sonnet-4-5', 'claude-sonnet-4', 'claude-haiku-4-5'],
    'gemini': ['gemini-2.5-pro', 'gemini-2.5-flash', 'gemini-2.5-flash-lite', 'gemini-2.0-flash-001', 'gemini-2.0-flash-lite-001'],
    'github_models': ['openai/gpt-4.1', 'openai/gpt-4.1-mini', 'openai/gpt-4.1-nano', 'meta/Llama-4-Maverick-17B-128E-Instruct-FP8', 'meta/Llama-3.3-70B-Instruct'],
    'groq': ['llama-3.3-70b-versatile', 'llama-3.1-8b-instant', 'mixtral-8x7b-32768', 'gemma2-9b-it'],
    'together': ['meta-llama/Meta-Llama-3.1-70B-Instruct-Turbo', 'meta-llama/Meta-Llama-3.1-8B-Instruct-Turbo', 'Qwen/Qwen2.5-72B-Instruct-Turbo'],
    'xai': ['grok-3', 'grok-3-mini', 'grok-2-1212'],
    'deepseek': ['deepseek-chat', 'deepseek-reasoner'],
    'openrouter': ['openai/gpt-4.1-mini', 'openai/gpt-4o-mini', 'anthropic/claude-sonnet-4.5', 'google/gemini-2.5-flash'],
    'mistral': ['mistral-large-latest', 'mistral-medium-latest', 'mistral-small-latest', 'open-mixtral-8x22b'],
    'perplexity': ['sonar', 'sonar-pro', 'sonar-reasoning', 'sonar-reasoning-pro'],
    'cohere': ['command-a-03-2025', 'command-r-plus', 'command-r'],
}


class LLMOnlyClassifier:
    def __init__(self, config: Dict[str, Any], cache: DecisionCache):
        self.config = config
        self.cache = cache
        self.mode = config.get('classifier', {}).get('mode', 'llm_only')
        self.cache_enabled = bool(config.get('classifier', {}).get('cache_decisions', True))
        self.refresh_config(config)

    def refresh_config(self, config: Dict[str, Any]) -> None:
        self.config = config
        llm = config.get('llm', {})
        self.enabled = bool(llm.get('enabled', True))
        self.timeout_seconds = float((llm.get('routing') or {}).get('timeout_seconds') or (llm.get('tiered_strategy') or {}).get('timeout_seconds') or 8)
        self.init_error = ''
        self.provider_settings = self._resolved_provider_settings(llm)
        saved_order = (llm.get('routing') or {}).get('provider_order') or PROVIDER_ORDER
        self.provider_order = [key for key in saved_order if key in PROVIDER_ORDER] + [key for key in PROVIDER_ORDER if key not in saved_order]
        self.enabled_providers = [key for key in self.provider_order if self.provider_settings.get(key, {}).get('enabled') and self.provider_settings.get(key, {}).get('api_key')]
        self.default_model = next((self.provider_settings[p].get('model', '') for p in self.enabled_providers if self.provider_settings.get(p, {}).get('model')), 'gemini-2.5-flash')

    def _resolved_provider_settings(self, llm: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        providers = {k: dict(v) for k, v in DEFAULT_PROVIDERS.items()}
        nested = llm.get('providers') or {}
        for key in providers:
            section = dict(nested.get(key) or llm.get(key) or {})
            providers[key].update({k: v for k, v in section.items() if v is not None})
        legacy_provider = (llm.get('provider') or '').strip().lower()
        if legacy_provider in providers and not any(p.get('enabled') for p in providers.values()):
            providers[legacy_provider]['enabled'] = True
        if legacy_provider == 'hybrid':
            for key in ('github_models', 'gemini'):
                if providers[key].get('api_key'):
                    providers[key]['enabled'] = True
        return providers

    def get_provider_catalog(self) -> Dict[str, Any]:
        return {'order': self.provider_order, 'providers': self.provider_settings, 'model_catalog': MODEL_CATALOG}

    def _make_client(self, provider_key: str):
        cfg = self.provider_settings[provider_key]
        if provider_key == 'gemini':
            return GeminiJudge(cfg['api_key'], cfg['model'])
        if provider_key == 'github_models':
            return GitHubModelsJudge(cfg['api_key'], cfg.get('base_url', ''), cfg.get('api_version', ''), cfg.get('organization', ''))
        if provider_key == 'anthropic':
            return AnthropicJudge(cfg['api_key'], cfg['model'], cfg.get('base_url', DEFAULT_PROVIDERS['anthropic']['base_url']), cfg.get('api_version', '2023-06-01'))
        if provider_key == 'cohere':
            return CohereJudge(cfg['api_key'], cfg['model'], cfg.get('base_url', DEFAULT_PROVIDERS['cohere']['base_url']))
        return OpenAICompatibleJudge(provider_key, cfg['api_key'], cfg.get('base_url', ''), cfg['model'])

    def _attempt_provider(self, provider_key: str, tag_title: str, product_title: str, strictness: str, tier_index: int) -> Dict[str, Any]:
        cfg = self.provider_settings[provider_key]
        client = self._make_client(provider_key)
        tier_label = f'provider_{tier_index}'
        if provider_key == 'gemini':
            return client.judge(tag_title, product_title, strictness, tier_label=tier_label)
        if provider_key == 'github_models':
            return client.judge_with_model(cfg['model'], tag_title, product_title, strictness, timeout_seconds=self.timeout_seconds, tier_label=tier_label)
        return client.judge(tag_title, product_title, strictness, timeout_seconds=self.timeout_seconds, tier_label=tier_label)

    def classify(self, tag_title: str, product_title: str, strictness: str) -> Dict[str, Any]:
        key = self.cache.make_key(self.mode, strictness, tag_title, product_title)
        if self.cache_enabled:
            cached = self.cache.get(key)
            if cached:
                out = dict(cached)
                out['source'] = 'cache'
                out['cache_hit'] = True
                out['llm_called'] = False
                out.setdefault('parse_status', 'ok')
                out.setdefault('raw_response', '')
                out.setdefault('api_error', '')
                out.setdefault('provider_used', out.get('provider_used', 'cache'))
                out.setdefault('model_used', out.get('model_used', self.default_model))
                out.setdefault('tier_used', 'cache')
                out.setdefault('attempt_log', [])
                return out

        if not self.enabled:
            return {
                'decision': 'review', 'confidence': 0.0, 'reason': 'LLM disabled', 'source': 'fallback', 'cache_hit': False,
                'llm_called': False, 'parse_status': 'disabled', 'raw_response': '', 'api_error': 'LLM disabled',
                'provider_used': '', 'model_used': '', 'tier_used': 'disabled', 'attempt_log': []
            }
        if not self.enabled_providers:
            return {
                'decision': 'review', 'confidence': 0.0, 'reason': 'No enabled LLM providers with API keys', 'source': 'fallback', 'cache_hit': False,
                'llm_called': False, 'parse_status': 'disabled', 'raw_response': '', 'api_error': 'No enabled providers',
                'provider_used': '', 'model_used': '', 'tier_used': 'disabled', 'attempt_log': []
            }

        attempt_log: List[Dict[str, Any]] = []
        errors: List[str] = []
        for idx, provider_key in enumerate(self.enabled_providers, start=1):
            cfg = self.provider_settings[provider_key]
            try:
                result = self._attempt_provider(provider_key, tag_title, product_title, strictness, idx)
                prior = [*attempt_log]
                prior.extend(result.get('attempt_log', []))
                result['attempt_log'] = prior
                result['source'] = provider_key
                result['cache_hit'] = False
                result['llm_called'] = True
                if self.cache_enabled:
                    self.cache.set(key, {k: v for k, v in result.items() if k in {'decision', 'confidence', 'reason', 'parse_status', 'provider_used', 'model_used', 'tier_used'}})
                return result
            except requests.Timeout:
                msg = f'timeout after {self.timeout_seconds:.1f}s'
            except requests.HTTPError as exc:
                body = ''
                try:
                    body = exc.response.text[:300]
                except Exception:
                    body = ''
                msg = f'http {getattr(exc.response, "status_code", "?")}: {body}'
            except Exception as exc:
                msg = str(exc)
            attempt_log.append({'provider': provider_key, 'model': cfg.get('model', ''), 'tier_label': f'provider_{idx}', 'ok': False, 'error': msg, 'timeout_seconds': self.timeout_seconds})
            errors.append(f'{provider_key}: {msg}')

        return {
            'decision': 'review', 'confidence': 0.0, 'reason': 'LLM/API failure', 'source': 'fallback', 'cache_hit': False,
            'llm_called': True, 'parse_status': 'api_failure', 'raw_response': '', 'api_error': ' | '.join(errors),
            'provider_used': 'fallback', 'model_used': '', 'tier_used': 'failed', 'attempt_log': attempt_log,
        }
