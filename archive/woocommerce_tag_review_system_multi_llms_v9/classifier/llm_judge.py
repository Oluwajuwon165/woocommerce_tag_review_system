import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests

try:
    import google.generativeai as genai
except Exception:
    genai = None


PROMPT_TEMPLATE = """
You are judging whether a WooCommerce product belongs under a product tag.
Think like a human merchandiser and store reviewer, not a keyword matcher.
Use category relevance, product purpose, customer expectation, merchandising judgment, and practical fit.
Do not use rigid token matching logic.

Strictness mode: {strictness}

Tag title: {tag_title}
Product title: {product_title}

Decision definitions:
- keep: clearly belongs under the tag.
- mark: clearly does not fit the tag and should likely be removed from it.
- review: uncertain, borderline, ambiguous, partial fit, or needs a human check.

Be conservative about mark. Do not over-mark.
Return STRICT JSON only in exactly this shape:
{{
  "decision": "keep" | "mark" | "review",
  "confidence": 0.0,
  "reason": "short explanation"
}}
""".strip()


@dataclass
class ModelAttempt:
    provider: str
    model: str
    tier_label: str
    ok: bool
    error: str = ""
    timeout_seconds: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "provider": self.provider,
            "model": self.model,
            "tier_label": self.tier_label,
            "ok": self.ok,
            "error": self.error,
            "timeout_seconds": self.timeout_seconds,
        }


class BaseJudge:
    @staticmethod
    def build_prompt(tag_title: str, product_title: str, strictness: str) -> str:
        return PROMPT_TEMPLATE.format(tag_title=tag_title, product_title=product_title, strictness=strictness)

    @staticmethod
    def safe_parse(raw_text: str) -> Dict[str, Any]:
        text = (raw_text or '').strip()
        text = re.sub(r'^```(?:json)?\s*', '', text)
        text = re.sub(r'\s*```$', '', text)
        match = re.search(r'\{.*\}', text, flags=re.DOTALL)
        candidate = match.group(0) if match else text
        try:
            parsed = json.loads(candidate)
            decision = parsed.get("decision", "review")
            confidence = float(parsed.get("confidence", 0.0) or 0.0)
            reason = str(parsed.get("reason", ""))
            if decision not in {"keep", "mark", "review"}:
                decision = "review"
            confidence = max(0.0, min(1.0, confidence))
            return {
                "decision": decision,
                "confidence": confidence,
                "reason": reason,
                "parse_status": "ok",
            }
        except Exception:
            return {
                "decision": "review",
                "confidence": 0.0,
                "reason": "LLM response parse failure",
                "parse_status": "parse_failure",
            }


class GeminiJudge(BaseJudge):
    def __init__(self, api_key: str, model_name: str):
        if not genai:
            raise RuntimeError("google-generativeai is not installed")
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel(model_name)
        self.model_name = model_name

    def judge(self, tag_title: str, product_title: str, strictness: str, tier_label: str = 'primary') -> Dict[str, Any]:
        prompt = self.build_prompt(tag_title, product_title, strictness)
        response = self.model.generate_content(prompt)
        text = getattr(response, 'text', '') or ''
        parsed = self.safe_parse(text)
        parsed['raw_response'] = text
        parsed['provider_used'] = 'gemini'
        parsed['model_used'] = self.model_name
        parsed['tier_used'] = tier_label
        parsed['attempt_log'] = [ModelAttempt('gemini', self.model_name, tier_label, True).to_dict()]
        return parsed


class OpenAICompatibleJudge(BaseJudge):
    def __init__(self, provider_key: str, api_key: str, base_url: str, model_name: str, extra_headers: Optional[Dict[str, str]] = None, payload_overrides: Optional[Dict[str, Any]] = None):
        self.provider_key = provider_key
        self.api_key = api_key
        self.base_url = (base_url or '').rstrip('/')
        self.model_name = model_name
        self.extra_headers = extra_headers or {}
        self.payload_overrides = payload_overrides or {}

    def judge(self, tag_title: str, product_title: str, strictness: str, timeout_seconds: float = 8.0, tier_label: str = 'primary') -> Dict[str, Any]:
        prompt = self.build_prompt(tag_title, product_title, strictness)
        payload = {
            'model': self.model_name,
            'messages': [
                {'role': 'system', 'content': 'Return only the requested JSON object.'},
                {'role': 'user', 'content': prompt},
            ],
            'temperature': 0.1,
            'max_tokens': 220,
        }
        payload.update(self.payload_overrides)
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json',
            **self.extra_headers,
        }
        response = requests.post(f'{self.base_url}/chat/completions', headers=headers, json=payload, timeout=timeout_seconds)
        response.raise_for_status()
        data = response.json()
        content = ''
        choices = data.get('choices') or []
        if choices:
            message = choices[0].get('message', {})
            content = message.get('content', '') or ''
        parsed = self.safe_parse(content)
        parsed['raw_response'] = content
        parsed['provider_used'] = self.provider_key
        parsed['model_used'] = self.model_name
        parsed['tier_used'] = tier_label
        parsed['attempt_log'] = [ModelAttempt(self.provider_key, self.model_name, tier_label, True, timeout_seconds=timeout_seconds).to_dict()]
        return parsed


class AnthropicJudge(BaseJudge):
    def __init__(self, api_key: str, model_name: str, base_url: str = 'https://api.anthropic.com/v1', api_version: str = '2023-06-01'):
        self.api_key = api_key
        self.model_name = model_name
        self.base_url = base_url.rstrip('/')
        self.api_version = api_version

    def judge(self, tag_title: str, product_title: str, strictness: str, timeout_seconds: float = 8.0, tier_label: str = 'primary') -> Dict[str, Any]:
        prompt = self.build_prompt(tag_title, product_title, strictness)
        payload = {
            'model': self.model_name,
            'max_tokens': 220,
            'temperature': 0.1,
            'system': 'Return only the requested JSON object.',
            'messages': [{'role': 'user', 'content': prompt}],
        }
        headers = {
            'x-api-key': self.api_key,
            'anthropic-version': self.api_version,
            'Content-Type': 'application/json',
        }
        response = requests.post(f'{self.base_url}/messages', headers=headers, json=payload, timeout=timeout_seconds)
        response.raise_for_status()
        data = response.json()
        content_blocks = data.get('content') or []
        text = ''
        for block in content_blocks:
            if isinstance(block, dict) and block.get('type') == 'text':
                text += block.get('text', '')
        parsed = self.safe_parse(text)
        parsed['raw_response'] = text
        parsed['provider_used'] = 'anthropic'
        parsed['model_used'] = self.model_name
        parsed['tier_used'] = tier_label
        parsed['attempt_log'] = [ModelAttempt('anthropic', self.model_name, tier_label, True, timeout_seconds=timeout_seconds).to_dict()]
        return parsed


class CohereJudge(BaseJudge):
    def __init__(self, api_key: str, model_name: str, base_url: str = 'https://api.cohere.com/v2'):
        self.api_key = api_key
        self.model_name = model_name
        self.base_url = base_url.rstrip('/')

    def judge(self, tag_title: str, product_title: str, strictness: str, timeout_seconds: float = 8.0, tier_label: str = 'primary') -> Dict[str, Any]:
        prompt = self.build_prompt(tag_title, product_title, strictness)
        payload = {
            'model': self.model_name,
            'messages': [
                {'role': 'system', 'content': 'Return only the requested JSON object.'},
                {'role': 'user', 'content': prompt},
            ],
            'temperature': 0.1,
        }
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json',
        }
        response = requests.post(f'{self.base_url}/chat', headers=headers, json=payload, timeout=timeout_seconds)
        response.raise_for_status()
        data = response.json()
        text = ''
        if isinstance(data.get('message'), dict):
            content = data['message'].get('content') or []
            for block in content:
                if isinstance(block, dict):
                    text += block.get('text', '')
        text = text or data.get('text', '') or ''
        parsed = self.safe_parse(text)
        parsed['raw_response'] = text
        parsed['provider_used'] = 'cohere'
        parsed['model_used'] = self.model_name
        parsed['tier_used'] = tier_label
        parsed['attempt_log'] = [ModelAttempt('cohere', self.model_name, tier_label, True, timeout_seconds=timeout_seconds).to_dict()]
        return parsed


class GitHubModelsJudge(BaseJudge):
    def __init__(self, api_key: str, base_url: str, api_version: str, organization: str = ''):
        self.api_key = api_key
        self.base_url = (base_url or 'https://models.github.ai').rstrip('/')
        self.api_version = api_version or '2026-03-10'
        self.organization = (organization or '').strip()

    def _endpoint(self) -> str:
        if self.organization:
            return f"{self.base_url}/orgs/{self.organization}/inference/chat/completions"
        return f"{self.base_url}/inference/chat/completions"

    def judge_with_model(self, model_name: str, tag_title: str, product_title: str, strictness: str, timeout_seconds: float, tier_label: str = '') -> Dict[str, Any]:
        prompt = self.build_prompt(tag_title, product_title, strictness)
        payload = {
            'model': model_name,
            'messages': [
                {'role': 'system', 'content': 'Return only the requested JSON object.'},
                {'role': 'user', 'content': prompt},
            ],
            'temperature': 0.1,
            'max_tokens': 220,
            'stream': False,
        }
        headers = {
            'Accept': 'application/vnd.github+json',
            'Authorization': f'Bearer {self.api_key}',
            'X-GitHub-Api-Version': self.api_version,
            'Content-Type': 'application/json',
        }
        response = requests.post(self._endpoint(), headers=headers, json=payload, timeout=timeout_seconds)
        response.raise_for_status()
        data = response.json()
        choices = data.get('choices') or []
        message = choices[0].get('message', {}) if choices else {}
        content = message.get('content', '') or ''
        if isinstance(content, list):
            joined = []
            for part in content:
                if isinstance(part, dict):
                    joined.append(part.get('text', '') or part.get('content', '') or json.dumps(part))
                else:
                    joined.append(str(part))
            content = ''.join(joined)
        parsed = self.safe_parse(content)
        parsed['raw_response'] = content
        parsed['provider_used'] = 'github_models'
        parsed['model_used'] = model_name
        parsed['tier_used'] = tier_label or 'primary'
        return parsed
