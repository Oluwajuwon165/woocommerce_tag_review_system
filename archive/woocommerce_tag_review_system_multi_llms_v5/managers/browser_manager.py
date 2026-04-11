import threading
import uuid
from typing import Any, Dict, Optional
from playwright.sync_api import sync_playwright


class BrowserManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._lock = threading.RLock()
        self.playwright = None
        self.browser = None
        self.context = None
        self.logged_in = False
        self.tab_registry: Dict[str, Any] = {}
        self.tab_meta: Dict[str, Dict[str, Any]] = {}

    def ensure_browser(self) -> None:
        with self._lock:
            if self.browser:
                return
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(headless=False)
            self.context = self.browser.new_context()

    def close_browser(self) -> None:
        with self._lock:
            self.tab_registry.clear()
            self.tab_meta.clear()
            self.logged_in = False
            if self.context:
                self.context.close()
                self.context = None
            if self.browser:
                self.browser.close()
                self.browser = None
            if self.playwright:
                self.playwright.stop()
                self.playwright = None

    def login_once(self) -> None:
        with self._lock:
            self.ensure_browser()
            if self.logged_in:
                return
            page = self.context.new_page()
            selectors = self.config['selectors']
            creds = self.config['credentials']
            site = self.config['site']
            timeout = self.config.get('runtime', {}).get('navigation_timeout_ms', 45000)
            page.goto(site['login_url'], wait_until='domcontentloaded', timeout=timeout)
            page.locator(selectors['username']).fill(creds['username'])
            page.locator(selectors['password']).fill(creds['password'])
            page.locator(selectors['login_button']).click()
            page.wait_for_load_state('networkidle', timeout=timeout)
            if 'wp-login' in page.url:
                raise RuntimeError('Login/session issue: still on wp-login after submit')
            self.logged_in = True
            self.register_tab(page, note='login_session')

    def new_page(self, url: Optional[str] = None):
        with self._lock:
            self.ensure_browser()
            page = self.context.new_page()
            if url:
                timeout = self.config.get('runtime', {}).get('navigation_timeout_ms', 45000)
                page.goto(url, wait_until='domcontentloaded', timeout=timeout)
            return page

    def register_tab(self, page, note: str = '') -> str:
        token = uuid.uuid4().hex
        self.tab_registry[token] = page
        self.tab_meta[token] = {'url': page.url, 'note': note}
        return token

    def unregister_tab(self, token: Optional[str]) -> None:
        if not token:
            return
        self.tab_registry.pop(token, None)
        self.tab_meta.pop(token, None)

    def focus_tab(self, token: str) -> bool:
        with self._lock:
            page = self.tab_registry.get(token)
            if not page or page.is_closed():
                self.unregister_tab(token)
                return False
            page.bring_to_front()
            return True

    def browser_state(self) -> str:
        if self.browser and self.context:
            return 'open_logged_in' if self.logged_in else 'open'
        return 'closed'
