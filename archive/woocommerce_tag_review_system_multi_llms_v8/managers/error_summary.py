from typing import Tuple


def classify_error(exc: Exception | str) -> Tuple[str, str]:
    text = str(exc).lower()
    if "timeout" in text:
        return "timeout", "Timeout during page processing or row wait."
    if "selector" in text or "not found" in text:
        return "selector issue", "A configured selector was missing or not found."
    if "login" in text or "redirect" in text or "session" in text:
        return "login/session issue", "Login or admin session likely failed."
    if "acp" in text or "bulk row" in text:
        return "ACP row missing", "Admin Columns Pro bulk area did not appear as expected."
    if "next-page" in text or "pagination" in text:
        return "pagination issue", "The system could not move through pagination cleanly."
    if "quota" in text or "gemini" in text or "api" in text or "429" in text:
        return "LLM/API issue", "LLM request failed, timed out, or a rate/quota limit was hit."
    return "unexpected error", "Unexpected runtime error during processing."
