"""
quiz_generator.py — Gemini API quiz generation with weak-area reinforcement.
Uses the new google-genai SDK.
"""

from __future__ import annotations

import json
import os
import threading
from typing import Any

from dotenv import load_dotenv
from google import genai
from google.genai import types

# ---------------------------------------------------------------------------
# Load API key: Streamlit Cloud secrets → .env → environment variable
# ---------------------------------------------------------------------------

load_dotenv()


def _get_api_key() -> str:
    """Get GEMINI_API_KEY from Streamlit secrets or environment."""
    # Try Streamlit Cloud secrets first
    try:
        import streamlit as st
        if "GEMINI_API_KEY" in st.secrets:
            return st.secrets["GEMINI_API_KEY"]
    except Exception:
        pass
    # Fall back to environment variable / .env
    return os.environ.get("GEMINI_API_KEY", "")


def _get_client() -> genai.Client:
    """Return a configured Gemini client instance."""
    api_key = _get_api_key()
    if not api_key:
        raise RuntimeError(
            "GEMINI_API_KEY が設定されていません。\n"
            "ローカル: .env ファイルに設定\n"
            "Streamlit Cloud: Settings → Secrets に設定"
        )
    return genai.Client(api_key=api_key)


# ---------------------------------------------------------------------------
# Prompt construction
# ---------------------------------------------------------------------------

_SYSTEM_INSTRUCTIONS = {
    "4択問題": """\
あなたは日本史の教育専門AIです。高校3年生が大学入試に向けて学習するための4択問題を作成してください。

## ルール
- 必ず **10問** 作成すること。
- 各問題は **4つの選択肢** (A, B, C, D) を持つ。
- 正解は1つだけ。
- 各問題に **詳しい解説** を付ける。解説は学習に役立つよう、背景知識も含めること。
- 出力は **JSON配列のみ** 。余計なテキスト・マークダウンは一切不要。

## JSON フォーマット (厳守)
```json
[
  {
    "question": "問題文",
    "choices": ["A: 選択肢1", "B: 選択肢2", "C: 選択肢3", "D: 選択肢4"],
    "answer_index": 0,
    "explanation": "解説文",
    "era": "時代名",
    "field": "分野名"
  }
]
```
`answer_index` は 0-based (A=0, B=1, C=2, D=3)。
""",
    "一問一答": """\
あなたは日本史の教育専門AIです。高校生が用語を暗記するための一問一答形式の問題を作成してください。

## ルール
- 必ず **10問** 作成すること。
- 記述式（テキスト入力）で答える形式にする。
- 正解（解答となる用語）は1つ。
- 各問題に **詳しい解説** を付ける。
- 出力は **JSON配列のみ** 。

## JSON フォーマット (厳守)
```json
[
  {
    "question": "問題文（例：江戸幕府の初代将軍は？）",
    "answer": "徳川家康",
    "explanation": "解説文",
    "era": "時代名",
    "field": "分野名"
  }
]
```
""",
    "共通テスト形式（正誤判定）": """\
あなたは日本史の教育専門AIです。大学入学共通テスト形式の正誤判定問題（2つの文章の正誤の組み合わせを選ぶ形式）を作成してください。

## ルール
- 必ず **10問** 作成すること。
- 各問題について、2つの歴史的な文章（文a、文b）を生成する。
- 選択肢は必ず以下の4パターンの組み合わせとして生成する（この順序を厳守）。
  0: a: 正しい、b: 正しい
  1: a: 正しい、b: 誤っている
  2: a: 誤っている、b: 正しい
  3: a: 誤っている、b: 誤っている
- 各問題に **詳しい解説** を付ける。文aと文b、それぞれの正誤の理由を明記すること。
- 出力は **JSON配列のみ** 。

## JSON フォーマット (厳守)
```json
[
  {
    "statement_a": "歴史的な文章a",
    "statement_b": "歴史的な文章b",
    "choices": ["a: 正しい、b: 正しい", "a: 正しい、b: 誤っている", "a: 誤っている、b: 正しい", "a: 誤っている、b: 誤っている"],
    "answer_index": 0,
    "explanation": "解説文",
    "era": "時代名",
    "field": "分野名"
  }
]
```
`answer_index` は 0, 1, 2, 3 のいずれか。
"""
}


def _build_user_prompt(
    pdf_text: str,
    era: str,
    field: str,
    quiz_type: str = "4択問題",
    weak_areas: list[dict] | None = None,
    wrong_questions: list[str] | None = None,
) -> str:
    """Build the user prompt including context and weak-area data."""
    parts: list[str] = []

    # Target scope
    parts.append(f"## 出題形式\n- {quiz_type}\n")
    parts.append(f"## 出題範囲\n- 時代: {era}\n- 分野: {field}\n")

    # Weak areas
    if weak_areas:
        lines = "\n".join(
            f"  - {wa['era']} / {wa['field']} (誤答率 {wa['error_rate']}%)"
            for wa in weak_areas
        )
        parts.append(
            "## ユーザーの苦手分野（重点的に出題してください）\n"
            f"{lines}\n"
        )

    # Recent wrong questions
    if wrong_questions:
        qs = "\n".join(f"  - {q}" for q in wrong_questions[:10])
        parts.append(
            "## 過去に間違えた問題（類似問題を出して復習させてください）\n"
            f"{qs}\n"
        )

    # PDF content
    if pdf_text:
        # Truncate to reduce token usage and stay within free-tier limits
        truncated = pdf_text[:6000]
        parts.append(f"## 参考資料（プリントの内容）\n{truncated}\n")

    parts.append(
        f"上記の情報を踏まえて、10問の{quiz_type}をJSON配列で出力してください。"
    )

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Quiz generation
# ---------------------------------------------------------------------------


def _parse_quiz_json(text: str, quiz_type: str) -> list[dict[str, Any]]:
    """
    Parse the model output to extract the JSON array of questions.
    Handles markdown fences and truncated/incomplete JSON gracefully.
    """
    cleaned = text.strip()

    # Remove markdown code fences if present
    if cleaned.startswith("```"):
        try:
            first_newline = cleaned.index("\n")
            cleaned = cleaned[first_newline + 1 :]
            if cleaned.endswith("```"):
                cleaned = cleaned[: -len("```")]
        except ValueError:
            pass
        cleaned = cleaned.strip()

    # First try: parse as-is
    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError:
        # Truncated JSON recovery: find the last complete object "}"
        # and close the array
        data = _try_recover_json(cleaned)

    if not isinstance(data, list):
        raise ValueError("Expected a JSON array of questions")

    # Validate and keep only well-formed questions
    valid = []
    for item in data:
        if not isinstance(item, dict):
            continue
        
        if quiz_type == "4択問題":
            has_keys = all(k in item for k in ("question", "choices", "answer_index"))
            if has_keys and isinstance(item.get("choices"), list) and len(item["choices"]) == 4:
                valid.append(item)
        elif quiz_type == "一問一答":
            has_keys = all(k in item for k in ("question", "answer"))
            if has_keys:
                valid.append(item)
        elif quiz_type == "共通テスト形式（正誤判定）":
            has_keys = all(k in item for k in ("statement_a", "statement_b", "choices", "answer_index"))
            if has_keys and isinstance(item.get("choices"), list) and len(item["choices"]) == 4:
                # Add a 'question' field for consistency in app.py logic
                item["question"] = f"文a: {item['statement_a']}\n文b: {item['statement_b']}"
                valid.append(item)

        # Common fields
        if valid and valid[-1] == item:
            item.setdefault("explanation", "解説は省略されました。")
            item.setdefault("era", "未分類")
            item.setdefault("field", "未分類")

    if not valid:
        raise ValueError("有効な問題が1つもパースできませんでした。再度お試しください。")

    return valid


def _try_recover_json(text: str) -> list:
    """Attempt to recover a truncated JSON array by finding complete objects."""
    import re

    # Strategy: find all complete JSON objects {...} in the text
    # and wrap them in an array
    results = []
    depth = 0
    obj_start = None
    in_string = False
    escape_next = False

    for i, ch in enumerate(text):
        if escape_next:
            escape_next = False
            continue
        if ch == '\\' and in_string:
            escape_next = True
            continue
        if ch == '"' and not escape_next:
            in_string = not in_string
            continue
        if in_string:
            continue

        if ch == '{':
            if depth == 0:
                obj_start = i
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0 and obj_start is not None:
                try:
                    obj = json.loads(text[obj_start:i + 1])
                    results.append(obj)
                except json.JSONDecodeError:
                    pass
                obj_start = None

    if not results:
        raise ValueError(f"JSONのパースに失敗しました: レスポンスが不完全です")

    return results


def generate_quiz(
    pdf_text: str,
    era: str,
    field: str,
    quiz_type: str = "4択問題",
    weak_areas: list[dict] | None = None,
    wrong_questions: list[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Generate 10 questions using Gemini API.
    Includes automatic retry with backoff and model fallback.

    Returns:
        A list of dicts with keys:
        question, (choices), (answer_index), (answer), explanation, era, field
    """
    import time

    client = _get_client()
    user_prompt = _build_user_prompt(pdf_text, era, field, quiz_type, weak_areas, wrong_questions)
    system_instruction = _SYSTEM_INSTRUCTIONS.get(quiz_type, _SYSTEM_INSTRUCTIONS["4択問題"])

    # Models to try in order (fallback chain)
    models = ["gemini-2.0-flash-lite", "gemini-2.0-flash", "gemini-2.5-flash"]
    max_retries = 3
    last_error = None

    for model_name in models:
        for attempt in range(max_retries):
            try:
                response = client.models.generate_content(
                    model=model_name,
                    contents=user_prompt,
                    config=types.GenerateContentConfig(
                        system_instruction=system_instruction,
                        temperature=0.7,
                        max_output_tokens=8192,
                    ),
                )
                return _parse_quiz_json(response.text, quiz_type)
            except Exception as e:
                last_error = e
                error_str = str(e)
                if "429" in error_str or "RESOURCE_EXHAUSTED" in error_str:
                    if attempt < max_retries - 1:
                        wait = 3 * (2 ** attempt)  # 3s, 6s, 12s
                        time.sleep(wait)
                        continue
                    else:
                        break  # Try next model
                else:
                    raise  # Non-quota error, raise immediately

    raise RuntimeError(
        f"全てのモデルでクォータ制限に達しました。\n"
        f"しばらく時間をおいてから再度お試しください（通常1〜2分で回復します）。\n"
        f"最後のエラー: {last_error}"
    )


# ---------------------------------------------------------------------------
# Background prefetching
# ---------------------------------------------------------------------------


def prefetch_quiz_async(
    pdf_text: str,
    era: str,
    field: str,
    quiz_type: str,
    weak_areas: list[dict] | None,
    wrong_questions: list[str] | None,
    result_holder: dict,
) -> threading.Thread:
    """
    Start quiz generation in a background thread.

    The generated questions (or error) will be stored in `result_holder`
    under keys "questions" and "error".

    Args:
        result_holder: A dict (typically from st.session_state) where
                       results will be written.

    Returns:
        The started Thread.
    """

    def _worker():
        try:
            questions = generate_quiz(
                pdf_text, era, field, quiz_type, weak_areas, wrong_questions
            )
            result_holder["questions"] = questions
            result_holder["error"] = None
        except Exception as e:
            result_holder["questions"] = None
            result_holder["error"] = str(e)

    thread = threading.Thread(target=_worker, daemon=True)
    thread.start()
    return thread
