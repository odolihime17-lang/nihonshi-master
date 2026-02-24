"""
app.py — 日本史学習Webアプリ (Streamlit)
"""

import uuid
import streamlit as st
from streamlit.components.v1 import html as st_html
from db import init_db, save_result, get_weak_areas, get_recent_wrong_questions, get_stats
from pdf_utils import extract_text_from_pdf, extract_text_from_drive_url
from quiz_generator import generate_quiz, prefetch_quiz_async

# ---------------------------------------------------------------------------
# Page config & DB init
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="日本史マスター",
    page_icon="🏯",
    layout="centered",
    initial_sidebar_state="expanded",
)

init_db()

# ---------------------------------------------------------------------------
# Cookie-based user identification
# ---------------------------------------------------------------------------

try:
    user_id = st.context.cookies.get("nihonshi_user_id", "")
except Exception:
    user_id = ""

if not user_id:
    user_id = str(uuid.uuid4())
    try:
        st_html(f"""
            <script>
            document.cookie = "nihonshi_user_id={user_id}; path=/; max-age=31536000; SameSite=Lax";
            </script>
        """, height=0)
    except Exception:
        pass

if not user_id:
    user_id = "anonymous"

# ---------------------------------------------------------------------------
# Custom CSS
# ---------------------------------------------------------------------------

st.markdown(
    """
<style>
/* ── Import Google Font ── */
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+JP:wght@300;400;500;700;900&display=swap');

html, body, [class*="st-"] {
    font-family: 'Noto Sans JP', sans-serif;
}

/* ── Global Background ── */
.stApp {
    background: #f8f9fb;
}
.stMainBlockContainer {
    background: #f8f9fb;
}

/* ── Sidebar toggle: base styling (JS handles the text replacement) ── */
@media (max-width: 768px) {
    header[data-testid="stHeader"] [data-testid="stToolbar"] {
        display: none !important;
    }
}

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: #ffffff;
    border-right: 1px solid #e8eaed;
}
section[data-testid="stSidebar"] .stMarkdown h2 {
    color: #1a1a2e !important;
}
section[data-testid="stSidebar"] .stSelectbox label,
section[data-testid="stSidebar"] .stTextInput label,
section[data-testid="stSidebar"] .stFileUploader label {
    color: #555 !important;
    font-weight: 500;
}

/* ── Cards ── */
.quiz-card {
    background: #ffffff;
    border: 1px solid #e8eaed;
    border-radius: 16px;
    padding: 2rem;
    margin: 1rem 0;
    box-shadow: 0 2px 12px rgba(0, 0, 0, 0.06);
    transition: transform 0.2s ease, box-shadow 0.2s ease;
}
.quiz-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 6px 20px rgba(0, 0, 0, 0.08);
}

/* ── Question number badge ── */
.question-badge {
    display: inline-block;
    background: linear-gradient(135deg, #4f46e5, #6366f1);
    color: #fff;
    font-weight: 700;
    font-size: 0.85rem;
    padding: 0.3rem 0.9rem;
    border-radius: 999px;
    margin-bottom: 0.75rem;
    letter-spacing: 0.05em;
}

/* ── Question text ── */
.question-text {
    font-size: 1.15rem;
    font-weight: 500;
    color: #1a1a2e;
    line-height: 1.8;
    margin-bottom: 1rem;
}

/* ── Choice buttons ── */
div.stButton > button {
    width: 100%;
    text-align: left;
    padding: 0.85rem 1.25rem;
    margin-bottom: 0.5rem;
    border-radius: 12px;
    border: 1px solid #e0e0e0;
    background: #ffffff;
    color: #333;
    font-size: 1rem;
    font-weight: 400;
    transition: all 0.2s ease;
    cursor: pointer;
}
div.stButton > button:hover {
    background: #f0f0ff;
    border-color: #6366f1;
    color: #4f46e5;
    transform: translateX(4px);
}

/* ── Correct / Wrong box ── */
.correct-box {
    background: #f0fdf4;
    border: 1px solid #bbf7d0;
    border-radius: 12px;
    padding: 1.25rem;
    margin: 1rem 0;
    color: #166534;
}
.wrong-box {
    background: #fef2f2;
    border: 1px solid #fecaca;
    border-radius: 12px;
    padding: 1.25rem;
    margin: 1rem 0;
    color: #991b1b;
}

/* ── Explanation box ── */
.explanation-box {
    background: #f5f3ff;
    border-left: 4px solid #6366f1;
    border-radius: 0 12px 12px 0;
    padding: 1.25rem;
    margin: 0.75rem 0;
    color: #3b3b5c;
    line-height: 1.8;
}

/* ── Score display ── */
.score-box {
    background: #ffffff;
    border: 1px solid #e8eaed;
    border-radius: 20px;
    padding: 2.5rem;
    text-align: center;
    margin: 2rem 0;
    box-shadow: 0 4px 16px rgba(0, 0, 0, 0.06);
}
.score-number {
    font-size: 4rem;
    font-weight: 900;
    background: linear-gradient(135deg, #4f46e5, #7c3aed);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    line-height: 1.2;
}
.score-label {
    font-size: 1.1rem;
    color: #666;
    margin-top: 0.5rem;
}

/* ── Stats metric cards ── */
.stat-card {
    background: #ffffff;
    border: 1px solid #e8eaed;
    border-radius: 14px;
    padding: 1.25rem;
    text-align: center;
    margin: 0.5rem 0;
    box-shadow: 0 1px 4px rgba(0, 0, 0, 0.04);
}
.stat-value {
    font-size: 2rem;
    font-weight: 700;
    color: #4f46e5;
}
.stat-label {
    font-size: 0.85rem;
    color: #888;
    margin-top: 0.25rem;
}

/* ── Title styling ── */
.app-title {
    font-size: 2.2rem;
    font-weight: 900;
    text-align: center;
    background: linear-gradient(135deg, #1a1a2e 0%, #4f46e5 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    margin-bottom: 0.25rem;
}
.app-subtitle {
    text-align: center;
    color: #888;
    font-size: 0.95rem;
    margin-bottom: 2rem;
}

/* ── Progress bar ── */
.progress-container {
    background: #e8eaed;
    border-radius: 999px;
    height: 8px;
    margin: 1rem 0 1.5rem;
    overflow: hidden;
}
.progress-fill {
    height: 100%;
    border-radius: 999px;
    background: linear-gradient(90deg, #4f46e5, #7c3aed);
    transition: width 0.4s ease;
}

/* ── Sidebar button styles ── */
section[data-testid="stSidebar"] div.stButton > button {
    background: linear-gradient(135deg, #4f46e5, #6366f1);
    border: none;
    color: #fff !important;
    font-weight: 600;
    border-radius: 12px;
    padding: 0.7rem 1.5rem;
    width: 100%;
    transition: all 0.2s ease;
}
section[data-testid="stSidebar"] div.stButton > button:hover {
    background: linear-gradient(135deg, #4338ca, #4f46e5);
    transform: translateY(-1px);
    box-shadow: 0 4px 12px rgba(79, 70, 229, 0.3);
}

/* ── Weak-area tags ── */
.weak-tag {
    display: inline-block;
    background: #fef2f2;
    border: 1px solid #fecaca;
    color: #dc2626;
    border-radius: 999px;
    padding: 0.2rem 0.75rem;
    font-size: 0.8rem;
    margin: 0.2rem;
}

/* ── PDF file chip ── */
.pdf-chip {
    display: inline-flex;
    align-items: center;
    gap: 0.3rem;
    background: #eef2ff;
    border: 1px solid #c7d2fe;
    color: #4338ca;
    border-radius: 999px;
    padding: 0.25rem 0.75rem;
    font-size: 0.8rem;
    margin: 0.2rem;
    font-weight: 500;
}
</style>
""",
    unsafe_allow_html=True,
)

# (Custom JS hamburger/swipe removed — using restyled native Streamlit controls)

# ---------------------------------------------------------------------------
# JavaScript: fix sidebar buttons + swipe gesture
# (Finds buttons by their text content to work across all Streamlit versions)
# ---------------------------------------------------------------------------

st_html("""
<script>
(function() {
    const parentDoc = window.parent.document;

    function fixButtons() {
        // Find all buttons/spans containing the icon text
        const allElements = parentDoc.querySelectorAll('button, span');
        for (const el of allElements) {
            const text = el.textContent.trim();
            if (text === 'keyboard_double_arrow_right') {
                // This is the sidebar OPEN button
                el.textContent = '';
                el.style.fontSize = '0';
                el.innerHTML = '<span style="font-size:1.5rem;color:#4f46e5;">☰</span>';
            } else if (text === 'keyboard_double_arrow_left') {
                // This is the sidebar CLOSE button (in header when sidebar open)
                el.textContent = '';
                el.style.fontSize = '0';
                el.innerHTML = '<span style="font-size:1.5rem;color:#4f46e5;">☰</span>';
            } else if (text === 'close') {
                // Sidebar internal close button
                el.textContent = '';
                el.style.fontSize = '0';
                el.innerHTML = '<span style="font-size:1.2rem;color:#666;">✕</span>';
            }
        }
    }

    // Run immediately and periodically (Streamlit re-renders the DOM)
    fixButtons();
    setInterval(fixButtons, 500);

    // Also observe DOM changes
    const observer = new MutationObserver(fixButtons);
    observer.observe(parentDoc.body, { childList: true, subtree: true });

    // --- Swipe gesture support ---
    let touchStartX = 0;
    let touchStartY = 0;
    const SWIPE_THRESHOLD = 50;

    parentDoc.addEventListener('touchstart', function(e) {
        touchStartX = e.changedTouches[0].screenX;
        touchStartY = e.changedTouches[0].screenY;
    }, {passive: true});

    parentDoc.addEventListener('touchend', function(e) {
        const dx = e.changedTouches[0].screenX - touchStartX;
        const dy = e.changedTouches[0].screenY - touchStartY;

        if (Math.abs(dx) > Math.abs(dy) && Math.abs(dx) > SWIPE_THRESHOLD) {
            if (dx > 0 && touchStartX < 50) {
                // Right swipe from left edge -> open sidebar
                const openBtn = parentDoc.querySelector('button[data-testid="collapsedControl"]');
                if (openBtn) openBtn.click();
            } else if (dx < 0) {
                // Left swipe -> close sidebar
                const closeBtn = parentDoc.querySelector('[data-testid="stSidebarCollapseButton"] button');
                if (closeBtn) closeBtn.click();
            }
        }
    }, {passive: true});
})();
</script>
""", height=0)
# ---------------------------------------------------------------------------
# Session state defaults
# ---------------------------------------------------------------------------

_DEFAULTS = {
    "pdf_texts": {},        # dict[str, str] — filename → extracted text
    "questions": [],
    "current_index": 0,
    "score": 0,
    "answered": False,
    "selected_choice": None,
    "quiz_finished": False,
    "prefetch_holder": {},
    "prefetch_thread": None,
    "generating": False,
    "total_sessions": 0,
    "user_answers": [],  # Track per-question: list of chosen indices
    "drive_urls": [],    # List of Google Drive URLs added
}
for key, val in _DEFAULTS.items():
    if key not in st.session_state:
        st.session_state[key] = val

# ---------------------------------------------------------------------------
# Helper: combined PDF text
# ---------------------------------------------------------------------------


def _get_combined_pdf_text() -> str:
    """Combine all uploaded PDF texts into one string."""
    if not st.session_state.pdf_texts:
        return ""
    return "\n\n".join(
        f"=== {fname} ===\n{text}"
        for fname, text in st.session_state.pdf_texts.items()
    )


# ---------------------------------------------------------------------------
# Era / Field options
# ---------------------------------------------------------------------------

ERAS = [
    "指定なし（全時代）",
    "縄文・弥生時代",
    "古墳時代",
    "飛鳥時代",
    "奈良時代",
    "平安時代",
    "鎌倉時代",
    "室町時代",
    "安土桃山時代",
    "江戸時代",
    "明治時代",
    "大正時代",
    "昭和時代（戦前）",
    "昭和時代（戦後）",
    "平成・令和",
]

FIELDS = [
    "指定なし（全分野）",
    "政治史",
    "外交史",
    "文化史",
    "社会・経済史",
    "宗教史",
    "法制史",
    "人物史",
]

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

with st.sidebar:
    st.markdown("## 🏯 設定")
    st.markdown("---")

    # PDF input: upload or Google Drive URL
    pdf_method = st.radio(
        "📄 PDFの読み込み方法",
        ["ファイルアップロード", "Google Drive URL"],
        horizontal=True,
    )

    if pdf_method == "ファイルアップロード":
        uploaded_pdfs = st.file_uploader(
            "PDFファイルを選択（複数可）",
            type=["pdf"],
            accept_multiple_files=True,
            help="学校のプリントや参考資料をアップロード（各10MB以下）",
            label_visibility="collapsed",
        )
        if uploaded_pdfs:
            for uploaded_pdf in uploaded_pdfs:
                fname = uploaded_pdf.name
                if fname not in st.session_state.pdf_texts:
                    file_size_mb = len(uploaded_pdf.getvalue()) / (1024 * 1024)
                    if file_size_mb > 10:
                        st.error(f"⚠️ {fname} が大きすぎます（{file_size_mb:.1f}MB）。10MB以下にしてください。")
                    else:
                        with st.spinner(f"📖 {fname} を読み込み中... ({file_size_mb:.1f}MB)"):
                            text = extract_text_from_pdf(uploaded_pdf)
                            st.session_state.pdf_texts[fname] = text
                        st.success(f"✅ {fname}: {len(text):,} 文字を抽出")
    else:
        drive_url = st.text_input(
            "Google Drive の共有リンク",
            placeholder="https://drive.google.com/file/d/.../view?usp=sharing",
            label_visibility="collapsed",
        )
        st.caption("💡 Google Drive で「リンクを知っている全員が閲覧可」に設定してください")
        if drive_url:
            if st.button("📥 ダウンロードして読み込み", use_container_width=True):
                # Generate a short label from the URL
                url_label = f"Drive_{len(st.session_state.pdf_texts) + 1}"
                if url_label not in st.session_state.pdf_texts:
                    with st.spinner("📖 Google Drive からダウンロード中..."):
                        try:
                            text = extract_text_from_drive_url(drive_url)
                            st.session_state.pdf_texts[url_label] = text
                            st.success(f"✅ {url_label}: {len(text):,} 文字を抽出")
                        except (ValueError, RuntimeError) as e:
                            st.error(f"❌ {e}")

    # Show loaded files
    if st.session_state.pdf_texts:
        st.markdown("---")
        st.markdown(f"**📎 読み込み済みファイル（{len(st.session_state.pdf_texts)}件）**")
        total_chars = sum(len(t) for t in st.session_state.pdf_texts.values())
        st.caption(f"合計 {total_chars:,} 文字")

        for fname in list(st.session_state.pdf_texts.keys()):
            char_count = len(st.session_state.pdf_texts[fname])
            col_name, col_del = st.columns([4, 1])
            with col_name:
                st.markdown(
                    f'<span class="pdf-chip">📄 {fname} ({char_count:,}字)</span>',
                    unsafe_allow_html=True,
                )
            with col_del:
                if st.button("🗑", key=f"del_{fname}", help=f"{fname} を削除"):
                    del st.session_state.pdf_texts[fname]
                    st.rerun()

        if st.button("🗑️ 全てクリア", use_container_width=True):
            st.session_state.pdf_texts = {}
            st.rerun()

    st.markdown("---")

    # Era & field selection
    selected_era = st.selectbox("🕰️ 時代を選択", ERAS, index=0)
    custom_era = st.text_input("または自由入力（時代）", placeholder="例: 南北朝時代")

    selected_field = st.selectbox("📚 分野を選択", FIELDS, index=0)
    custom_field = st.text_input("または自由入力（分野）", placeholder="例: 建築史")

    era = custom_era if custom_era else selected_era
    field = custom_field if custom_field else selected_field

    st.markdown("---")

    # Generate button
    generate_clicked = st.button("🚀 問題を生成する", use_container_width=True)

    # Stats in sidebar
    st.markdown("---")
    st.markdown("## 📊 学習統計")
    stats = get_stats(user_id=user_id)
    if stats["total"] > 0:
        col1, col2 = st.columns(2)
        with col1:
            st.metric("回答数", stats["total"])
        with col2:
            st.metric("正答率", f"{stats['accuracy']}%")

        # Weak areas
        weak = get_weak_areas(user_id=user_id)
        if weak:
            st.markdown("#### 🔴 苦手分野")
            for wa in weak:
                st.markdown(
                    f'<span class="weak-tag">{wa["era"]} / {wa["field"]} '
                    f'({wa["error_rate"]}%)</span>',
                    unsafe_allow_html=True,
                )
    else:
        st.caption("まだ学習データがありません")

# ---------------------------------------------------------------------------
# Helper: start quiz generation
# ---------------------------------------------------------------------------

combined_pdf_text = _get_combined_pdf_text()


def _generate_new_quiz():
    """Generate a new set of 10 questions."""
    weak_areas = get_weak_areas(user_id=user_id)
    wrong_questions = get_recent_wrong_questions(user_id=user_id)

    questions = generate_quiz(
        pdf_text=combined_pdf_text,
        era=era,
        field=field,
        weak_areas=weak_areas,
        wrong_questions=wrong_questions,
    )
    return questions


def _start_prefetch():
    """Start prefetching the next quiz set in the background."""
    holder: dict = {}
    st.session_state.prefetch_holder = holder
    weak_areas = get_weak_areas(user_id=user_id)
    wrong_questions = get_recent_wrong_questions(user_id=user_id)
    thread = prefetch_quiz_async(
        pdf_text=combined_pdf_text,
        era=era,
        field=field,
        weak_areas=weak_areas,
        wrong_questions=wrong_questions,
        result_holder=holder,
    )
    st.session_state.prefetch_thread = thread


# ---------------------------------------------------------------------------
# Main area
# ---------------------------------------------------------------------------

# Title
st.markdown('<div class="app-title">🏯 日本史マスター</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="app-subtitle">AIが生成する4択問題で日本史の実力を鍛えよう</div>',
    unsafe_allow_html=True,
)

# ── Handle generate button ──
if generate_clicked:
    if not st.session_state.pdf_texts:
        st.warning("⚠️ まず PDF をアップロードしてください。")
    else:
        st.session_state.generating = True
        st.session_state.quiz_finished = False
        st.session_state.current_index = 0
        st.session_state.score = 0
        st.session_state.answered = False
        st.session_state.selected_choice = None
        st.session_state.user_answers = []

        with st.spinner("🤖 Gemini が問題を作成中... しばらくお待ちください"):
            try:
                questions = _generate_new_quiz()
                st.session_state.questions = questions
                st.session_state.generating = False
                st.session_state.total_sessions += 1
                # Start prefetching next batch
                _start_prefetch()
                st.rerun()
            except Exception as e:
                st.session_state.generating = False
                st.error(f"❌ 問題生成に失敗しました: {e}")

# ── No questions yet ──
if not st.session_state.questions and not st.session_state.generating:
    st.markdown("---")
    st.markdown(
        """
        <div class="quiz-card" style="text-align:center;">
            <p style="font-size:3rem; margin-bottom:0.5rem;">📚</p>
            <p class="question-text">サイドバーからPDFをアップロードし、<br>時代・分野を選択して問題を生成しましょう！</p>
            <p style="color:#94a3b8; font-size:0.9rem;">
                Gemini AIがあなた専用の4択問題を作成します<br>
                📱 スマホの方は左上の ☰ ボタンまたは左端からスワイプで設定を開けます
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Show stats if available
    if stats["total"] > 0:
        st.markdown("---")
        st.markdown("### 📈 これまでの学習成績")

        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(
                f'<div class="stat-card"><div class="stat-value">{stats["total"]}</div>'
                f'<div class="stat-label">総回答数</div></div>',
                unsafe_allow_html=True,
            )
        with col2:
            st.markdown(
                f'<div class="stat-card"><div class="stat-value">{stats["correct"]}</div>'
                f'<div class="stat-label">正解数</div></div>',
                unsafe_allow_html=True,
            )
        with col3:
            st.markdown(
                f'<div class="stat-card"><div class="stat-value">{stats["accuracy"]}%</div>'
                f'<div class="stat-label">正答率</div></div>',
                unsafe_allow_html=True,
            )

        # Per-era stats
        if stats["by_era"]:
            st.markdown("#### 時代別")
            for s in stats["by_era"]:
                progress = s["accuracy"] / 100
                st.markdown(
                    f'**{s["era"]}** — {s["accuracy"]}%  ({s["correct"]}/{s["total"]})'
                )
                st.progress(progress)

# ── Quiz in progress ──
elif st.session_state.questions and not st.session_state.quiz_finished:
    questions = st.session_state.questions
    idx = st.session_state.current_index
    total = len(questions)
    q = questions[idx]

    # Progress bar
    progress_pct = int((idx / total) * 100)
    st.markdown(
        f"""
        <div style="display:flex; justify-content:space-between; color:#94a3b8; font-size:0.85rem; margin-bottom:0.25rem;">
            <span>問題 {idx + 1} / {total}</span>
            <span>スコア: {st.session_state.score} / {idx if st.session_state.answered else idx}</span>
        </div>
        <div class="progress-container">
            <div class="progress-fill" style="width:{progress_pct}%;"></div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Question card
    st.markdown(
        f"""
        <div class="quiz-card">
            <span class="question-badge">Q{idx + 1}</span>
            <div class="question-text">{q["question"]}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Not answered yet: show choice buttons ──
    if not st.session_state.answered:
        for i, choice in enumerate(q["choices"]):
            if st.button(choice, key=f"choice_{idx}_{i}", use_container_width=True):
                is_correct = i == q["answer_index"]
                st.session_state.answered = True
                st.session_state.selected_choice = i
                st.session_state.user_answers.append(i)
                if is_correct:
                    st.session_state.score += 1

                # Save to DB
                save_result(
                    question=q["question"],
                    user_answer=q["choices"][i],
                    correct_answer=q["choices"][q["answer_index"]],
                    is_correct=is_correct,
                    era=q.get("era", era),
                    field=q.get("field", field),
                    user_id=user_id,
                )
                st.rerun()

    # ── Answered: show result + explanation ──
    else:
        selected = st.session_state.selected_choice
        correct_idx = q["answer_index"]
        is_correct = selected == correct_idx

        if is_correct:
            st.markdown(
                '<div class="correct-box">✅ <strong>正解！</strong></div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f'<div class="wrong-box">❌ <strong>不正解</strong>　'
                f'正解: <strong>{q["choices"][correct_idx]}</strong></div>',
                unsafe_allow_html=True,
            )

        # Show all choices with visual indicators
        for i, choice in enumerate(q["choices"]):
            if i == correct_idx:
                st.markdown(f"🟢 **{choice}**")
            elif i == selected and not is_correct:
                st.markdown(f"🔴 ~~{choice}~~")
            else:
                st.markdown(f"⚪ {choice}")

        # Explanation
        st.markdown(
            f'<div class="explanation-box">💡 <strong>解説</strong><br>{q["explanation"]}</div>',
            unsafe_allow_html=True,
        )

        # Next button
        if idx < total - 1:
            if st.button("▶️ 次の問題へ", use_container_width=True):
                st.session_state.current_index += 1
                st.session_state.answered = False
                st.session_state.selected_choice = None
                st.rerun()
        else:
            if st.button("📊 結果を見る", use_container_width=True):
                st.session_state.quiz_finished = True
                st.rerun()

# ── Quiz finished: show results ──
elif st.session_state.quiz_finished:
    total = len(st.session_state.questions)
    score = st.session_state.score
    pct = round(score / total * 100) if total > 0 else 0
    user_answers = st.session_state.user_answers

    # Score display
    if pct >= 80:
        emoji = "🎉"
        message = "素晴らしい！"
    elif pct >= 60:
        emoji = "👏"
        message = "よく頑張りました！"
    elif pct >= 40:
        emoji = "💪"
        message = "もう少し頑張ろう！"
    else:
        emoji = "📖"
        message = "復習して再チャレンジ！"

    st.markdown(
        f"""
        <div class="score-box">
            <div style="font-size:3rem;">{emoji}</div>
            <div class="score-number">{score} / {total}</div>
            <div class="score-label">{message}（正答率 {pct}%）</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Count wrong answers
    wrong_count = total - score

    # Tab view: all / wrong only
    if wrong_count > 0 and wrong_count < total:
        tab_all, tab_wrong = st.tabs([f"📋 全問題 ({total}問)", f"❌ 間違えた問題 ({wrong_count}問)"])
    else:
        tab_all = st.container()
        tab_wrong = None
        st.markdown(f"### 📋 全問題の振り返り（{total}問）")

    # Build review cards
    def _render_review(questions_list, answers_list, indices):
        for idx in indices:
            q = questions_list[idx]
            user_ans = answers_list[idx] if idx < len(answers_list) else -1
            correct_idx = q["answer_index"]
            is_correct = user_ans == correct_idx

            # Status badge
            if is_correct:
                status_badge = '<span style="background:#dcfce7; color:#166534; padding:0.2rem 0.7rem; border-radius:999px; font-size:0.8rem; font-weight:600;">✅ 正解</span>'
            else:
                status_badge = '<span style="background:#fef2f2; color:#991b1b; padding:0.2rem 0.7rem; border-radius:999px; font-size:0.8rem; font-weight:600;">❌ 不正解</span>'

            # Border color
            border_color = "#bbf7d0" if is_correct else "#fecaca"
            bg_color = "#fafffe" if is_correct else "#fffafa"

            st.markdown(
                f"""
                <div style="
                    background: {bg_color};
                    border: 1px solid {border_color};
                    border-radius: 14px;
                    padding: 1.5rem;
                    margin: 0.75rem 0;
                ">
                    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:0.75rem;">
                        <span style="background:linear-gradient(135deg,#4f46e5,#6366f1); color:#fff; font-weight:700; font-size:0.8rem; padding:0.25rem 0.75rem; border-radius:999px;">Q{idx + 1}</span>
                        {status_badge}
                    </div>
                    <div style="font-size:1.05rem; font-weight:500; color:#1a1a2e; line-height:1.7; margin-bottom:1rem;">
                        {q["question"]}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

            # Choices grid
            for ci, choice in enumerate(q["choices"]):
                if ci == correct_idx and ci == user_ans:
                    icon = "✅"
                    style = "background:#f0fdf4; border:1px solid #bbf7d0; color:#166534;"
                    label = "あなたの回答（正解）"
                elif ci == correct_idx:
                    icon = "🟢"
                    style = "background:#f0fdf4; border:1px solid #bbf7d0; color:#166534;"
                    label = "正解"
                elif ci == user_ans:
                    icon = "🔴"
                    style = "background:#fef2f2; border:1px solid #fecaca; color:#991b1b;"
                    label = "あなたの回答"
                else:
                    icon = "⚪"
                    style = "background:#f9fafb; border:1px solid #e5e7eb; color:#6b7280;"
                    label = ""

                label_html = f' <span style="font-size:0.75rem; color:#888;">← {label}</span>' if label else ""
                st.markdown(
                    f'<div style="{style} border-radius:10px; padding:0.6rem 1rem; margin:0.3rem 0; font-size:0.95rem;">{icon} {choice}{label_html}</div>',
                    unsafe_allow_html=True,
                )

            # Explanation
            st.markdown(
                f"""
                <div style="background:#f5f3ff; border-left:4px solid #6366f1; border-radius:0 10px 10px 0; padding:1rem; margin:0.5rem 0 1.5rem; color:#3b3b5c; line-height:1.7; font-size:0.95rem;">
                    💡 <strong>解説</strong><br>{q["explanation"]}
                </div>
                """,
                unsafe_allow_html=True,
            )

    all_indices = list(range(total))
    wrong_indices = [i for i in all_indices if i < len(user_answers) and user_answers[i] != st.session_state.questions[i]["answer_index"]]

    if tab_wrong is not None:
        with tab_all:
            _render_review(st.session_state.questions, user_answers, all_indices)
        with tab_wrong:
            if wrong_indices:
                _render_review(st.session_state.questions, user_answers, wrong_indices)
            else:
                st.success("🎉 全問正解です！")
    else:
        _render_review(st.session_state.questions, user_answers, all_indices)

    st.markdown("---")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 次の10問へ", use_container_width=True):
            # Check if prefetched questions are available
            holder = st.session_state.prefetch_holder
            thread = st.session_state.prefetch_thread

            if thread is not None:
                thread.join(timeout=0.5)

            if holder.get("questions"):
                st.session_state.questions = holder["questions"]
                st.session_state.current_index = 0
                st.session_state.score = 0
                st.session_state.answered = False
                st.session_state.selected_choice = None
                st.session_state.quiz_finished = False
                st.session_state.user_answers = []
                st.session_state.total_sessions += 1
                # Start prefetching next batch
                _start_prefetch()
                st.rerun()
            else:
                # Generate synchronously
                st.session_state.quiz_finished = False
                st.session_state.generating = True
                with st.spinner("🤖 次の問題を生成中..."):
                    try:
                        questions = _generate_new_quiz()
                        st.session_state.questions = questions
                        st.session_state.current_index = 0
                        st.session_state.score = 0
                        st.session_state.answered = False
                        st.session_state.selected_choice = None
                        st.session_state.generating = False
                        st.session_state.user_answers = []
                        st.session_state.total_sessions += 1
                        _start_prefetch()
                        st.rerun()
                    except Exception as e:
                        st.session_state.generating = False
                        st.error(f"❌ 問題生成に失敗しました: {e}")

    with col2:
        if st.button("🔴 苦手分野を復習", use_container_width=True):
            weak = get_weak_areas(limit=1, user_id=user_id)
            if weak:
                st.session_state.quiz_finished = False
                st.session_state.generating = True
                review_era = weak[0]["era"]
                review_field = weak[0]["field"]
                with st.spinner(f"🤖 {review_era} / {review_field} の問題を生成中..."):
                    try:
                        wrong_qs_text = get_recent_wrong_questions(user_id=user_id)
                        questions = generate_quiz(
                            pdf_text=combined_pdf_text,
                            era=review_era,
                            field=review_field,
                            weak_areas=weak,
                            wrong_questions=wrong_qs_text,
                        )
                        st.session_state.questions = questions
                        st.session_state.current_index = 0
                        st.session_state.score = 0
                        st.session_state.answered = False
                        st.session_state.selected_choice = None
                        st.session_state.generating = False
                        st.session_state.user_answers = []
                        st.session_state.total_sessions += 1
                        _start_prefetch()
                        st.rerun()
                    except Exception as e:
                        st.session_state.generating = False
                        st.error(f"❌ 問題生成に失敗しました: {e}")
            else:
                st.info("まだ苦手分野のデータがありません。もう少し問題を解いてみましょう！")
