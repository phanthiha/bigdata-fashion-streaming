# -*- coding: utf-8 -*-
"""
Ứng dụng Big Data Streaming phân tích độ hài lòng khách hàng (Amazon Fashion).

Hai chế độ:
  1) DEMO      : phát lại dữ liệu mẫu / tải trực tiếp dataset Amazon Fashion,
                 không cần credential -> luôn chạy được trên Streamlit Cloud.
  2) OCI KAFKA : Producer -> OCI Streaming (Kafka API) -> Consumer -> Dashboard,
                 cần secrets (SASL_USERNAME, OCI_AUTH_TOKEN, ...).

Mô hình cảm xúc: cardiffnlp/twitter-roberta-base-sentiment-latest,
tự động fallback sang bộ luật (rating + từ điển) nếu không tải được.
"""

from __future__ import annotations

import gzip
import json
import os
import queue
import re
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401,E402

APP_DIR = Path(__file__).parent
SAMPLE_FILE = APP_DIR / "data" / "sample_events.jsonl"

DATA_URL = (
    "https://mcauleylab.ucsd.edu/public_datasets/data/amazon_v2/"
    "categoryFiles/AMAZON_FASHION.json.gz"
)

st.set_page_config(
    page_title="Big Data Streaming - Amazon Fashion",
    page_icon="📊",
    layout="wide",
)


# ==============================================================
# 1. TIỆN ÍCH CẤU HÌNH (đọc secrets an toàn, không lỗi khi thiếu)
# ==============================================================
def secret(key: str, default: str = "") -> str:
    try:
        if key in st.secrets:
            return str(st.secrets[key])
    except Exception:
        pass
    return os.environ.get(key, default)


BOOTSTRAP_SERVERS = secret(
    "BOOTSTRAP_SERVERS",
    "cell-1.streaming.sa-saopaulo-1.oci.oraclecloud.com:9092",
)
TOPIC = secret("TOPIC", "DemoStreamingFashion")
SASL_USERNAME = secret("SASL_USERNAME", "")
OCI_AUTH_TOKEN = secret("OCI_AUTH_TOKEN", "")



def check_oci_connection(timeout: float = 10.0):
    """Thử lấy metadata từ OCI Streaming để xác nhận cấu hình đã thông."""
    if not (SASL_USERNAME and OCI_AUTH_TOKEN):
        return False, "Chưa có SASL_USERNAME / OCI_AUTH_TOKEN trong Secrets."
    try:
        from confluent_kafka.admin import AdminClient
        import certifi
    except Exception as exc:
        return False, f"Thiếu thư viện confluent-kafka: {exc}"
    try:
        admin = AdminClient({
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": SASL_USERNAME,
            "sasl.password": OCI_AUTH_TOKEN,
            "ssl.ca.location": certifi.where(),
        })
        meta = admin.list_topics(timeout=timeout)
        topics = sorted(meta.topics.keys())
        if TOPIC in meta.topics:
            return True, f"Kết nối OK - tìm thấy topic '{TOPIC}' ({len(topics)} topic khả dụng)."
        return False, (f"Đăng nhập được nhưng không thấy topic '{TOPIC}'. "
                       f"Topic khả dụng: {', '.join(topics[:10]) or 'không có'}")
    except Exception as exc:
        return False, f"Không kết nối được OCI: {exc}"

# ==============================================================
# 2. BỘ PHÂN TÍCH CẢM XÚC (RoBERTa + fallback luật)
# ==============================================================
POS_WORDS = {
    "love", "loved", "great", "perfect", "excellent", "awesome", "nice",
    "good", "beautiful", "comfortable", "recommend", "happy", "best",
    "amazing", "fast", "cute", "quality", "worth", "pretty", "soft",
}
NEG_WORDS = {
    "bad", "poor", "terrible", "awful", "cheap", "broke", "broken",
    "return", "returned", "refund", "disappointed", "disappointing",
    "waste", "small", "tight", "uncomfortable", "fake", "worst", "never",
    "defective", "wrong", "late", "torn",
}


class RuleSentiment:
    """Fallback: chấm điểm bằng rating + từ điển cảm xúc."""

    name = "Rule-based (rating + lexicon)"

    def __call__(self, text):
        tokens = set(re.findall(r"[a-z']+", str(text).lower()))
        pos = len(tokens & POS_WORDS)
        neg = len(tokens & NEG_WORDS)
        if pos > neg:
            label, score = "positive", min(0.5 + 0.1 * (pos - neg), 0.99)
        elif neg > pos:
            label, score = "negative", min(0.5 + 0.1 * (neg - pos), 0.99)
        else:
            label, score = "neutral", 0.5
        return [{"label": label, "score": float(score)}]


@st.cache_resource(show_spinner="Đang tải mô hình RoBERTa (lần đầu ~1-3 phút)...")
def load_sentiment_model(use_transformer: bool):
    """Trả về (callable, tên_mô_hình). Không bao giờ raise."""
    if not use_transformer:
        st.session_state.pop("model_error", None)
        return RuleSentiment(), RuleSentiment.name
    try:
        from transformers import pipeline

        pipe = pipeline(
            "sentiment-analysis",
            model="cardiffnlp/twitter-roberta-base-sentiment-latest",
            truncation=True,
            max_length=128,
        )
        return pipe, "cardiffnlp/twitter-roberta-base-sentiment-latest"
    except Exception as exc:  # thiếu RAM, thiếu torch, mạng lỗi...
        st.session_state["model_error"] = str(exc)
        return RuleSentiment(), RuleSentiment.name + " (fallback)"


# ==============================================================
# 3. LOGIC GÁN NHÃN CẢM XÚC (giữ nguyên quy tắc của notebook)
# ==============================================================
EMOTION_STYLE = {
    "Rất tích cực": ("#052c11", "#a3cfbb"),
    "Tích cực": ("#0f5132", "#d1e7dd"),
    "Trung lập": ("#41464b", "#e2e3e5"),
    "Tiêu cực": ("#842029", "#f8d7da"),
    "Rất tiêu cực": ("#58151c", "#f1aeb5"),
}
CATEGORIES = ["Rất tích cực", "Tích cực", "Trung lập", "Tiêu cực", "Rất tiêu cực"]


def label_emotion(amazon_rating: float, ai_label: str):
    """Kết hợp rating của Amazon với nhãn AI -> nhãn cảm xúc tiếng Việt."""
    ai_label = str(ai_label).lower()

    if amazon_rating <= 2.0 and "pos" in ai_label:
        effective = "negative"          # rating thấp thắng nhãn positive
    elif amazon_rating >= 4.0 and "neg" in ai_label:
        effective = "negative"          # giữ đúng logic bản gốc
    elif "pos" in ai_label:
        effective = "positive"
    elif "neg" in ai_label:
        effective = "negative"
    else:
        effective = "neutral"

    if effective == "positive":
        emotion = "Rất tích cực" if amazon_rating >= 4.5 else "Tích cực"
    elif effective == "negative":
        emotion = "Rất tiêu cực" if amazon_rating <= 1.5 else "Tiêu cực"
    else:
        emotion = "Trung lập"

    t_color, b_color = EMOTION_STYLE[emotion]
    return emotion, t_color, b_color, effective


def build_event(raw_record: dict, analyzer, run_id: str) -> dict:
    amazon_rating = float(raw_record.get("overall", raw_record.get("amazon_rating", 0.0)) or 0.0)
    title = str(
        raw_record.get("summary")
        or raw_record.get("title")
        or raw_record.get("reviewText")
        or raw_record.get("review_text")
        or "Không có tiêu đề"
    ).strip() or "Không có tiêu đề"

    try:
        ai = analyzer(title[:500])[0]
        ai_label, confidence = ai["label"], float(ai.get("score", 0.0))
    except Exception:
        ai_label, confidence = "neutral", 0.0

    emotion, t_color, b_color, effective = label_emotion(amazon_rating, ai_label)

    return {
        "run_id": run_id,
        "review_id": uuid.uuid4().hex[:12],
        "amazon_rating": amazon_rating,
        "title": title,
        "sentiment": effective,
        "emotion": emotion,
        "confidence": round(confidence, 4),
        "t_color": t_color,
        "b_color": b_color,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }


# ==============================================================
# 4. NGUỒN DỮ LIỆU
# ==============================================================
def iter_sample_records(loop: bool = True):
    """Phát lại file dữ liệu mẫu đi kèm repo (không cần mạng)."""
    if not SAMPLE_FILE.exists():
        return
    while True:
        with SAMPLE_FILE.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        yield json.loads(line)
                    except Exception:
                        continue
        if not loop:
            break


def iter_remote_records():
    """Đọc trực tiếp dataset Amazon Fashion (91MB .gz) theo luồng."""
    import requests

    response = requests.get(DATA_URL, stream=True, timeout=60)
    response.raise_for_status()
    with gzip.GzipFile(fileobj=response.raw) as gz:
        for line in gz:
            if not line.strip():
                continue
            try:
                yield json.loads(line.decode("utf-8"))
            except Exception:
                continue


def get_source(source_name: str):
    if source_name == "Dataset gốc Amazon Fashion (tải trực tuyến)":
        return iter_remote_records()
    return iter_sample_records(loop=True)


# ==============================================================
# 5. BIỂU ĐỒ
# ==============================================================
def count_by_category(rows):
    counts = {c: 0 for c in CATEGORIES}
    for r in rows:
        emo = r.get("emotion", "")
        if emo in counts:
            counts[emo] += 1
    return counts


def generate_3d_chart(rows):
    counts = count_by_category(rows)
    y_vals = [counts[c] for c in CATEGORIES]

    fig = plt.figure(figsize=(7, 4.8), facecolor="white")
    ax = fig.add_subplot(projection="3d")

    x = np.arange(len(CATEGORIES))
    zeros = np.zeros(len(CATEGORIES))
    dx = np.ones(len(CATEGORIES)) * 0.4
    dy = np.ones(len(CATEGORIES)) * 0.4

    ax.bar3d(
        x - 0.2, zeros, zeros, dx, dy, y_vals,
        color=["#2b8a3e", "#51cf66", "#ced4da", "#ff6b6b", "#c92a2a"],
        shade=True, edgecolor="none", alpha=0.92,
    )
    ax.view_init(elev=28, azim=-55)
    ax.set_xticks(x)
    ax.set_xticklabels(CATEGORIES, fontsize=8, rotation=30, ha="right")
    ax.set_zlabel("Số lượng", fontsize=9, fontweight="bold")
    ax.set_title("Biểu đồ 3D phân phối cảm xúc", fontsize=11,
                 fontweight="bold", color="#1d3557", pad=12)
    for pane in (ax.xaxis, ax.yaxis, ax.zaxis):
        pane.set_pane_color((0.95, 0.95, 0.95, 0.8))
    fig.subplots_adjust(left=0.02, right=0.98, top=0.88, bottom=0.22)
    return fig


# ==============================================================
# 6. DASHBOARD
# ==============================================================
def render_dashboard(placeholder, rows, stats, elapsed, duration, mode_label):
    counts = count_by_category(rows)
    total = max(len(rows), 1)
    positive_rate = (counts["Rất tích cực"] + counts["Tích cực"]) / total * 100

    with placeholder.container():
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Đã xử lý", f"{stats['generated']:,}")
        c2.metric("Đã gửi Kafka/OCI", f"{stats['delivered']:,}")
        c3.metric("Đã nhận (consumer)", f"{stats['consumed']:,}")
        c4.metric("Tỷ lệ hài lòng", f"{positive_rate:.1f}%")
        c5.metric("Thời gian", f"{min(elapsed, duration):.0f}s / {duration}s")

        st.caption(f"Chế độ: **{mode_label}** · Throughput: "
                   f"{stats['generated'] / max(elapsed, 0.1):.1f} bản ghi/giây")
        st.divider()

        left, right = st.columns([1.4, 1])
        with left:
            st.subheader("7 đánh giá gần nhất")
            recent = rows[-7:][::-1]
            if recent:
                table = pd.DataFrame([
                    {
                        "Rating": f"{r['amazon_rating']:.1f} " +
                                  "⭐" * max(1, min(5, int(round(r["amazon_rating"])))),
                        "Nội dung phản hồi": r["title"][:120],
                        "Cảm xúc (AI)": r["emotion"],
                        "Độ tin cậy": f"{r.get('confidence', 0):.2f}",
                    }
                    for r in recent
                ])
                st.dataframe(table, width="stretch", hide_index=True)
            else:
                st.info("Đang chờ dữ liệu...")

            st.subheader("Phân phối cảm xúc")
            st.bar_chart(pd.DataFrame(
                {"Số lượng": [counts[c] for c in CATEGORIES]}, index=CATEGORIES
            ))

        with right:
            st.subheader("Biểu đồ 3D")
            if rows:
                fig = generate_3d_chart(rows)
                st.pyplot(fig, width="stretch")
                plt.close(fig)
            else:
                st.info("Chưa có dữ liệu để vẽ biểu đồ.")


# ==============================================================
# 7. CHẾ ĐỘ DEMO (không cần credential)
# ==============================================================
def run_demo(source_name, analyzer, duration, max_events, delay, placeholder, mode_label):
    stats = {"generated": 0, "delivered": 0, "consumed": 0, "failed": 0}
    rows, run_id = [], uuid.uuid4().hex[:8]
    started, last_render = time.monotonic(), 0.0

    try:
        source = get_source(source_name)
    except Exception as exc:
        st.error(f"Không đọc được nguồn dữ liệu: {exc}")
        return pd.DataFrame()

    for raw in source:
        elapsed = time.monotonic() - started
        if elapsed >= duration or stats["generated"] >= max_events:
            break

        event = build_event(raw, analyzer, run_id)
        rows.append(event)
        stats["generated"] += 1
        stats["consumed"] += 1

        now = time.monotonic()
        if now - last_render >= 0.5:
            render_dashboard(placeholder, rows, stats, now - started,
                             duration, mode_label)
            last_render = now
        if delay:
            time.sleep(delay)

    render_dashboard(placeholder, rows, stats, time.monotonic() - started,
                     duration, mode_label)
    return pd.DataFrame(rows)


# ==============================================================
# 8. CHẾ ĐỘ OCI STREAMING (Producer -> Kafka -> Consumer)
# ==============================================================
def run_oci_stream(source_name, analyzer, duration, max_events, placeholder, mode_label):
    try:
        from confluent_kafka import Consumer, Producer
        import certifi
    except Exception as exc:
        st.error(f"Thiếu thư viện confluent-kafka: {exc}")
        return pd.DataFrame()

    if not SASL_USERNAME or not OCI_AUTH_TOKEN:
        st.error(
            "Chưa cấu hình SASL_USERNAME / OCI_AUTH_TOKEN trong Streamlit Secrets. "
            "Xem file .streamlit/secrets.toml.example."
        )
        return pd.DataFrame()

    run_id = uuid.uuid4().hex[:8]
    common = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": SASL_USERNAME,
        "sasl.password": OCI_AUTH_TOKEN,
        "ssl.ca.location": certifi.where(),
    }
    producer_conf = {**common, "client.id": f"prod_{run_id}",
                     "linger.ms": 10, "acks": "1"}
    consumer_conf = {**common, "client.id": f"cons_{run_id}",
                     "group.id": f"fashion_stream_{run_id}",
                     "auto.offset.reset": "latest", "enable.auto.commit": True}

    stats = {"generated": 0, "delivered": 0, "consumed": 0, "failed": 0,
             "error": ""}
    lock = threading.Lock()
    local_queue: "queue.Queue[bytes]" = queue.Queue()
    stop_event = threading.Event()
    state = {"fallback": False}

    def delivery_report(err, _msg):
        with lock:
            if err:
                stats["failed"] += 1
                stats["error"] = str(err)
            else:
                stats["delivered"] += 1

    def producer_worker(producer):
        try:
            for raw in get_source(source_name):
                if stop_event.is_set() or stats["generated"] >= max_events:
                    break
                event = build_event(raw, analyzer, run_id)
                payload = json.dumps(event, ensure_ascii=False).encode("utf-8")
                local_queue.put(payload)
                if not state["fallback"]:
                    try:
                        producer.produce(TOPIC, value=payload,
                                         on_delivery=delivery_report)
                        producer.poll(0)
                    except Exception as exc:
                        with lock:
                            stats["error"] = str(exc)
                        state["fallback"] = True
                with lock:
                    stats["generated"] += 1
        except Exception as exc:
            with lock:
                stats["error"] = f"Lỗi nguồn dữ liệu: {exc}"
        finally:
            try:
                producer.flush(5)
            except Exception:
                pass

    try:
        producer = Producer(producer_conf)
        consumer = Consumer(consumer_conf)
        consumer.subscribe([TOPIC])
    except Exception as exc:
        st.error(f"Không kết nối được OCI Streaming: {exc}")
        return pd.DataFrame()

    thread = threading.Thread(target=producer_worker, args=(producer,), daemon=True)
    thread.start()

    rows = []
    started, last_render = time.monotonic(), 0.0
    try:
        while time.monotonic() - started < duration and stats["consumed"] < max_events:
            elapsed = time.monotonic() - started

            # Sau 12 giây chưa gửi được bản ghi nào -> chuyển sang local queue
            if not state["fallback"] and elapsed > 12:
                with lock:
                    if stats["delivered"] == 0:
                        state["fallback"] = True

            payload = None
            if not state["fallback"]:
                message = consumer.poll(0.1)
                if message is not None and not message.error():
                    payload = message.value()
            else:
                try:
                    payload = local_queue.get(timeout=0.05)
                except queue.Empty:
                    payload = None

            if payload:
                try:
                    event = json.loads(payload.decode("utf-8"))
                    if event.get("run_id") == run_id:
                        rows.append(event)
                        stats["consumed"] += 1
                except Exception:
                    pass

            now = time.monotonic()
            if now - last_render >= 0.5:
                label = mode_label + (" · fallback nội bộ" if state["fallback"] else "")
                render_dashboard(placeholder, rows, stats, now - started,
                                 duration, label)
                last_render = now
    finally:
        stop_event.set()
        thread.join(timeout=2)
        try:
            consumer.close()
        except Exception:
            pass

    if stats.get("error"):
        st.warning(f"Ghi nhận lỗi từ Kafka/OCI: {stats['error']}")
    render_dashboard(placeholder, rows, stats, time.monotonic() - started,
                     duration, mode_label)
    return pd.DataFrame(rows)


# ==============================================================
# 9. GIAO DIỆN CHÍNH
# ==============================================================
st.title("📊 Big Data Streaming – Phân tích độ hài lòng khách hàng")
st.caption("Amazon Fashion → Phân tích cảm xúc (RoBERTa) → OCI Streaming (Kafka) → Dashboard thời gian thực")

with st.sidebar:
    st.header("⚙️ Cấu hình")

    mode = st.radio(
        "Chế độ chạy",
        ["Demo (không cần credential)", "OCI Streaming (Kafka)"],
        help="Demo phát lại dữ liệu ngay trong ứng dụng. "
             "OCI Streaming gửi/nhận qua Kafka của Oracle Cloud.",
    )

    source_name = st.selectbox(
        "Nguồn dữ liệu",
        ["Dữ liệu mẫu đi kèm (100 bản ghi, lặp lại)",
         "Dataset gốc Amazon Fashion (tải trực tuyến)"],
    )

    use_transformer = st.toggle(
        "Dùng mô hình RoBERTa", value=True,
        help="Tắt để chạy bằng bộ luật nhẹ (nhanh, không cần torch).",
    )

    duration = st.slider("Thời lượng chạy (giây)", 10, 600, 60, step=10)
    max_events = st.slider("Số bản ghi tối đa", 20, 3000, 300, step=20)
    delay = st.slider("Độ trễ giữa 2 bản ghi (giây)", 0.0, 1.0, 0.05, step=0.05)

    st.divider()
    oci_ready = bool(SASL_USERNAME and OCI_AUTH_TOKEN)
    st.write("**Trạng thái OCI:**",
             "✅ đã cấu hình" if oci_ready else "⚠️ chưa cấu hình")
    st.caption(f"Topic: `{TOPIC}`")
    st.caption(f"Broker: `{BOOTSTRAP_SERVERS}`")
    if st.button("🔌 Kiểm tra kết nối OCI", disabled=not oci_ready,
                 help="Gọi metadata của broker để xác nhận username/token và topic."):
        with st.spinner("Đang kiểm tra kết nối tới OCI Streaming..."):
            st.session_state["oci_check"] = check_oci_connection()
    if not oci_ready:
        st.caption("Thêm SASL_USERNAME và OCI_AUTH_TOKEN vào Streamlit Secrets "
                   "(Manage app → Settings → Secrets) để bật chế độ OCI.")

analyzer, model_name = load_sentiment_model(use_transformer)
st.info(f"Mô hình đang dùng: **{model_name}**")
if st.session_state.get("model_error"):
    st.warning(
        "Không tải được RoBERTa nên ứng dụng đã tự chuyển sang bộ luật. "
        f"Chi tiết: {st.session_state['model_error'][:200]}"
    )


oci_check = st.session_state.get("oci_check")
if oci_check:
    ok_check, message_check = oci_check
    (st.success if ok_check else st.error)(f"Kiểm tra kết nối OCI: {message_check}")

col_run, col_stop = st.columns([1, 5])
start = col_run.button("▶️ Bắt đầu streaming", type="primary", width="stretch")

placeholder = st.empty()

if start:
    mode_label = "Demo nội bộ" if mode.startswith("Demo") else "OCI Streaming (Kafka)"
    with st.spinner("Đang chạy luồng dữ liệu..."):
        if mode.startswith("Demo"):
            df = run_demo(source_name, analyzer, duration, max_events,
                          delay, placeholder, mode_label)
        else:
            df = run_oci_stream(source_name, analyzer, duration, max_events,
                                placeholder, mode_label)

    if not df.empty:
        st.session_state["last_result"] = df
        st.success(f"Hoàn tất: đã xử lý {len(df):,} bản ghi.")

if "last_result" in st.session_state and not st.session_state["last_result"].empty:
    df = st.session_state["last_result"]
    st.divider()
    st.subheader("Kết quả chi tiết")
    export_cols = [c for c in
                   ["timestamp_utc", "amazon_rating", "title", "sentiment",
                    "emotion", "confidence"] if c in df.columns]
    st.dataframe(df[export_cols], width="stretch", hide_index=True)
    st.download_button(
        "⬇️ Tải kết quả (CSV)",
        df[export_cols].to_csv(index=False).encode("utf-8-sig"),
        file_name="ket_qua_sentiment.csv",
        mime="text/csv",
    )
else:
    st.caption("Chọn cấu hình ở thanh bên rồi bấm **Bắt đầu streaming**.")
