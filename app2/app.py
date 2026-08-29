# -*- coding: utf-8 -*-
"""
Hệ thống phân tích nguồn gốc và phân lớp dữ liệu thời gian thực (Wikimedia).

Luồng: Wikimedia EventStreams (SSE) -> phân lớp thực thể -> [OCI Streaming/Kafka] -> Dashboard.

Ba chế độ:
  1) SSE trực tiếp   : đọc thẳng https://stream.wikimedia.org (không cần credential).
  2) Demo ngoại tuyến: phát lại dữ liệu mẫu đi kèm khi mạng bị chặn.
  3) OCI Kafka       : Producer đẩy sự kiện lên OCI Streaming, Consumer đọc về.

Chuyển thể từ notebook Colab: Hà_Big_Data_Streaming_Wikipedia.ipynb
"""

from __future__ import annotations

import json
import os
import queue
import re
import threading
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import streamlit as st

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

APP_DIR = Path(__file__).parent
SAMPLE_FILE = APP_DIR / "data" / "sample_wiki_events.jsonl"
WIKI_SSE_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

st.set_page_config(
    page_title="Wikimedia Streaming - Phân lớp thực thể",
    page_icon="🌐",
    layout="wide",
)


# ==============================================================
# 1. CẤU HÌNH
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



def test_produce(timeout: float = 15.0):
    """Gửi thử 1 bản ghi lên OCI và chờ xác nhận -> chẩn đoán quyền publish."""
    if not (SASL_USERNAME and OCI_AUTH_TOKEN):
        return False, "Chưa cấu hình SASL_USERNAME / OCI_AUTH_TOKEN."
    try:
        from confluent_kafka import Producer
        import certifi
    except Exception as exc:
        return False, f"Thiếu thư viện confluent-kafka: {exc}"

    topic, _note = resolve_topic()
    result = {}

    def _cb(err, msg):
        result["err"] = err
        result["msg"] = msg

    try:
        probe = Producer({
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": SASL_USERNAME,
            "sasl.password": OCI_AUTH_TOKEN,
            "ssl.ca.location": certifi.where(),
            "client.id": "probe",
            "acks": "1",
            "message.timeout.ms": int(timeout * 1000),
        })
        probe.produce(topic, value=json.dumps({"probe": True}).encode("utf-8"),
                      on_delivery=_cb)
        probe.flush(timeout)
    except Exception as exc:
        return False, f"Lỗi khi gửi thử: {exc}"

    if "err" not in result:
        return False, (f"Không nhận được phản hồi sau {timeout:.0f}s - bản ghi vẫn nằm "
                       f"trong hàng đợi của client (topic '{topic}').")
    if result["err"]:
        return False, f"Broker từ chối bản ghi: {result['err']} (topic '{topic}')."
    msg = result["msg"]
    return True, (f"Gửi thành công 1 bản ghi lên topic '{topic}' "
                  f"(partition {msg.partition()}, offset {msg.offset()}).")


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
# 2. THUẬT TOÁN PHÂN LỚP THỰC THỂ (giữ nguyên quy tắc notebook)
# ==============================================================
ACTOR_CLASSES = [
    "Tác tử tự động",
    "Thực thể ẩn danh",
    "Tác vụ hệ thống",
    "Thực thể định danh",
]
ACTOR_COLORS = {
    "Tác tử tự động": ("#8E24AA", "#F3E5F5"),
    "Thực thể ẩn danh": ("#F39C12", "#FFF8E1"),
    "Tác vụ hệ thống": ("#E53935", "#FFEBEE"),
    "Thực thể định danh": ("#1E88E5", "#E3F2FD"),
}
IPV4_RE = re.compile(r"^(\d{1,3}\.){3}\d{1,3}$")
SYSTEM_KEYWORDS = ("admin", "sysop", "script", "tool", "bot")


def classify_actor(event: dict):
    """Phân lớp đối tượng phát sinh sự kiện -> (nhãn, màu chữ, màu nền)."""
    user_id = str(event.get("user", ""))

    if event.get("bot", False):
        actor = "Tác tử tự động"
    elif IPV4_RE.match(user_id) or ":" in user_id:
        actor = "Thực thể ẩn danh"
    elif any(kw in user_id.lower() for kw in SYSTEM_KEYWORDS):
        actor = "Tác vụ hệ thống"
    else:
        actor = "Thực thể định danh"

    t_color, b_color = ACTOR_COLORS[actor]
    return actor, t_color, b_color


def build_event(raw: dict, run_id: str) -> dict:
    actor, t_color, b_color = classify_actor(raw)
    title = str(raw.get("title", "N/A"))
    user = str(raw.get("user", "N/A"))
    wiki = str(raw.get("wiki") or raw.get("server_name") or "n/a")

    return {
        "run_id": run_id,
        "event_type": raw.get("type", "edit"),
        "wiki": wiki,
        "page": title,
        "user": user,
        "actor": actor,
        "t_color": t_color,
        "b_color": b_color,
        "length_new": ((raw.get("length") or {}) or {}).get("new"),
        "title": f"Thực thi cập nhật tại: [{title}] bởi định danh: {user}"[:180],
        "received_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "ts": time.time(),
    }



def resolve_topic(timeout: float = 10.0):
    """Trả về (topic sẽ dùng, ghi chú). Tự chọn topic khả dụng nếu tên trong cấu hình không tồn tại."""
    try:
        from confluent_kafka.admin import AdminClient
        import certifi

        meta = AdminClient({
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": SASL_USERNAME,
            "sasl.password": OCI_AUTH_TOKEN,
            "ssl.ca.location": certifi.where(),
        }).list_topics(timeout=timeout)
    except Exception as exc:
        return TOPIC, f"Không đọc được danh sách topic từ OCI: {exc}"

    if TOPIC in meta.topics:
        return TOPIC, ""

    candidates = sorted(t for t in meta.topics if not t.startswith("__"))
    if not candidates:
        return TOPIC, ""

    best = min(candidates, key=lambda t: (0 if (t in TOPIC or TOPIC in t) else 1, len(t)))
    return best, (f"Không tìm thấy topic '{TOPIC}' trên stream pool. "
                  f"Đang dùng topic '{best}'. Sửa TOPIC trong Streamlit Secrets để dùng lâu dài.")

# ==============================================================
# 3. NGUỒN DỮ LIỆU
# ==============================================================
def iter_sse_events(stop_event: threading.Event, status: dict):
    """Đọc Wikimedia EventStreams (SSE) bằng requests, tự kết nối lại khi lỗi."""
    headers = {
        "Accept": "text/event-stream",
        "User-Agent": "Academic Research Application / 1.0",
    }
    while not stop_event.is_set():
        try:
            with requests.get(WIKI_SSE_URL, headers=headers,
                              stream=True, timeout=30) as response:
                response.raise_for_status()
                status["message"] = "Luồng dữ liệu duy trì tính ổn định"
                for line in response.iter_lines(decode_unicode=True):
                    if stop_event.is_set():
                        return
                    if not line or not line.startswith("data: "):
                        continue
                    try:
                        event = json.loads(line[6:])
                    except Exception:
                        continue
                    if event.get("type") in ("edit", "new", "log"):
                        yield event
        except Exception as exc:
            status["message"] = f"Thiết lập lại kết nối ({type(exc).__name__})..."
            time.sleep(3)


def iter_sample_events(stop_event: threading.Event, status: dict, delay: float = 0.15):
    """Phát lại dữ liệu mẫu khi không có mạng ra Wikimedia."""
    status["message"] = "Đang phát lại dữ liệu mẫu ngoại tuyến"
    if not SAMPLE_FILE.exists():
        status["message"] = "Không tìm thấy file dữ liệu mẫu"
        return
    while not stop_event.is_set():
        with SAMPLE_FILE.open("r", encoding="utf-8") as fh:
            for line in fh:
                if stop_event.is_set():
                    return
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except Exception:
                    continue
                time.sleep(delay)


# ==============================================================
# 4. BIỂU ĐỒ
# ==============================================================
def bar_chart_by_class(rows):
    counts = Counter(r["actor"] for r in rows)
    values = [counts.get(c, 0) for c in ACTOR_CLASSES]
    labels = ["Tác tử\ntự động", "Thực thể\nẩn danh", "Tác vụ\nhệ thống", "Thực thể\nđịnh danh"]
    colors = ["#8E24AA", "#FFB300", "#E53935", "#1E88E5"]

    fig, ax = plt.subplots(figsize=(6.5, 4.2), facecolor="white")
    bars = ax.bar(np.arange(len(labels)), values, 0.55, color=colors,
                  edgecolor="white", linewidth=2, alpha=0.95)
    for bar in bars:
        height = bar.get_height()
        ax.annotate(f"{height}", xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 6), textcoords="offset points", ha="center",
                    va="bottom", fontsize=11, fontweight="900", color="#2c3e50")

    ax.set_xticks(np.arange(len(labels)))
    ax.set_xticklabels(labels, fontsize=9, fontweight="bold", color="#34495e")
    ax.set_ylabel("Tần suất phát sinh sự kiện", fontsize=10, fontweight="bold",
                  color="#2c3e50", labelpad=10)
    ax.set_title("Phân bố tần suất theo phân lớp thực thể", fontsize=12,
                 fontweight="bold", color="#1a252f", pad=18)
    ax.yaxis.grid(True, linestyle="--", alpha=0.4, color="#95a5a6")
    ax.set_axisbelow(True)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color("#bdc3c7")
    ax.tick_params(axis="y", length=0)
    fig.subplots_adjust(left=0.12, right=0.96, top=0.86, bottom=0.16)
    return fig


def timeline_frame(rows):
    """Số sự kiện theo từng giây, tính từ sự kiện đầu tiên của phiên."""
    if not rows:
        return pd.DataFrame({"Sự kiện": []})
    base = rows[0]["ts"]
    counts = Counter(max(0, int(r["ts"] - base)) for r in rows)
    span = min(max(counts), 3600)
    idx = list(range(span + 1))
    return pd.DataFrame({"Sự kiện": [counts.get(s, 0) for s in idx]}, index=idx)


def update_progress(bar, consumed, max_events, elapsed, duration):
    """Cập nhật thanh tiến trình theo cả số sự kiện lẫn thời gian còn lại."""
    fraction = min(max(consumed / max(max_events, 1), elapsed / max(duration, 1)), 1.0)
    remaining = max(duration - elapsed, 0)
    bar.progress(
        fraction,
        text=(f"Đã nhận {consumed:,}/{max_events:,} sự kiện · "
              f"{elapsed:.0f}s/{duration}s · còn ~{remaining:.0f}s"),
    )


# ==============================================================
# 5. DASHBOARD
# ==============================================================
def render_dashboard(placeholder, rows, stats, started_at, duration, status_msg, mode_label):
    elapsed = max(time.monotonic() - started_at, 0.001)
    counts = Counter(r["actor"] for r in rows)
    total = max(len(rows), 1)
    bot_rate = counts.get("Tác tử tự động", 0) / total * 100
    anon_rate = counts.get("Thực thể ẩn danh", 0) / total * 100

    with placeholder.container():
        st.caption(f"**Chế độ:** {mode_label} · **Trạng thái:** {status_msg}")

        if stats.get("topic"):
            st.caption(f"Topic đang dùng: `{stats['topic']}`")
        if stats.get("error"):
            st.warning(f"Kafka/OCI: {stats['error']}")

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Sự kiện thu thập", f"{stats['generated']:,}")
        c2.metric("Sự kiện xử lý", f"{stats['consumed']:,}")
        c3.metric("Lưu lượng", f"{stats['consumed'] / elapsed:.1f} evt/s")
        c4.metric("Tỷ lệ bot", f"{bot_rate:.1f}%")
        c5.metric("Thời gian", f"{min(elapsed, duration):.0f}s / {duration}s")

        if stats.get("delivered"):
            st.caption(f"Đã gửi Kafka/OCI: {stats['delivered']:,} · lỗi: {stats['failed']:,}")

        st.divider()
        left, right = st.columns([1.3, 1])

        with left:
            st.subheader("Bản ghi thời gian thực")
            recent = rows[-8:][::-1]
            if recent:
                st.dataframe(
                    pd.DataFrame([
                        {
                            "Thời điểm": r["received_at"][11:19],
                            "Wiki": r["wiki"],
                            "Nội dung biến đổi": r["title"],
                            "Phân lớp": r["actor"],
                        } for r in recent
                    ]),
                    width="stretch", hide_index=True,
                )
            else:
                st.info("Hệ thống đang tiến hành thu thập mẫu dữ liệu...")

            st.subheader("Nhịp luồng dữ liệu (sự kiện/giây)")
            st.line_chart(timeline_frame(rows))

        with right:
            st.subheader("Phân lớp thực thể")
            if rows:
                fig = bar_chart_by_class(rows)
                st.pyplot(fig, width="stretch")
                plt.close(fig)
            else:
                st.info("Chưa có dữ liệu để vẽ biểu đồ.")

            st.caption(f"Thực thể ẩn danh chiếm {anon_rate:.1f}% tổng sự kiện")

        st.divider()
        top_left, top_right = st.columns(2)
        with top_left:
            st.subheader("Top trang bị sửa nhiều nhất")
            pages = Counter(r["page"] for r in rows).most_common(8)
            if pages:
                st.dataframe(pd.DataFrame(pages, columns=["Trang", "Số lần"]),
                             width="stretch", hide_index=True)
            else:
                st.caption("—")

        with top_right:
            st.subheader("Top thực thể hoạt động nhiều nhất")
            users = Counter(f"{r['user']} ({r['actor']})" for r in rows).most_common(8)
            if users:
                st.dataframe(pd.DataFrame(users, columns=["Thực thể", "Số sự kiện"]),
                             width="stretch", hide_index=True)
            else:
                st.caption("—")


# ==============================================================
# 6. VÒNG CHẠY: SSE TRỰC TIẾP / DEMO NGOẠI TUYẾN
# ==============================================================
def source_iterator(use_live, stop_event, status):
    return iter_sse_events(stop_event, status) if use_live \
        else iter_sample_events(stop_event, status)


def wiki_matches(event: dict, wiki_filter: str) -> bool:
    if not wiki_filter:
        return True
    haystack = f"{event.get('wiki', '')} {event.get('server_name', '')}".lower()
    return wiki_filter.lower() in haystack


def run_stream(use_live, duration, max_events, wiki_filter, placeholder, mode_label):
    run_id = uuid.uuid4().hex[:8]
    stats = {"generated": 0, "consumed": 0, "delivered": 0, "failed": 0}
    status = {"message": "Khởi tạo luồng kết nối dữ liệu máy chủ..."}
    stop_event = threading.Event()
    events_queue: "queue.Queue[dict]" = queue.Queue(maxsize=5000)

    def worker():
        for raw in source_iterator(use_live, stop_event, status):
            if stop_event.is_set():
                break
            if not wiki_matches(raw, wiki_filter):
                continue
            try:
                events_queue.put(build_event(raw, run_id), timeout=1)
                stats["generated"] += 1
            except queue.Full:
                continue

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()

    rows, started_at, last_render = [], time.monotonic(), 0.0
    progress_slot.progress(0.0, text="Đang kết nối nguồn dữ liệu...")
    try:
        while time.monotonic() - started_at < duration and stats["consumed"] < max_events:
            try:
                event = events_queue.get(timeout=0.2)
                rows.append(event)
                stats["consumed"] += 1
                if len(rows) > 2000:
                    rows.pop(0)
            except queue.Empty:
                pass

            now = time.monotonic()
            if now - last_render >= 0.5:
                render_dashboard(placeholder, rows, stats, started_at, duration,
                                 status["message"], mode_label)
                update_progress(progress_slot, stats["consumed"], max_events,
                                now - started_at, duration)
                last_render = now
    finally:
        stop_event.set()

    render_dashboard(placeholder, rows, stats, started_at, duration,
                     status["message"], mode_label)
    progress_slot.progress(1.0, text=f"Hoàn tất: {len(rows):,} sự kiện "
                                f"trong {time.monotonic() - started_at:.0f} giây")
    return pd.DataFrame(rows)


# ==============================================================
# 7. VÒNG CHẠY: OCI STREAMING (Producer -> Kafka -> Consumer)
# ==============================================================
def run_kafka_stream(use_live, duration, max_events, wiki_filter, placeholder, mode_label):
    try:
        from confluent_kafka import Consumer, Producer
        import certifi
    except Exception as exc:
        st.error(f"Thiếu thư viện confluent-kafka: {exc}")
        return pd.DataFrame()

    if not SASL_USERNAME or not OCI_AUTH_TOKEN:
        st.error("Chưa cấu hình SASL_USERNAME / OCI_AUTH_TOKEN trong Streamlit Secrets "
                 "(xem .streamlit/secrets.toml.example ở thư mục gốc repo).")
        return pd.DataFrame()

    active_topic, topic_note = resolve_topic()
    if topic_note:
        st.warning(topic_note)

    run_id = uuid.uuid4().hex[:8]
    common = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": SASL_USERNAME,
        "sasl.password": OCI_AUTH_TOKEN,
        "ssl.ca.location": certifi.where(),
    }

    stats = {"generated": 0, "consumed": 0, "delivered": 0, "failed": 0,
             "error": "", "topic": active_topic}
    status = {"message": "Khởi tạo kết nối OCI Streaming..."}
    lock = threading.Lock()
    stop_event = threading.Event()
    local_queue: "queue.Queue[bytes]" = queue.Queue()
    state = {"fallback": False, "produce": True}

    def delivery_report(err, _msg):
        with lock:
            if err:
                stats["failed"] += 1
                status["message"] = f"Ngoại lệ: {err}"
            else:
                stats["delivered"] += 1

    def worker(producer):
        for raw in source_iterator(use_live, stop_event, status):
            if stop_event.is_set() or stats["generated"] >= max_events:
                break
            if not wiki_matches(raw, wiki_filter):
                continue
            payload = json.dumps(build_event(raw, run_id), ensure_ascii=False).encode("utf-8")
            local_queue.put(payload)
            # Vẫn gửi lên Kafka kể cả khi phần đọc đã chuyển sang hàng đợi nội bộ.
            if state["produce"]:
                try:
                    producer.produce(active_topic, value=payload, on_delivery=delivery_report)
                    producer.poll(0)
                except BufferError:
                    producer.poll(0.2)
                except Exception as exc:
                    with lock:
                        stats["error"] = str(exc)
                        status["message"] = f"Ngừng gửi lên OCI ({exc})"
                    state["produce"] = False
            with lock:
                stats["generated"] += 1
        try:
            producer.flush(10)
        except Exception:
            pass

    try:
        producer = Producer({**common, "client.id": f"prod_{run_id}",
                             "linger.ms": 10, "acks": "1"})
        consumer = Consumer({**common, "client.id": f"cons_{run_id}",
                             "group.id": f"wiki_actor_{run_id}",
                             "auto.offset.reset": "latest",
                             "enable.auto.commit": True})
        consumer.subscribe([active_topic])
    except Exception as exc:
        st.error(f"Không kết nối được OCI Streaming: {exc}")
        return pd.DataFrame()

    thread = threading.Thread(target=worker, args=(producer,), daemon=True)
    thread.start()

    rows, started_at, last_render = [], time.monotonic(), 0.0
    progress_slot.progress(0.0, text="Đang kết nối nguồn dữ liệu...")
    try:
        while time.monotonic() - started_at < duration and stats["consumed"] < max_events:
            elapsed = time.monotonic() - started_at

            # Sau 20 giây consumer chưa nhận được bản ghi nào (thường do
            # group rebalance) thì đọc hàng đợi nội bộ; producer vẫn gửi lên OCI.
            if not state["fallback"] and elapsed > 20:
                with lock:
                    if stats["consumed"] == 0:
                        state["fallback"] = True
                        status["message"] = ("Consumer chưa nhận được bản ghi sau 20 giây "
                                             "- đọc từ hàng đợi nội bộ")

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
                        if len(rows) > 2000:
                            rows.pop(0)
                except Exception:
                    pass

            now = time.monotonic()
            if now - last_render >= 0.5:
                render_dashboard(placeholder, rows, stats, started_at, duration,
                                 status["message"], mode_label)
                update_progress(progress_slot, stats["consumed"], max_events,
                                now - started_at, duration)
                last_render = now
    finally:
        stop_event.set()
        thread.join(timeout=6)
        try:
            producer.flush(5)
        except Exception:
            pass
        try:
            consumer.close()
        except Exception:
            pass

    render_dashboard(placeholder, rows, stats, started_at, duration,
                     status["message"], mode_label)
    progress_slot.progress(1.0, text=f"Hoàn tất: {len(rows):,} sự kiện "
                                f"trong {time.monotonic() - started_at:.0f} giây")
    return pd.DataFrame(rows)


# ==============================================================
# 8. GIAO DIỆN CHÍNH
# ==============================================================
st.title("🌐 Phân tích nguồn gốc và phân lớp dữ liệu thời gian thực")
st.caption("Wikimedia EventStreams (SSE) → Phân lớp thực thể → OCI Streaming (Kafka) → Dashboard")

st.markdown(
    "> Dạ em chào Thầy! Em là **Phan Thị Hà** — MSHV: **C25611256**."
)

with st.sidebar:
    st.header("⚙️ Cấu hình")

    mode = st.radio(
        "Chế độ chạy",
        ["SSE trực tiếp (Wikimedia)", "Demo ngoại tuyến (dữ liệu mẫu)",
         "OCI Streaming (Kafka)"],
        help="SSE trực tiếp đọc thẳng luồng thay đổi của Wikipedia. "
             "Demo ngoại tuyến dùng khi mạng bị chặn.",
    )

    wiki_filter = st.text_input(
        "Lọc theo wiki (để trống = tất cả)",
        value="",
        placeholder="viwiki, enwiki, commonswiki...",
        help="Khớp chuỗi con với mã wiki hoặc tên miền của sự kiện.",
    ).strip()

    duration = st.slider("Thời lượng chạy (giây)", 10, 600, 60, step=10)
    max_events = st.slider("Số sự kiện tối đa", 50, 5000, 500, step=50)

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
    if st.button("📤 Gửi thử 1 bản ghi lên OCI", disabled=not oci_ready,
                 help="Kiểm tra quyền publish: gửi 1 bản ghi và chờ xác nhận từ broker."):
        with st.spinner("Đang gửi thử lên OCI Streaming..."):
            st.session_state["oci_check"] = test_produce()
    if not oci_ready:
        st.caption("Thêm SASL_USERNAME và OCI_AUTH_TOKEN vào Streamlit Secrets "
                   "(Manage app → Settings → Secrets) để bật chế độ OCI.")
    st.caption(f"Nguồn SSE: `{WIKI_SSE_URL}`")

    st.divider()
    st.markdown("**Học viên thực hiện**")
    st.markdown("Phan Thị Hà  \nMSHV: `C25611256`")


oci_check = st.session_state.get("oci_check")
if oci_check:
    ok_check, message_check = oci_check
    (st.success if ok_check else st.error)(f"Kiểm tra kết nối OCI: {message_check}")

start = st.button("▶️ Bắt đầu giám sát", type="primary")
progress_slot = st.empty()
placeholder = st.empty()

if start:
    use_live = not mode.startswith("Demo")
    if mode.startswith("OCI"):
        df = run_kafka_stream(use_live, duration, max_events, wiki_filter,
                              placeholder, "OCI Streaming (Kafka)")
    else:
        label = "SSE trực tiếp" if use_live else "Demo ngoại tuyến"
        df = run_stream(use_live, duration, max_events, wiki_filter, placeholder, label)

    if df.empty:
        st.warning(
            "Không thu được sự kiện nào. Nếu đang chạy 'SSE trực tiếp' mà mạng chặn "
            "stream.wikimedia.org, hãy chuyển sang 'Demo ngoại tuyến'."
        )
    else:
        st.session_state["last_result_app2"] = df
        st.success(f"Hoàn tất: đã xử lý {len(df):,} sự kiện.")

if "last_result_app2" in st.session_state and not st.session_state["last_result_app2"].empty:
    df = st.session_state["last_result_app2"]
    st.divider()
    st.subheader("Kết quả chi tiết")
    cols = [c for c in ["received_at", "wiki", "event_type", "page", "user", "actor"]
            if c in df.columns]
    export_df = df[cols]

    actor_counts = export_df["actor"].value_counts() if "actor" in export_df else pd.Series(dtype=int)
    wiki_top = export_df["wiki"].value_counts().index[0] if "wiki" in export_df and len(export_df) else "—"
    if "received_at" in export_df and len(export_df):
        span = f"{export_df['received_at'].iloc[0][11:19]} → {export_df['received_at'].iloc[-1][11:19]} UTC"
    else:
        span = "—"

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Số dòng dữ liệu", f"{len(export_df):,}")
    m2.metric("Số cột", f"{len(cols)}")
    m3.metric("Khoảng thời gian", span)
    m4.metric("Wiki nhiều nhất", wiki_top)

    left_sum, right_sum = st.columns([1, 1])
    with left_sum:
        st.caption("Phân bổ phân lớp thực thể trong file")
        if len(actor_counts):
            st.bar_chart(actor_counts.rename("Số dòng"))
    with right_sum:
        st.caption("Xem trước 5 dòng đầu của file CSV")
        st.dataframe(export_df.head(5), width="stretch", hide_index=True)

    st.download_button(
        f"⬇️ Tải kết quả ({len(export_df):,} dòng, CSV)",
        export_df.to_csv(index=False).encode("utf-8-sig"),
        file_name="wikimedia_actor_events.csv",
        mime="text/csv",
        type="primary",
    )

    with st.expander(f"Xem toàn bộ {len(export_df):,} dòng"):
        st.dataframe(export_df, width="stretch", hide_index=True)
else:
    st.caption("Chọn cấu hình ở thanh bên rồi bấm **Bắt đầu giám sát**.")
