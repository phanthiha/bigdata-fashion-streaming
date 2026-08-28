# 🌐 App2 – Phân tích nguồn gốc và phân lớp dữ liệu thời gian thực (Wikimedia)

Chuyển thể từ notebook Colab `Hà_Big_Data_Streaming_Wikipedia.ipynb`.

```
Wikimedia EventStreams (SSE)  →  Phân lớp thực thể  →  [OCI Streaming / Kafka]  →  Dashboard
```

## Ba chế độ

| Chế độ | Mô tả |
|---|---|
| **SSE trực tiếp** | Đọc thẳng `https://stream.wikimedia.org/v2/stream/recentchange`, không cần credential |
| **Demo ngoại tuyến** | Phát lại `data/sample_wiki_events.jsonl` khi mạng chặn Wikimedia (dữ liệu mẫu mô phỏng) |
| **OCI Streaming** | Producer → OCI Streaming (Kafka API) → Consumer, tự fallback hàng đợi nội bộ sau 12 giây |

## Thuật toán phân lớp thực thể

Giữ nguyên quy tắc của notebook, xét theo thứ tự:

1. `bot = true` → **Tác tử tự động** (tím)
2. Tên người dùng là địa chỉ IPv4/IPv6 → **Thực thể ẩn danh** (vàng)
3. Tên chứa `admin`/`sysop`/`script`/`tool`/`bot` → **Tác vụ hệ thống** (đỏ)
4. Còn lại → **Thực thể định danh** (xanh)

## Dashboard

5 chỉ số vận hành (số sự kiện, lưu lượng evt/s, tỷ lệ bot, thời gian), bảng bản ghi thời gian thực,
biểu đồ cột phân lớp, biểu đồ nhịp luồng theo giây, top trang bị sửa nhiều nhất, top thực thể hoạt động
nhiều nhất, lọc theo wiki (viwiki/enwiki/...) và xuất CSV.

## Chạy tại máy

```bash
pip install -r app2/requirements.txt
streamlit run app2/app.py
```

## Deploy

Trên Streamlit Community Cloud: cùng repo này, **Main file path** = `app2/app.py`.
Secrets (chỉ cần cho chế độ OCI) theo mẫu `.streamlit/secrets.toml.example` ở thư mục gốc.

> Notebook gốc hard-code SASL username + Auth Token của OCI; bản trong `notebooks/` đã xoá,
> và token đó cần được rotate trong OCI Console.
