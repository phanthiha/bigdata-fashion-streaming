# 📊 Big Data Streaming – Phân tích độ hài lòng khách hàng (Amazon Fashion)

Ứng dụng Streamlit mô phỏng một pipeline dữ liệu lớn thời gian thực:

```
Amazon Fashion reviews  →  Sentiment AI (RoBERTa)  →  OCI Streaming (Kafka API)  →  Dashboard
```

Được chuyển thể từ notebook Colab `notebooks/Demo_BigData_Streaming.ipynb`.

## ✨ Tính năng

- **Hai chế độ chạy**
  - `Demo`: phát lại dữ liệu ngay trong ứng dụng (không cần credential) – luôn chạy được trên Streamlit Cloud.
  - `OCI Streaming`: Producer đẩy sự kiện lên OCI Streaming (giao thức Kafka), Consumer đọc về và vẽ dashboard; tự động fallback sang hàng đợi nội bộ nếu sau 12 giây chưa gửi được bản ghi nào.
- **Hai nguồn dữ liệu**: file mẫu `data/sample_events.jsonl` hoặc tải trực tuyến dataset gốc `AMAZON_FASHION.json.gz` (~91 MB, đọc theo luồng).
- **Mô hình cảm xúc**: `cardiffnlp/twitter-roberta-base-sentiment-latest`, **tự động fallback** sang bộ luật (rating + từ điển) nếu thiếu RAM/torch.
- **Quy tắc gán nhãn** kết hợp rating Amazon và nhãn AI → 5 mức: Rất tích cực / Tích cực / Trung lập / Tiêu cực / Rất tiêu cực.
- **Dashboard**: 5 chỉ số vận hành, bảng 7 đánh giá gần nhất, biểu đồ cột và biểu đồ 3D phân phối cảm xúc, xuất kết quả ra CSV.

## 🚀 Chạy tại máy

```bash
python -m venv venv
venv\Scripts\activate          # Windows
pip install -r requirements.txt
streamlit run app.py
```

## ☁️ Deploy lên Streamlit Community Cloud

1. Đẩy repo này lên GitHub.
2. Vào https://share.streamlit.io → **Create app** → chọn repo, branch `main`, file `app.py`.
3. Mở **Advanced settings → Secrets** và dán nội dung theo mẫu `.streamlit/secrets.toml.example`
   (chỉ cần khi dùng chế độ OCI Streaming).
4. Bấm **Deploy**.

> Lần khởi động đầu tiên tải mô hình RoBERTa mất vài phút. Nếu Streamlit Cloud thiếu RAM,
> ứng dụng tự chuyển sang bộ luật thay vì báo lỗi. Có thể tắt hẳn RoBERTa bằng công tắc ở thanh bên.

## 🔐 Bảo mật

- **Không** commit `.streamlit/secrets.toml` (đã có trong `.gitignore`).
- Notebook trong `notebooks/` đã được xoá credential; bản gốc trên Colab từng chứa
  SASL username và Auth Token của OCI ở dạng plaintext → **hãy revoke/rotate Auth Token đó**
  trong OCI Console (Identity → Users → Auth Tokens) trước khi chia sẻ repo công khai.


## 🌐 App2 – Wikimedia streaming

Repo còn chứa ứng dụng thứ hai trong thư mục `app2/`: stream sự kiện thay đổi của Wikipedia
(Wikimedia EventStreams) và phân lớp thực thể phát sinh sự kiện (bot / ẩn danh / hệ thống / định danh).
Chi tiết trong [app2/README.md](app2/README.md); deploy với **Main file path** = `app2/app.py`.

## 📁 Cấu trúc

```
app.py                          Ứng dụng Streamlit (demo + OCI streaming)
requirements.txt                Thư viện (torch CPU-only để nhẹ hơn)
data/sample_events.jsonl        100 bản ghi mẫu đã gán nhãn
notebooks/                      Notebook Colab gốc (đã xoá credential)
docs/PHAN_TICH_NOTEBOOK.md      Phân tích kiến trúc & rủi ro của notebook
.streamlit/                     Theme + mẫu secrets
```
