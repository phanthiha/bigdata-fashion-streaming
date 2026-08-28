# Phân tích notebook `Demo_BigData_Streaming.ipynb`

## 1. Tổng quan

Notebook gồm 2 cell: một cell cài thư viện (torch CUDA 12.1, confluent-kafka, transformers,
plotly, requests, matplotlib) và một cell ~370 dòng chứa toàn bộ pipeline.

Kiến trúc thực thi:

| Thành phần | Vai trò |
|---|---|
| `file_streaming_producer_worker` (thread) | Tải `AMAZON_FASHION.json.gz` theo luồng, gán nhãn cảm xúc, đẩy vào Kafka + hàng đợi nội bộ |
| `Producer` / `Consumer` (confluent-kafka) | Gửi/nhận qua OCI Streaming, SASL_SSL + PLAIN |
| `run_stream_demo` (main loop) | Poll consumer, tích luỹ `rows`, render lại dashboard mỗi 1 giây trong 1800 giây |
| `generate_full_dashboard` | Sinh HTML + CSS, nhúng biểu đồ 3D dưới dạng base64 |

## 2. Logic nghiệp vụ

- Nhãn AI lấy từ `cardiffnlp/twitter-roberta-base-sentiment-latest` chạy trên trường `summary`.
- Quy tắc hoà giải giữa rating và nhãn AI:
  - rating ≤ 2.0 mà AI nói *positive* → coi là **negative**;
  - rating ≥ 4.0 mà AI nói *negative* → vẫn giữ **negative**;
  - còn lại theo nhãn AI.
- Chia 5 mức hiển thị theo rating: ≥4.5 *Rất tích cực*, ≤1.5 *Rất tiêu cực*, …

## 3. Điểm mạnh

- Có **cơ chế fallback**: sau 12 giây không có bản ghi nào được Kafka xác nhận, chuyển sang đọc
  hàng đợi nội bộ nên demo không bao giờ "trắng màn hình".
- Lọc theo `run_id` giúp tránh lẫn dữ liệu của các lần chạy trước còn tồn trong topic.
- Dashboard cập nhật tăng dần, không chặn luồng producer.

## 4. Vấn đề phát hiện

| Mức độ | Vấn đề | Hướng xử lý trong bản Streamlit |
|---|---|---|
| **Nghiêm trọng** | `SASL_USERNAME` và `OCI_AUTH_TOKEN` hard-code trong notebook được chia sẻ công khai | Chuyển sang `st.secrets`/biến môi trường; notebook trong repo đã xoá credential; **cần rotate token** |
| Cao | Suy luận RoBERTa chạy tuần tự cho từng dòng, không batch → nghẽn cổ chai | Giới hạn số bản ghi, cho phép tắt RoBERTa, truncation 128 token |
| Cao | Cố định 1800 giây, không có nút dừng | Cho chọn thời lượng 10–600 giây và số bản ghi tối đa |
| Trung bình | `except:` trần ở nhiều chỗ, nuốt lỗi | Bắt `Exception` cụ thể, hiển thị lỗi Kafka lên giao diện |
| Trung bình | `local_queue` không giới hạn kích thước → rủi ro tràn RAM khi producer nhanh hơn consumer | Giới hạn qua `max_events` |
| Trung bình | Dashboard vẽ lại toàn bộ biểu đồ 3D mỗi giây (matplotlib + base64) | Giãn nhịp render còn 0,5 giây và dùng `st.pyplot` thay vì base64 |
| Thấp | Cài `torch` bản CUDA 12.1 – không cần thiết cho môi trường CPU | `requirements.txt` dùng index CPU-only, nhẹ hơn nhiều |
| Thấp | Không lưu/xuất kết quả | Thêm nút tải CSV |

## 5. Bản chuyển thể

`app.py` giữ nguyên quy tắc nghiệp vụ và cơ chế fallback của notebook, bổ sung: chế độ Demo
không cần credential, chọn nguồn dữ liệu, công tắc mô hình, các chỉ số throughput và xuất CSV.
