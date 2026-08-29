# Phân tích bài toán và các bước xử lý của App 1

> App 1: **Big Data Streaming – Phân tích độ hài lòng khách hàng (Amazon Fashion)**
> https://bigdata-fashion-streaming.streamlit.app · main file `app.py`

## 1. Bài toán

**Câu hỏi nghiệp vụ:** khách hàng đang hài lòng hay không hài lòng với sản phẩm thời trang, *ngay tại thời điểm này*, và cảm xúc đó biến động ra sao khi dữ liệu mới liên tục đổ về?

Vì sao phải giải bằng kiến trúc streaming chứ không phải báo cáo theo lô (batch):

| Đặc điểm dữ liệu | Hệ quả kỹ thuật |
|---|---|
| Khối lượng lớn (dataset gốc ~91 MB nén, hàng trăm nghìn đánh giá) | Không nạp hết vào RAM → phải đọc theo luồng (streaming read) |
| Dữ liệu đến liên tục, không có điểm kết thúc | Không thể "chạy xong rồi báo cáo" → cần vòng lặp xử lý và dashboard cập nhật liên tục |
| Rating (số sao) không phản ánh đúng cảm xúc trong lời bình | Cần mô hình NLP đọc nội dung, rồi hoà giải với rating |
| Cần tách khâu thu thập khỏi khâu hiển thị | Chèn message broker (OCI Streaming – Kafka API) ở giữa |

**Đầu vào:** đánh giá Amazon Fashion (`overall` = số sao, `summary`/`reviewText` = nội dung).
**Đầu ra:** dashboard thời gian thực (số bản ghi, thông lượng, tỷ lệ hài lòng, bảng đánh giá gần nhất, phân phối 5 mức cảm xúc, biểu đồ 3D) và file CSV kết quả.

## 2. Kiến trúc

```
Nguồn dữ liệu            Xử lý (Producer thread)         Vận chuyển            Tiêu thụ + Hiển thị
─────────────            ───────────────────────         ──────────            ───────────────────
AMAZON_FASHION.json.gz   parse JSON → RoBERTa →          OCI Streaming         Consumer.poll()
  hoặc file mẫu     ──▶  hoà giải rating × AI    ──▶     (Kafka API,      ──▶  lọc theo run_id  ──▶ Dashboard
                         → sự kiện JSON                  SASL_SSL/PLAIN)       tích luỹ rows        (0,5 giây/lần)
                                   │                                                  ▲
                                   └────────── hàng đợi nội bộ (fallback) ────────────┘
```

Hai luồng chạy song song: **thread Producer** vừa xử lý vừa đẩy sự kiện đi, **luồng chính** đọc về và vẽ dashboard. Nhờ vậy việc suy luận mô hình không làm đứng giao diện.

## 3. Các bước xử lý chi tiết

**Bước 1 – Chọn chế độ và nguồn.** Người dùng chọn Demo (không cần credential) hay OCI Streaming, chọn dữ liệu mẫu 100 bản ghi hay tải trực tuyến dataset gốc, đặt thời lượng chạy và số bản ghi tối đa.

**Bước 2 – Đọc dữ liệu theo luồng.** `requests.get(..., stream=True)` kết hợp `gzip.GzipFile` đọc từng dòng JSON ngay trong lúc tải, không giải nén toàn bộ 91 MB xuống đĩa.

**Bước 3 – Chuẩn hoá bản ghi.** Lấy `overall` làm rating; nội dung lấy theo thứ tự `summary` → `title` → `reviewText`, thiếu thì gán "Không có tiêu đề"; cắt còn 500 ký tự trước khi đưa vào mô hình.

**Bước 4 – Suy luận cảm xúc.** Mô hình `cardiffnlp/twitter-roberta-base-sentiment-latest` trả về nhãn (positive/neutral/negative) kèm độ tin cậy. Mô hình được nạp một lần và giữ trong cache (`@st.cache_resource`); nếu môi trường thiếu RAM hoặc thiếu torch, ứng dụng **tự chuyển** sang bộ luật rating + từ điển cảm xúc thay vì báo lỗi.

**Bước 5 – Hoà giải rating và nhãn AI.** Đây là phần nghiệp vụ cốt lõi, giữ nguyên quy tắc của notebook gốc:

| Điều kiện | Kết luận |
|---|---|
| rating ≤ 2.0 nhưng AI nói *positive* | coi là **tiêu cực** (số sao thấp thắng) |
| rating ≥ 4.0 nhưng AI nói *negative* | giữ **tiêu cực** (lời bình cảnh báo vấn đề) |
| còn lại | theo nhãn AI |

Sau đó quy về 5 mức hiển thị: rating ≥ 4.5 → *Rất tích cực*; ≤ 1.5 → *Rất tiêu cực*; trung lập → *Trung lập*; còn lại là *Tích cực* / *Tiêu cực*.

**Bước 6 – Đóng gói sự kiện.** Mỗi bản ghi thành một JSON gồm `run_id`, rating, nội dung, nhãn cảm xúc, độ tin cậy, màu hiển thị, mốc thời gian UTC. `run_id` sinh mới mỗi phiên để lọc bỏ dữ liệu còn tồn của lần chạy trước trong topic.

**Bước 7 – Gửi lên OCI Streaming.** `Producer.produce(topic, ...)` kèm callback `delivery_report` đếm số bản ghi được xác nhận / thất bại. Trước khi gửi, ứng dụng gọi metadata của broker để **kiểm tra tên topic**: nếu topic trong cấu hình không tồn tại, tự chuyển sang topic có thật và báo cảnh báo. Mỗi bản ghi đồng thời được đưa vào hàng đợi nội bộ để làm dự phòng.

**Bước 8 – Tiêu thụ và dự phòng.** Luồng chính `Consumer.poll(0.1)`, lọc theo `run_id`, tích luỹ vào `rows`. Nếu sau 20 giây consumer vẫn chưa nhận được bản ghi nào (thường do group rebalance của OCI), ứng dụng chuyển sang đọc hàng đợi nội bộ — nhưng **producer vẫn tiếp tục gửi lên OCI**, nên số liệu "Đã gửi Kafka/OCI" vẫn phản ánh đúng thực tế.

**Bước 9 – Hiển thị.** Cứ 0,5 giây vẽ lại: 5 chỉ số vận hành, bảng 7 đánh giá gần nhất, biểu đồ cột phân phối, biểu đồ 3D, topic đang dùng và lỗi Kafka (nếu có).

**Bước 10 – Kết xuất.** Kết thúc phiên, toàn bộ bản ghi thành `DataFrame`, hiển thị bảng chi tiết và cho tải CSV (UTF-8 BOM để mở đúng trong Excel).

## 4. Thiết kế chịu lỗi

| Rủi ro | Cơ chế xử lý |
|---|---|
| Streamlit Cloud thiếu RAM cho RoBERTa | Tự chuyển sang bộ luật, có công tắc tắt mô hình |
| Không có credential OCI | Chế độ Demo chạy độc lập, không cần secrets |
| Sai tên topic | Tự dò topic có thật trên stream pool |
| Consumer chưa nhận được dữ liệu | Hàng đợi nội bộ sau 20 giây, dashboard không bao giờ trống |
| Producer bị broker từ chối | Đếm `failed`, hiện lỗi ngay trên dashboard; nút "Gửi thử 1 bản ghi" để chẩn đoán riêng |
| Tràn RAM khi producer nhanh hơn consumer | Giới hạn `max_events` và thời lượng chạy |

## 5. Kết quả đo thực tế (Streamlit Community Cloud, 28/08/2026)

- Thông lượng với RoBERTa: **~5–25 bản ghi/giây** tuỳ tải máy chủ (suy luận từng bản ghi, chưa batch).
- Trên 300 bản ghi mẫu: tỷ lệ hài lòng ~35%, nhóm *Trung lập* chiếm đa số — phù hợp với đặc điểm tiêu đề đánh giá ngắn ("Five Stars", "Too small").
- Xác thực OCI thành công; **producer chưa được broker xác nhận** (bản ghi hết hạn trong hàng đợi client) → xem `docs/` và phần chẩn đoán: nhiều khả năng tài khoản chỉ có quyền đọc trên stream pool.

## 6. Hạn chế và hướng phát triển

1. **Batch inference**: gom 16–32 tiêu đề mỗi lần gọi mô hình thay vì từng bản ghi → thông lượng có thể tăng nhiều lần.
2. **Lưu trữ bền vững**: hiện kết quả chỉ nằm trong RAM phiên chạy; nên ghi xuống Object Storage hoặc cơ sở dữ liệu để phân tích theo thời gian.
3. **Cửa sổ thời gian**: bổ sung thống kê theo cửa sổ trượt (5 phút gần nhất) thay vì cộng dồn cả phiên.
4. **Cảnh báo**: khi tỷ lệ *Rất tiêu cực* vượt ngưỡng thì bắn thông báo — đúng tinh thần streaming analytics.
5. **Quyền publish OCI**: cần mở quyền `STREAM_PUSH` để chứng minh trọn vẹn luồng Producer → Kafka → Consumer.
