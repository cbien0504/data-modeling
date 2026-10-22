# Thiết kế mô hình dữ liệu

## 1. Bối cảnh nghiệp vụ

Một shop bán lẻ đa ngành hàng (điện tử, thời trang, gia dụng, mỹ phẩm, sách…) bán online trên toàn
quốc. Đơn hàng có vòng đời `PENDING → CONFIRMED → SHIPPING → DELIVERED`, có thể bị `CANCELLED`
hoặc `RETURNED`. Khách thanh toán bằng COD, chuyển khoản, thẻ hoặc ví điện tử. Một đơn có thể phát
sinh nhiều giao dịch thanh toán: lần đầu thất bại rồi thành công, hoặc thành công rồi được hoàn tiền.

Các câu hỏi phân tích mà kho dữ liệu cần trả lời:

- Doanh thu, số đơn, giá trị đơn trung bình (AOV) và biên lợi nhuận theo thời gian, danh mục, sản phẩm, vùng miền
- Hiệu quả các ngày sale lớn (11/11, 12/12), cuối tuần và khung giờ trong ngày
- Xu hướng phương thức thanh toán, tỷ lệ thanh toán lỗi, tỷ lệ hoàn tiền
- Tỷ lệ hủy và trả hàng theo ngành hàng
- Phân khúc khách hàng (RFM)
- Doanh thu theo địa chỉ khách **tại thời điểm mua** (đòi hỏi lưu lịch sử thuộc tính)

## 2. OLTP (schema `sales`, chuẩn 3NF)

| Bảng | Vai trò | Ghi chú thiết kế |
|---|---|---|
| `categories` | Danh mục 2 cấp (cha → con) | Tự tham chiếu qua `parent_category_id` |
| `customers` | Khách hàng | `email` UNIQUE; `city` là text tự nhập nên có thể "bẩn" |
| `products` | Sản phẩm và **giá hiện hành** | Không lưu lịch sử giá, đó là việc của kho dữ liệu |
| `orders` | Header đơn hàng | `status`, `payment_method` có CHECK constraint, `shipping_fee` ở cấp đơn |
| `order_items` | Dòng sản phẩm của đơn | `unit_price` là giá **lúc mua**, UNIQUE (`order_id`, `product_id`) |
| `payments` | Giao dịch thanh toán | 1 đơn có thể có nhiều giao dịch: `SUCCESS`, `FAILED`, `REFUNDED` |

Mỗi bảng master có trigger cập nhật `updated_at` khi có UPDATE. ETL dùng cột này làm **ngày
hiệu lực** (effective date) cho version mới trong SCD2.

## 3. Thiết kế chiều (Kimball 4 bước)

1. **Chọn quy trình nghiệp vụ**: bán hàng (order lines) và thanh toán (payment transactions)
2. **Khai báo grain**:
   - `fact_sales`: 1 dòng = 1 sản phẩm trong 1 đơn hàng (`order_item_id`), là grain chi tiết nhất nên trả lời được mọi câu hỏi theo sản phẩm và danh mục
   - `fact_payments`: 1 dòng = 1 giao dịch thanh toán (`payment_id`)
3. **Xác định dimension**: ngày, khách hàng, sản phẩm (kèm danh mục), phương thức thanh toán, trạng thái đơn
4. **Xác định fact (measure)**: số lượng, doanh thu gộp, giảm giá, doanh thu thuần, giá vốn, lợi nhuận gộp, phí ship phân bổ, số tiền thanh toán, tiền thu ròng

Hai fact có grain khác nhau nên tách thành 2 bảng. Chúng dùng chung các **conformed dimension**
(`dim_date`, `dim_customer`, `dim_payment_method`), tạo thành một *fact constellation*.

## 4. Dimensions

### `dim_date`

Sinh bằng `sequence()` trong Spark, phủ từ 1/1 của năm nhỏ nhất đến 31/12 của năm lớn nhất trong
dữ liệu. Khóa thông minh `date_key = YYYYMMDD` vừa dễ đọc vừa cho phép partition theo khoảng.
Các thuộc tính: thứ (ISO, tên tiếng Việt), tuần, tháng, quý, `year_month`, `is_weekend`,
`is_holiday` và `holiday_name` (ngày lễ dương lịch cố định), `is_double_day` (ngày đôi 9/9, 11/11…).

### `dim_customer` (SCD Type 2 + Type 1)

| Cột | Nguồn | Xử lý | SCD |
|---|---|---|---|
| `customer_key` | sinh mới | surrogate key, `-1` là unknown | — |
| `customer_id` | `customers.customer_id` | natural key | — |
| `address` | `customers.address` | trim, gộp khoảng trắng | **2** |
| `city` | `customers.city` | chuẩn hóa: `HCM`, `Sài Gòn`, `tp.hcm` → `TP. Hồ Chí Minh` | **2** |
| `region` | suy ra từ `city` | ánh xạ thành phố → Miền Bắc / Trung / Nam | **2** |
| `full_name` | `customers.full_name` | trim, gộp khoảng trắng | 1 |
| `email` | `customers.email` | lower + trim | 1 |
| `phone` | `customers.phone` | chỉ giữ chữ số (`0901.234.567` → `0901234567`) | 1 |
| `gender` | `customers.gender` | `M/F` → `Nam/Nữ` | 1 |
| `date_of_birth`, `signup_date` | `date_of_birth`, `created_at` | — | 1 |
| `row_hash` | sha256 của các cột Type 2 | dùng để phát hiện thay đổi | — |
| `valid_from`, `valid_to`, `is_current` | — | khoảng hiệu lực `[valid_from, valid_to)` | — |

Lý do: địa chỉ thay đổi làm thay đổi ý nghĩa phân tích (doanh thu theo vùng), nên cần giữ lịch
sử. Ngược lại, sửa số điện thoại hay sửa lỗi chính tả tên chỉ là cập nhật thông tin, nên ghi đè.

### `dim_product` (SCD Type 2 + Type 1)

`products` join với `categories` 2 lần (danh mục con và danh mục cha) để **phẳng hóa snowflake
thành star**. Người phân tích không cần join thêm bảng danh mục.

- Type 2: `category_id`, `category_name`, `parent_category_name`, `list_price`, `cost_price`, `price_tier`
- Type 1: `sku`, `product_name`, `brand`, `is_active`
- `price_tier` là phân khúc giá suy ra từ `list_price`: Bình dân < 200k ≤ Trung cấp < 2tr ≤ Cao cấp < 10tr ≤ Hạng sang
- Ngày hiệu lực là `greatest(product.updated_at, category.updated_at, parent.updated_at)`, nên đổi tên danh mục cũng được tính là thay đổi

`cost_price` nằm trong Type 2 để `fact_sales.cost_amount` lấy đúng giá vốn **tại thời điểm bán**.

### `dim_payment_method`, `dim_order_status`

Đây là các dimension tĩnh, sinh từ danh mục mã trong [reference.py](../spark/jobs/etl/reference.py),
bổ sung tên tiếng Việt, kênh (`Online`/`Offline`), nhóm trạng thái và cờ `is_final`.

### Unknown member

Mỗi dimension có một dòng khóa `-1` mang nhãn "Không xác định". Khi tra cứu khóa thất bại, fact
vẫn được nạp và trỏ về `-1` thay vì bị mất dòng. Quality gate sẽ chặn publish nếu có dòng như vậy.

## 5. Facts

### `fact_sales` (grain: order line)

| Cột | Công thức | Loại measure |
|---|---|---|
| `order_item_id`, `order_id` | từ nguồn | degenerate dimension |
| `date_key` | `YYYYMMDD(order_date)` | FK |
| `customer_key` | tra cứu SCD2 theo `customer_id` và `order_date` | FK |
| `product_key` | tra cứu SCD2 theo `product_id` và `order_date` | FK |
| `payment_method_key`, `order_status_key` | tra cứu theo mã | FK |
| `order_ts` | `orders.order_date` | để phân tích theo giờ |
| `quantity` | `order_items.quantity` | additive |
| `unit_price` | `order_items.unit_price` | non-additive |
| `gross_amount` | `quantity × unit_price` | additive |
| `discount_amount` | `order_items.discount_amount` | additive |
| `net_amount` | `gross_amount − discount_amount` | additive |
| `cost_amount` | `quantity × dim_product.cost_price` (version lúc bán) | additive |
| `gross_profit` | `net_amount − cost_amount` | additive |
| `shipping_fee_allocated` | `orders.shipping_fee × net_amount / Σ net_amount của đơn` | additive |

**Phân bổ phí ship**: phí ship thuộc grain đơn hàng nên phải phân bổ xuống grain dòng để cộng
được. Mỗi dòng được làm tròn 2 chữ số; phần dư do làm tròn dồn vào dòng cuối của đơn, nhờ vậy
tổng phân bổ luôn **đúng bằng** phí ship gốc. Quality gate có kiểm tra điều này.

`order_status_key` phản ánh trạng thái **hiện tại** của đơn. Fact được ghi lại toàn bộ mỗi lần
chạy nên trạng thái luôn mới nhất. Câu hỏi "doanh thu" mặc định lọc `DELIVERED`.

### `fact_payments` (grain: payment transaction)

| Cột | Công thức |
|---|---|
| `date_key`, `payment_ts` | từ `payments.payment_date` |
| `customer_key` | tra cứu SCD2 theo khách của đơn, tại thời điểm thanh toán |
| `payment_status` | `SUCCESS` / `FAILED` / `REFUNDED` (degenerate) |
| `amount` | số tiền giao dịch |
| `net_collected_amount` | `SUCCESS: +amount`, `REFUNDED: −amount`, `FAILED: 0`, nên cộng lại ra dòng tiền thu ròng |

## 6. Ánh xạ nguồn → đích

| Bảng đích | Bảng nguồn | Phép biến đổi chính |
|---|---|---|
| `dim_date` | `orders.order_date`, `payments.payment_date` (lấy khoảng) | sinh lịch |
| `dim_customer` | `customers` | làm sạch, chuẩn hóa thành phố, suy ra vùng, SCD2/SCD1 |
| `dim_product` | `products` ⨝ `categories` ⨝ `categories` (cha) | phẳng hóa danh mục, phân khúc giá, SCD2/SCD1 |
| `dim_payment_method` | danh mục mã | bổ sung tên, kênh |
| `dim_order_status` | danh mục mã | bổ sung tên, nhóm, `is_final` |
| `fact_sales` | `order_items` ⨝ `orders` | tra cứu khóa point-in-time, tính measure, phân bổ phí ship |
| `fact_payments` | `payments` ⨝ `orders` | tra cứu khóa, tính tiền thu ròng |

## 7. Các quyết định kỹ thuật

| Quyết định | Lý do |
|---|---|
| **Surrogate key** cho dimension | Natural key không đủ khi một khách có nhiều version; tách kho khỏi khóa của hệ nguồn |
| Version đầu tiên có `valid_from = 1900-01-01` | Fact phát sinh trước lần load đầu vẫn tìm được version |
| Khoảng hiệu lực nửa mở `[from, to)` | Không có thời điểm nào thuộc 2 version, cũng không có khe hở giữa các version |
| Bronze snapshot (Parquet) trước khi transform | Chỉ đọc DB nguồn 1 lần mỗi lần chạy; có thể chạy lại transform bằng `--skip-extract` |
| Ghi `stg.*` rồi publish trong 1 transaction | Spark JDBC không có transaction đa bảng; cách này giúp người đọc không bao giờ thấy kho "nạp dở", và FK fact → dim vẫn được giữ |
| Dimension SCD2 được merge, fact được ghi lại toàn bộ | Khóa dimension ổn định giữa các lần chạy; ghi lại toàn bộ fact đơn giản, idempotent và cập nhật được trạng thái đơn. Khi dữ liệu lớn hơn thì chuyển sang incremental với watermark |
| Quality gate **trước** publish | Dữ liệu sai bị chặn lại thay vì lọt vào báo cáo |
| Múi giờ `Asia/Ho_Chi_Minh` ở PostgreSQL, JVM và Spark session | Cột `TIMESTAMP` (không timezone) giữ nguyên giờ địa phương khi đi qua JDBC; ngày trong `date_key` là ngày theo giờ Việt Nam |
| FK thật trong DWH | Ràng buộc toàn vẹn được DB đảm bảo; TRUNCATE nhiều bảng trong cùng một lệnh nên không vướng FK |

## 8. Data quality checks

| # | Kiểm tra |
|---|---|
| 1-2 | Số dòng và tổng `net_amount` của `fact_sales` khớp với `order_items` trong OLTP |
| 3 | `fact_sales` không có `customer_key`/`product_key = -1` |
| 4 | Tổng `shipping_fee_allocated` = tổng `shipping_fee` của các đơn |
| 5-7 | Số dòng, tổng `amount` và khóa khách hàng của `fact_payments` khớp với nguồn |
| 8-15 | Với mỗi dimension SCD2: surrogate key duy nhất, mỗi natural key có tối đa 1 version hiện hành, mọi key trong nguồn đều có version hiện hành, `valid_from <= valid_to` |
| 16-17 | Mọi `date_key` của fact đều có trong `dim_date` |

Kết quả của các phép kiểm và số liệu SCD (số key mới, số key thay đổi) được lưu ở cột
`audit.etl_run_log.metrics` (JSONB) của mỗi lần chạy.
