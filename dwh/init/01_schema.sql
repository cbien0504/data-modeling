-- =====================================================================
-- OLAP / Data Warehouse: mô hình chiều (Kimball)
-- 2 bảng fact dùng chung các dimension (fact constellation):
--   fact_sales    : grain = 1 dòng sản phẩm trong đơn hàng
--   fact_payments : grain = 1 giao dịch thanh toán
-- Schema:
--   stg   : vùng đệm Spark ghi vào (bị ghi đè mỗi lần chạy)
--   dwh   : bảng phục vụ phân tích (publish trong 1 transaction)
--   audit : nhật ký các lần chạy ETL
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS audit;

-- ---------------------------------------------------------------------
-- dim_date: sinh từ khoảng ngày của dữ liệu, khóa thông minh YYYYMMDD
-- ---------------------------------------------------------------------
CREATE TABLE dwh.dim_date (
    date_key      INT PRIMARY KEY,
    full_date     DATE        NOT NULL UNIQUE,
    day_of_month  SMALLINT    NOT NULL,
    day_of_week   SMALLINT    NOT NULL,          -- 1 = Thứ Hai ... 7 = Chủ Nhật (ISO)
    day_name      VARCHAR(20) NOT NULL,
    week_of_year  SMALLINT    NOT NULL,
    month         SMALLINT    NOT NULL,
    month_name    VARCHAR(20) NOT NULL,
    quarter       SMALLINT    NOT NULL,
    year          SMALLINT    NOT NULL,
    year_month    CHAR(7)     NOT NULL,          -- 2025-03
    year_quarter  CHAR(7)     NOT NULL,          -- 2025-Q1
    is_weekend    BOOLEAN     NOT NULL,
    is_holiday    BOOLEAN     NOT NULL,
    holiday_name  VARCHAR(50),
    is_double_day BOOLEAN     NOT NULL           -- ngày đôi 9/9, 10/10, 11/11...: ngày sale lớn
);

-- ---------------------------------------------------------------------
-- dim_customer: SCD Type 2 trên (address, city, region)
--               SCD Type 1 trên các thuộc tính còn lại
-- ---------------------------------------------------------------------
CREATE TABLE dwh.dim_customer (
    customer_key  INT PRIMARY KEY,               -- surrogate key
    customer_id   INT          NOT NULL,         -- natural key từ OLTP
    address       VARCHAR(255),
    city          VARCHAR(100),
    region        VARCHAR(50),
    full_name     VARCHAR(150),
    email         VARCHAR(150),
    phone         VARCHAR(20),
    gender        VARCHAR(20),
    date_of_birth DATE,
    signup_date   DATE,
    row_hash      TEXT,
    valid_from    TIMESTAMP    NOT NULL,
    valid_to      TIMESTAMP    NOT NULL,
    is_current    BOOLEAN      NOT NULL
);
CREATE UNIQUE INDEX uq_dim_customer_current ON dwh.dim_customer (customer_id) WHERE is_current;
CREATE INDEX idx_dim_customer_nk ON dwh.dim_customer (customer_id, valid_from);

-- ---------------------------------------------------------------------
-- dim_product: phi chuẩn hóa products + categories (cha/con) thành 1 bảng
--   SCD Type 2 trên (danh mục, giá bán, giá vốn, phân khúc giá)
--   SCD Type 1 trên (sku, tên, thương hiệu, trạng thái)
-- ---------------------------------------------------------------------
CREATE TABLE dwh.dim_product (
    product_key          INT PRIMARY KEY,
    product_id           INT          NOT NULL,
    category_id          INT,
    category_name        VARCHAR(100),
    parent_category_name VARCHAR(100),
    list_price           NUMERIC(14, 2),
    cost_price           NUMERIC(14, 2),
    price_tier           VARCHAR(30),
    sku                  VARCHAR(30),
    product_name         VARCHAR(200),
    brand                VARCHAR(100),
    is_active            BOOLEAN,
    row_hash             TEXT,
    valid_from           TIMESTAMP    NOT NULL,
    valid_to             TIMESTAMP    NOT NULL,
    is_current           BOOLEAN      NOT NULL
);
CREATE UNIQUE INDEX uq_dim_product_current ON dwh.dim_product (product_id) WHERE is_current;
CREATE INDEX idx_dim_product_nk ON dwh.dim_product (product_id, valid_from);

-- ---------------------------------------------------------------------
-- Dimension tĩnh (sinh từ danh mục mã trong ETL)
-- ---------------------------------------------------------------------
CREATE TABLE dwh.dim_payment_method (
    payment_method_key  SMALLINT PRIMARY KEY,
    payment_method_code VARCHAR(20)  NOT NULL UNIQUE,
    payment_method_name VARCHAR(100) NOT NULL,
    payment_channel     VARCHAR(30)  NOT NULL    -- Online / Offline
);

CREATE TABLE dwh.dim_order_status (
    order_status_key  SMALLINT PRIMARY KEY,
    order_status_code VARCHAR(20)  NOT NULL UNIQUE,
    order_status_name VARCHAR(100) NOT NULL,
    status_group      VARCHAR(50)  NOT NULL,
    is_final          BOOLEAN      NOT NULL
);

-- ---------------------------------------------------------------------
-- fact_sales: grain = 1 dòng order_item
-- ---------------------------------------------------------------------
CREATE TABLE dwh.fact_sales (
    order_item_id          BIGINT PRIMARY KEY,   -- degenerate dimension
    order_id               BIGINT         NOT NULL, -- degenerate dimension
    date_key               INT            NOT NULL REFERENCES dwh.dim_date (date_key),
    customer_key           INT            NOT NULL REFERENCES dwh.dim_customer (customer_key),
    product_key            INT            NOT NULL REFERENCES dwh.dim_product (product_key),
    payment_method_key     SMALLINT       NOT NULL REFERENCES dwh.dim_payment_method (payment_method_key),
    order_status_key       SMALLINT       NOT NULL REFERENCES dwh.dim_order_status (order_status_key),
    order_ts               TIMESTAMP      NOT NULL,
    quantity               INT            NOT NULL,
    unit_price             NUMERIC(14, 2) NOT NULL,
    gross_amount           NUMERIC(16, 2) NOT NULL, -- quantity * unit_price
    discount_amount        NUMERIC(16, 2) NOT NULL,
    net_amount             NUMERIC(16, 2) NOT NULL, -- gross - discount (doanh thu thuần)
    cost_amount            NUMERIC(16, 2) NOT NULL, -- quantity * giá vốn (theo version sản phẩm)
    gross_profit           NUMERIC(16, 2) NOT NULL, -- net - cost
    shipping_fee_allocated NUMERIC(14, 2) NOT NULL, -- phí ship của đơn phân bổ theo net_amount
    etl_loaded_at          TIMESTAMP      NOT NULL
);
CREATE INDEX idx_fact_sales_date     ON dwh.fact_sales (date_key);
CREATE INDEX idx_fact_sales_customer ON dwh.fact_sales (customer_key);
CREATE INDEX idx_fact_sales_product  ON dwh.fact_sales (product_key);
CREATE INDEX idx_fact_sales_order    ON dwh.fact_sales (order_id);

-- ---------------------------------------------------------------------
-- fact_payments: grain = 1 giao dịch thanh toán
-- ---------------------------------------------------------------------
CREATE TABLE dwh.fact_payments (
    payment_id           BIGINT PRIMARY KEY,
    order_id             BIGINT         NOT NULL,
    date_key             INT            NOT NULL REFERENCES dwh.dim_date (date_key),
    customer_key         INT            NOT NULL REFERENCES dwh.dim_customer (customer_key),
    payment_method_key   SMALLINT       NOT NULL REFERENCES dwh.dim_payment_method (payment_method_key),
    payment_status       VARCHAR(20)    NOT NULL,
    payment_ts           TIMESTAMP      NOT NULL,
    amount               NUMERIC(16, 2) NOT NULL,
    net_collected_amount NUMERIC(16, 2) NOT NULL, -- SUCCESS: +amount, REFUNDED: -amount, FAILED: 0
    etl_loaded_at        TIMESTAMP      NOT NULL
);
CREATE INDEX idx_fact_payments_date     ON dwh.fact_payments (date_key);
CREATE INDEX idx_fact_payments_customer ON dwh.fact_payments (customer_key);

-- ---------------------------------------------------------------------
-- Nhật ký ETL
-- ---------------------------------------------------------------------
CREATE TABLE audit.etl_run_log (
    run_id      VARCHAR(36) PRIMARY KEY,
    run_date    DATE        NOT NULL,
    started_at  TIMESTAMP   NOT NULL,
    finished_at TIMESTAMP   NOT NULL,
    status      VARCHAR(20) NOT NULL,
    metrics     JSONB,
    error       TEXT
);
