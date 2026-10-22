-- =====================================================================
-- OLTP: hệ thống bán hàng online (chuẩn hóa 3NF)
-- Tối ưu cho ghi/đọc từng giao dịch: khóa chính, khóa ngoại, ràng buộc.
-- =====================================================================
CREATE SCHEMA IF NOT EXISTS sales;
SET search_path TO sales;

-- Tự động cập nhật updated_at khi có UPDATE (ETL dùng cột này làm
-- "effective date" cho SCD Type 2 ở kho dữ liệu).
CREATE OR REPLACE FUNCTION sales.set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at := LOCALTIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ---------------------------------------------------------------------
-- 1. categories: danh mục sản phẩm, phân cấp 2 tầng (cha -> con)
-- ---------------------------------------------------------------------
CREATE TABLE categories (
    category_id        SERIAL PRIMARY KEY,
    category_name      VARCHAR(100) NOT NULL UNIQUE,
    parent_category_id INT REFERENCES categories (category_id),
    description        TEXT,
    created_at         TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP,
    updated_at         TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP
);

-- ---------------------------------------------------------------------
-- 2. customers: khách hàng
-- ---------------------------------------------------------------------
CREATE TABLE customers (
    customer_id   SERIAL PRIMARY KEY,
    full_name     VARCHAR(150) NOT NULL,
    email         VARCHAR(150) NOT NULL UNIQUE,
    phone         VARCHAR(20),
    gender        CHAR(1) CHECK (gender IN ('M', 'F')),
    date_of_birth DATE,
    address       VARCHAR(255),
    city          VARCHAR(100) NOT NULL,
    created_at    TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP,
    updated_at    TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP
);

-- ---------------------------------------------------------------------
-- 3. products: sản phẩm (giá hiện hành)
-- ---------------------------------------------------------------------
CREATE TABLE products (
    product_id     SERIAL PRIMARY KEY,
    sku            VARCHAR(30)    NOT NULL UNIQUE,
    product_name   VARCHAR(200)   NOT NULL,
    category_id    INT            NOT NULL REFERENCES categories (category_id),
    brand          VARCHAR(100),
    unit_price     NUMERIC(14, 2) NOT NULL CHECK (unit_price >= 0),
    cost_price     NUMERIC(14, 2) NOT NULL CHECK (cost_price >= 0),
    stock_quantity INT            NOT NULL DEFAULT 0 CHECK (stock_quantity >= 0),
    is_active      BOOLEAN        NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMP      NOT NULL DEFAULT LOCALTIMESTAMP,
    updated_at     TIMESTAMP      NOT NULL DEFAULT LOCALTIMESTAMP
);

-- ---------------------------------------------------------------------
-- 4. orders: đơn hàng (header)
-- ---------------------------------------------------------------------
CREATE TABLE orders (
    order_id         BIGSERIAL PRIMARY KEY,
    customer_id      INT            NOT NULL REFERENCES customers (customer_id),
    order_date       TIMESTAMP      NOT NULL,
    status           VARCHAR(20)    NOT NULL
        CHECK (status IN ('PENDING', 'CONFIRMED', 'SHIPPING', 'DELIVERED', 'CANCELLED', 'RETURNED')),
    payment_method   VARCHAR(20)    NOT NULL
        CHECK (payment_method IN ('COD', 'BANK_TRANSFER', 'CREDIT_CARD', 'E_WALLET')),
    shipping_address VARCHAR(255),
    shipping_city    VARCHAR(100),
    shipping_fee     NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (shipping_fee >= 0),
    created_at       TIMESTAMP      NOT NULL DEFAULT LOCALTIMESTAMP,
    updated_at       TIMESTAMP      NOT NULL DEFAULT LOCALTIMESTAMP
);

-- ---------------------------------------------------------------------
-- 5. order_items: chi tiết đơn hàng (giá tại thời điểm mua)
-- ---------------------------------------------------------------------
CREATE TABLE order_items (
    order_item_id   BIGSERIAL PRIMARY KEY,
    order_id        BIGINT         NOT NULL REFERENCES orders (order_id) ON DELETE CASCADE,
    product_id      INT            NOT NULL REFERENCES products (product_id),
    quantity        INT            NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(14, 2) NOT NULL CHECK (unit_price >= 0),
    discount_amount NUMERIC(14, 2) NOT NULL DEFAULT 0 CHECK (discount_amount >= 0),
    UNIQUE (order_id, product_id)
);

-- ---------------------------------------------------------------------
-- 6. payments: giao dịch thanh toán (1 đơn có thể có nhiều giao dịch:
--    thất bại -> thành công, hoặc thành công -> hoàn tiền)
-- ---------------------------------------------------------------------
CREATE TABLE payments (
    payment_id     BIGSERIAL PRIMARY KEY,
    order_id       BIGINT         NOT NULL REFERENCES orders (order_id) ON DELETE CASCADE,
    payment_date   TIMESTAMP      NOT NULL,
    payment_method VARCHAR(20)    NOT NULL
        CHECK (payment_method IN ('COD', 'BANK_TRANSFER', 'CREDIT_CARD', 'E_WALLET')),
    amount         NUMERIC(14, 2) NOT NULL CHECK (amount >= 0),
    status         VARCHAR(20)    NOT NULL CHECK (status IN ('SUCCESS', 'FAILED', 'REFUNDED')),
    created_at     TIMESTAMP      NOT NULL DEFAULT LOCALTIMESTAMP
);

-- Index cho các truy vấn giao dịch & khóa ngoại
CREATE INDEX idx_categories_parent   ON categories (parent_category_id);
CREATE INDEX idx_products_category   ON products (category_id);
CREATE INDEX idx_orders_customer     ON orders (customer_id);
CREATE INDEX idx_orders_order_date   ON orders (order_date);
CREATE INDEX idx_order_items_order   ON order_items (order_id);
CREATE INDEX idx_order_items_product ON order_items (product_id);
CREATE INDEX idx_payments_order      ON payments (order_id);

CREATE TRIGGER trg_categories_updated BEFORE UPDATE ON categories
    FOR EACH ROW EXECUTE FUNCTION sales.set_updated_at();
CREATE TRIGGER trg_customers_updated BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION sales.set_updated_at();
CREATE TRIGGER trg_products_updated BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION sales.set_updated_at();
CREATE TRIGGER trg_orders_updated BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION sales.set_updated_at();
