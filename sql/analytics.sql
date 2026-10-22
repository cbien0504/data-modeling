-- =====================================================================
-- Truy vấn phân tích trên star schema (dwh)
-- Quy ước: "doanh thu" = net_amount của các đơn đã giao thành công (DELIVERED)
-- =====================================================================
\pset null '∅'
SET search_path TO dwh;

\echo '\n=== 1. KPI theo năm ==='
SELECT d.year,
       count(DISTINCT f.order_id)                                            AS orders,
       count(DISTINCT c.customer_id)                                         AS customers,
       to_char(sum(f.net_amount), 'FM999,999,999,999')                       AS revenue,
       to_char(sum(f.net_amount) / count(DISTINCT f.order_id), 'FM999,999,999') AS aov,
       round(100 * sum(f.gross_profit) / sum(f.net_amount), 1)              AS gross_margin_pct
FROM fact_sales f
JOIN dim_date d          USING (date_key)
JOIN dim_customer c      USING (customer_key)
JOIN dim_order_status s  USING (order_status_key)
WHERE s.order_status_code = 'DELIVERED'
GROUP BY d.year
ORDER BY d.year;

\echo '\n=== 2. Doanh thu 12 tháng gần nhất & tăng trưởng MoM (window function) ==='
WITH monthly AS (
    SELECT d.year_month, sum(f.net_amount) AS revenue, count(DISTINCT f.order_id) AS orders
    FROM fact_sales f
    JOIN dim_date d         USING (date_key)
    JOIN dim_order_status s USING (order_status_key)
    WHERE s.order_status_code = 'DELIVERED'
    GROUP BY d.year_month
)
SELECT year_month, orders,
       to_char(revenue, 'FM999,999,999,999') AS revenue,
       round(100 * (revenue / lag(revenue) OVER (ORDER BY year_month) - 1), 1) AS mom_growth_pct,
       round(100 * (revenue / lag(revenue, 12) OVER (ORDER BY year_month) - 1), 1) AS yoy_growth_pct
FROM monthly
ORDER BY year_month DESC
LIMIT 12;

\echo '\n=== 3. Doanh thu & biên lợi nhuận theo danh mục (ROLLUP: tổng phụ theo danh mục cha) ==='
SELECT coalesce(p.parent_category_name, '== TỔNG ==')                   AS parent_category,
       coalesce(p.category_name, CASE WHEN p.parent_category_name IS NOT NULL THEN '-- cộng --' END) AS category,
       to_char(sum(f.net_amount), 'FM999,999,999,999')                   AS revenue,
       round(100 * sum(f.gross_profit) / sum(f.net_amount), 1)          AS margin_pct,
       sum(f.quantity)                                                   AS units
FROM fact_sales f
JOIN dim_product p      USING (product_key)
JOIN dim_order_status s USING (order_status_key)
WHERE s.order_status_code = 'DELIVERED'
GROUP BY ROLLUP (p.parent_category_name, p.category_name)
ORDER BY p.parent_category_name NULLS LAST, sum(f.net_amount) DESC;

\echo '\n=== 4. Top 10 sản phẩm theo doanh thu (gộp mọi phiên bản SCD2 theo product_id) ==='
SELECT cur.product_id, cur.product_name, cur.brand, cur.category_name,
       sum(f.quantity) AS units,
       to_char(sum(f.net_amount), 'FM999,999,999,999') AS revenue,
       rank() OVER (ORDER BY sum(f.net_amount) DESC) AS rnk
FROM fact_sales f
JOIN dim_product p      USING (product_key)
JOIN dim_product cur    ON cur.product_id = p.product_id AND cur.is_current
JOIN dim_order_status s USING (order_status_key)
WHERE s.order_status_code = 'DELIVERED'
GROUP BY cur.product_id, cur.product_name, cur.brand, cur.category_name
ORDER BY sum(f.net_amount) DESC
LIMIT 10;

\echo '\n=== 5. Doanh thu theo miền (địa chỉ khách TẠI THỜI ĐIỂM MUA nhờ SCD2) ==='
SELECT c.region,
       count(DISTINCT c.customer_id)                     AS customers,
       count(DISTINCT f.order_id)                        AS orders,
       to_char(sum(f.net_amount), 'FM999,999,999,999')   AS revenue,
       round(100 * sum(f.net_amount) / sum(sum(f.net_amount)) OVER (), 1) AS share_pct
FROM fact_sales f
JOIN dim_customer c     USING (customer_key)
JOIN dim_order_status s USING (order_status_key)
WHERE s.order_status_code = 'DELIVERED'
GROUP BY c.region
ORDER BY sum(f.net_amount) DESC;

\echo '\n=== 6. Cơ cấu phương thức thanh toán theo quý (% số đơn, pivot bằng FILTER) ==='
WITH orders AS (
    SELECT DISTINCT f.order_id, d.year_quarter, pm.payment_method_code
    FROM fact_sales f
    JOIN dim_date d            USING (date_key)
    JOIN dim_payment_method pm USING (payment_method_key)
)
SELECT year_quarter,
       count(*) AS orders,
       round(100.0 * count(*) FILTER (WHERE payment_method_code = 'COD')           / count(*), 1) AS cod_pct,
       round(100.0 * count(*) FILTER (WHERE payment_method_code = 'E_WALLET')      / count(*), 1) AS e_wallet_pct,
       round(100.0 * count(*) FILTER (WHERE payment_method_code = 'BANK_TRANSFER') / count(*), 1) AS bank_pct,
       round(100.0 * count(*) FILTER (WHERE payment_method_code = 'CREDIT_CARD')   / count(*), 1) AS card_pct
FROM orders
GROUP BY year_quarter
ORDER BY year_quarter;

\echo '\n=== 7. Tỷ lệ hủy / trả hàng theo danh mục cha ==='
SELECT p.parent_category_name,
       count(DISTINCT f.order_id) AS orders,
       round(100.0 * count(DISTINCT f.order_id) FILTER (WHERE s.order_status_code = 'CANCELLED')
             / count(DISTINCT f.order_id), 2) AS cancel_rate_pct,
       round(100.0 * count(DISTINCT f.order_id) FILTER (WHERE s.order_status_code = 'RETURNED')
             / count(DISTINCT f.order_id), 2) AS return_rate_pct
FROM fact_sales f
JOIN dim_product p      USING (product_key)
JOIN dim_order_status s USING (order_status_key)
GROUP BY p.parent_category_name
ORDER BY cancel_rate_pct DESC;

\echo '\n=== 8. Hiệu ứng ngày đôi (11/11, 12/12...) và cuối tuần: doanh thu trung bình mỗi ngày ==='
WITH daily AS (
    SELECT d.date_key, d.is_double_day, d.is_weekend, coalesce(sum(f.net_amount), 0) AS revenue
    FROM dim_date d
    LEFT JOIN fact_sales f ON f.date_key = d.date_key
                          AND f.order_status_key = (SELECT order_status_key FROM dim_order_status
                                                    WHERE order_status_code = 'DELIVERED')
    WHERE d.full_date BETWEEN (SELECT min(order_ts)::date FROM fact_sales)
                          AND (SELECT max(order_ts)::date FROM fact_sales) - 1
    GROUP BY 1, 2, 3
)
SELECT CASE WHEN is_double_day THEN 'Ngày đôi' WHEN is_weekend THEN 'Cuối tuần' ELSE 'Ngày thường' END AS day_type,
       count(*) AS days,
       to_char(avg(revenue), 'FM999,999,999,999') AS avg_daily_revenue
FROM daily
GROUP BY 1
ORDER BY avg(revenue) DESC;

\echo '\n=== 9. Khung giờ mua sắm (số đơn theo giờ) ==='
SELECT extract(hour FROM order_ts)::int AS hour,
       count(DISTINCT order_id) AS orders,
       repeat('█', (count(DISTINCT order_id) / 250)::int) AS chart
FROM fact_sales
GROUP BY 1
ORDER BY 1;

\echo '\n=== 10. Phân khúc khách hàng RFM (NTILE) ==='
WITH base AS (
    SELECT c.customer_id,
           max(f.order_ts)::date               AS last_order,
           count(DISTINCT f.order_id)          AS frequency,
           sum(f.net_amount)                   AS monetary
    FROM fact_sales f
    JOIN dim_customer c     USING (customer_key)
    JOIN dim_order_status s USING (order_status_key)
    WHERE s.order_status_code = 'DELIVERED'
    GROUP BY c.customer_id
), scored AS (
    SELECT *,
           ntile(4) OVER (ORDER BY last_order) AS r,
           ntile(4) OVER (ORDER BY frequency)  AS f,
           ntile(4) OVER (ORDER BY monetary)   AS m
    FROM base
)
SELECT CASE
           WHEN r = 4 AND f = 4 AND m >= 3 THEN '1. Champions'
           WHEN r >= 3 AND f >= 3          THEN '2. Loyal'
           WHEN r = 4 AND f <= 2           THEN '3. New / Promising'
           WHEN r <= 2 AND f >= 3          THEN '4. At risk'
           WHEN r = 1                      THEN '6. Lost'
           ELSE '5. Need attention'
       END AS segment,
       count(*) AS customers,
       round(avg(frequency), 1) AS avg_orders,
       to_char(avg(monetary), 'FM999,999,999,999') AS avg_spent,
       round(100.0 * sum(monetary) / sum(sum(monetary)) OVER (), 1) AS revenue_share_pct
FROM scored
GROUP BY 1
ORDER BY 1;

\echo '\n=== 11. Dòng tiền theo phương thức (fact_payments): thu ròng, tỷ lệ lỗi, tỷ lệ hoàn tiền ==='
SELECT pm.payment_method_name, pm.payment_channel,
       count(*) AS transactions,
       to_char(sum(p.net_collected_amount), 'FM999,999,999,999') AS net_collected,
       round(100.0 * count(*) FILTER (WHERE p.payment_status = 'FAILED') / count(*), 2) AS failed_pct,
       round(100.0 * sum(p.amount) FILTER (WHERE p.payment_status = 'REFUNDED')
             / nullif(sum(p.amount) FILTER (WHERE p.payment_status = 'SUCCESS'), 0), 2) AS refund_pct
FROM fact_payments p
JOIN dim_payment_method pm USING (payment_method_key)
GROUP BY 1, 2
ORDER BY sum(p.net_collected_amount) DESC;
