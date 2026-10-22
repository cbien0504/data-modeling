-- =====================================================================
-- Demo SCD Type 2 — chạy sau: make etl -> make simulate -> make etl
-- =====================================================================
\pset null '∅'

\echo '\n=== 1. Khách hàng đã chuyển nhà: mỗi lần đổi địa chỉ là 1 phiên bản (SCD2) ==='
SELECT customer_id, customer_key, full_name, phone, city, region, valid_from, valid_to, is_current
FROM dwh.dim_customer
WHERE customer_id IN (SELECT customer_id FROM dwh.dim_customer
                      GROUP BY customer_id HAVING count(*) > 1
                      ORDER BY customer_id LIMIT 3)
ORDER BY customer_id, valid_from;

\echo '\n=== 2. Fact trỏ tới phiên bản có hiệu lực tại thời điểm mua (point-in-time) ==='
SELECT d.customer_id, d.customer_key, d.city AS city_at_order_time, d.is_current,
       count(*) AS order_lines, min(f.order_ts) AS first_order, max(f.order_ts) AS last_order
FROM dwh.fact_sales f
JOIN dwh.dim_customer d USING (customer_key)
WHERE d.customer_id IN (SELECT customer_id FROM dwh.dim_customer WHERE NOT is_current
                        ORDER BY customer_id LIMIT 4)
GROUP BY 1, 2, 3, 4
ORDER BY 1, first_order;

\echo '\n=== 3. Doanh thu theo thành phố: "as-was" (lúc mua) vs "as-is" (nơi ở hiện tại) ==='
WITH as_was AS (
    SELECT d.city, sum(f.net_amount) AS revenue
    FROM dwh.fact_sales f JOIN dwh.dim_customer d USING (customer_key)
    GROUP BY 1
), as_is AS (
    SELECT cur.city, sum(f.net_amount) AS revenue
    FROM dwh.fact_sales f
    JOIN dwh.dim_customer d   USING (customer_key)
    JOIN dwh.dim_customer cur ON cur.customer_id = d.customer_id AND cur.is_current
    GROUP BY 1
)
SELECT coalesce(w.city, i.city) AS city,
       to_char(w.revenue, 'FM999,999,999,999') AS revenue_as_was,
       to_char(i.revenue, 'FM999,999,999,999') AS revenue_as_is,
       to_char(coalesce(i.revenue, 0) - coalesce(w.revenue, 0), 'FM999,999,999,999') AS difference
FROM as_was w FULL JOIN as_is i ON i.city = w.city
ORDER BY abs(coalesce(i.revenue, 0) - coalesce(w.revenue, 0)) DESC
LIMIT 8;

\echo '\n=== 4. Sản phẩm đổi giá / đổi danh mục ==='
SELECT product_id, product_key, product_name, category_name, list_price, cost_price, price_tier,
       valid_from, valid_to, is_current
FROM dwh.dim_product
WHERE product_id IN (SELECT product_id FROM dwh.dim_product
                     GROUP BY product_id HAVING count(*) > 1
                     ORDER BY product_id LIMIT 3)
ORDER BY product_id, valid_from;

\echo '\n=== 5. Lịch sử các lần chạy ETL ==='
SELECT run_id, run_date, started_at, finished_at - started_at AS duration, status,
       metrics -> 'rows' ->> 'fact_sales' AS fact_sales_rows,
       metrics -> 'scd2' -> 'dim_customer' AS dim_customer_scd2,
       metrics -> 'scd2' -> 'dim_product'  AS dim_product_scd2
FROM audit.etl_run_log
ORDER BY started_at;
