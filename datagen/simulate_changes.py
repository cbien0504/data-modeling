"""Simulate one more business day in the OLTP system so the next ETL run has changes to process.

  * in-flight orders progress (PENDING -> CONFIRMED -> SHIPPING -> DELIVERED) -> facts change
  * new orders arrive during the day
  * some customers move to another city            -> new SCD2 version in dim_customer
  * some customers update their phone number       -> SCD1 overwrite, no new version
  * some products change price                     -> new SCD2 version in dim_product
  * gaming products move to a new category         -> new SCD2 version in dim_product
  * new customers sign up, then some customers order *after* the changes
    (those facts must point to the new dimension versions)
"""
import argparse
import itertools
import random
from datetime import datetime, timedelta

import reference as ref
from common import connect, copy_rows, log, now_vn, reset_sequence, round_vnd
from generate_data import (ITEM_COLS, ORDER_COLS, PAYMENT_COLS, CUSTOMER_COLS, OrderFactory,
                           make_customer, random_ts)

CANONICAL_CITY = {variant: city for city, variants in ref.CITY_DIRTY_VARIANTS.items() for variant in variants}


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--moves", type=int, default=40, help="customers moving to another city")
    p.add_argument("--phone-changes", type=int, default=30)
    p.add_argument("--price-changes", type=int, default=25)
    p.add_argument("--new-customers", type=int, default=80)
    p.add_argument("--orders-today", type=int, default=400)
    p.add_argument("--post-change-orders", type=int, default=60)
    p.add_argument("--seed", type=int, default=None)
    return p.parse_args()


def fetch_customers(cur, ids=None) -> list[dict]:
    sql = "SELECT customer_id, address, city, created_at FROM sales.customers"
    if ids is not None:
        cur.execute(sql + " WHERE customer_id = ANY(%s)", (list(ids),))
    else:
        cur.execute(sql)
    return [{"customer_id": cid, "address": addr, "city": city,
             "city_canonical": CANONICAL_CITY.get(city, city), "created_at": created}
            for cid, addr, city, created in cur.fetchall()]


def fetch_products(cur) -> list[tuple[int, int]]:
    cur.execute("SELECT product_id, unit_price FROM sales.products WHERE is_active ORDER BY product_id")
    return [(pid, int(price)) for pid, price in cur.fetchall()]


def next_id(cur, table: str, column: str) -> int:
    cur.execute(f"SELECT COALESCE(max({column}), 0) + 1 FROM sales.{table}")
    return cur.fetchone()[0]


def insert_orders(cur, factory, customers, stamps, now, rnd):
    order_ids = itertools.count(next_id(cur, "orders", "order_id"))
    item_ids = itertools.count(next_id(cur, "order_items", "order_item_id"))
    payment_ids = itertools.count(next_id(cur, "payments", "payment_id"))
    orders, items, payments = [], [], []
    for ts in sorted(stamps):
        customer = rnd.choice(customers)
        o, its, pays = factory.make(next(order_ids), customer, ts, now, item_ids, payment_ids)
        orders.append(o)
        items.extend(its)
        payments.extend(pays)
    copy_rows(cur, "orders", ORDER_COLS, orders)
    copy_rows(cur, "order_items", ITEM_COLS, items)
    copy_rows(cur, "payments", PAYMENT_COLS, payments)
    for table, pk in [("orders", "order_id"), ("order_items", "order_item_id"), ("payments", "payment_id")]:
        reset_sequence(cur, table, pk)
    return len(orders)


def main():
    args = parse_args()
    rnd = random.Random(args.seed)
    conn = connect()
    now = now_vn()
    today = datetime.combine(now.date(), datetime.min.time())

    # 1) In-flight orders move forward; paid states get their payment transaction
    with conn, conn.cursor() as cur:
        cur.execute("""
            UPDATE sales.orders
               SET status = CASE status WHEN 'PENDING'   THEN 'CONFIRMED'
                                        WHEN 'CONFIRMED' THEN 'SHIPPING'
                                        ELSE 'DELIVERED' END
             WHERE status IN ('PENDING', 'CONFIRMED', 'SHIPPING') AND order_date < %s
        """, (today,))
        progressed = cur.rowcount
        cur.execute("""
            INSERT INTO sales.payments (order_id, payment_date, payment_method, amount, status)
            SELECT o.order_id, LOCALTIMESTAMP, o.payment_method,
                   o.shipping_fee + SUM(i.quantity * i.unit_price - i.discount_amount), 'SUCCESS'
              FROM sales.orders o
              JOIN sales.order_items i ON i.order_id = o.order_id
             WHERE o.status IN ('CONFIRMED', 'SHIPPING', 'DELIVERED')
               AND (o.payment_method <> 'COD' OR o.status = 'DELIVERED')
               AND NOT EXISTS (SELECT 1 FROM sales.payments p
                                WHERE p.order_id = o.order_id AND p.status = 'SUCCESS')
             GROUP BY o.order_id, o.payment_method, o.shipping_fee
        """)
        new_payments = cur.rowcount
    log.info("Progressed %d in-flight orders, recorded %d payments", progressed, new_payments)

    # 2) Orders placed earlier today (before the master-data changes below)
    with conn, conn.cursor() as cur:
        customers = fetch_customers(cur)
        factory = OrderFactory(rnd, fetch_products(cur), today - timedelta(days=1000), now)
        stamps = [random_ts(rnd, today, now) for _ in range(args.orders_today)]
        eligible = [c for c in customers if c["created_at"] < today]
        n_today = insert_orders(cur, factory, eligible, stamps, now, rnd)
    log.info("Inserted %d orders for today", n_today)

    # 3) Master-data changes (the trigger stamps updated_at = now)
    with conn, conn.cursor() as cur:
        cur.execute("""SELECT customer_id, city FROM sales.customers
                        WHERE customer_id IN (SELECT DISTINCT customer_id FROM sales.orders)
                        ORDER BY random() LIMIT %s""", (args.moves,))
        moved_ids = []
        for customer_id, city in cur.fetchall():
            new_city = rnd.choice([c for c, _ in ref.CITIES if c != CANONICAL_CITY.get(city, city)])
            new_address = f"{rnd.randint(1, 499)} {rnd.choice(ref.STREETS)}"
            cur.execute("UPDATE sales.customers SET city = %s, address = %s WHERE customer_id = %s",
                        (new_city, new_address, customer_id))
            moved_ids.append(customer_id)

        cur.execute("SELECT customer_id FROM sales.customers ORDER BY random() LIMIT %s", (args.phone_changes,))
        for (customer_id,) in cur.fetchall():
            phone = rnd.choice(ref.PHONE_PREFIXES) + f"{rnd.randrange(10**7):07d}"
            cur.execute("UPDATE sales.customers SET phone = %s WHERE customer_id = %s", (phone, customer_id))

        cur.execute("SELECT product_id, unit_price FROM sales.products ORDER BY random() LIMIT %s",
                    (args.price_changes,))
        for product_id, price in cur.fetchall():
            factor = rnd.uniform(0.85, 1.15)
            cur.execute("UPDATE sales.products SET unit_price = %s, cost_price = round(cost_price * %s::numeric, -3) "
                        "WHERE product_id = %s", (round_vnd(float(price) * factor), factor, product_id))

        # Catalogue restructuring: gaming products get a category of their own
        cur.execute("""
            INSERT INTO sales.categories (category_name, parent_category_id, description)
            SELECT 'Gaming Gear', category_id, 'Thiết bị chơi game (tách từ Bàn phím & Chuột)'
              FROM sales.categories WHERE category_name = 'Máy tính & Laptop'
            ON CONFLICT (category_name) DO NOTHING""")
        cur.execute("""
            UPDATE sales.products p SET category_id = g.category_id
              FROM sales.categories g
             WHERE g.category_name = 'Gaming Gear' AND p.category_id <> g.category_id
               AND (p.product_name ILIKE '%gaming%' OR p.product_name ILIKE 'Bàn phím cơ%')""")
        recategorized = cur.rowcount

        first_id = next_id(cur, "customers", "customer_id")
        signup_at = now_vn()
        new_customers = [make_customer(rnd, first_id + i, signup_at) for i in range(args.new_customers)]
        copy_rows(cur, "customers", CUSTOMER_COLS, ([c[k] for k in CUSTOMER_COLS] for c in new_customers))
        reset_sequence(cur, "customers", "customer_id")
    log.info("Moved %d customers, changed %d phones, repriced %d products, moved %d products to "
             "'Gaming Gear', %d new customers", len(moved_ids), args.phone_changes, args.price_changes,
             recategorized, len(new_customers))

    # 4) Orders placed after the changes: must map to the *new* dimension versions
    with conn, conn.cursor() as cur:
        cur.execute("SELECT LOCALTIMESTAMP")
        changed_at = cur.fetchone()[0].replace(microsecond=0) + timedelta(seconds=1)
        buyers = fetch_customers(cur, moved_ids + [c["customer_id"] for c in new_customers])
        factory = OrderFactory(rnd, fetch_products(cur), today - timedelta(days=1000), changed_at)
        stamps = [changed_at + timedelta(seconds=rnd.randint(0, 59)) for _ in range(args.post_change_orders)]
        n_post = insert_orders(cur, factory, buyers, stamps, max(stamps, default=changed_at), rnd)
    log.info("Inserted %d orders after the master-data changes", n_post)
    conn.close()


if __name__ == "__main__":
    main()
