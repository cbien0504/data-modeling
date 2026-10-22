"""Generate mock OLTP data for the shop and bulk-load it into PostgreSQL.

The data has realistic shapes so the analytics are interesting:
  * growth trend, weekend / year-end / "double day" (11/11, 12/12...) peaks
  * Pareto-like product popularity and loyal customers ordering more
  * order status depends on order age, payments follow the status
    (failed attempts, refunds for cancelled/returned orders, COD paid on delivery)
  * a small share of dirty values (city spellings, phone formats, e-mail casing)
    that the ETL has to standardise
"""
import argparse
import bisect
import itertools
import math
import os
import random
from datetime import date, datetime, timedelta

import reference as ref
from common import connect, copy_rows, log, now_vn, reset_sequence, round_vnd, strip_accents

CUSTOMER_COLS = ["customer_id", "full_name", "email", "phone", "gender", "date_of_birth",
                 "address", "city", "created_at", "updated_at"]
CATEGORY_COLS = ["category_id", "category_name", "parent_category_id", "description",
                 "created_at", "updated_at"]
PRODUCT_COLS = ["product_id", "sku", "product_name", "category_id", "brand", "unit_price",
                "cost_price", "stock_quantity", "is_active", "created_at", "updated_at"]
ORDER_COLS = ["order_id", "customer_id", "order_date", "status", "payment_method",
              "shipping_address", "shipping_city", "shipping_fee", "created_at", "updated_at"]
ITEM_COLS = ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_amount"]
PAYMENT_COLS = ["payment_id", "order_id", "payment_date", "payment_method", "amount", "status",
                "created_at"]

HOUR_WEIGHTS = [1.0, 0.5, 0.3, 0.2, 0.2, 0.3, 0.8, 1.5, 2.5, 3.0, 3.2, 3.5,
                3.8, 3.0, 2.6, 2.5, 2.6, 2.8, 3.0, 3.8, 4.5, 5.0, 4.5, 2.5]
BIG_CITIES = {"TP. Hồ Chí Minh", "Hà Nội"}


def weighted(rnd: random.Random, pairs):
    values, weights = zip(*pairs)
    return rnd.choices(values, weights=weights, k=1)[0]


def random_ts(rnd: random.Random, start: datetime, end: datetime) -> datetime:
    seconds = max(1, int((end - start).total_seconds()))
    return start + timedelta(seconds=rnd.randrange(seconds))


# ---------------------------------------------------------------------------
# Customers
# ---------------------------------------------------------------------------
def make_customer(rnd: random.Random, customer_id: int, created_at: datetime) -> dict:
    gender = "M" if rnd.random() < 0.48 else "F"
    last = weighted(rnd, ref.LAST_NAMES)
    if gender == "M":
        middle, first = rnd.choice(ref.MIDDLE_NAMES_MALE), rnd.choice(ref.FIRST_NAMES_MALE)
    else:
        middle, first = rnd.choice(ref.MIDDLE_NAMES_FEMALE), rnd.choice(ref.FIRST_NAMES_FEMALE)
    full_name = f"{last} {middle} {first}"

    email = (f"{strip_accents(first).lower()}.{strip_accents(last).lower()}{customer_id}"
             f"@{weighted(rnd, ref.EMAIL_DOMAINS)}")
    phone = rnd.choice(ref.PHONE_PREFIXES) + f"{rnd.randrange(10**7):07d}"

    city = weighted(rnd, ref.CITIES)
    city_raw = city
    if city in ref.CITY_DIRTY_VARIANTS and rnd.random() < 0.08:
        city_raw = rnd.choice(ref.CITY_DIRTY_VARIANTS[city])

    # A bit of dirty input the ETL must clean
    r = rnd.random()
    if r < 0.03:
        email = f"  {email.upper()} "
    elif r < 0.06:
        full_name = f" {last}  {middle} {first} "
    if rnd.random() < 0.05:
        phone = f"{phone[:4]} {phone[4:7]} {phone[7:]}" if rnd.random() < 0.5 else f"{phone[:4]}.{phone[4:7]}.{phone[7:]}"
    elif rnd.random() < 0.03:
        phone = None

    age = int(min(65, max(18, rnd.gauss(30, 8))))
    dob = created_at.date() - timedelta(days=age * 365 + rnd.randrange(365))
    address = f"{rnd.randint(1, 499)} {rnd.choice(ref.STREETS)}"

    return {
        "customer_id": customer_id, "full_name": full_name, "email": email, "phone": phone,
        "gender": gender, "date_of_birth": dob, "address": address,
        "city": city_raw, "city_canonical": city,
        "created_at": created_at, "updated_at": created_at,
    }


def build_customers(rnd, n, start: datetime, end: datetime) -> list[dict]:
    pre_start = start - timedelta(days=730)
    created = []
    for _ in range(n):
        if rnd.random() < 0.3:          # existing customer base before the shop's first order
            created.append(random_ts(rnd, pre_start, start))
        else:                           # sign-ups accelerate over time (sqrt => linear density)
            created.append(start + (end - start) * math.sqrt(rnd.random()))
    created.sort()
    # customer_id grows with created_at, like a real serial column
    return [make_customer(rnd, i + 1, ts.replace(microsecond=0)) for i, ts in enumerate(created)]


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------
def build_categories(created_at: datetime):
    rows, children = [], []
    ids = itertools.count(1)
    for parent_name, subcats in ref.CATALOG:
        parent_id = next(ids)
        rows.append((parent_id, parent_name, None, f"Danh mục {parent_name}", created_at, created_at))
        for child_name, brands, nouns, price_range in subcats:
            child_id = next(ids)
            rows.append((child_id, child_name, parent_id, f"{child_name} ({parent_name})",
                         created_at, created_at))
            children.append({"id": child_id, "brands": brands, "nouns": nouns, "price": price_range})
    return rows, children


def build_products(rnd, n, children, start: datetime):
    rows = []
    for product_id in range(1, n + 1):
        cat = children[(product_id - 1) % len(children)] if product_id <= len(children) else rnd.choice(children)
        brand = rnd.choice(cat["brands"])
        name = (f"{rnd.choice(cat['nouns'])} {brand} "
                f"{rnd.choice('ABCDEFGHKMNPRSTVX')}{rnd.randint(1, 99)}{rnd.choice(ref.MODEL_SUFFIXES)}")
        lo, hi = cat["price"]
        raw_price = math.exp(rnd.uniform(math.log(lo), math.log(hi)))
        price = round_vnd(raw_price, 10_000 if raw_price > 1_000_000 else 1_000)
        cost = round_vnd(price * rnd.uniform(0.55, 0.8))
        created = random_ts(rnd, start - timedelta(days=365), start)
        rows.append((product_id, f"SKU-{cat['id']:02d}-{product_id:05d}", name, cat["id"], brand,
                     price, cost, rnd.randint(0, 500), rnd.random() > 0.05, created, created))
    return rows


# ---------------------------------------------------------------------------
# Orders, items, payments
# ---------------------------------------------------------------------------
class OrderFactory:
    """Builds one order with its items and payments. Reused by simulate_changes.py."""

    def __init__(self, rnd: random.Random, products: list[tuple[int, int]], start: datetime, end: datetime):
        self.rnd = rnd
        self.start, self.end = start, end
        self.products = products  # (product_id, current unit_price)
        # long-tail popularity, cheaper products sell more often
        weights = [rnd.lognormvariate(0, 1.0) / (price ** 0.3) for _, price in products]
        self.cum_weights = list(itertools.accumulate(weights))

    def _status(self, age_days: float) -> str:
        if age_days > 7:
            return weighted(self.rnd, [("DELIVERED", 86), ("CANCELLED", 9), ("RETURNED", 5)])
        if age_days > 3:
            return weighted(self.rnd, [("DELIVERED", 55), ("SHIPPING", 32), ("CANCELLED", 13)])
        return weighted(self.rnd, [("PENDING", 30), ("CONFIRMED", 35), ("SHIPPING", 20), ("CANCELLED", 15)])

    def _payment_method(self, ts: datetime, subtotal: int) -> str:
        span = max(1.0, (self.end - self.start).total_seconds())
        frac = min(1.0, max(0.0, (ts - self.start).total_seconds() / span))
        cod = 0.45 - 0.15 * frac            # COD share shrinks over time
        if subtotal > 10_000_000:
            cod *= 0.4                      # expensive orders are rarely COD
        wallet = 0.20 + 0.15 * frac
        bank = 0.20
        card = max(0.05, 1 - cod - wallet - bank)
        return weighted(self.rnd, [("COD", cod), ("E_WALLET", wallet), ("BANK_TRANSFER", bank), ("CREDIT_CARD", card)])

    def make(self, order_id: int, customer: dict, ts: datetime, now: datetime,
             item_ids, payment_ids):
        rnd = self.rnd
        n_items = weighted(rnd, [(1, 45), (2, 30), (3, 15), (4, 7), (5, 3)])
        picked = []
        for idx in rnd.choices(range(len(self.products)), cum_weights=self.cum_weights, k=n_items * 3):
            if idx not in picked:
                picked.append(idx)
            if len(picked) == n_items:
                break

        double_day = ts.month == ts.day
        items, subtotal = [], 0
        for idx in picked:
            product_id, unit_price = self.products[idx]
            if unit_price < 300_000:
                qty = weighted(rnd, [(1, 60), (2, 25), (3, 10), (4, 5)])
            else:
                qty = weighted(rnd, [(1, 90), (2, 10)])
            discount = 0
            if rnd.random() < (0.6 if double_day else 0.12):
                pct = rnd.uniform(0.10, 0.30) if double_day else rnd.uniform(0.05, 0.15)
                discount = round_vnd(qty * unit_price * pct)
            items.append((next(item_ids), order_id, product_id, qty, unit_price, discount))
            subtotal += qty * unit_price - discount

        city = customer["city_canonical"]
        if subtotal >= 500_000:
            shipping_fee = 0
        else:
            shipping_fee = 15_000 if city in BIG_CITIES else rnd.choice([22_000, 25_000, 30_000, 35_000])
        total = subtotal + shipping_fee

        status = self._status((now - ts).total_seconds() / 86400)
        method = self._payment_method(ts, subtotal)
        payments = []

        def pay(at: datetime, pay_status: str):
            at = min(at, now)
            payments.append((next(payment_ids), order_id, at, method, total, pay_status, at))
            return at

        last_event = ts
        delivered_at = ts + timedelta(days=rnd.randint(2, 5), hours=rnd.randint(0, 10))
        if method != "COD":
            t = ts + timedelta(minutes=rnd.randint(1, 15))
            awaiting_payment = status == "PENDING" and rnd.random() < 0.5
            if not awaiting_payment:
                if rnd.random() < 0.05:
                    pay(t, "FAILED")
                    t += timedelta(minutes=rnd.randint(1, 5))
                if not (status == "CANCELLED" and rnd.random() < 0.3):
                    last_event = pay(t, "SUCCESS")
                    if status == "CANCELLED":
                        last_event = pay(t + timedelta(days=rnd.randint(1, 3)), "REFUNDED")
                    elif status == "RETURNED":
                        last_event = pay(delivered_at + timedelta(days=rnd.randint(3, 7)), "REFUNDED")
        elif status in ("DELIVERED", "RETURNED"):
            last_event = pay(delivered_at, "SUCCESS")
            if status == "RETURNED":
                last_event = pay(delivered_at + timedelta(days=rnd.randint(3, 7)), "REFUNDED")

        if status in ("DELIVERED", "RETURNED"):
            last_event = max(last_event, min(delivered_at, now))
        elif status == "CANCELLED":
            last_event = max(last_event, min(ts + timedelta(hours=rnd.randint(1, 30)), now))

        order = (order_id, customer["customer_id"], ts, status, method, customer["address"],
                 customer["city"], shipping_fee, ts, last_event)
        return order, items, payments


def order_timestamps(rnd, n, start: date, end: date) -> list[datetime]:
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    weights = []
    for i, d in enumerate(days):
        w = 1.0 + 1.2 * i / max(1, len(days) - 1)   # business growth
        if d.weekday() >= 5:
            w *= 1.25
        if d.month in (11, 12):
            w *= 1.3                                  # year-end shopping season
        if d.month == d.day:
            w *= 3.0                                  # double-day mega sales
        weights.append(w)
    picked = rnd.choices(days, weights=weights, k=n)
    hours = rnd.choices(range(24), weights=HOUR_WEIGHTS, k=n)
    stamps = [datetime(d.year, d.month, d.day, h, rnd.randrange(60), rnd.randrange(60))
              for d, h in zip(picked, hours)]
    stamps.sort()
    return stamps


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def parse_args():
    yesterday = now_vn().date() - timedelta(days=1)
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--customers", type=int, default=int(os.getenv("N_CUSTOMERS", 5000)))
    p.add_argument("--products", type=int, default=int(os.getenv("N_PRODUCTS", 400)))
    p.add_argument("--orders", type=int, default=int(os.getenv("N_ORDERS", 60000)))
    p.add_argument("--start-date", type=date.fromisoformat, default=date.fromisoformat(os.getenv("START_DATE", "2024-01-01")))
    p.add_argument("--end-date", type=date.fromisoformat, default=yesterday)
    p.add_argument("--seed", type=int, default=int(os.getenv("SEED", 42)))
    p.add_argument("--reset", action="store_true", help="truncate all OLTP tables before loading")
    return p.parse_args()


def main():
    args = parse_args()
    rnd = random.Random(args.seed)
    now = now_vn()
    start = datetime.combine(args.start_date, datetime.min.time())
    end = datetime.combine(args.end_date, datetime.max.time()).replace(microsecond=0)

    conn = connect()
    with conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM sales.orders")
        if cur.fetchone()[0] > 0 and not args.reset:
            log.info("OLTP already has data — nothing to do (use --reset to regenerate).")
            return
        if args.reset:
            log.info("Truncating OLTP tables ...")
            cur.execute("TRUNCATE sales.payments, sales.order_items, sales.orders, sales.products, "
                        "sales.categories, sales.customers RESTART IDENTITY CASCADE")

        log.info("Generating %s customers, %s products, %s orders (%s -> %s)",
                 args.customers, args.products, args.orders, args.start_date, args.end_date)
        customers = build_customers(rnd, args.customers, start, end)
        categories, children = build_categories(start - timedelta(days=800))
        products = build_products(rnd, args.products, children, start)

        factory = OrderFactory(rnd, [(p[0], p[5]) for p in products], start, end)
        cust_created = [c["created_at"] for c in customers]
        item_ids, payment_ids = itertools.count(1), itertools.count(1)
        orders, items, payments = [], [], []
        for order_id, ts in enumerate(order_timestamps(rnd, args.orders, args.start_date, args.end_date), 1):
            eligible = max(1, bisect.bisect_right(cust_created, ts))
            customer = customers[int(eligible * rnd.random() ** 1.6)]   # loyal (older) customers buy more
            ts = max(ts, customer["created_at"] + timedelta(hours=1))
            o, its, pays = factory.make(order_id, customer, ts, now, item_ids, payment_ids)
            orders.append(o)
            items.extend(its)
            payments.extend(pays)

        log.info("Loading with COPY ...")
        copy_rows(cur, "customers", CUSTOMER_COLS, ([c[k] for k in CUSTOMER_COLS] for c in customers))
        copy_rows(cur, "categories", CATEGORY_COLS, categories)
        copy_rows(cur, "products", PRODUCT_COLS, products)
        copy_rows(cur, "orders", ORDER_COLS, orders)
        copy_rows(cur, "order_items", ITEM_COLS, items)
        copy_rows(cur, "payments", PAYMENT_COLS, payments)
        for table, pk in [("customers", "customer_id"), ("categories", "category_id"),
                          ("products", "product_id"), ("orders", "order_id"),
                          ("order_items", "order_item_id"), ("payments", "payment_id")]:
            reset_sequence(cur, table, pk)
            cur.execute(f"ANALYZE sales.{table}")

    conn.close()
    log.info("Done: %d customers, %d categories, %d products, %d orders, %d order_items, %d payments",
             len(customers), len(categories), len(products), len(orders), len(items), len(payments))


if __name__ == "__main__":
    main()
