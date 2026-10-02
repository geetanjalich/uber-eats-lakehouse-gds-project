#!/usr/bin/env python3
"""Generate realistic Uber Eats-style marketplace source data.

The data is synthetic but shaped to look like operational marketplace feeds: nested
orders, changing merchants, courier telemetry, payments, refunds, ratings, and support
records. It intentionally includes controlled defects so downstream quality handling,
quarantine, and reprocessing can be demonstrated.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4


# These lists are deterministic seed data used to make the generated files feel like
# operational feeds from a multi-city food delivery marketplace. They are intentionally
# small enough to understand, while the transaction volume is controlled by CLI options.
CITIES = [
    {"city_id": "nyc", "city_name": "New York", "country": "US", "timezone": "America/New_York"},
    {"city_id": "sfo", "city_name": "San Francisco", "country": "US", "timezone": "America/Los_Angeles"},
    {"city_id": "chi", "city_name": "Chicago", "country": "US", "timezone": "America/Chicago"},
    {"city_id": "sea", "city_name": "Seattle", "country": "US", "timezone": "America/Los_Angeles"},
    {"city_id": "aus", "city_name": "Austin", "country": "US", "timezone": "America/Chicago"},
]

CUISINES = [
    "Burgers",
    "Pizza",
    "Mexican",
    "Thai",
    "Indian",
    "Japanese",
    "Mediterranean",
    "Coffee",
    "Salads",
    "Dessert",
]

MERCHANT_BRANDS = [
    "Urban Tandoor",
    "Market Street Pizza",
    "Bay Curry House",
    "Sunset Sushi",
    "Green Bowl Kitchen",
    "Northside Burgers",
    "Luna Tacos",
    "Harbor Thai",
    "Metro Deli",
    "Bean & Steam",
    "Golden Wok",
    "Cedar Grill",
]

FIRST_NAMES = [
    "Aarav",
    "Maya",
    "Noah",
    "Sophia",
    "Liam",
    "Olivia",
    "Ethan",
    "Isabella",
    "Rohan",
    "Anika",
]

LAST_NAMES = [
    "Patel",
    "Smith",
    "Johnson",
    "Nguyen",
    "Garcia",
    "Brown",
    "Miller",
    "Davis",
    "Wilson",
    "Sharma",
]

SUPPORT_REASONS = [
    "missing_item",
    "late_delivery",
    "refund_request",
    "wrong_order",
    "driver_unreachable",
    "quality_issue",
]


def iso_ts(value: datetime) -> str:
    """Return an ISO timestamp with UTC offset."""

    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def weighted_choice(items: list[tuple[str, float]]) -> str:
    """Pick an item using simple cumulative weights."""

    # The weights in this file approximate real marketplace skew: most customers sit
    # in lower loyalty tiers, dinner traffic is busier than breakfast, and most orders
    # are delivered successfully. The sum is expected to be close to 1.0.
    threshold = random.random()
    running = 0.0
    for item, weight in items:
        running += weight
        if threshold <= running:
            return item
    return items[-1][0]


def write_jsonl(path: Path, rows: list[dict]) -> None:
    """Write newline-delimited JSON so Auto Loader can ingest each row independently."""

    # JSONL keeps each source record independent, which is friendly for Auto Loader
    # because a malformed row can be rescued without rejecting an entire JSON array.
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, separators=(",", ":"), sort_keys=True))
            handle.write("\n")


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    """Write CSV data for source systems that export tabular files."""

    # Payments are emitted as CSV to show that one ingestion framework can handle
    # different upstream file formats with dataset-specific Auto Loader options.
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_merchants() -> list[dict]:
    """Create stable merchant master data with realistic operating attributes."""

    # Merchant attributes such as commission rate, tier, and prep time are later
    # written as daily snapshots so the gold merchant dimension can track history.
    merchants: list[dict] = []
    merchant_number = 1
    for city in CITIES:
        for brand in MERCHANT_BRANDS:
            cuisine = random.choice(CUISINES)
            merchants.append(
                {
                    "merchant_id": f"m_{merchant_number:05d}",
                    "merchant_name": f"{brand} {city['city_name']}",
                    "city_id": city["city_id"],
                    "cuisine_type": cuisine,
                    "commission_rate": round(random.uniform(0.16, 0.31), 4),
                    "is_active": True,
                    "merchant_tier": random.choice(["standard", "growth", "premium"]),
                    "avg_prep_minutes": random.randint(8, 28),
                }
            )
            merchant_number += 1
    return merchants


def build_customers(count: int) -> list[dict]:
    """Create customer master data with masked contact attributes."""

    # Contact fields are represented as hashes because a production analytics platform
    # should avoid landing raw personal contact values when they are not needed.
    customers: list[dict] = []
    for index in range(1, count + 1):
        city = random.choice(CITIES)
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        customers.append(
            {
                "customer_id": f"c_{index:07d}",
                "first_name": first,
                "last_name": last,
                "email_hash": f"sha256_{uuid4().hex}",
                "phone_hash": f"sha256_{uuid4().hex}",
                "home_city_id": city["city_id"],
                "loyalty_tier": weighted_choice(
                    [("blue", 0.55), ("silver", 0.27), ("gold", 0.14), ("diamond", 0.04)]
                ),
                "marketing_opt_in": random.random() < 0.68,
            }
        )
    return customers


def build_couriers(count: int) -> list[dict]:
    """Create courier master records used by delivery facts."""

    # Courier records support both current-state analytics and delivery-event facts.
    # Controlled changes to active status and delivery mode appear in later snapshots.
    modes = ["bike", "scooter", "car", "walk"]
    couriers: list[dict] = []
    for index in range(1, count + 1):
        city = random.choice(CITIES)
        couriers.append(
            {
                "courier_id": f"d_{index:06d}",
                "home_city_id": city["city_id"],
                "vehicle_type": weighted_choice(
                    [("car", 0.42), ("bike", 0.22), ("scooter", 0.25), ("walk", 0.11)]
                ),
                "signup_channel": random.choice(["referral", "organic", "paid_search", "partner"]),
                "is_active": random.random() < 0.93,
                "delivery_mode": random.choice(modes),
            }
        )
    return couriers


def menu_items_for_merchants(merchants: list[dict]) -> list[dict]:
    """Create a menu catalog with price points by cuisine."""

    # The menu item catalog behaves like a product dimension. Prices and availability
    # can change over time, which gives the SCD2 logic another realistic entity.
    menu: list[dict] = []
    item_number = 1
    item_names = {
        "Burgers": ["Signature Burger", "Spicy Chicken Sandwich", "Loaded Fries"],
        "Pizza": ["Margherita Pizza", "Pepperoni Pizza", "Garlic Knots"],
        "Mexican": ["Chicken Burrito", "Veggie Taco Trio", "Quesadilla"],
        "Thai": ["Pad Thai", "Green Curry", "Basil Fried Rice"],
        "Indian": ["Butter Chicken", "Paneer Tikka Bowl", "Masala Dosa"],
        "Japanese": ["Salmon Roll", "Chicken Katsu", "Miso Ramen"],
        "Mediterranean": ["Falafel Plate", "Chicken Shawarma", "Hummus Bowl"],
        "Coffee": ["Cold Brew", "Latte", "Breakfast Sandwich"],
        "Salads": ["Harvest Salad", "Caesar Salad", "Protein Bowl"],
        "Dessert": ["Cheesecake", "Chocolate Brownie", "Gelato Cup"],
    }
    for merchant in merchants:
        names = item_names.get(merchant["cuisine_type"], ["House Special"])
        for name in names:
            base_price = round(random.uniform(6.5, 24.0), 2)
            menu.append(
                {
                    "menu_item_id": f"i_{item_number:07d}",
                    "merchant_id": merchant["merchant_id"],
                    "item_name": name,
                    "category": merchant["cuisine_type"],
                    "base_price": base_price,
                    "is_available": random.random() < 0.94,
                    "tax_category": random.choice(["prepared_food", "beverage", "dessert"]),
                }
            )
            item_number += 1
    return menu


def generate_order(
    order_day: date,
    merchants: list[dict],
    menu_by_merchant: dict[str, list[dict]],
    customers: list[dict],
    couriers: list[dict],
    order_number: int,
    defect_rate: float,
) -> tuple[dict, dict, list[dict], list[dict], dict | None, dict | None, list[dict]]:
    """Generate one order plus related operational events."""

    # One business transaction fans out into several source feeds. That is deliberate:
    # the lakehouse has to reconstruct a reliable analytic model from loosely coupled
    # order, payment, item, delivery, refund, rating, and support data.
    merchant = random.choice(merchants)
    customer = random.choice(customers)
    courier = random.choice(couriers)
    placed_hour = weighted_choice(
        [
            ("8", 0.07),
            ("11", 0.16),
            ("12", 0.18),
            ("13", 0.12),
            ("18", 0.19),
            ("19", 0.17),
            ("20", 0.11),
        ]
    )
    placed_at = datetime.combine(order_day, datetime.min.time(), tzinfo=timezone.utc)
    placed_at += timedelta(
        hours=int(placed_hour),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )

    prep_minutes = max(5, int(random.gauss(merchant["avg_prep_minutes"], 5)))
    courier_wait = max(1, int(random.gauss(5, 3)))
    travel_minutes = max(6, int(random.gauss(23, 9)))

    accepted_at = placed_at + timedelta(minutes=random.randint(1, 4))
    prepared_at = accepted_at + timedelta(minutes=prep_minutes)
    picked_up_at = prepared_at + timedelta(minutes=courier_wait)
    delivered_at = picked_up_at + timedelta(minutes=travel_minutes)

    status = weighted_choice(
        [("delivered", 0.89), ("cancelled_by_customer", 0.045), ("cancelled_by_merchant", 0.035), ("failed_delivery", 0.03)]
    )
    if status != "delivered":
        delivered_at = None

    order_id = f"o_{order_day.strftime('%Y%m%d')}_{order_number:08d}"

    # Order items are generated from the selected merchant's menu so downstream tests
    # can validate joins between facts, menu dimensions, and merchant dimensions.
    selected_items = random.sample(menu_by_merchant[merchant["merchant_id"]], random.randint(1, 3))
    item_rows: list[dict] = []
    subtotal = 0.0
    for sequence, item in enumerate(selected_items, start=1):
        quantity = random.randint(1, 3)
        item_total = round(item["base_price"] * quantity, 2)
        subtotal += item_total
        item_rows.append(
            {
                "order_id": order_id,
                "line_number": sequence,
                "menu_item_id": item["menu_item_id"],
                "merchant_id": merchant["merchant_id"],
                "quantity": quantity,
                "unit_price": item["base_price"],
                "line_total": item_total,
            }
        )

    discount = round(subtotal * random.choice([0, 0, 0, 0.05, 0.1, 0.15]), 2)
    delivery_fee = round(random.uniform(0.49, 6.99), 2)
    service_fee = round(subtotal * random.uniform(0.08, 0.16), 2)
    tax_amount = round((subtotal - discount) * random.uniform(0.06, 0.1025), 2)
    total = round(subtotal - discount + delivery_fee + service_fee + tax_amount, 2)

    has_key_defect = random.random() < defect_rate
    has_time_defect = random.random() < defect_rate
    has_payment_defect = random.random() < defect_rate

    # The three defect flags intentionally create critical DQ scenarios:
    # missing required keys, impossible order lifecycle timestamps, and negative
    # monetary amounts. Keeping the defects controlled makes reprocessing repeatable.
    order_row = {
        "event_id": f"evt_{uuid4().hex}",
        "order_id": None if has_key_defect else order_id,
        "customer_id": customer["customer_id"],
        "merchant_id": merchant["merchant_id"],
        "courier_id": courier["courier_id"],
        "city_id": merchant["city_id"],
        "order_status": status,
        "placed_at": iso_ts(placed_at),
        "accepted_at": iso_ts(accepted_at),
        "prepared_at": iso_ts(prepared_at),
        "picked_up_at": iso_ts(picked_up_at),
        "delivered_at": iso_ts(delivered_at) if delivered_at else None,
        "currency": "USD",
        "subtotal_amount": round(subtotal, 2),
        "discount_amount": discount,
        "estimated_delivery_minutes": random.randint(25, 48),
        "schema_version": 1,
    }
    if has_time_defect and delivered_at:
        order_row["delivered_at"] = iso_ts(placed_at - timedelta(minutes=3))

    payment_row = {
        "payment_id": f"pay_{uuid4().hex[:20]}",
        "order_id": order_id,
        "payment_status": "captured" if status == "delivered" else random.choice(["voided", "refunded"]),
        "payment_method": random.choice(["card", "wallet", "apple_pay", "paypal"]),
        "subtotal_amount": -subtotal if has_payment_defect else round(subtotal, 2),
        "delivery_fee": delivery_fee,
        "service_fee": service_fee,
        "tax_amount": tax_amount,
        "discount_amount": discount,
        "total_amount": total,
        "authorized_at": iso_ts(placed_at + timedelta(seconds=random.randint(5, 90))),
        "captured_at": iso_ts(delivered_at + timedelta(minutes=1)) if delivered_at else None,
        "currency": "USD",
    }

    order_events = []
    # Status events model the operational event stream separately from the order
    # snapshot. The silver layer keeps the newest event per event_id.
    for event_name, event_ts in [
        ("placed", placed_at),
        ("accepted", accepted_at),
        ("prepared", prepared_at),
        ("picked_up", picked_up_at),
        ("delivered", delivered_at),
    ]:
        if event_ts:
            order_events.append(
                {
                    "event_id": f"evt_{uuid4().hex}",
                    "order_id": order_id,
                    "status": event_name,
                    "event_ts": iso_ts(event_ts),
                    "actor_type": "system" if event_name == "placed" else random.choice(["merchant", "courier", "system"]),
                }
            )

    telemetry = []
    if delivered_at:
        # Courier locations are a higher-volume child feed. They are aggregated hourly
        # in gold so users can inspect movement density without scanning raw points.
        points = max(4, int((delivered_at - picked_up_at).total_seconds() // 300))
        base_lat = random.uniform(37.70, 40.75)
        base_lon = random.uniform(-122.45, -73.95)
        for point in range(points):
            telemetry_ts = picked_up_at + timedelta(minutes=point * 5)
            telemetry.append(
                {
                    "courier_id": courier["courier_id"],
                    "order_id": order_id,
                    "event_ts": iso_ts(telemetry_ts),
                    "latitude": round(base_lat + random.uniform(-0.05, 0.05), 6),
                    "longitude": round(base_lon + random.uniform(-0.05, 0.05), 6),
                    "speed_mph": round(random.uniform(3, 42), 2),
                    "battery_pct": random.randint(12, 100),
                }
            )

    refund_row = None
    if status in {"failed_delivery", "cancelled_by_merchant"} or random.random() < 0.025:
        # Refunds are sparse by design, matching the real shape of exception feeds.
        refund_row = {
            "refund_id": f"ref_{uuid4().hex[:20]}",
            "order_id": order_id,
            "refund_reason": random.choice(["late_delivery", "merchant_cancelled", "missing_item", "service_recovery"]),
            "refund_amount": round(random.uniform(3.0, max(total, 3.1)), 2),
            "requested_at": iso_ts(placed_at + timedelta(hours=random.randint(1, 72))),
            "refund_status": random.choice(["approved", "rejected", "pending"]),
        }

    rating_row = None
    if status == "delivered" and random.random() < 0.42:
        # Ratings arrive only for a subset of delivered orders, which keeps the model
        # honest about optional downstream relationships.
        rating_row = {
            "rating_id": f"rat_{uuid4().hex[:20]}",
            "order_id": order_id,
            "customer_id": customer["customer_id"],
            "merchant_id": merchant["merchant_id"],
            "courier_id": courier["courier_id"],
            "food_rating": random.randint(2, 5),
            "delivery_rating": random.randint(2, 5),
            "rated_at": iso_ts(delivered_at + timedelta(minutes=random.randint(10, 240))),
        }

    support_rows = []
    if random.random() < 0.055:
        # Support tickets are kept in the same return bundle as telemetry and separated
        # by shape below. This avoids adding extra control flow to the generator.
        opened_at = placed_at + timedelta(hours=random.randint(1, 96))
        support_rows.append(
            {
                "ticket_id": f"tkt_{uuid4().hex[:20]}",
                "order_id": order_id,
                "customer_id": customer["customer_id"],
                "reason": random.choice(SUPPORT_REASONS),
                "priority": weighted_choice([("low", 0.44), ("medium", 0.36), ("high", 0.16), ("urgent", 0.04)]),
                "opened_at": iso_ts(opened_at),
                "closed_at": iso_ts(opened_at + timedelta(hours=random.randint(1, 72))),
                "status": random.choice(["closed", "closed", "escalated", "waiting_on_customer"]),
            }
        )

    return order_row, payment_row, item_rows, order_events, refund_row, rating_row, telemetry + support_rows


def write_snapshot_files(
    output_dir: Path,
    batch_day: date,
    merchants: list[dict],
    customers: list[dict],
    couriers: list[dict],
    menu_items: list[dict],
) -> None:
    """Write snapshot files with controlled changes for SCD2 dimensions."""

    # Each snapshot includes an effective timestamp. Gold dimensions use that timestamp
    # as `valid_from`, closing the previous version only when tracked attributes change.
    effective_ts = iso_ts(datetime.combine(batch_day, datetime.min.time(), tzinfo=timezone.utc))
    merchant_rows = []
    for merchant in merchants:
        # Changes are concentrated on predictable days so the generated data reliably
        # contains historical versions without making every daily snapshot noisy.
        changed = batch_day.day in {8, 18, 28} and random.random() < 0.14
        row = dict(merchant)
        if changed:
            row["commission_rate"] = round(min(0.35, row["commission_rate"] + random.uniform(0.01, 0.035)), 4)
            row["merchant_tier"] = random.choice(["standard", "growth", "premium"])
        row["effective_ts"] = effective_ts
        merchant_rows.append(row)

    customer_rows = []
    for customer in random.sample(customers, min(len(customers), 850)):
        row = dict(customer)
        if batch_day.day in {10, 20, 30} and random.random() < 0.22:
            row["loyalty_tier"] = random.choice(["blue", "silver", "gold", "diamond"])
            row["marketing_opt_in"] = random.random() < 0.7
        row["effective_ts"] = effective_ts
        customer_rows.append(row)

    courier_rows = []
    for courier in random.sample(couriers, min(len(couriers), 420)):
        row = dict(courier)
        if batch_day.day in {12, 24} and random.random() < 0.12:
            row["is_active"] = random.random() < 0.9
            row["delivery_mode"] = random.choice(["bike", "scooter", "car", "walk"])
        row["effective_ts"] = effective_ts
        courier_rows.append(row)

    menu_rows = []
    for item in menu_items:
        row = dict(item)
        if batch_day.day in {7, 17, 27} and random.random() < 0.08:
            row["base_price"] = round(row["base_price"] * random.uniform(0.94, 1.11), 2)
            row["is_available"] = random.random() < 0.92
        row["effective_ts"] = effective_ts
        menu_rows.append(row)

    partition = f"batch_date={batch_day.isoformat()}"
    # The `batch_date=YYYY-MM-DD` folder pattern is central to the Auto Loader demo:
    # users can upload one date at a time and rerun the workflow to process only the
    # files that were not already recorded in the checkpoint.
    write_jsonl(output_dir / "merchant_snapshots" / partition / "merchant_snapshot.jsonl", merchant_rows)
    write_jsonl(output_dir / "customer_snapshots" / partition / "customer_snapshot.jsonl", customer_rows)
    write_jsonl(output_dir / "courier_snapshots" / partition / "courier_snapshot.jsonl", courier_rows)
    write_jsonl(output_dir / "menu_item_snapshots" / partition / "menu_item_snapshot.jsonl", menu_rows)


def generate_dataset(
    output_dir: Path,
    start_date: date,
    days: int,
    orders_per_day: int,
    seed: int,
    defect_rate: float,
) -> None:
    """Generate source files partitioned by batch date."""

    # A fixed seed makes the same command reproducible, which is helpful for comparing
    # dev and prod deployments or reproducing a failed DQ scenario later.
    random.seed(seed)
    merchants = build_merchants()
    customers = build_customers(max(orders_per_day * 4, 10000))
    couriers = build_couriers(max(orders_per_day, 2500))
    menu_items = menu_items_for_merchants(merchants)
    menu_by_merchant: dict[str, list[dict]] = {}
    for item in menu_items:
        menu_by_merchant.setdefault(item["merchant_id"], []).append(item)

    for day_offset in range(days):
        batch_day = start_date + timedelta(days=day_offset)
        write_snapshot_files(output_dir, batch_day, merchants, customers, couriers, menu_items)

        # These lists represent the independent files a marketplace platform might
        # receive from different operational services for the same business day.
        orders = []
        payments = []
        order_items = []
        order_events = []
        refunds = []
        ratings = []
        telemetry = []
        support = []

        for order_number in range(1, orders_per_day + 1):
            order, payment, items, events, refund, rating, mixed_rows = generate_order(
                batch_day,
                merchants,
                menu_by_merchant,
                customers,
                couriers,
                order_number + day_offset * orders_per_day,
                defect_rate,
            )
            if day_offset >= max(5, days // 4):
                # Late-appearing columns simulate schema evolution. Auto Loader should
                # add these fields to bronze while older files continue to process.
                order["app_version"] = random.choice(["6.221.0", "6.222.1", "6.223.0"])
                order["order_surface"] = random.choice(["ios", "android", "web"])
            orders.append(order)
            payments.append(payment)
            order_items.extend(items)
            order_events.extend(events)
            if refund:
                refunds.append(refund)
            if rating:
                ratings.append(rating)
            for row in mixed_rows:
                if "latitude" in row:
                    telemetry.append(row)
                else:
                    support.append(row)

        partition = f"batch_date={batch_day.isoformat()}"
        # Files are written per source dataset rather than as one combined extract so
        # bronze ingestion can maintain independent checkpoints and rescued-data state.
        write_jsonl(output_dir / "orders" / partition / "orders.jsonl", orders)
        write_jsonl(output_dir / "order_events" / partition / "order_events.jsonl", order_events)
        write_jsonl(output_dir / "order_items" / partition / "order_items.jsonl", order_items)
        write_jsonl(output_dir / "refunds" / partition / "refunds.jsonl", refunds)
        write_jsonl(output_dir / "ratings" / partition / "ratings.jsonl", ratings)
        write_jsonl(output_dir / "courier_locations" / partition / "courier_locations.jsonl", telemetry)
        write_jsonl(output_dir / "support_tickets" / partition / "support_tickets.jsonl", support)
        write_csv(
            output_dir / "payments" / partition / "payments.csv",
            payments,
            [
                "payment_id",
                "order_id",
                "payment_status",
                "payment_method",
                "subtotal_amount",
                "delivery_fee",
                "service_fee",
                "tax_amount",
                "discount_amount",
                "total_amount",
                "authorized_at",
                "captured_at",
                "currency",
            ],
        )

    manifest = {
        # The manifest is not required by the pipeline. It gives the operator a quick
        # audit of how much data was generated with which seed and defect rate.
        "generated_at": iso_ts(datetime.now(timezone.utc)),
        "source": "synthetic Uber Eats-style marketplace generator",
        "days": days,
        "orders_per_day": orders_per_day,
        "expected_orders": days * orders_per_day,
        "seed": seed,
        "defect_rate": defect_rate,
    }
    write_jsonl(output_dir / "_manifest" / "manifest.jsonl", [manifest])


def parse_args() -> argparse.Namespace:
    """Read command-line options."""

    # Defaults produce enough records for meaningful joins and optimizations while
    # still staying practical for Databricks Free Edition serverless execution.
    parser = argparse.ArgumentParser(description="Generate Uber Eats-style marketplace data.")
    parser.add_argument("--output-dir", required=True, help="Directory where landing files are written.")
    parser.add_argument("--start-date", default="2026-06-01", help="First batch date in YYYY-MM-DD format.")
    parser.add_argument("--days", type=int, default=45, help="Number of daily batches.")
    parser.add_argument("--orders-per-day", type=int, default=1200, help="Orders generated per day.")
    parser.add_argument("--seed", type=int, default=20260819, help="Deterministic random seed.")
    parser.add_argument("--defect-rate", type=float, default=0.0, help="Controlled critical bad-record rate.")
    return parser.parse_args()


def main() -> None:
    """Generate all landing datasets."""

    # The script intentionally writes to a local directory only. Uploading selected
    # batch folders into the UC volume remains a separate operator action.
    args = parse_args()
    output_dir = Path(args.output_dir)
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    generate_dataset(output_dir, start_date, args.days, args.orders_per_day, args.seed, args.defect_rate)
    print(f"Generated {args.days * args.orders_per_day:,} orders under {output_dir}")


if __name__ == "__main__":
    main()
