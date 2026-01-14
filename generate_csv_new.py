import csv
import random
from datetime import datetime, timedelta

# -----------------------------
# Config
# -----------------------------
TOTAL_ROWS = 10_000
UNIQUE_EMAILS = 2_000

first_names = ["Alice", "Alicia", "Bob", "Charlie", "Diana", "Eve", "Frank"]
last_names = ["Smith", "Johnson", "Brown", "Taylor", "Williams"]
cities = ["London", "Bristol", "Manchester", "Leeds", None]
sources = ["instagram", "facebook", "google", "linkedin"]
phone_numbers = ["0712345678", "0798765432", "0788888888", None]

emails = [f"user{i}@test.com" for i in range(1, UNIQUE_EMAILS + 1)]
base_date = datetime(2024, 1, 1)

rows = []
next_customer_id = 1

# -----------------------------
# Generate customer data
# -----------------------------
for email in emails:
    customer_id = next_customer_id
    next_customer_id += 1

    versions = random.randint(1, 8)

    first_name = random.choice(first_names)
    last_name = random.choice(last_names)
    city = random.choice(cities)
    phone = random.choice(phone_numbers)
    source = random.choice(sources)

    current_date = base_date + timedelta(days=random.randint(0, 365))

    for v in range(versions):
        rows.append([
            customer_id,
            email,
            first_name,
            last_name,
            city,
            phone,
            source,
            current_date.strftime("%Y-%m-%d %H:%M:%S")
        ])

        if v < versions - 1:
            change_type = random.random()

            # 1️⃣ Exact duplicate (including customer_id change)
            if change_type < 0.10:
                customer_id = next_customer_id
                next_customer_id += 1

            # 2️⃣ Real changes (these SHOULD create new rows)
            elif change_type < 0.35:
                first_name = random.choice(first_names)
            elif change_type < 0.55:
                city = random.choice(cities)
            elif change_type < 0.70:
                phone = random.choice(phone_numbers)
            elif change_type < 0.85:
                source = random.choice(sources)
            else:
                if random.random() < 0.5:
                    first_name = random.choice(first_names)
                if random.random() < 0.5:
                    city = random.choice(cities)
                if random.random() < 0.5:
                    phone = random.choice(phone_numbers)

                # customer_id may change WITH real change
                if random.random() < 0.5:
                    customer_id = next_customer_id
                    next_customer_id += 1

            current_date += timedelta(days=random.randint(1, 90))

# -----------------------------
# Edge case: explicit customer_id re-key with no business change
# -----------------------------
for i in range(5):
    email = f"rekey_only{i}@test.com"
    cid1 = next_customer_id
    cid2 = next_customer_id + 1
    next_customer_id += 2

    for cid in [cid1, cid2]:
        rows.append([
            cid,
            email,
            "Alice",
            "Smith",
            "London",
            "0712345678",
            "google",
            (base_date + timedelta(days=i)).strftime("%Y-%m-%d %H:%M:%S")
        ])

# -----------------------------
# Shuffle + limit
# -----------------------------
random.shuffle(rows)
rows = rows[:TOTAL_ROWS]

# -----------------------------
# Write CSV
# -----------------------------
with open("customers_seed_new.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "customer_id",
        "email",
        "first_name",
        "last_name",
        "city",
        "phone_number",
        "source_system",
        "timestamp"
    ])
    writer.writerows(rows)

print("✅ Generated customers_seed_new.csv")
print("   - email = single person identity")
print("   - customer_id is non-key")
print("   - customer_id-only changes exist (should NOT create SCD rows)")
print("   - business attribute changes DO create new rows")
