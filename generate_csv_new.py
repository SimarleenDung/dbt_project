import csv
import random
from datetime import datetime, timedelta

UNIQUE_EMAILS = 2000  # Will generate ~10k rows across versions
TOTAL_TARGET_ROWS = 10000

first_names = ["Alice", "Alicia", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Ivy"]
last_names = ["Smith", "Johnson", "Brown", "Taylor", "Williams", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
cities = ["London", "Bristol", "Manchester", "Leeds", "Birmingham", "Liverpool", "Newcastle", None]
sources = ["instagram", "facebook", "google", "linkedin"]
phone_numbers = ["0712345678", "0798765432", "0788888888", "0755555555", None]

def generate_daily_file(filename, day_offset, emails_list):
    """
    Generate a daily CSV file.
    
    On day 1: Each email appears 1 time (initial load)
    On day 2+: Some emails appear again with changes, others stay same
    """
    
    base_date = datetime(2025, 1, 1) + timedelta(days=day_offset)
    rows = []
    
    if day_offset == 0:
        # DAY 1: Initial load - each email appears once
        for customer_id, email in enumerate(emails_list, start=1):
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            city = random.choice(cities)
            phone = random.choice(phone_numbers)
            source = random.choice(sources)
            
            # Random time on day 1
            random_hour = random.randint(0, 23)
            random_minute = random.randint(0, 59)
            random_second = random.randint(0, 59)
            timestamp = base_date.replace(hour=random_hour, minute=random_minute, second=random_second)
            
            rows.append([
                customer_id,
                email,
                first_name,
                last_name,
                city,
                phone,
                source,
                timestamp.strftime("%Y-%m-%d %H:%M:%S")
            ])
    else:
        # DAY 2+: Load all customers again, but ~20% will have changes
        for customer_id, email in enumerate(emails_list, start=1):
            # Load the previous version from the same customer
            first_name = random.choice(first_names)
            last_name = random.choice(last_names)
            city = random.choice(cities)
            phone = random.choice(phone_numbers)
            source = random.choice(sources)
            
            # Decide if this email changes on this day (20% chance)
            if random.random() < 0.20:
                # Apply a change
                change_type = random.choice(['name', 'city', 'phone', 'source', 'multiple'])
                
                if change_type == 'name':
                    first_name = random.choice(first_names)
                elif change_type == 'city':
                    city = random.choice(cities)
                elif change_type == 'phone':
                    phone = random.choice(phone_numbers)
                elif change_type == 'source':
                    source = random.choice(sources)
                else:  # multiple
                    if random.random() < 0.5:
                        first_name = random.choice(first_names)
                    if random.random() < 0.5:
                        city = random.choice(cities)
                    if random.random() < 0.5:
                        phone = random.choice(phone_numbers)
            
            # Random time on this day
            random_hour = random.randint(0, 23)
            random_minute = random.randint(0, 59)
            random_second = random.randint(0, 59)
            timestamp = base_date.replace(hour=random_hour, minute=random_minute, second=random_second)
            
            rows.append([
                customer_id,
                email,
                first_name,
                last_name,
                city,
                phone,
                source,
                timestamp.strftime("%Y-%m-%d %H:%M:%S")
            ])
            
            # ~5% chance of duplicate on same day
            if random.random() < 0.05:
                dup_hour = random.randint(0, 23)
                dup_minute = random.randint(0, 59)
                dup_second = random.randint(0, 59)
                dup_timestamp = base_date.replace(hour=dup_hour, minute=dup_minute, second=dup_second)
                
                rows.append([
                    customer_id,
                    email,
                    first_name,
                    last_name,
                    city,
                    phone,
                    source,
                    dup_timestamp.strftime("%Y-%m-%d %H:%M:%S")
                ])
    
    # Write to CSV
    with open(filename, "w", newline="") as f:
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
    
    print(f"✅ Generated {filename} with {len(rows)} rows")
    return rows

# ===== GENERATE TEST DATA =====

print("Generating test data with consistent emails across days...\n")

# Create list of unique emails (same across all days)
emails_list = [f"user{i}@test.com" for i in range(1, UNIQUE_EMAILS + 1)]

# Generate day 1
generate_daily_file("customers_day_1.csv", day_offset=0, emails_list=emails_list)

# Generate day 2 (same emails, ~20% with changes)
generate_daily_file("customers_day_2.csv", day_offset=1, emails_list=emails_list)

# Generate day 3 (same emails, ~20% with changes)
generate_daily_file("customers_day_3.csv", day_offset=2, emails_list=emails_list)

print("\n" + "="*60)
print("Test files generated successfully!")
print("="*60)
print("\nExpected SCD2 behavior:")
print("- Day 1: 2,000 unique emails × ~1 version = 2,000 rows, all is_current='Y'")
print("- Day 2: 2,000 emails reloaded, ~400 (20%) have changes")
print("        → Old versions of 400 closed (is_current='N')")
print("        → New versions of 400 added (is_current='Y')")
print("        → 1,600 unchanged records still marked 'Y'")
print("- Day 3: 2,000 emails reloaded, ~400 (20%) have changes")
print("        → Same pattern repeats")
print("\nTotal expected rows in final SCD2 table: ~2,800")
print("="*60)