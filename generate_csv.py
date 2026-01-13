import csv
import random
from datetime import datetime, timedelta

TOTAL_ROWS = 10_000
UNIQUE_EMAILS = 2_000  # Adjusted to ensure we hit 10k rows

first_names = ["Alice", "Alicia", "Bob", "Charlie", "Diana", "Eve", "Frank"]
last_names = ["Smith", "Johnson", "Brown", "Taylor", "Williams"]
cities = ["London", "Bristol", "Manchester", "Leeds", None]
sources = ["instagram", "facebook", "google", "linkedin"]
phone_numbers = ["0712345678", "0798765432", "0788888888", None]

emails = [f"user{i}@test.com" for i in range(1, UNIQUE_EMAILS + 1)]

# Base date for timestamps
base_date = datetime(2024, 1, 1)

rows = []

for email in emails:
    # Determine how many versions this email will have (1-8)
    versions = random.randint(1, 8)
    
    # Initial values
    first_name = random.choice(first_names)
    last_name = random.choice(last_names)
    city = random.choice(cities)
    phone = random.choice(phone_numbers)
    source = random.choice(sources)
    
    current_date = base_date + timedelta(days=random.randint(0, 365))
    
    for v in range(versions):
        # Add timestamp
        timestamp = current_date.strftime("%Y-%m-%d %H:%M:%S")
        
        # Add the row
        rows.append([
            email,
            first_name,
            last_name,
            city,
            phone,
            source,
            timestamp
        ])
        
        # For next version, introduce changes
        if v < versions - 1:  # Don't mutate on last iteration
            change_type = random.random()
            
            if change_type < 0.05:  # 5% - exact duplicate (no change)
                pass
            elif change_type < 0.30:  # 25% - change first name
                first_name = random.choice(first_names)
            elif change_type < 0.50:  # 20% - change city
                city = random.choice(cities)
            elif change_type < 0.65:  # 15% - change phone
                phone = random.choice(phone_numbers)
            elif change_type < 0.80:  # 15% - change source
                source = random.choice(sources)
            else:  # 20% - change multiple fields
                if random.random() < 0.5:
                    first_name = random.choice(first_names)
                if random.random() < 0.5:
                    city = random.choice(cities)
                if random.random() < 0.5:
                    phone = random.choice(phone_numbers)
            
            # Advance time by 1-90 days
            current_date += timedelta(days=random.randint(1, 90))

# Add intentional edge cases for testing
# 1. Multiple exact duplicates
for i in range(5):
    dup_email = f"duplicate{i}@test.com"
    dup_data = ["Alice", "Smith", "London", "0712345678", "google"]
    for j in range(3):
        rows.append([dup_email] + dup_data + [
            (base_date + timedelta(days=i*10 + j)).strftime("%Y-%m-%d %H:%M:%S")
        ])

# 2. NULL transitions
null_test_email = "null_test@test.com"
null_date = base_date + timedelta(days=100)
rows.append([null_test_email, "Bob", "Brown", None, None, "facebook", 
             null_date.strftime("%Y-%m-%d %H:%M:%S")])
rows.append([null_test_email, "Bob", "Brown", "Leeds", "0712345678", "facebook", 
             (null_date + timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S")])
rows.append([null_test_email, "Bob", "Brown", None, None, "facebook", 
             (null_date + timedelta(days=10)).strftime("%Y-%m-%d %H:%M:%S")])

# Shuffle to simulate realistic data arrival
random.shuffle(rows)

# Ensure exactly 10k rows
rows = rows[:TOTAL_ROWS]

with open("customers_seed.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "email",
        "first_name",
        "last_name",
        "city",
        "phone_number",
        "source_system",
        "timestamp"
    ])
    writer.writerows(rows)

print(f"✅ Generated customers_seed.csv with {len(rows)} rows")
print(f"   - ~{UNIQUE_EMAILS} unique emails with multiple versions")
print(f"   - Includes exact duplicates, NULL transitions, and multiple changes")