import pandas as pd
from datetime import datetime, timedelta
import random
import string

random.seed(42)

# Sample data pools for variety
first_names = ["Alice", "Bob", "Charlie", "David", "Emma", "Frank", "Grace", "Henry", "Ivy", "Jack",
               "Karen", "Liam", "Mary", "Nathan", "Olivia", "Peter", "Quinn", "Rachel", "Sam", "Tina"]

last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
              "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"]

cities = ["NYC", "LA", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
          "Austin", "Jacksonville", "Portland", "Seattle", "Denver", "Boston", "Miami", "Atlanta", "Nashville", "Detroit"]

sources = ["web", "mobile", "api", "partner"]

def generate_email(first_name, last_name, customer_id):
    """Generate a unique email"""
    return f"{first_name.lower()}.{last_name.lower()}{customer_id}@example.com"

def generate_phone():
    """Generate a random phone number"""
    return f"555{random.randint(1000000, 9999999)}"

def generate_base_customers(num_customers=10000):
    """Generate base customer dataset"""
    customers = []
    
    for i in range(num_customers):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        
        customers.append({
            "customer_id": i + 1,
            "email": generate_email(first_name, last_name, i + 1),
            "first_name": first_name,
            "last_name": last_name,
            "city": random.choice(cities),
            "phone": generate_phone(),
            "source_system": random.choice(sources)
        })
    
    return customers

def generate_daily_file(base_customers, day_offset, change_percentage=0.2, duplicate_percentage=0.05, output_path=None):
    """
    Generate a CSV file for a specific day with optional changes.
    
    day_offset: 0 for day 1, 1 for day 2, etc.
    change_percentage: % of customers that should have changes (default 20%)
    duplicate_percentage: % of customers with duplicate records same day (default 5%)
    output_path: where to save the CSV
    """
    
    base_date = datetime(2025, 1, 1) + timedelta(days=day_offset)
    
    rows = []
    num_to_change = int(len(base_customers) * change_percentage)
    num_to_duplicate = int(len(base_customers) * duplicate_percentage)
    
    customers_to_change = random.sample(range(len(base_customers)), num_to_change)
    customers_to_duplicate = random.sample(range(len(base_customers)), num_to_duplicate)
    
    for idx, customer in enumerate(base_customers):
        row = customer.copy()
        
        # Generate random time for this record (spread throughout the day)
        random_hour = random.randint(0, 23)
        random_minute = random.randint(0, 59)
        random_second = random.randint(0, 59)
        timestamp = base_date.replace(hour=random_hour, minute=random_minute, second=random_second)
        timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')
        
        # Apply random changes to selected customers
        if idx in customers_to_change:
            change_type = random.choice(['name', 'city', 'phone', 'multiple'])
            
            if change_type == 'name':
                row["first_name"] = random.choice(first_names)
            elif change_type == 'city':
                row["city"] = random.choice(cities)
            elif change_type == 'phone':
                row["phone"] = generate_phone()
            else:  # multiple changes
                if random.random() > 0.5:
                    row["first_name"] = random.choice(first_names)
                if random.random() > 0.5:
                    row["city"] = random.choice(cities)
                if random.random() > 0.5:
                    row["phone"] = generate_phone()
        
        row["timestamp"] = timestamp_str
        rows.append(row)
        
        # Add duplicate record for selected customers (with different values)
        if idx in customers_to_duplicate:
            dup_row = row.copy()
            dup_row["first_name"] = random.choice(first_names)  # Change first_name in duplicate
            # Give duplicate a slightly later timestamp on same day
            dup_timestamp = base_date.replace(hour=random.randint(0, 23), minute=random.randint(0, 59), second=random.randint(0, 59))
            dup_row["timestamp"] = dup_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            rows.append(dup_row)
    
    df = pd.DataFrame(rows)
    
    if output_path is None:
        output_path = f"customers_day_{day_offset + 1}.csv"
    
    df.to_csv(output_path, index=False)
    print(f"Generated: {output_path} ({len(df)} rows)")
    return df

# ===== GENERATE TEST DATA =====

print("Generating base customer data (10,000 rows)...")
base_customers = generate_base_customers(num_customers=10000)

print("\n" + "="*60)
print("Generating daily files...")
print("="*60 + "\n")

# DAY 1: Initial load
generate_daily_file(
    base_customers,
    day_offset=0,
    change_percentage=0.0,  # No changes on day 1
    duplicate_percentage=0.0,  # No duplicates on day 1
    output_path="customers_day_1.csv"
)

# DAY 2: 20% of customers changed, 5% have duplicates
generate_daily_file(
    base_customers,
    day_offset=1,
    change_percentage=0.2,
    duplicate_percentage=0.05,
    output_path="customers_day_2.csv"
)

# DAY 3: 15% of customers changed, 3% have duplicates
generate_daily_file(
    base_customers,
    day_offset=2,
    change_percentage=0.15,
    duplicate_percentage=0.03,
    output_path="customers_day_3.csv"
)

print("\n" + "="*60)
print("Test files generated successfully!")
print("="*60)
print("\nExpected SCD2 behavior:")
print("- Day 1: 10,000 current records (is_current = 'Y')")
print("- Day 2: ~2,000 records closed (is_current = 'N'), 2,000 new versions added (is_current = 'Y')")
print("        Plus ~500 duplicates that get deduplicated")
print("- Day 3: ~1,500 records closed (is_current = 'N'), 1,500 new versions added (is_current = 'Y')")
print("        Plus ~300 duplicates that get deduplicated")
print("\nTotal expected rows in final SCD2 table:")
print("- 10,000 (original) + 2,000 (day 2 old) + 1,500 (day 3 old) = 13,500 rows")
print("="*60)