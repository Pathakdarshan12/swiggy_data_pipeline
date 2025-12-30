import csv
import random
from datetime import datetime, timedelta
from faker import Faker
import json
import os
from pathlib import Path
from tqdm import tqdm
import multiprocessing as mp
from functools import partial

fake = Faker('en_IN')
Faker.seed(42)
random.seed(42)

# Create base data directory structure
BASE_DIR = Path(__file__).parent.parent / 'data'
TABLES = ['location', 'restaurant', 'customer', 'customer_address', 'menu',
          'delivery_agent', 'delivery', 'order', 'order_item']

# Create directories if they don't exist
for table in TABLES:
    table_dir = BASE_DIR / table
    table_dir.mkdir(parents=True, exist_ok=True)

# ===== CONFIGURATION: ADJUST BASED ON YOUR NEEDS =====
# Small Scale (Testing - runs in seconds)
NUM_LOCATIONS = 600
NUM_RESTAURANTS = 5_000
NUM_CUSTOMERS = 50_000
NUM_CUSTOMER_ADDRESSES = 75_000
NUM_MENU_ITEMS = 150_000
NUM_DELIVERY_AGENTS = 10_000
NUM_ORDERS = 200_000
NUM_ORDER_ITEMS = 500_000
NUM_DELIVERIES = 200_000

# Medium Scale (Development - runs in 1-2 minutes)
# NUM_LOCATIONS = 600
# NUM_RESTAURANTS = 25_000
# NUM_CUSTOMERS = 500_000
# NUM_CUSTOMER_ADDRESSES = 750_000
# NUM_MENU_ITEMS = 750_000
# NUM_DELIVERY_AGENTS = 50_000
# NUM_ORDERS = 2_000_000
# NUM_ORDER_ITEMS = 5_000_000
# NUM_DELIVERIES = 2_000_000

# Large Scale (Production - runs in 5-10 minutes with chunking)
# NUM_LOCATIONS = 600
# NUM_RESTAURANTS = 250_000
# NUM_CUSTOMERS = 5_000_000
# NUM_CUSTOMER_ADDRESSES = 7_500_000
# NUM_MENU_ITEMS = 7_500_000
# NUM_DELIVERY_AGENTS = 350_000
# NUM_ORDERS = 15_000_000
# NUM_ORDER_ITEMS = 40_000_000
# NUM_DELIVERIES = 15_000_000

# Updated realistic numbers
# NUM_LOCATIONS = 600  # Cities where service operates
# NUM_RESTAURANTS = 250_000  # Restaurant partners
# NUM_CUSTOMERS = 18_000_000  # Monthly active customers
# NUM_CUSTOMER_ADDRESSES = 27_000_000  # 1.5 addresses per customer
# NUM_MENU_ITEMS = 7_500_000  # ~30 items per restaurant
# NUM_DELIVERY_AGENTS = 350_000  # Active delivery partners
# NUM_ORDERS_PER_DAY = 1_500_000  # Daily orders
# NUM_ORDERS_PER_MONTH = 45_000_000  # Monthly orders
# NUM_ORDER_ITEMS = 120_000_000  # ~2.7 items per order average
# NUM_DELIVERIES = 45_000_000  # One delivery per order
# NUM_ORDERS = 15_000_000

# ORDERS BY TIME PERIOD:
# Per Hour (Average): 62,500 orders
# Per Hour (Peak 6-10 PM): 225,000 orders
# Per Day: 1,500,000 orders
# Per Week: 10,500,000 orders
# Per Month: 45,000,000 orders

CHUNK_SIZE = 50_000  # Write in chunks to avoid memory issues

# ===== REFERENCE DATA POOLS (Pre-generated for speed) =====
CITIES = [
    ('Mumbai', 'Maharashtra'),
    ('Delhi', 'Delhi'),
    ('Bangalore', 'Karnataka'),
    ('Hyderabad', 'Telangana'),
    ('Chennai', 'Tamil Nadu'),
    ('Kolkata', 'West Bengal'),
    ('Pune', 'Maharashtra'),
    ('Ahmedabad', 'Gujarat'),
    ('Jaipur', 'Rajasthan'),
    ('Lucknow', 'Uttar Pradesh'),
    ('Surat', 'Gujarat'),
    ('Visakhapatnam', 'Andhra Pradesh'),
    ('Vadodara', 'Gujarat'),
    ('Indore', 'Madhya Pradesh'),
    ('Nagpur', 'Maharashtra'),
    ('Bhopal', 'Madhya Pradesh'),
    ('Patna', 'Bihar'),
    ('Ludhiana', 'Punjab'),
    ('Coimbatore', 'Tamil Nadu'),
    ('Kanpur', 'Uttar Pradesh'),
    ('Nagpur', 'Maharashtra'),
    ('Thane', 'Maharashtra'),
    ('Meerut', 'Uttar Pradesh'),
    ('Varanasi', 'Uttar Pradesh'),
    ('Bhubaneswar', 'Odisha'),
    ('Guwahati', 'Assam'),
    ('Dehradun', 'Uttarakhand'),
    ('Ranchi', 'Jharkhand'),
    ('Raipur', 'Chhattisgarh'),
    ('Amritsar', 'Punjab'),
    ('Jabalpur', 'Madhya Pradesh'),
    ('Guntur', 'Andhra Pradesh'),
    ('Tiruchirappalli', 'Tamil Nadu'),
    ('Kochi', 'Kerala'),
    ('Vijayawada', 'Andhra Pradesh'),
    ('Madurai', 'Tamil Nadu'),
    ('Jalandhar', 'Punjab'),
    ('Hubli-Dharwad', 'Karnataka'),
    ('Mysore', 'Karnataka'),
    ('Bareilly', 'Uttar Pradesh')
]


CUISINES = ['North Indian', 'South Indian', 'Chinese', 'Italian', 'Continental',
            'Mexican', 'Thai', 'Japanese', 'Fast Food', 'Biryani', 'Street Food',
            'Desserts', 'Cafe', 'Bakery', 'Bengali', 'Gujarati', 'Punjabi']

CATEGORIES = ['Appetizer', 'Main Course', 'Dessert', 'Beverage', 'Breakfast', 'Salad', 'Snacks']
ITEM_TYPES = ['Veg', 'Non-Veg', 'Vegan', 'Egg']
VEHICLE_TYPES = ['Bike', 'Scooter', 'Bicycle', 'Car']
PAYMENT_METHODS = ['Credit Card', 'Debit Card', 'UPI', 'Cash', 'Wallet']
ORDER_STATUSES = ['Pending', 'Confirmed', 'Preparing', 'Ready', 'Completed', 'Cancelled']
DELIVERY_STATUSES = ['Assigned', 'Picked Up', 'In Transit', 'Delivered', 'Failed']
LOGIN_METHODS = ['Google', 'Facebook', 'Email', 'Phone', 'Apple']
ADDRESS_TYPES = ['Home', 'Work', 'Other']

# Pre-generate name pools for speed
print("Pre-generating data pools...")
NAME_POOL = [fake.name() for _ in range(10000)]
RESTAURANT_SUFFIXES = ['Restaurant', 'Cafe', 'Kitchen', 'Dhaba', 'Bistro', 'Eatery', 'Express', 'Hub']
COMPANY_POOL = [fake.company() for _ in range(5000)]
STREET_POOL = [fake.street_name() for _ in range(5000)]
BUILDING_SUFFIXES = ['Tower', 'Apartment', 'Complex', 'Villa', 'Residency', 'Heights']
LANDMARKS = ['Mall', 'Park', 'School', 'Hospital', 'Metro Station', 'Market', 'Temple', 'Cinema']

# Food item pools
ITEM_NAMES = {
    'Veg': ['Paneer Tikka', 'Veg Biryani', 'Dal Makhani', 'Palak Paneer', 'Mushroom Masala',
            'Aloo Gobi', 'Chole Bhature', 'Veg Pulao', 'Paneer Butter Masala', 'Mix Veg'],
    'Non-Veg': ['Chicken Tikka', 'Butter Chicken', 'Mutton Biryani', 'Fish Curry', 'Prawn Masala',
                'Chicken Dum Biryani', 'Tandoori Chicken', 'Chicken Curry', 'Fish Fry', 'Mutton Rogan Josh'],
    'Vegan': ['Tofu Curry', 'Vegan Burger', 'Salad Bowl', 'Veggie Wrap', 'Quinoa Bowl',
              'Vegan Pizza', 'Smoothie Bowl', 'Falafel Wrap', 'Vegan Pasta', 'Buddha Bowl'],
    'Egg': ['Egg Curry', 'Omelette', 'Egg Biryani', 'Egg Fried Rice', 'Boiled Eggs',
            'Egg Bhurji', 'Egg Roll', 'Masala Omelette', 'Egg Sandwich', 'Egg Paratha']
}


def generate_indian_pincode():
    return str(random.randint(110001, 855118))


def write_csv_chunked(filepath, data_generator, fieldnames, total, chunk_size=CHUNK_SIZE):
    """Write CSV in chunks to handle large datasets"""
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        chunk = []
        for i, row in enumerate(tqdm(data_generator, total=total, desc=f"Writing {filepath.name}")):
            chunk.append(row)
            if len(chunk) >= chunk_size:
                writer.writerows(chunk)
                chunk = []

        # Write remaining rows
        if chunk:
            writer.writerows(chunk)


# ===== DATA GENERATORS (Memory-efficient) =====

def generate_locations():
    for i in range(1, NUM_LOCATIONS + 1):
        city, state = random.choice(CITIES)
        yield {
            'LOCATION_BRZ_ID': i,
            'CITY': city,
            'STATE': state,
            'ZIP_CODE': generate_indian_pincode()
        }


def generate_restaurants(location_ids):
    for i in range(1, NUM_RESTAURANTS + 1):
        lat = round(random.uniform(8.0, 35.0), 6)
        lon = round(random.uniform(68.0, 97.0), 6)

        yield {
            'RESTAURANT_BRZ_ID': i,
            'FSSAI_REGISTRATION_NO': random.randint(10000000000000, 99999999999999),
            'RESTAURANT_NAME': random.choice(COMPANY_POOL) + ' ' + random.choice(RESTAURANT_SUFFIXES),
            'CUISINE_TYPE': random.choice(CUISINES),
            'PRICING_FOR_TWO': str(random.choice([300, 400, 500, 600, 800, 1000, 1200, 1500, 2000])),
            'RESTAURANT_PHONE': f"+91{random.randint(7000000000, 9999999999)}",
            'OPERATING_HOURS': '10:00 AM - 11:00 PM',
            'LOCATION_ID': random.choice(location_ids),
            'ACTIVE_FLAG': random.choice(['Y', 'Y', 'Y', 'Y', 'N']),
            'OPEN_STATUS': random.choice(['Open', 'Open', 'Open', 'Closed', 'Temporarily Closed']),
            'LOCALITY': random.choice(STREET_POOL),
            'RESTAURANT_ADDRESS': f"{random.randint(1, 500)}, {random.choice(STREET_POOL)}",
            'LATITUDE': lat,
            'LONGITUDE': lon
        }


def generate_customers():
    for i in range(1, NUM_CUSTOMERS + 1):
        gender = random.choice(['Male', 'Female', 'Other'])
        dob = fake.date_of_birth(minimum_age=18, maximum_age=70)
        has_anniversary = random.random() > 0.4
        anniversary = fake.date_between(start_date='-20y', end_date='today') if has_anniversary else None

        preferences = {
            'favorite_cuisines': random.sample(CUISINES, k=random.randint(1, 3)),
            'dietary_restrictions': random.choice([[], ['Vegetarian'], ['Vegan'], ['Gluten-Free']]),
            'spice_level': random.choice(['Mild', 'Medium', 'Hot'])
        }

        yield {
            'CUSTOMER_BRZ_ID': i,
            'CUSTOMER_NAME': random.choice(NAME_POOL),
            'MOBILE': f"+91{random.randint(7000000000, 9999999999)}",
            'EMAIL': f"user{i}@{random.choice(['gmail.com', 'yahoo.com', 'outlook.com'])}",
            'LOGIN_BY_USING': random.choice(LOGIN_METHODS),
            'GENDER': gender,
            'DOB': dob.strftime('%Y-%m-%d'),
            'ANNIVERSARY': anniversary.strftime('%Y-%m-%d') if anniversary else '',
            'PREFERENCES': json.dumps(preferences)
        }


def generate_customer_addresses(customer_ids):
    for i in range(1, NUM_CUSTOMER_ADDRESSES + 1):
        city, state = random.choice(CITIES)
        lat = round(random.uniform(8.0, 35.0), 6)
        lon = round(random.uniform(68.0, 97.0), 6)

        yield {
            'CUSTOMER_ADDRESS_BRZ_ID': i,
            'CUSTOMER_ID': random.choice(customer_ids),
            'FLAT_NO': random.randint(1, 500),
            'HOUSE_NO': random.randint(1, 999),
            'FLOOR_NO': random.randint(0, 20),
            'BUILDING': f"{random.choice(COMPANY_POOL)[:15]} {random.choice(BUILDING_SUFFIXES)}",
            'LANDMARK': f"Near {random.choice(LANDMARKS)}",
            'LOCALITY': random.choice(STREET_POOL),
            'CITY': city,
            'STATE': state,
            'ZIPCODE': int(generate_indian_pincode()),
            'COORDINATES': f"{lat},{lon}",
            'PRIMARYFLAG': 'Y' if random.random() > 0.7 else 'N',
            'ADDRESSTYPE': random.choice(ADDRESS_TYPES)
        }


def generate_menu_items(restaurant_ids):
    for i in range(1, NUM_MENU_ITEMS + 1):
        item_type = random.choice(ITEM_TYPES)
        category = random.choice(CATEGORIES)

        yield {
            'MENU_ID': i,
            'RESTAURANT_ID': random.choice(restaurant_ids),
            'ITEM_NAME': random.choice(ITEM_NAMES[item_type]),
            'DESCRIPTION': f"Delicious {item_type} {category.lower()}",
            'PRICE': round(random.uniform(50, 500), 2),
            'CATEGORY': category,
            'AVAILABILITY': random.choice(['Available', 'Available', 'Available', 'Out of Stock']),
            'ITEM_TYPE': item_type
        }


def generate_delivery_agents(location_ids):
    for i in range(1, NUM_DELIVERY_AGENTS + 1):
        yield {
            'DELIVERY_AGENT_ID': i,
            'DELIVERY_AGENT_NAME': random.choice(NAME_POOL),
            'PHONE': random.randint(7000000000, 9999999999),
            'VEHICLE_TYPE': random.choice(VEHICLE_TYPES),
            'LOCATION_ID': random.choice(location_ids),
            'IS_ACTIVE': random.choice(['Y', 'Y', 'Y', 'N']),
            'GENDER': random.choice(['Male', 'Female']),
            'RATING': round(random.uniform(3.5, 5.0), 1)
        }


def generate_orders(customer_ids, restaurant_ids):
    start_date = datetime.now() - timedelta(days=365)

    for i in range(1, NUM_ORDERS + 1):
        order_date = start_date + timedelta(days=random.randint(0, 365))

        yield {
            'ORDER_ID': f'ORD{i:08d}',
            'CUSTOMER_ID': random.choice(customer_ids),
            'RESTAURANT_ID': random.choice(restaurant_ids),
            'ORDER_DATE': order_date.strftime('%Y-%m-%d'),
            'TOTAL_AMOUNT': round(random.uniform(200, 2000), 2),
            'ORDER_STATUS': random.choice(ORDER_STATUSES),
            'PAYMENT_METHOD': random.choice(PAYMENT_METHODS)
        }


def generate_order_items(order_ids, menu_ids):
    for i in range(1, NUM_ORDER_ITEMS + 1):
        quantity = random.randint(1, 5)
        price = round(random.uniform(50, 500), 2)
        subtotal = round(quantity * price, 2)

        yield {
            'ORDER_ITEM_ID': f'OI{i:09d}',
            'ORDER_ID': random.choice(order_ids),
            'MENU_ID': random.choice(menu_ids),
            'QUANTITY': quantity,
            'PRICE': price,
            'SUBTOTAL': subtotal,
            'ORDER_TIMESTAMP': (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
        }


def generate_deliveries(order_ids, agent_ids, address_ids):
    for i in range(1, NUM_DELIVERIES + 1):
        delivery_date = datetime.now() - timedelta(days=random.randint(0, 365),
                                                   minutes=random.randint(30, 120))

        # Simulate data quality issues
        order_id_raw = random.choice(order_ids) if random.random() > 0.02 else ''
        agent_id_raw = str(random.choice(agent_ids)) if random.random() > 0.03 else 'NULL'
        status_raw = random.choice(DELIVERY_STATUSES) if random.random() > 0.01 else ''
        estimated_time_raw = f"{random.randint(20, 60)} mins" if random.random() > 0.05 else 'TBD'
        address_id_raw = str(random.choice(address_ids)) if random.random() > 0.02 else ''
        delivery_date_raw = delivery_date.strftime('%Y-%m-%d %H:%M:%S%z') if random.random() > 0.04 else ''

        yield {
            'DELIVERY_ID': f'DEL{i:08d}',
            'ORDER_ID': random.choice(order_ids),
            'DELIVERY_AGENT_ID': random.choice(agent_ids),
            'DELIVERY_STATUS': random.choice(DELIVERY_STATUSES),
            'ESTIMATED_TIME': f"{random.randint(20, 60)} mins",
            'CUSTOMER_ADDRESS_ID': random.choice(address_ids),
            'DELIVERY_DATE': delivery_date.strftime('%Y-%m-%d %H:%M:%S+05:30'),
            'ORDER_ID_RAW': order_id_raw,
            'DELIVERY_AGENT_ID_RAW': agent_id_raw,
            'DELIVERY_STATUS_RAW': status_raw,
            'ESTIMATED_TIME_RAW': estimated_time_raw,
            'CUSTOMER_ADDRESS_ID_RAW': address_id_raw,
            'DELIVERY_DATE_RAW': delivery_date_raw,
            'INGEST_RUN_ID': f'RUN_{datetime.now().strftime("%Y%m%d_%H%M%S")}',
            'CREATED_AT': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'UPDATED_AT': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }


# ===== MAIN EXECUTION =====
if __name__ == '__main__':
    print("\n🚀 Starting Optimized Food Delivery Data Generation")
    print(f"📊 Scale: {NUM_ORDERS:,} orders | {NUM_CUSTOMERS:,} customers | {NUM_RESTAURANTS:,} restaurants\n")

    start_time = datetime.now()

    # Generate Location
    print("1/9 Generating LOCATION_BRZ...")
    write_csv_chunked(
        BASE_DIR / 'location' / 'location_brz.csv',
        generate_locations(),
        ['LOCATION_BRZ_ID', 'CITY', 'STATE', 'ZIP_CODE'],
        NUM_LOCATIONS
    )
    location_ids = list(range(1, NUM_LOCATIONS + 1))

    # Generate Restaurant
    print("\n2/9 Generating RESTAURANT_BRZ...")
    write_csv_chunked(
        BASE_DIR / 'restaurant' / 'restaurant_brz.csv',
        generate_restaurants(location_ids),
        ['RESTAURANT_BRZ_ID', 'FSSAI_REGISTRATION_NO', 'RESTAURANT_NAME', 'CUISINE_TYPE',
         'PRICING_FOR_TWO', 'RESTAURANT_PHONE', 'OPERATING_HOURS', 'LOCATION_ID',
         'ACTIVE_FLAG', 'OPEN_STATUS', 'LOCALITY', 'RESTAURANT_ADDRESS', 'LATITUDE', 'LONGITUDE'],
        NUM_RESTAURANTS
    )
    restaurant_ids = list(range(1, NUM_RESTAURANTS + 1))

    # Generate Customer
    print("\n3/9 Generating CUSTOMER_BRZ...")
    write_csv_chunked(
        BASE_DIR / 'customer' / 'customer_brz.csv',
        generate_customers(),
        ['CUSTOMER_BRZ_ID', 'CUSTOMER_NAME', 'MOBILE', 'EMAIL', 'LOGIN_BY_USING',
         'GENDER', 'DOB', 'ANNIVERSARY', 'PREFERENCES'],
        NUM_CUSTOMERS
    )
    customer_ids = list(range(1, NUM_CUSTOMERS + 1))

    # Generate Customer Address
    print("\n4/9 Generating CUSTOMER_ADDRESS_BRZ...")
    write_csv_chunked(
        BASE_DIR / 'customer_address' / 'customer_address_brz.csv',
        generate_customer_addresses(customer_ids),
        ['CUSTOMER_ADDRESS_BRZ_ID', 'CUSTOMER_ID', 'FLAT_NO', 'HOUSE_NO', 'FLOOR_NO',
         'BUILDING', 'LANDMARK', 'LOCALITY', 'CITY', 'STATE', 'ZIPCODE',
         'COORDINATES', 'PRIMARYFLAG', 'ADDRESSTYPE'],
        NUM_CUSTOMER_ADDRESSES
    )
    address_ids = list(range(1, NUM_CUSTOMER_ADDRESSES + 1))

    # Generate Menu
    print("\n5/9 Generating MENU_BRZ...")
    write_csv_chunked(
        BASE_DIR / 'menu' / 'menu_brz.csv',
        generate_menu_items(restaurant_ids),
        ['MENU_ID', 'RESTAURANT_ID', 'ITEM_NAME', 'DESCRIPTION', 'PRICE',
         'CATEGORY', 'AVAILABILITY', 'ITEM_TYPE'],
        NUM_MENU_ITEMS
    )
    menu_ids = list(range(1, NUM_MENU_ITEMS + 1))

    # Generate Delivery Agent
    print("\n6/9 Generating DELIVERY_AGENT_BRZ...")
    write_csv_chunked(
        BASE_DIR / 'delivery_agent' / 'delivery_agent_brz.csv',
        generate_delivery_agents(location_ids),
        ['DELIVERY_AGENT_ID', 'DELIVERY_AGENT_NAME', 'PHONE', 'VEHICLE_TYPE',
         'LOCATION_ID', 'IS_ACTIVE', 'GENDER', 'RATING'],
        NUM_DELIVERY_AGENTS
    )
    agent_ids = list(range(1, NUM_DELIVERY_AGENTS + 1))

    # Generate Order
    print("\n7/9 Generating ORDER_BRZ...")
    write_csv_chunked(
        BASE_DIR / 'order' / 'order_brz.csv',
        generate_orders(customer_ids, restaurant_ids),
        ['ORDER_ID', 'CUSTOMER_ID', 'RESTAURANT_ID', 'ORDER_DATE',
         'TOTAL_AMOUNT', 'ORDER_STATUS', 'PAYMENT_METHOD'],
        NUM_ORDERS
    )
    order_ids = [f'ORD{i:08d}' for i in range(1, min(NUM_ORDERS + 1, 100001))]  # Sample for memory

    # Generate Order Item
    print("\n8/9 Generating ORDER_ITEM_BRZ...")
    write_csv_chunked(
        BASE_DIR / 'order_item' / 'order_item_brz.csv',
        generate_order_items(order_ids, menu_ids),
        ['ORDER_ITEM_ID', 'ORDER_ID', 'MENU_ID', 'QUANTITY', 'PRICE', 'SUBTOTAL', 'ORDER_TIMESTAMP'],
        NUM_ORDER_ITEMS
    )

    # Generate Delivery
    print("\n9/9 Generating DELIVERY_BRZ...")
    write_csv_chunked(
        BASE_DIR / 'delivery' / 'delivery_brz.csv',
        generate_deliveries(order_ids, agent_ids, address_ids),
        ['DELIVERY_ID', 'ORDER_ID', 'DELIVERY_AGENT_ID', 'DELIVERY_STATUS', 'ESTIMATED_TIME',
         'CUSTOMER_ADDRESS_ID', 'DELIVERY_DATE', 'ORDER_ID_RAW', 'DELIVERY_AGENT_ID_RAW',
         'DELIVERY_STATUS_RAW', 'ESTIMATED_TIME_RAW', 'CUSTOMER_ADDRESS_ID_RAW',
         'DELIVERY_DATE_RAW', 'INGEST_RUN_ID', 'CREATED_AT', 'UPDATED_AT'],
        NUM_DELIVERIES
    )

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    print(f"\n✅ All CSV files generated successfully in {duration:.1f} seconds!")
    print(f"\n📈 Generated:")
    print(f"   • {NUM_LOCATIONS:,} locations")
    print(f"   • {NUM_RESTAURANTS:,} restaurants")
    print(f"   • {NUM_CUSTOMERS:,} customers")
    print(f"   • {NUM_CUSTOMER_ADDRESSES:,} customer addresses")
    print(f"   • {NUM_MENU_ITEMS:,} menu items")
    print(f"   • {NUM_DELIVERY_AGENTS:,} delivery agents")
    print(f"   • {NUM_ORDERS:,} orders")
    print(f"   • {NUM_ORDER_ITEMS:,} order items")
    print(f"   • {NUM_DELIVERIES:,} deliveries")
    print(f"\n💾 Files saved to: {BASE_DIR}")