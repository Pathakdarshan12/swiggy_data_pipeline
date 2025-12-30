#!/usr/bin/env python3
"""
Optimized Kafka Producer for DATAVELOCITY Streaming Pipeline
Loads real data from generated CSVs and streams realistic events
"""

import json
import random
import time
import csv
from datetime import datetime, timedelta
from pathlib import Path
from kafka import KafkaProducer
from kafka.errors import KafkaError
from tqdm import tqdm

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
TOPICS = {
    'orders': 'orders-events',
    'order_items': 'order-items-events',
    'delivery': 'delivery-events'
}

# Data directory
DATA_DIR = Path(__file__).parent.parent / 'data'

# Global data pools (loaded from CSV)
CUSTOMERS = []
RESTAURANTS = []
MENU_ITEMS = []
DELIVERY_AGENTS = []
ADDRESSES = []

# Event counters
order_counter = 100000
order_item_counter = 500000
delivery_counter = 200000

# Event types
ORDER_EVENTS = ['ORDER_CREATED', 'ORDER_UPDATED', 'ORDER_CANCELLED']
ITEM_EVENTS = ['ITEM_ADDED', 'ITEM_UPDATED', 'ITEM_REMOVED']
DELIVERY_EVENTS = ['DELIVERY_ASSIGNED', 'STATUS_UPDATED', 'LOCATION_UPDATED', 'DELIVERY_COMPLETED']

ORDER_STATUSES = ['PLACED', 'CONFIRMED', 'PREPARING', 'READY', 'OUT_FOR_DELIVERY', 'DELIVERED', 'CANCELLED']
DELIVERY_STATUSES = ['ASSIGNED', 'PICKED_UP', 'IN_TRANSIT', 'NEARBY', 'DELIVERED', 'FAILED']
PAYMENT_METHODS = ['Credit Card', 'Debit Card', 'UPI', 'Cash', 'Wallet']

# Initialize Kafka producer
producer = None


def init_kafka_producer():
    """Initialize Kafka producer"""
    global producer
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8') if k else None,
        acks='all',
        retries=3,
        compression_type='gzip',
        batch_size=16384,
        linger_ms=10
    )


def load_csv_data():
    """Load data from generated CSV files"""
    global CUSTOMERS, RESTAURANTS, MENU_ITEMS, DELIVERY_AGENTS, ADDRESSES

    print("\n📂 Loading data from CSV files...")

    # Load customers (sample to avoid memory issues)
    customer_file = DATA_DIR / 'customer' / 'customer_brz.csv'
    if customer_file.exists():
        with open(customer_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            CUSTOMERS = [int(row['CUSTOMER_BRZ_ID']) for row in reader]
            print(f"   ✓ Loaded {len(CUSTOMERS):,} customers")

    # Load restaurants
    restaurant_file = DATA_DIR / 'restaurant' / 'restaurant_brz.csv'
    if restaurant_file.exists():
        with open(restaurant_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            RESTAURANTS = [int(row['RESTAURANT_BRZ_ID']) for row in reader]
            print(f"   ✓ Loaded {len(RESTAURANTS):,} restaurants")

    # Load menu items
    menu_file = DATA_DIR / 'menu' / 'menu_brz.csv'
    if menu_file.exists():
        with open(menu_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            MENU_ITEMS = [
                {
                    'menu_id': int(row['MENU_ID']),
                    'restaurant_id': int(row['RESTAURANT_ID']),
                    'item_name': row['ITEM_NAME'],
                    'price': float(row['PRICE']),
                    'category': row['CATEGORY'],
                    'item_type': row['ITEM_TYPE']
                }
                for row in reader
            ]
            print(f"   ✓ Loaded {len(MENU_ITEMS):,} menu items")

    # Load delivery agents
    agent_file = DATA_DIR / 'delivery_agent' / 'delivery_agent_brz.csv'
    if agent_file.exists():
        with open(agent_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            DELIVERY_AGENTS = [int(row['DELIVERY_AGENT_ID']) for row in reader]
            print(f"   ✓ Loaded {len(DELIVERY_AGENTS):,} delivery agents")

    # Load customer addresses
    address_file = DATA_DIR / 'customer_address' / 'customer_address_brz.csv'
    if address_file.exists():
        with open(address_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            ADDRESSES = [int(row['CUSTOMER_ADDRESS_BRZ_ID']) for row in reader]
            print(f"   ✓ Loaded {len(ADDRESSES):,} addresses")

    if not all([CUSTOMERS, RESTAURANTS, MENU_ITEMS, DELIVERY_AGENTS, ADDRESSES]):
        print("\n⚠️  Warning: Some CSV files are missing. Using fallback data.")
        generate_fallback_data()


def generate_fallback_data():
    """Generate fallback data if CSV files don't exist"""
    global CUSTOMERS, RESTAURANTS, MENU_ITEMS, DELIVERY_AGENTS, ADDRESSES

    if not CUSTOMERS:
        CUSTOMERS = list(range(1, 1001))
    if not RESTAURANTS:
        RESTAURANTS = list(range(1, 501))
    if not MENU_ITEMS:
        MENU_ITEMS = [
            {
                'menu_id': i,
                'restaurant_id': random.randint(1, 500),
                'item_name': f'Item {i}',
                'price': round(random.uniform(50, 500), 2),
                'category': random.choice(['Main Course', 'Appetizer', 'Dessert', 'Beverage']),
                'item_type': random.choice(['Veg', 'Non-Veg', 'Vegan'])
            }
            for i in range(1, 2001)
        ]
    if not DELIVERY_AGENTS:
        DELIVERY_AGENTS = list(range(1, 301))
    if not ADDRESSES:
        ADDRESSES = list(range(1, 1501))


def generate_order_event(event_type='ORDER_CREATED', existing_order=None):
    """Generate order event"""
    global order_counter

    if existing_order:
        order = existing_order.copy()
        order['event_type'] = event_type
        order['event_timestamp'] = datetime.now().isoformat()
        if event_type == 'ORDER_UPDATED':
            # Progress order status
            current_idx = ORDER_STATUSES.index(order.get('order_status', 'PLACED'))
            if current_idx < len(ORDER_STATUSES) - 2:  # Don't auto-cancel
                order['order_status'] = ORDER_STATUSES[current_idx + 1]
    else:
        order_counter += 1
        order = {
            'event_type': event_type,
            'event_timestamp': datetime.now().isoformat(),
            'order_id': f'ORD{order_counter:08d}',
            'customer_id': random.choice(CUSTOMERS) if CUSTOMERS else random.randint(1, 1000),
            'restaurant_id': random.choice(RESTAURANTS) if RESTAURANTS else random.randint(1, 500),
            'order_date': datetime.now().isoformat(),
            'total_amount': round(random.uniform(200, 2000), 2),
            'order_status': 'PLACED',
            'payment_method': random.choice(PAYMENT_METHODS),
            'metadata': {
                'platform': random.choice(['Android', 'iOS', 'Web']),
                'promo_code': random.choice([None, 'FIRST50', 'SAVE20', 'WEEKEND30']),
                'special_instructions': random.choice([None, 'Extra spicy', 'No onions', 'Less oil'])
            }
        }

    return order


def generate_order_items(order_id, restaurant_id, num_items=None):
    """Generate order items for an order"""
    global order_item_counter

    if num_items is None:
        num_items = random.randint(1, 5)

    # Filter menu items by restaurant
    restaurant_menu = [m for m in MENU_ITEMS if m['restaurant_id'] == restaurant_id]
    if not restaurant_menu:
        restaurant_menu = random.sample(MENU_ITEMS, min(10, len(MENU_ITEMS)))

    items = []
    selected_items = random.sample(restaurant_menu, min(num_items, len(restaurant_menu)))

    for menu_item in selected_items:
        order_item_counter += 1
        quantity = random.randint(1, 3)
        price = menu_item['price']

        item = {
            'event_type': 'ITEM_ADDED',
            'event_timestamp': datetime.now().isoformat(),
            'order_item_id': f'OI{order_item_counter:09d}',
            'order_id': order_id,
            'menu_id': menu_item['menu_id'],
            'item_name': menu_item['item_name'],
            'quantity': quantity,
            'price': price,
            'subtotal': round(price * quantity, 2),
            'category': menu_item['category'],
            'item_type': menu_item['item_type'],
            'customizations': random.choice([
                None,
                ['Extra cheese', 'Well done'],
                ['No mayo'],
                ['Extra spicy', 'Less salt']
            ])
        }
        items.append(item)

    return items


def generate_delivery_event(order_id, event_type='DELIVERY_ASSIGNED', existing_delivery=None):
    """Generate delivery event"""
    global delivery_counter

    if existing_delivery:
        delivery = existing_delivery.copy()
        delivery['event_type'] = event_type
        delivery['event_timestamp'] = datetime.now().isoformat()

        if event_type == 'STATUS_UPDATED':
            # Progress delivery status
            current_idx = DELIVERY_STATUSES.index(delivery.get('delivery_status', 'ASSIGNED'))
            if current_idx < len(DELIVERY_STATUSES) - 2:  # Don't auto-fail
                delivery['delivery_status'] = DELIVERY_STATUSES[current_idx + 1]

        if event_type == 'LOCATION_UPDATED':
            # Update location slightly
            delivery['location']['latitude'] += random.uniform(-0.01, 0.01)
            delivery['location']['longitude'] += random.uniform(-0.01, 0.01)

        if event_type == 'DELIVERY_COMPLETED':
            delivery['delivery_status'] = 'DELIVERED'
            delivery['delivery_date'] = datetime.now().isoformat()
            delivery['actual_time'] = random.randint(20, 90)
    else:
        delivery_counter += 1
        delivery = {
            'event_type': event_type,
            'event_timestamp': datetime.now().isoformat(),
            'delivery_id': f'DEL{delivery_counter:08d}',
            'order_id': order_id,
            'delivery_agent_id': random.choice(DELIVERY_AGENTS) if DELIVERY_AGENTS else random.randint(1, 300),
            'delivery_status': 'ASSIGNED',
            'estimated_time': random.randint(20, 60),
            'customer_address_id': random.choice(ADDRESSES) if ADDRESSES else random.randint(1, 1500),
            'assigned_at': datetime.now().isoformat(),
            'location': {
                'latitude': round(random.uniform(18.4, 18.6), 6),
                'longitude': round(random.uniform(73.8, 74.0), 6)
            },
            'metadata': {
                'vehicle_type': random.choice(['Bike', 'Scooter', 'Bicycle']),
                'distance_km': round(random.uniform(1.0, 15.0), 2)
            }
        }

    return delivery


def send_event(topic, key, value, verbose=True):
    """Send event to Kafka"""
    try:
        future = producer.send(topic, key=key, value=value)
        result = future.get(timeout=10)
        if verbose:
            print(f"✓ {topic}: {value['event_type']} | Key: {key}")
        return True
    except KafkaError as e:
        print(f"✗ Failed to send to {topic}: {e}")
        return False


def simulate_order_lifecycle(verbose=True):
    """Simulate complete order lifecycle"""
    if verbose:
        print("\n" + "=" * 70)
        print("SIMULATING ORDER LIFECYCLE")
        print("=" * 70)

    # 1. Create order
    order = generate_order_event('ORDER_CREATED')
    send_event(TOPICS['orders'], order['order_id'], order, verbose)
    time.sleep(0.2)

    # 2. Add order items
    items = generate_order_items(
        order['order_id'],
        order['restaurant_id'],
        num_items=random.randint(2, 4)
    )
    total = 0
    for item in items:
        send_event(TOPICS['order_items'], item['order_item_id'], item, verbose)
        total += item['subtotal']
        time.sleep(0.1)

    # Update total amount
    order['total_amount'] = round(total, 2)

    # 3. Confirm order
    time.sleep(0.5)
    order = generate_order_event('ORDER_UPDATED', order)
    order['order_status'] = 'CONFIRMED'
    send_event(TOPICS['orders'], order['order_id'], order, verbose)

    # 4. Assign delivery
    time.sleep(0.5)
    delivery = generate_delivery_event(order['order_id'], 'DELIVERY_ASSIGNED')
    send_event(TOPICS['delivery'], delivery['delivery_id'], delivery, verbose)

    # 5. Restaurant preparing
    time.sleep(1)
    order = generate_order_event('ORDER_UPDATED', order)
    order['order_status'] = 'PREPARING'
    send_event(TOPICS['orders'], order['order_id'], order, verbose)

    # 6. Order ready
    time.sleep(1.5)
    order = generate_order_event('ORDER_UPDATED', order)
    order['order_status'] = 'READY'
    send_event(TOPICS['orders'], order['order_id'], order, verbose)

    # 7. Agent picked up
    time.sleep(0.5)
    delivery = generate_delivery_event(order['order_id'], 'STATUS_UPDATED', delivery)
    delivery['delivery_status'] = 'PICKED_UP'
    send_event(TOPICS['delivery'], delivery['delivery_id'], delivery, verbose)

    # 8. In transit with location updates
    order['order_status'] = 'OUT_FOR_DELIVERY'
    send_event(TOPICS['orders'], order['order_id'], order, verbose)

    for _ in range(2):
        time.sleep(0.8)
        delivery = generate_delivery_event(order['order_id'], 'LOCATION_UPDATED', delivery)
        send_event(TOPICS['delivery'], delivery['delivery_id'], delivery, verbose)

    # 9. Nearby
    time.sleep(0.8)
    delivery = generate_delivery_event(order['order_id'], 'STATUS_UPDATED', delivery)
    delivery['delivery_status'] = 'NEARBY'
    send_event(TOPICS['delivery'], delivery['delivery_id'], delivery, verbose)

    # 10. Delivered
    time.sleep(1)
    delivery = generate_delivery_event(order['order_id'], 'DELIVERY_COMPLETED', delivery)
    send_event(TOPICS['delivery'], delivery['delivery_id'], delivery, verbose)

    order = generate_order_event('ORDER_UPDATED', order)
    order['order_status'] = 'DELIVERED'
    send_event(TOPICS['orders'], order['order_id'], order, verbose)

    if verbose:
        print(f"\n✓ Order {order['order_id']} lifecycle completed\n")

    return order


def continuous_stream(num_orders=10, delay=1, show_progress=True):
    """Generate continuous stream of orders"""
    print("\n" + "=" * 70)
    print(f"STARTING CONTINUOUS STREAM - {num_orders:,} ORDERS")
    print("=" * 70)

    if show_progress:
        iterator = tqdm(range(num_orders), desc="Streaming Orders", unit="order")
    else:
        iterator = range(num_orders)

    for i in iterator:
        simulate_order_lifecycle(verbose=not show_progress)

        if i < num_orders - 1 and delay > 0:
            time.sleep(delay)

    print("\n" + "=" * 70)
    print(f"✓ STREAM COMPLETED - {num_orders:,} orders generated")
    print("=" * 70)


def high_velocity_stream(duration_seconds=60, orders_per_second=10):
    """Generate high velocity stream for stress testing"""
    print("\n" + "=" * 70)
    print(f"HIGH VELOCITY STREAM - {orders_per_second} orders/sec for {duration_seconds}s")
    print("=" * 70)

    start_time = time.time()
    orders_generated = 0
    delay = 1.0 / orders_per_second

    with tqdm(total=duration_seconds, desc="Streaming", unit="sec") as pbar:
        while (time.time() - start_time) < duration_seconds:
            simulate_order_lifecycle(verbose=False)
            orders_generated += 1
            time.sleep(delay)
            pbar.update(delay)

    elapsed = time.time() - start_time
    actual_rate = orders_generated / elapsed

    print(f"\n✓ Generated {orders_generated:,} orders in {elapsed:.1f}s")
    print(f"  Actual rate: {actual_rate:.1f} orders/second")


def main():
    """Main function"""
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║   DATAVELOCITY Optimized Kafka Streaming Producer            ║
    ║   Real-time Order, Order Items & Delivery Events             ║
    ║   Powered by Generated CSV Data                              ║
    ╚══════════════════════════════════════════════════════════════╝
    """)

    try:
        # Load CSV data
        load_csv_data()

        # Initialize Kafka
        print("\n📡 Connecting to Kafka broker...")
        print(f"   Broker: {KAFKA_BROKER}")
        print(f"   Topics: {', '.join(TOPICS.values())}")

        init_kafka_producer()

        # Test connection
        producer.send(TOPICS['orders'], key=None, value={'test': 'connection'})
        producer.flush()
        print("✓ Connected successfully!\n")

        while True:
            print("\n" + "=" * 70)
            print("SELECT STREAMING MODE:")
            print("=" * 70)
            print("1. Single order lifecycle (detailed)")
            print("2. Continuous stream - 10 orders")
            print("3. Continuous stream - 100 orders")
            print("4. Continuous stream - 1,000 orders")
            print("5. High velocity - 10 orders/sec for 60 seconds")
            print("6. High velocity - 50 orders/sec for 30 seconds")
            print("7. High velocity - 100 orders/sec for 10 seconds")
            print("8. Custom continuous stream")
            print("9. Exit")
            print("=" * 70)

            choice = input("\nEnter choice (1-9): ").strip()

            if choice == '1':
                simulate_order_lifecycle(verbose=True)
            elif choice == '2':
                continuous_stream(num_orders=10, delay=3)
            elif choice == '3':
                continuous_stream(num_orders=100, delay=1)
            elif choice == '4':
                continuous_stream(num_orders=1000, delay=0.1)
            elif choice == '5':
                high_velocity_stream(duration_seconds=60, orders_per_second=10)
            elif choice == '6':
                high_velocity_stream(duration_seconds=30, orders_per_second=50)
            elif choice == '7':
                high_velocity_stream(duration_seconds=10, orders_per_second=100)
            elif choice == '8':
                try:
                    num = int(input("Number of orders: "))
                    delay = float(input("Delay between orders (seconds): "))
                    continuous_stream(num_orders=num, delay=delay)
                except ValueError:
                    print("❌ Invalid input")
            elif choice == '9':
                print("\n👋 Goodbye!")
                break
            else:
                print("❌ Invalid choice")

    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if producer:
            print("\n🔒 Closing producer...")
            producer.close()
            print("✓ Producer closed")


if __name__ == "__main__":
    main()