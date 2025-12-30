# Kafka → Snowflake Streaming

This is the **full setup I used to push Kafka events into Snowflake** using the Snowflake Kafka Sink Connector.
Read this later, if something broke or rebuilding the stack — so follow the steps **in order**.

---

## Step 1: Install Docker Desktop

Nothing works without this.

* Install Docker Desktop
* Start it
* Make sure containers can run

---

## Step 2: Install Snowflake Kafka Sink Connector

I’m using Kafka Connect running inside Docker.

* The Snowflake Sink Connector is installed via a **Dockerfile**
* Once Kafka Connect starts, verify it’s alive:

```bash
http://localhost:8080
```

If this URL responds, Kafka Connect is up and ready.

---

## Step 3: Generate RSA Keys (This Part Is Important)

Snowflake **does not use passwords** here.
It uses **key-pair authentication**, so this step must be done carefully.

### 3.1 Generate the Keys

```bash
# Generate private key (Snowflake requires PKCS8 format)
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowflake_key.p8 -nocrypt

# Generate public key from the private key
openssl rsa -in snowflake_key.p8 -pubout -out snowflake_key.pub
```

You will get two files:

* `snowflake_key.p8` → private key (used by Kafka Connector)
* `snowflake_key.pub` → public key (stored in Snowflake)

### 3.2 Clean the Keys

Open both files and:

* Remove `BEGIN` / `END` lines
* Remove line breaks
* Keep **only the base64 string**

Snowflake and Kafka **will fail** if headers are left in.

---

## Step 4: Configure Snowflake for Streaming

Run this file in Snowflake:

```
Kafka_Streaming_Setup.sql
```

This script:

* Creates streaming tables
* Creates tasks and views
* Sets up Kafka ingestion logic

While running this:

* Paste the **public key** (from `snowflake_key.pub`)
* Set it as `RSA_PUBLIC_KEY` for the Kafka connector user

---

## Step 5: Configure Connector JSON Files

Now update the connector config files.

For **each** connector JSON:

* Paste **private key** from `snowflake_key.p8`
* Set:

  * Snowflake account URL
  * Database and schema
  * Target streaming table
  * Kafka topic name

Files involved:

* `orders-connector.json`
* `order-items-connector.json`
* `delivery-connector.json`

If keys or table names don’t match → connector will fail silently.

---

## Step 6: Create Kafka Topics

Enter the Kafka container:

```bash
docker exec -it kafka bash
```

Create topics:

```bash
# Orders
kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic orders-events \
  --config retention.ms=604800000

# Order items
kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic order-items-events \
  --config retention.ms=604800000

# Delivery
kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic delivery-events \
  --config retention.ms=604800000
```

Verify:

```bash
kafka-topics --list --bootstrap-server localhost:9092
```

Exit Kafka container:

```bash
exit
```

---

## Step 7: Create Kafka Sink Connectors

Register the connectors with Kafka Connect.

```bash
# Orders connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @orders-connector.json

# Order items connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @order-items-connector.json

# Delivery connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @delivery-connector.json
```

---

## Step 8: Verify Everything Is Running

List connectors:

```bash
curl http://localhost:8083/connectors | jq
```

Check individual status:

```bash
curl http://localhost:8083/connectors/swiggy-orders-sink/status | jq
curl http://localhost:8083/connectors/swiggy-order-items-sink/status | jq
curl http://localhost:8083/connectors/swiggy-delivery-sink/status | jq
```

What I want to see:

* `RUNNING` state
* No failed tasks

If not:

* Check keys
* Check table names
* Check Snowflake user permissions

---

## Step 9: Run the Kafka Producer

Install Kafka Python client:

```bash
pip install kafka-python
```

Run producer:

```bash
python producer.py
```

Follow the prompts to generate test data.

---

## Final Sanity Check (Mental Model)

* Producer → Kafka topics
* Kafka Connect → Snowflake streaming tables
* Snowflake tasks → views / downstream layers

If data isn’t showing up:

1. Check connector status
2. Check RSA keys
3. Check topic ↔ table mapping

---

### Note to Future Me

This is **not a toy setup**.
This is production-style Kafka → Snowflake streaming with:

* Key-pair auth
* Multiple topics
* Separate connectors
* Streaming tables + tasks

If this breaks, it’s usually **keys or config**, not Kafka.
