import os
import json
import pyarrow.parquet as pq
from collections import defaultdict


SILVER_FILE = "D:/DataEngineering/data/silver/orders/orders.parquet"

GOLD_DIR = "D:/DataEngineering/data/gold/city_revenue"
GOLD_FILE = os.path.join(
    GOLD_DIR,
    "city_revenue.json"
)


print("===================================")
print("SILVER TO GOLD")
print("===================================")


# --------------------------------------------------
# 1. Read Silver
# --------------------------------------------------

table = pq.read_table(SILVER_FILE)

records = table.to_pylist()

print(f"Silver records: {len(records)}")


# --------------------------------------------------
# 2. Calculate city-level metrics
# --------------------------------------------------

city_data = defaultdict(
    lambda: {
        "order_count": 0,
        "total_revenue": 0
    }
)


for order in records:

    city = order["city"]
    amount = order["amount"]

    city_data[city]["order_count"] += 1
    city_data[city]["total_revenue"] += amount


# --------------------------------------------------
# 3. Build Gold records
# --------------------------------------------------

gold_records = []

for city, data in city_data.items():

    order_count = data["order_count"]
    total_revenue = data["total_revenue"]

    avg_order_value = (
        total_revenue / order_count
        if order_count > 0
        else 0
    )

    gold_records.append(
        {
            "city": city,
            "order_count": order_count,
            "total_revenue": total_revenue,
            "average_order_value": round(
                avg_order_value,
                2
            )
        }
    )


# Sort by revenue, highest first

gold_records.sort(
    key=lambda x: x["total_revenue"],
    reverse=True
)


# --------------------------------------------------
# 4. Write Gold
# --------------------------------------------------

os.makedirs(
    GOLD_DIR,
    exist_ok=True
)


with open(
    GOLD_FILE,
    "w",
    encoding="utf-8"
) as f:

    json.dump(
        gold_records,
        f,
        indent=4
    )


# --------------------------------------------------
# 5. Print results
# --------------------------------------------------

print("\nCity Revenue Summary:")
print("-----------------------------------")

for row in gold_records:

    print(
        f"{row['city']:12} | "
        f"Orders: {row['order_count']:3} | "
        f"Revenue: ₹{row['total_revenue']:10} | "
        f"Avg Order: ₹{row['average_order_value']:10.2f}"
    )


print("\n===================================")
print("Gold transformation completed!")
print("===================================")

print(f"Cities: {len(gold_records)}")
print(f"Gold file: {GOLD_FILE}")