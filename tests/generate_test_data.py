import pandas as pd
import random
from datetime import datetime, timedelta
import argparse
import os

def generate_orders(num_orders=50, start_date=datetime(2024, 1, 1)):
    statuses = ['completed', 'pending', 'cancelled']
    orders = []
    for i in range(1, num_orders + 1):
        order_date = start_date + timedelta(days=random.randint(0, 10))
        ingested_at = order_date + timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
        orders.append({
            'order_id': i,
            'customer_id': random.choice([random.randint(100, 999), None]),  
            'order_date': order_date.strftime('%m/%d/%Y'),
            'status': random.choice(statuses),
            'ingested_at': ingested_at.isoformat() + 'Z'
        })
    return pd.DataFrame(orders)

def generate_order_items(num_items=50, orders_df=None, start_date=datetime(2024, 1, 1)):
    items = []
    for _ in range(1, num_items + 1):
        if orders_df is not None and random.random() > 0.5: 
            order = orders_df.sample(1).iloc[0]
            order_id = order['order_id']
            ingested_at = order['ingested_at']
        else: 
            order_id = random.choice([
                random.randint(len(orders_df) + 1, len(orders_df) + 100) if orders_df is not None else random.randint(101, 200),
                None
            ]) 
            ingested_at = (start_date + timedelta(days=random.randint(0, 10), hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))).isoformat() + 'Z'

        items.append({
            'order_id': order_id,
            'product_id': random.randint(1, 100),
            'quantity': random.choice([None, random.randint(1, 10)]),
            'unit_price': random.choice([None, round(random.uniform(1.0, 100.0), 2)]),
            'ingested_at': ingested_at
        })
    return pd.DataFrame(items)

def main(run_date: str, input_dir: str):
    start_date = datetime.strptime(run_date, "%Y-%m-%d")

    os.makedirs(input_dir, exist_ok=True)

    orders = generate_orders(start_date=start_date)
    order_items = generate_order_items(orders_df=orders, start_date=start_date)

    orders.to_csv(f"{input_dir}/orders_{run_date}.csv", index=False)
    order_items.to_csv(f"{input_dir}/order_items_{run_date}.csv", index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True, help="YYYY-MM-DD (e.g. 2024-01-01)")
    parser.add_argument("--input-dir", default="data")

    args = parser.parse_args()
    main(args.run_date, args.input_dir)