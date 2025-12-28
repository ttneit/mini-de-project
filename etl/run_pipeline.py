import argparse
from pathlib import Path
import json
import pandas as pd
from typing import Tuple

def process_orders(order: pd.DataFrame) -> pd.DataFrame:
    # Parse datetime field : order_date, ingested_at
    order['ingested_at'] = pd.to_datetime(order['ingested_at'])
    order['order_date'] = pd.to_datetime(order['order_date'])

    # Cast types
    order['order_id'] = order['order_id'].astype('int', errors="ignore")
    order['customer_id'] = order['customer_id'].astype('int', errors="ignore")
    order['status'] = order['status'].astype('str', errors="ignore")

    # Standardize status field
    order['status'] = order['status'].str.strip().str.lower()

    #Deduplicate by order_id: keep the row with latest ingested_at
    order.sort_values(by=['order_id', 'ingested_at'], ascending=[True, True], ignore_index=True, inplace=True)

    order.drop_duplicates(subset=['order_id'], keep='last', ignore_index=True, inplace=True)

    return order


def process_order_items(order_items: pd.DataFrame) -> pd.DataFrame:
    # Parse datetime field : order_date, ingested_at
    order_items['ingested_at'] = pd.to_datetime(order_items['ingested_at'])

    # Cast types
    order_items['order_id'] = order_items['order_id'].astype('int', errors="ignore")
    order_items['product_id'] = order_items['product_id'].astype('int', errors="ignore")
    order_items['quantity'] = order_items['quantity'].astype('int', errors='ignore')
    order_items['unit_price'] = order_items['unit_price'].astype('int', errors='ignore')

    return order_items

def validate_order(order: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # Find order_id, customer_id, order_date, status must be NOT NULL
    mask = (
        ~ order['order_id'].isna() &
        ~ order['customer_id'].isna() &
        ~ order['order_date'].isna() &
        ~ order['status'].isna()
    )

    valid_orders = order.loc[mask].copy()
    rejected_orders = order.loc[~mask].copy()

    return valid_orders, rejected_orders

def validate_order_items(items: pd.DataFrame, orders: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # Find quantity must be NOT NULL, unit_price must be > 0
    field_valid_mask = (
        ~ items['quantity'].isna() &
        ~ items['unit_price'].isna() &
        (items['unit_price'] > 0)
    )

    valid_field_items = items[field_valid_mask]
    invalid_field_items = items[~field_valid_mask]

    # Find orphan items
    non_orphan_mask = valid_field_items['order_id'].isin(orders['order_id'])
    non_orphan_valid_items = valid_field_items[non_orphan_mask]
    orphan_items = valid_field_items[~non_orphan_mask]

    # Final outputs
    valid_items = non_orphan_valid_items
    rejected_items = pd.concat([invalid_field_items, orphan_items], ignore_index=True)

    return valid_items, rejected_items


def compute_revenue(orders: pd.DataFrame, items: pd.DataFrame) -> pd.DataFrame:
    completed_orders = orders[orders['status'] == 'completed']

    full_df = pd.merge(left=completed_orders, right=items, how='inner',on=['order_id', 'ingested_at'])

    full_df['revenue'] = full_df['quantity'] * full_df['unit_price']

    full_df['ingested_at'] = full_df['ingested_at'].dt.date
    
    revenue = full_df.groupby('ingested_at').agg({'order_id': 'count', 'revenue' : 'sum'})

    return revenue



def main(run_date: str, input_dir: str, output_dir: str) -> None:
    input_path = Path(input_dir)
    out_path = Path(f"{output_dir}/{run_date}")
    out_path.mkdir(parents=True, exist_ok=True)
    
    orders_file = input_path / f"orders_{run_date}.csv"
    items_file = input_path / f"order_items_{run_date}.csv"

    if not orders_file.exists():
        raise FileNotFoundError(f"Missing input file: {orders_file}")
    if not items_file.exists():
        raise FileNotFoundError(f"Missing input file: {items_file}")

    orders = pd.read_csv(orders_file)
    items = pd.read_csv(items_file)

    cleaned_orders = process_orders(orders)
    cleaned_order_items = process_order_items(items)

    valid_orders, rejected_orders = validate_order(cleaned_orders)
    valid_items, rejected_items = validate_order_items(cleaned_order_items, cleaned_orders)

    revenue = compute_revenue(valid_orders, valid_items)


    if not revenue.empty:
        revenue.reset_index(inplace=True)
        revenue.rename(columns={"ingested_at": "order_date", "revenue": "total_revenue", "order_id": "orders_count"}, inplace=True)
        revenue.to_csv(f"{output_dir}/{run_date}/daily_revenue.csv", index=False)
    else:
        Path(f"{output_dir}/{run_date}/daily_revenue.csv").write_text("order_date,total_revenue,orders_count\n")

    if not rejected_orders.empty:
        rejected_orders.to_csv(f"{output_dir}/{run_date}/rejected_orders.csv", index=False)

    if not rejected_items.empty:
        rejected_items.to_csv(f"{output_dir}/{run_date}/rejected_items.csv", index=False)

    report = {
        "run_date": run_date,
        "input": {
            "orders": len(orders),
            "order_items": len(items)
        },
        "note" : "Clean + Standardize raw data, Validate the cleansed data, Compute the daily revenue, Write to output file",
        "valid": {
            "orders": len(valid_orders),
            "order_items": valid_items['product_id'].nunique()
        },
        "rejected": {
            "orders": len(orders) - len(valid_orders),
            "order_items": (items['product_id'].nunique()) - (valid_items['product_id'].nunique())
        },
        "completed_orders": int(len(valid_orders[valid_orders['status'] == "completed"])),
        "total_revenue": float(revenue["total_revenue"].sum()) if not revenue.empty else 0.0,
        "output_files": {
            "daily_revenue": f"{output_dir}/{run_date}/daily_revenue.csv",
            "rejected_orders": f"{output_dir}/{run_date}/rejected_orders.csv",
            "rejected_items": f"{output_dir}/{run_date}/rejected_items.csv"
        },
        "status": "SUCCESS"
    }

    with open(out_path / "quality_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True, help="YYYY-MM-DD (e.g. 2024-01-01)")
    parser.add_argument("--input-dir", default="data")
    parser.add_argument("--output-dir", default="output")
    args = parser.parse_args()
    main(args.run_date, args.input_dir, args.output_dir)
