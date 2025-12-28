# Mini Data Engineering Project — Orders Revenue Pipeline

## 1. Goal
Build a mini batch pipeline to compute **daily revenue** from raw CSV files:
- Standardize & clean input
- Validate data quality (reject invalid records)
- Compute daily revenue for BI
- Produce a **quality_report.json** for monitoring


## 2. Input data
You will find sample inputs in `/data/`:
- `orders_{date}.csv` (e.g., `orders_2024-01-01.csv`)
- `order_items_{date}.csv` (e.g., `order_items_2024-01-01.csv`)

### Schema of Raw Files

#### **Orders File**
| Column Name   | Data Type | Description                     |
|---------------|-----------|---------------------------------|
| order_id      | int       | Unique identifier for the order | 
| customer_id   | int       | Unique identifier for the customer |
| order_date    | datetime  | Date when the order was placed  |
| status        | string    | Status of the order (e.g., completed, pending) |
| ingested_at   | datetime  | Timestamp when the record was ingested |

Assumptions: 
- Field `status`: the values in this field are correctly spelled
- Field `order_date`: this values are in the right format (DD/MM/YYYY)
- Field `ingested_at`: this values follow the ISO 8601 standard (whch are not null)


#### **Order Items File**
| Column Name   | Data Type | Description                     |
|---------------|-----------|---------------------------------|
| order_id      | int       | Identifier linking to the orders table |
| product_id    | int       | Unique identifier for the product |
| quantity      | int       | Quantity of the product ordered |
| unit_price    | float     | Price per unit of the product   |
| ingested_at   | datetime  | Timestamp when the record was ingested |

Assumptions: 
- Field `ingested_at`: this values follow the ISO 8601 standard (whch are not null) 
- Field `ingested_at` of item in `order_items` table will equal to field `ingested_at` of `orders` if having the same `order_id` 


## 3. Requirements

### 3.1 Staging (clean + standardize)
#### Implementation:
- **Orders**:
  - Parse `order_date` and `ingested_at` as datetime fields using pandas.
  - Cast `order_id` (int), `customer_id` (int), and `status` (string) to appropriate types.
  - Standardize `status` by trimming whitespace and converting to lowercase.
  - Deduplicate by `order_id`, keeping the row with the latest `ingested_at`.
- **Order Items**:
  - Parse `ingested_at` as a datetime field.
  - Cast `order_id` (int), `product_id` (int), `quantity` (int), and `unit_price`(int) to appropriate types.

### 3.2 Data Quality Rules (reject invalid rows)
#### Implementation:
- **Orders**:
  - Filter all valid orders which meet this requirement: `order_id`, `customer_id`, `order_date`, and `status` are not null.
- **Order Items**:
  - Filter all valid items which meet this requirement: `quantity` is not null and `unit_price` is greater than 0.
  - Filter orphan items where `order_id` does not exist in the orders table.

### 3.3 Business Logic
#### Implementation:
- Filter orders with `status = 'completed'`.
- Join completed tables with valid order items
- Compute revenue as `quantity * unit_price`.
- Aggregate daily revenue by `order_date` with the following fields:
  - `order_date`
  - `total_revenue`
  - `orders_count`

### 3.4 Outputs (write to `/output/`)
#### Implementation:
- `daily_revenue.csv`: Contains aggregated daily revenue.
- `rejected_orders.csv`: Contains invalid orders (if any).
- `rejected_items.csv`: Contains invalid or orphaned order items (if any).
- `quality_report.json`: Summarizes the pipeline run, including input counts, valid/rejected counts, and output file paths.

### 3.5 Idempotency (basic)
#### Implementation:
- Ensure the pipeline can be safely rerun without duplicating outputs by overwriting files for the specified `run_date` directory.

## 4. Suggested Repo Structure

Updated structure based on the implementation:

```
mini-de-project/
├── README.md
├── requirements.txt
├── .venv/
├── data/
│   ├── orders_2024-01-01.csv
│   └── order_items_2024-01-01.csv
├── etl/
│   └── run_pipeline.py
├── output/
│   ├── 2024-01-01/
│   │   ├── daily_revenue.csv
│   │   ├── rejected_orders.csv
│   │   ├── rejected_items.csv
│   │   └── quality_report.json
├── sql/
│   ├── models/
|       └── daily_revenue.sql
│   └── checks/
│       └── dq_checks.sql
├── tests/
│   └── generate_test_data.py


## 5. How to run

### 5.1 Prerequisites
- Python 3.10+ installed
- Recommended: use a virtual environment

### 5.2 Windows (recommended)
From the project root folder:

```Command Prompt
python -m venv .venv
.\.venv\Scripts\activate.bat 
pip install -r requirements.txt

python etl\run_pipeline.py --run-date 2024-01-01 --input-dir data --output-dir output

```

## 6. Test Data Generation

### Description:
The `generate_test_data.py` script is designed to create synthetic test data for the pipeline. It generates two datasets:

1. **Orders Dataset**:
   - Contains information about customer orders, including `order_id`, `customer_id`, `order_date`, `status`, and `ingested_at`.
   - Includes both valid and invalid data, such as missing `customer_id` or invalid `status`.

2. **Order Items Dataset**:
   - Contains details about items in each order, including `order_id`, `product_id`, `quantity`, `unit_price`, and `ingested_at`.
   - Ensures that if an item belongs to an existing order, its `ingested_at` matches the `ingested_at` of the corresponding order.
   - Generates random invalid data, such as mismatched `order_id` or missing `quantity`.

### Usage:
To generate test data, run the following command:
```
python tests\generate_test_data.py --run-date 2024-01-02 --input-dir data
```
This will create `orders_2024-01-02.csv` and `order_items_2024-01-02.csv` in the `data` directory.