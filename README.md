# Analytics Engineering with dbt and Snowflake

A complete analytics engineering pipeline built with dbt and Snowflake, 
demonstrating professional data transformation patterns used in 
modern data teams.

## Project Overview

This project transforms raw Snowflake sample data (TPCH) into clean, 
tested, and documented data models following the staging → marts 
architecture pattern.

## Tech Stack

- **dbt** 1.11.7 — data transformation tool
- **Snowflake** — cloud data warehouse (EU Ireland)
- **Git/GitHub** — version control

## Data Source

Snowflake TPCH sample data — a realistic supply chain dataset:
- 1.5 million orders
- 150,000 customers
- 10,000 suppliers

## Project Structure
```
models/
├── staging/          # Clean and rename raw source tables
│   ├── stg_orders.sql
│   ├── stg_customers.sql
│   ├── stg_suppliers.sql
│   └── sources.yml
├── marts/            # Business-ready fact tables
│   └── fct_orders.sql
└── schema.yml        # Data quality tests
```

## Models Built

| Model | Layer | Description |
|---|---|---|
| stg_orders | Staging | Cleaned orders data with renamed columns |
| stg_customers | Staging | Cleaned customer data with renamed columns |
| stg_suppliers | Staging | Cleaned supplier data with renamed columns |
| fct_orders | Marts | Orders joined with customers for business use |

## Data Quality Tests

11 automated tests implemented across all models:
- **unique** — no duplicate keys
- **not_null** — no missing values on key columns
- **accepted_values** — status values validated
- **relationships** — referential integrity between models

## How To Run
```bash
# Test connection
dbt debug

# Build all models
dbt run

# Run data quality tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

## Author

Seyi Falope
[LinkedIn](https://www.linkedin.com/in/falope/)
[Portfolio](https://www.datascienceportfol.io/samuelfalope)
