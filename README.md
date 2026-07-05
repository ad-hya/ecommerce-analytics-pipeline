# ecommerce-analytics-pipeline

An end-to-end data engineering and analytics project: a Snowflake data warehouse built on a star schema, a Power BI dashboard for business reporting, and a documented query performance optimization case study.

**Dataset:** [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (~100K orders, publicly available on Kaggle)

## What this project demonstrates

- **ELT pipeline design** — raw ingestion → staging → marts layering pattern in Snowflake
- **Dimensional modeling** — a proper star schema (1 fact table, 3 dimension tables) with verified referential integrity
- **BI reporting** — a Power BI dashboard connected live to Snowflake, with KPI cards and trend/breakdown visuals
- **Performance tuning judgment** — a controlled experiment testing warehouse sizing, clustering keys, and result caching, with real before/after numbers

## Tech stack

`Snowflake` · `SQL` · `Power BI` · `Star Schema Design`

## Project structure

```
ecommerce-analytics-pipeline/
├── README.md
├── sql/
│   ├── 01_full_pipeline.sql              # raw ingestion → staging views → star schema
│   └── 02_optimization_case_study.sql    # warehouse sizing, clustering, caching tests
├── case_study.md                          # full write-up of optimization findings
└── dashboard/
    ├── olist_dashboard.pbix
    └── dashboard_screenshot.png
```

## Architecture

```
Kaggle CSVs → Snowflake Internal Stage → RAW tables → STAGING views → MARTS (star schema) → Power BI
```

- **RAW**: six tables loaded directly from staged CSVs (`orders`, `order_items`, `customers`, `products`, `payments`, `sellers`)
- **STAGING**: cleaned, typed views with light standardization (null handling, category defaults)
- **MARTS**: `fact_orders` (grain: one row per order item) joined to `dim_customer`, `dim_product`, and `dim_date`

## Dashboard highlights

The Power BI dashboard includes:
- Monthly revenue trend
- Top 10 product categories by revenue
- Revenue breakdown by customer state
- KPI cards: total revenue, distinct order count, average delivery time

<img width="698" height="899" alt="image" src="https://github.com/user-attachments/assets/2ae2b5f2-3b1c-430b-b490-cdbfd0ca631e" />


## Optimization case study

Full write-up in [`case_study.md`](./case_study.md). Key findings:

| Test | Result |
|---|---|
| Warehouse sizing (XS vs. Medium) | Medium was **not faster** (988ms vs. 883ms) — 4x the cost for no benefit at this data scale |
| Clustering key | No measurable effect, because the table fits in a single micro-partition — a useful negative result |
| Result caching | Identical repeat query ran **4.7x faster** (278ms → 59ms) with zero compute cost |

The takeaway: not every optimization technique applies at every data scale, and confirming *why* something doesn't help is as valuable as finding something that does.

## Notes

- No fabricated metrics — all numbers in `case_study.md` come directly from Snowflake's Query Profile on this exact dataset.
- The `.pbix` file requires Power BI Desktop (Windows) or the Power BI web app to open; the screenshot is included for quick viewing.
