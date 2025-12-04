# Data-Warehouse-Project
build a Landlord Statement Data Warehouse with mySQL, including data modeling,ETL processes and analytics.

## Overview

This project implements a complete **ETL pipeline** and **data warehouse** for a property management agency. It transforms raw transactional data from CSV files into a fully normalized operational data store (ODS), then into a star schema optimized for analytical queries.

### Business Context

Property management agencies handle:
- Multiple landlords with multiple properties
- Monthly rent collection and statement generation
- Management fees, repairs, and other expenses
- Tenant leases and contractor relationships

This data warehouse enables:
- **Revenue Analysis**: Track rent collection trends over time
- **Expense Management**: Monitor repair costs and identify seasonal patterns
- **Landlord Reporting**: Generate performance summaries for each landlord
- **Agency Profitability**: Analyze management fee earnings

---

##  Architecture

```
┌───────────┐     ┌───────────┐     ┌───────────┐     ┌───────────┐
│    CSV    │────▶│  Staging  │────▶│    ODS    │────▶│    DW     │
│   File    │     │   Table   │     │   (3NF)   │     │  (Star)   │
└───────────┘     └───────────┘     └───────────┘     └─────┬─────┘
                                                            │
                                                            ▼
                                         Analytical Views ──▶ Dashboard
```
---

## Analytical Views

| View | Purpose |
|------|---------|
| `vw_monthly_trends` | Monthly rent, fees, net profit, and active property count |
| `vw_property_annual_performance` | Annual metrics per property with YoY comparison |
| `vw_landlord_performance` | Aggregated landlord profitability and property counts |
| `vw_agency_earnings` | Quarterly management fee income analysis |
| `vw_repair_analysis` | Repair costs and repair-to-rent ratios by property |
| `vw_seasonal_maintenance` | Monthly averages for maintenance planning |
| `vw_yoy_comparison` | Year-over-year growth rates |
| `vw_fee_analysis` | Management fee percentages by property |

---

## Project Structure

```
landlord-data-warehouse/
│
├── sql/
│   ├── 1_createdatabase_and_staging_table.sql    # Database and staging setup
│   ├── 2_ETL.sql                                  # ODS tables and ETL procedures
│   └── 3_data_warehouse_create_and_analysis.sql  # Star schema and views
│
├── python/
│   ├── landlord_dashboard.py          # Interactive Plotly dashboard
│
├── data/
│   └── spreadsheet.csv                # Sample data
│
├── output/
│   └── landlord_dashboard_output.html  # Generated dashboard（AI-generated）
│
└── README.md
```

---

##  Tech Stack

- **Database**: MySQL 9.4.0
- **Visualization**: Python, Plotly, Pandas

---


## Author

**Xinyu Wang**

- Course: CS779 

