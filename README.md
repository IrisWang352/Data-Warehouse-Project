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

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           DATA ARCHITECTURE                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ┌─────────────┐      ┌─────────────┐      ┌─────────────────────┐    │
│   │   CSV File  │ ──── │   Staging   │ ──── │   ODS (3NF)         │    │
│   │  (Raw Data) │ ETL  │    Table    │ ETL  │   Normalized        │    │
│   └─────────────┘      └─────────────┘      └─────────────────────┘    │
│                                                       │                 │
│                                                       │ ETL             │
│                                                       ▼                 │
│                              ┌─────────────────────────────────────┐    │
│                              │      DATA WAREHOUSE (Star Schema)   │    │
│                              │                                     │    │
│                              │   ┌─────────┐     ┌─────────┐      │    │
│                              │   │dim_date │     │dim_agent│      │    │
│                              │   └────┬────┘     └────┬────┘      │    │
│                              │        │              │            │    │
│                              │   ┌────┴──────────────┴────┐       │    │
│                              │   │  fact_statement_summary │       │    │
│                              │   │  fact_statement_charge  │       │    │
│                              │   └────┬──────────────┬────┘       │    │
│                              │        │              │            │    │
│                              │   ┌────┴────┐    ┌───┴─────┐      │    │
│                              │   │dim_prop │    │dim_tenant│      │    │
│                              │   └─────────┘    └─────────┘      │    │
│                              └─────────────────────────────────────┘    │
│                                              │                          │
│                                              ▼                          │
│                              ┌─────────────────────────────────────┐    │
│                              │       ANALYTICAL VIEWS              │    │
│                              │  • vw_monthly_trends                │    │
│                              │  • vw_property_annual_performance   │    │
│                              │  • vw_landlord_performance          │    │
│                              │  • vw_seasonal_maintenance          │    │
│                              └─────────────────────────────────────┘    │
│                                              │                          │
│                                              ▼                          │
│                              ┌─────────────────────────────────────┐    │
│                              │       PYTHON DASHBOARD              │    │
│                              │         (Plotly)                    │    │
│                              └─────────────────────────────────────┘    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
`

## ✨ Features

### Data Processing
- ✅ CSV import with date format handling (M/DD/YYYY → YYYY-MM-DD)
- ✅ 3NF normalization for operational data
- ✅ Star schema transformation for analytics
- ✅ SCD Type 2 support for slowly changing dimensions
- ✅ Automated ETL stored procedures

### Analytical Capabilities
- Monthly revenue trend analysis
- Year-over-year comparison
- Property performance ranking
- Landlord profitability reports
- Seasonal maintenance patterns
- Management fee analysis

### Visualization
-  Interactive Plotly dashboard
-  KPI summary cards
-  Multi-chart HTML report
-  Dark theme professional design


## Usage

### Generate Dashboard

```bash
python landlord_dashboard.py
```

This creates `landlord_dashboard_output.html` - open it in your browser.

### Query Examples

```sql
-- Monthly revenue trends
SELECT * FROM vw_monthly_trends ORDER BY year, month_number;

-- Top performing properties
SELECT * FROM vw_property_annual_performance 
WHERE year = 2024 ORDER BY net_profit DESC LIMIT 10;

-- Landlord profitability
SELECT * FROM vw_landlord_performance ORDER BY total_profit DESC;

-- Seasonal repair patterns
SELECT * FROM vw_seasonal_maintenance;
```


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

