# Data-Warehouse-Project
build a Landlord Statement Data Warehouse with mySQL, including data modeling,ETL processes and analytics.


##  Overview

This project implements a **complete data warehouse solution** for managing and analyzing landlord statements from a property letting agency. It transforms raw spreadsheet data into actionable business intelligence through a multi-layered architecture.

### Business Context

Property letting agencies generate monthly statements for landlords containing:
- Rental income collected
- Management fees charged
- Repair and maintenance costs
- Net payments to landlords

This data warehouse enables:
- **Performance tracking** across properties and landlords
- **Revenue analysis** for the agency
-  **Maintenance pattern detection**
-  **Year-over-year trend analysis**
-  **Geographic performance insights**

---

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DATA WAREHOUSE ARCHITECTURE                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│    ┌──────────────┐                                                         │
│    │  Excel/CSV   │  Raw Data Source                                        │
│    │  Spreadsheet │                                                         │
│    └──────┬───────┘                                                         │
│           │                                                                  │
│           ▼                                                                  │
│    ┌──────────────────────────────────────────────────────────────┐         │
│    │                    STAGING LAYER                              │         │
│    │  ┌────────────────────────────────────────────────────────┐  │         │
│    │  │  statements_raw                                         │  │         │
│    │  │  • Flat structure (mirrors spreadsheet)                 │  │         │
│    │  │  • No constraints (accepts dirty data)                  │  │         │
│    │  │  • Charges as columns                                   │  │         │
│    │  └────────────────────────────────────────────────────────┘  │         │
│    └──────────────────────────┬───────────────────────────────────┘         │
│                               │ ETL: Normalize & Deduplicate                 │
│                               ▼                                              │
│    ┌──────────────────────────────────────────────────────────────┐         │
│    │               OPERATIONAL DATA STORE (ODS)                    │         │
│    │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐        │         │
│    │  │ landlord │ │ property │ │  tenant  │ │  agent   │        │         │
│    │  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘        │         │
│    │       │            │            │            │               │         │
│    │       └────────────┴─────┬──────┴────────────┘               │         │
│    │                          ▼                                   │         │
│    │  ┌────────────────────────────────────────────────────────┐  │         │
│    │  │  statement ──► statement_charge                        │  │         │
│    │  │  (Normalized 3NF - charges as rows)                    │  │         │
│    │  └────────────────────────────────────────────────────────┘  │         │
│    └──────────────────────────┬───────────────────────────────────┘         │
│                               │ ETL: Denormalize & Aggregate                 │
│                               ▼                                              │
│    ┌──────────────────────────────────────────────────────────────┐         │
│    │                DATA WAREHOUSE (Star Schema)                   │         │
│    │                                                               │         │
│    │         ┌───────────┐     ┌───────────┐                      │         │
│    │         │ dim_date  │     │dim_landlord│                     │         │
│    │         └─────┬─────┘     └─────┬─────┘                      │         │
│    │               │                 │                            │         │
│    │               ▼                 ▼                            │         │
│    │  ┌─────────────────────────────────────────────────┐        │         │
│    │  │            FACT TABLES                          │        │         │
│    │  │  • fact_statement_charge (grain: charge)        │        │         │
│    │  │  • fact_statement_summary (grain: statement)    │        │         │
│    │  └─────────────────────────────────────────────────┘        │         │
│    │               ▲                 ▲                            │         │
│    │               │                 │                            │         │
│    │         ┌─────┴─────┐     ┌─────┴─────┐                      │         │
│    │         │dim_property│    │ dim_tenant │                     │         │
│    │         └───────────┘     └───────────┘                      │         │
│    └──────────────────────────┬───────────────────────────────────┘         │
│                               │                                              │
│                               ▼                                              │
│    ┌──────────────────────────────────────────────────────────────┐         │
│    │                  VISUALIZATION LAYER                          │         │
│    │  ┌────────────────────────────────────────────────────────┐  │         │
│    │  │  Tableau-Optimized Views (tableau_*)                   │  │         │
│    │  │  • tableau_master_fact                                 │  │         │
│    │  │  • tableau_monthly_kpi                                 │  │         │
│    │  │  • tableau_property_scorecard                          │  │         │
│    │  │  • ... and more                                        │  │         │
│    │  └────────────────────────────────────────────────────────┘  │         │
│    └──────────────────────────┬───────────────────────────────────┘         │
│                               │                                              │
│                               ▼                                              │
│    ┌──────────────────────────────────────────────────────────────┐         │
│    │                     TABLEAU DESKTOP                           │         │
│    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │         │
│    │  │  Executive  │  │  Property   │  │   Agent     │          │         │
│    │  │  Dashboard  │  │  Analysis   │  │ Performance │          │         │
│    │  └─────────────┘  └─────────────┘  └─────────────┘          │         │
│    └──────────────────────────────────────────────────────────────┘         │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## ✨ Features

### Data Processing
- ✅ **Staging Layer** - Accepts raw, denormalized spreadsheet data
- ✅ **3NF Normalization** - Eliminates data redundancy in ODS
- ✅ **Star Schema** - Optimized for analytical queries
- ✅ **SCD Type 2** - Tracks historical changes in dimensions
- ✅ **Data Quality Fixes** - Handles invalid dates and NULL values

### Analytics
- ✅ **Pre-built Views** - 8 analytical views for common business questions
- ✅ **KPI Calculations** - Profit margins, expense ratios, growth rates
- ✅ **Time Intelligence** - YoY comparisons, monthly trends, seasonality
- ✅ **Geographic Analysis** - Location-based aggregations

### Visualization
- ✅ **Tableau-Ready** - 8 optimized views for Tableau dashboards
- ✅ **Dashboard Templates** - Executive, Property, Agent, Geographic
- ✅ **Calculated Fields** - Pre-defined Tableau calculations

---

## 🚀 Quick Start

### Prerequisites

- MySQL 8.0 or higher
- MySQL Workbench (recommended)
- Tableau Desktop (optional, for visualization)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/landlord-data-warehouse.git
   cd landlord-data-warehouse
   ```

2. **Run scripts in order**
   ```sql
   -- Step 1: Create database and staging table
   SOURCE 01_create_database_and_staging.sql;
   
   -- Step 2: Import your data into statements_raw
   -- (Use MySQL Workbench Import Wizard or LOAD DATA INFILE)
   
   -- Step 3: Create ODS and load data
   SOURCE 02_etl_load_ods.sql;
   
   -- Step 4: Create data warehouse
   SOURCE 03_create_data_warehouse.sql;
   
   -- Step 5: Create Tableau views (optional)
   SOURCE 04_tableau_visualization_layer.sql;
   ```

3. **Verify installation**
   ```sql
   -- Check row counts
   SELECT 'dim_date' AS table_name, COUNT(*) FROM dim_date
   UNION ALL SELECT 'dim_landlord', COUNT(*) FROM dim_landlord
   UNION ALL SELECT 'dim_property', COUNT(*) FROM dim_property
   UNION ALL SELECT 'fact_statement_summary', COUNT(*) FROM fact_statement_summary;
   ```

---

## 📊 Database Schema

### Staging Layer

| Table | Description |
|-------|-------------|
| `statements_raw` | Flat staging table mirroring Excel/CSV structure |

### ODS Layer (3NF)

| Table | Type | Description |
|-------|------|-------------|
| `landlord` | Entity | Property owners |
| `property` | Entity | Rental properties |
| `tenant` | Entity | Renters |
| `agency_agent` | Entity | Letting agency staff |
| `contractor` | Entity | Service providers |
| `property_type` | Reference | Property categories |
| `charge_type` | Reference | Charge categories |
| `lease` | Relationship | Property-tenant links |
| `property_assignment` | Relationship | Property-agent links |
| `statement` | Transaction | Monthly statements |
| `statement_charge` | Transaction | Statement line items |
| `bank_account` | Entity | Payment details |

### Data Warehouse Layer (Star Schema)

#### Dimension Tables

| Table | Description | SCD Type |
|-------|-------------|----------|
| `dim_date` | Date attributes (2020-2030) | Type 0 |
| `dim_landlord` | Landlord attributes | Type 2 |
| `dim_property` | Property attributes (denormalized) | Type 2 |
| `dim_tenant` | Tenant attributes | Type 2 |
| `dim_agent` | Agent attributes | Type 1 |
| `dim_charge_type` | Charge type attributes | Type 0 |

#### Fact Tables

| Table | Grain | Measures |
|-------|-------|----------|
| `fact_statement_charge` | One charge line | charge_amount, is_income |
| `fact_statement_summary` | One statement | total_rent, total_fees, total_repair, net_to_landlord, agency_earnings |

---

## 🔄 ETL Pipeline

### Data Flow

```
Excel/CSV → Staging → ODS → Data Warehouse → Tableau
```

### Key Transformations

| Stage | Transformation |
|-------|----------------|
| Staging → ODS | Deduplication (SELECT DISTINCT) |
| Staging → ODS | Key resolution (JOINs to get surrogate keys) |
| Staging → ODS | Unpivot (columns → rows for charges) |
| Staging → ODS | Data type conversion |
| ODS → DW | Denormalization (flatten dimensions) |
| ODS → DW | Aggregation (summarize by statement) |
| ODS → DW | Date key generation (YYYYMMDD format) |

### ETL Stored Procedures

```sql
-- Load all dimensions and facts
CALL etl_run_full_load();

-- Or run individually:
CALL etl_load_dim_landlord();
CALL etl_load_dim_property();
CALL etl_load_dim_tenant();
CALL etl_load_dim_agent();
CALL etl_load_fact_statement_charge();
CALL etl_load_fact_statement_summary();
```

---

## 📈 Tableau Integration

### Connection Setup

1. Open Tableau Desktop
2. Connect → MySQL
3. Server: `localhost`, Database: `landlord_dw`
4. Use views prefixed with `tableau_`

### Available Views

| View | Best For |
|------|----------|
| `tableau_master_fact` | Primary data source - all dashboards |
| `tableau_monthly_kpi` | Executive KPI dashboards |
| `tableau_property_scorecard` | Property performance analysis |
| `tableau_landlord_portfolio` | Landlord portfolio overview |
| `tableau_yoy_analysis` | Year-over-year trends |
| `tableau_geographic` | Map visualizations |
| `tableau_agent_performance` | Agent performance tracking |
| `tableau_charge_detail` | Drill-down analysis |

### Sample Dashboard Layout

```
┌─────────────────────────────────────────────────────────────────┐
│                    EXECUTIVE KPI DASHBOARD                       │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │  Total   │  │  Total   │  │  Agency  │  │  Active  │        │
│  │   Rent   │  │  Profit  │  │ Earnings │  │Properties│        │
│  │ £XXX,XXX │  │ £XXX,XXX │  │  £XX,XXX │  │   XXX    │        │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘        │
│                                                                  │
│  ┌────────────────────────────────────────────────────────┐     │
│  │              Monthly Revenue Trend                      │     │
│  └────────────────────────────────────────────────────────┘     │
│                                                                  │
│  ┌────────────────────┐  ┌────────────────────┐                 │
│  │  Rent vs Expenses  │  │  Top Properties    │                 │
│  └────────────────────┘  └────────────────────┘                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📋 Analytical Views

### Pre-built Business Views

| View | Business Question |
|------|-------------------|
| `vw_property_annual_performance` | How profitable is each property by year? |
| `vw_agency_earnings` | What are our management fee revenues? |
| `vw_repair_analysis` | Which properties have high maintenance costs? |
| `vw_monthly_trends` | How is revenue trending month over month? |
| `vw_landlord_performance` | Who are our most profitable landlords? |
| `vw_fee_analysis` | Are fees consistent across properties? |
| `vw_yoy_comparison` | How do we compare to last year? |
| `vw_seasonal_maintenance` | When do repairs typically occur? |

### Example Queries

```sql
-- Top 5 properties by profit
SELECT * FROM vw_property_annual_performance 
WHERE year = 2024 
ORDER BY net_profit DESC 
LIMIT 5;

-- Monthly agency earnings
SELECT year, month_name, total_management_fees 
FROM vw_agency_earnings 
WHERE year = 2024;

-- Properties with high repair costs (>15% of rent)
SELECT * FROM vw_repair_analysis 
WHERE repair_to_rent_ratio > 15;
```

---

## 📁 File Structure

```
landlord-data-warehouse/
│
├── 📄 README.md                              # This file
├── 📄 TABLEAU_GUIDE.md                       # Tableau integration guide
│
├── 📂 sql/
│   ├── 01_create_database_and_staging.sql   # Step 1: Database & staging
│   ├── 02_etl_load_ods.sql                  # Step 2: ODS & ETL
│   ├── 03_create_data_warehouse.sql         # Step 3: Star schema
│   └── 04_tableau_visualization_layer.sql   # Step 4: Tableau views
│
├── 📂 docs/
│   ├── architecture.md                       # Detailed architecture docs
│   ├── data-dictionary.md                    # Column descriptions
│   └── etl-specifications.md                 # ETL logic documentation
│
└── 📂 sample-data/
    └── sample_statements.csv                 # Sample data for testing
```

---

## 💡 Usage Examples

### Import Data from CSV

```sql
LOAD DATA INFILE '/path/to/statements.csv'
INTO TABLE statements_raw
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
```

### Refresh Data Warehouse

```sql
-- Clear and reload fact tables
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE fact_statement_charge;
TRUNCATE TABLE fact_statement_summary;
SET FOREIGN_KEY_CHECKS = 1;

-- Reload
CALL etl_run_full_load();
```

### Query Property Performance

```sql
SELECT 
    property_alias,
    landlord_name,
    year,
    total_rent,
    net_profit,
    ROUND(net_profit / total_rent * 100, 1) AS margin_pct
FROM vw_property_annual_performance
WHERE year = YEAR(CURRENT_DATE)
ORDER BY net_profit DESC;
```


<p align="center">
  Made with ❤️ for property management analytics
</p>
