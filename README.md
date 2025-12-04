# Data-Warehouse-Project
build a Landlord Statement Data Warehouse with mySQL, including data modeling,ETL processes and analytics.
┌─────────────────────────────────────────────────────────────────────────────┐
│                         COMPLETE EXECUTION ORDER                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Step 1: 01_create_database_and_staging.sql                                 │
│          └── Create database + staging table                                 │
│                           │                                                  │
│                           ▼                                                  │
│          [Import CSV/Excel data into statements_raw]                         │
│                           │                                                  │
│                           ▼                                                  │
│  Step 2: 02_etl_load_ods.sql                                                │
│          └── Create ODS (3NF) + Load from staging                           │
│                           │                                                  │
│                           ▼                                                  │
│  Step 3: 03_create_data_warehouse.sql                                       │
│          └── Create Star Schema + Load dimensions & facts                   │
│                           │                                                  │
│                           ▼                                                  │
│  Step 4: 04_tableau_visualization_layer.sql                                 │
│          └── Create Tableau-optimized views                                  │
│                           │                                                  │
│                           ▼                                                  │
│  Step 5: Connect Tableau Desktop                                            │
│          └── Build dashboards using tableau_* views                         │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
