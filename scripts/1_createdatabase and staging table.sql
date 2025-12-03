-- ============================================================================
-- 1: CREATE DATABASE AND STAGING TABLE
-- ============================================================================
-- Execution Step 1
-- Purpose: Create database and staging table to store raw CSV data
-- ============================================================================

-- create database
drop database if exists landlord_dw;
create database landlord_dw;
use landlord_dw;

-- ============================================================================
-- STAGING TABLE: statements_raw
-- ============================================================================
-- Characteristics:
--   - Flat structure 
--   - No constraints 
--   - Denormalized 
--   - Charges stored as columns (will be handled during ETL)
-- ============================================================================

create table statements_raw (
    
    -- statement information
    statement_id           INT,                  -- Unique statement number
    statement_date         DATE,                 -- Statement generation date
    period_start           DATE,                 -- Billing period start
    period_end             DATE,                 -- Billing period end
    
    -- landlord information 
    landlord_reg_number    VARCHAR(50),           -- Registration number 
    landlord_name          VARCHAR(255),         -- Landlord full name
    landlord_email         VARCHAR(255),         -- Contact email
    landlord_phone         VARCHAR(50),          -- Contact phone
    landlord_address       VARCHAR(255),          -- Mailing address
    
    -- bank account information
    bank_account_name      VARCHAR(255),         -- Account holder name
    bank_name              VARCHAR(255),         -- Bank name
    bank_account_number    VARCHAR(50),          -- Account number 
    bank_sort_code         VARCHAR(50),          -- Sort code 
    
    -- property information
    property_alias         VARCHAR(255),         -- Property nickname (business key)
    property_type          VARCHAR(50),          -- Type: Flat, Terraced etc.
    property_address       VARCHAR(255),         -- Street address
    property_city          VARCHAR(100),        
    property_postcode      VARCHAR(20),          -- Postal code
    property_bedrooms      INT,                  -- Number of bedrooms
    
    -- tenant information
    tenant_first_name      VARCHAR(100),         -- Tenant first name
    tenant_last_name       VARCHAR(100),         -- Tenant last name
    tenant_email           VARCHAR(255),         -- Tenant email 
    tenant_phone           VARCHAR(50),          -- Tenant phone
    
    -- lease information
    lease_start_date       DATE,                 -- Lease start date
    lease_end_date         DATE,                 -- Lease end date
    lease_monthly_rent     DECIMAL(10,2),        -- Monthly rent amount
    lease_deposit_amount   DECIMAL(10,2),        -- Security deposit
    
    -- agent information
    assigned_agent_name    VARCHAR(255),         -- Agent name
    assigned_agent_role    VARCHAR(100),         -- Agent role/title
    assigned_agent_email   VARCHAR(255),         -- Agent email 
    
    -- charges (column - turn to rows during ETL)
    rent                   DECIMAL(10,2),        -- Rent income (positive)
    management_fee         DECIMAL(10,2),        -- Management fee expense (negative)
    repair                 DECIMAL(10,2),        -- Repair costs (negative)
    deposit                DECIMAL(10,2),        -- Deposit transactions
    misc                   DECIMAL(10,2),        -- Miscellaneous charges
    total                  DECIMAL(10,2),        -- Net amount to landlord
    
    -- contractor information
    contractor_company     VARCHAR(255),         -- Contractor company name
    contractor_contact     VARCHAR(255),         -- Contact person
    contractor_phone       VARCHAR(50),          -- Phone number
    contractor_service_type VARCHAR(100),        -- Service type: Plumbing, Electrical, etc.
    
    -- other data
    note                   TEXT,                 
    pay_date               DATE                  -- Payment date
);

-- ============================================================================
-- next:
-- Import my CSV spreadsheet use terminal: load data local infile '/Users/xinyuwang/Desktop/CS779 Project/spreadsheet.csv' into table statements_raw
-- ============================================================================

-- ============================================================================
-- see if it works
-- ============================================================================
select '1: Database and staging table created successfully' as status;
show tables;
SELECT COUNT(*) AS row_count from statements_raw;
