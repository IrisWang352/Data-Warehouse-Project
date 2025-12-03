
-- 1: CREATE DATABASE AND STAGING TABLE
-- Purpose: Create database and staging table to store raw CSV data


-- create database
drop database if exists landlord_dw;
create database landlord_dw;
use landlord_dw;

-- staging table:statements_raw

create table statements_raw (
    -- statement information
    statement_id           INT,                  
    statement_date         DATE,                 
    period_start           DATE,                
    period_end             DATE,                
    -- landlord information 
    landlord_reg_number    VARCHAR(50),            
    landlord_name          VARCHAR(255),         
    landlord_email         VARCHAR(255),         
    landlord_phone         VARCHAR(50),          
    landlord_address       VARCHAR(255),          
    -- bank account information
    bank_account_name      VARCHAR(255),         
    bank_name              VARCHAR(255),        
    bank_account_number    VARCHAR(50),          
    bank_sort_code         VARCHAR(50),          
    -- property information
    property_alias         VARCHAR(255),         
    property_type          VARCHAR(50),          
    property_address       VARCHAR(255),         
    property_city          VARCHAR(100),        
    property_postcode      VARCHAR(20),          
    property_bedrooms      INT,                 
    -- tenant information
    tenant_first_name      VARCHAR(100),         
    tenant_last_name       VARCHAR(100),         
    tenant_email           VARCHAR(255),         
    tenant_phone           VARCHAR(50),          
    -- lease information
    lease_start_date       DATE,                 
     lease_end_date         DATE,                 
    lease_monthly_rent     DECIMAL(10,2),        
    lease_deposit_amount   DECIMAL(10,2),        
    -- agent information
    assigned_agent_name    VARCHAR(255),         
    assigned_agent_role    VARCHAR(100),         
    assigned_agent_email   VARCHAR(255),         
    -- charges (column - turn to rows during ETL)
    rent                   DECIMAL(10,2),        
    management_fee         DECIMAL(10,2),        
    repair                 DECIMAL(10,2),        
    deposit                DECIMAL(10,2),        
    misc                   DECIMAL(10,2),        
    total                  DECIMAL(10,2),        
    -- contractor information
    contractor_company     VARCHAR(255),        
    contractor_contact     VARCHAR(255),         
    contractor_phone       VARCHAR(50),          
    contractor_service_type VARCHAR(100),        
    -- other data
    note                   TEXT,                 
    pay_date               DATE                  -- Payment date
);



-- next:
-- Import my CSV spreadsheet use terminal: load data local infile '/Users/xinyuwang/Desktop/CS779 Project/spreadsheet.csv' into table statements_raw


-- see if it works
select '1: Database and staging table created successfully' as status;
show tables;
select count(*) as row_count from statements_raw;
