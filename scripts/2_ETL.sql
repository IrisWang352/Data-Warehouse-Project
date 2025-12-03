-- ============================================================================
-- 2: ETL ( staging table to ODS)
-- ============================================================================
-- Execution  Step 2
-- Purpose: Create ODS tables (normalized 3NF) + ETL from staging
-- ============================================================================

use landlord_dw;

-- ============================================================================
-- PART A: create ODS tables (3NF Normalized)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- ENTITY TABLES
-- ----------------------------------------------------------------------------

-- Landlord 
CREATE TABLE landlord (
    landlord_id INT AUTO_INCREMENT PRIMARY KEY,
    landlord_reg_number VARCHAR(50) NOT NULL UNIQUE,
    landlord_name VARCHAR(100) NOT NULL,
    landlord_email VARCHAR(100),
    landlord_phone VARCHAR(20),
    landlord_address VARCHAR(255),
    created_date DATE DEFAULT (CURRENT_DATE),
    is_active BOOLEAN DEFAULT TRUE,
    INDEX idx_landlord_reg (landlord_reg_number)
);

-- Bank Account 
CREATE TABLE bank_account (
    bank_account_id INT AUTO_INCREMENT PRIMARY KEY,
    landlord_id INT NOT NULL,
    bank_account_name VARCHAR(100),
    bank_name VARCHAR(100),
    bank_account_number VARCHAR(20),
    bank_sort_code VARCHAR(20),
    is_primary BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (landlord_id) REFERENCES landlord(landlord_id),
    INDEX idx_bank_landlord (landlord_id)
);

-- Property Type 
CREATE TABLE property_type (
    property_type_id INT AUTO_INCREMENT PRIMARY KEY,
    property_type_name VARCHAR(50) NOT NULL UNIQUE
);

-- Property 
CREATE TABLE property (
    property_id INT AUTO_INCREMENT PRIMARY KEY,
    landlord_id INT NOT NULL,
    property_type_id INT,
    property_alias VARCHAR(100),
    property_address VARCHAR(255),
    property_city VARCHAR(100),
    property_postcode VARCHAR(20),
    property_bedrooms INT,
    is_active BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (landlord_id) REFERENCES landlord(landlord_id),
    FOREIGN KEY (property_type_id) REFERENCES property_type(property_type_id),
    INDEX idx_property_landlord (landlord_id),
    INDEX idx_property_postcode (property_postcode)
);

-- Tenant 
CREATE TABLE tenant (
    tenant_id INT AUTO_INCREMENT PRIMARY KEY,
    tenant_first_name VARCHAR(50),
    tenant_last_name VARCHAR(50),
    tenant_email VARCHAR(100),
    tenant_phone VARCHAR(20),
    INDEX idx_tenant_name (tenant_last_name, tenant_first_name)
);

-- Agency Agent 
CREATE TABLE agency_agent (
    agent_id INT AUTO_INCREMENT PRIMARY KEY,
    agent_name VARCHAR(100) NOT NULL,
    agent_role VARCHAR(50),
    agent_email VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    INDEX idx_agent_name (agent_name)
);

-- Contractor 
CREATE TABLE contractor (
    contractor_id INT AUTO_INCREMENT PRIMARY KEY,
    contractor_company VARCHAR(100),
    contractor_contact VARCHAR(100),
    contractor_phone VARCHAR(20),
    contractor_service_type VARCHAR(50),
    INDEX idx_contractor_service (contractor_service_type)
);

-- Charge Type 
CREATE TABLE charge_type (
    charge_type_id INT AUTO_INCREMENT PRIMARY KEY,
    charge_type_name VARCHAR(50) NOT NULL UNIQUE,
    charge_category VARCHAR(50),
    is_income BOOLEAN DEFAULT FALSE
);

-- ----------------------------------------------------------------------------
-- TRANSACTION TABLES
-- ----------------------------------------------------------------------------

-- Lease Table - Links Property to Tenant
CREATE TABLE lease (
    lease_id INT AUTO_INCREMENT PRIMARY KEY,
    property_id INT NOT NULL,
    tenant_id INT NOT NULL,
    lease_start_date DATE,
    lease_end_date DATE,
    monthly_rent DECIMAL(10,2),
    deposit_amount DECIMAL(10,2),
    is_active BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (property_id) REFERENCES property(property_id),
    FOREIGN KEY (tenant_id) REFERENCES tenant(tenant_id),
    INDEX idx_lease_property (property_id),
    INDEX idx_lease_dates (lease_start_date, lease_end_date)
);

-- Property Assignment Table - Links Property to Agent (M:M over time)
CREATE TABLE property_assignment (
    assignment_id INT AUTO_INCREMENT PRIMARY KEY,
    property_id INT NOT NULL,
    agent_id INT NOT NULL,
    assignment_start_date DATE,
    assignment_end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (property_id) REFERENCES property(property_id),
    FOREIGN KEY (agent_id) REFERENCES agency_agent(agent_id),
    INDEX idx_assignment_property (property_id),
    INDEX idx_assignment_agent (agent_id)
);

-- Statement Table - Monthly landlord statements
CREATE TABLE statement (
    statement_id INT PRIMARY KEY,
    property_id INT NOT NULL,
    lease_id INT,
    statement_date DATE,
    period_start DATE,
    period_end DATE,
    total_amount DECIMAL(10,2),
    pay_date DATE,
    note TEXT,
    FOREIGN KEY (property_id) REFERENCES property(property_id),
    FOREIGN KEY (lease_id) REFERENCES lease(lease_id),
    INDEX idx_statement_property (property_id),
    INDEX idx_statement_date (statement_date),
    INDEX idx_statement_period (period_start, period_end)
);

-- Statement Charge Table - Individual line items
CREATE TABLE statement_charge (
    charge_id INT AUTO_INCREMENT PRIMARY KEY,
    statement_id INT NOT NULL,
    charge_type_id INT NOT NULL,
    contractor_id INT,
    amount DECIMAL(10,2),
    description TEXT,
    FOREIGN KEY (statement_id) REFERENCES statement(statement_id),
    FOREIGN KEY (charge_type_id) REFERENCES charge_type(charge_type_id),
    FOREIGN KEY (contractor_id) REFERENCES contractor(contractor_id),
    INDEX idx_charge_statement (statement_id),
    INDEX idx_charge_type (charge_type_id)
);


-- ============================================================================
-- PART B: INSERT REFERENCE DATA
-- ============================================================================

-- Charge types
INSERT INTO charge_type (charge_type_name, charge_category, is_income) VALUES
('rent', 'income', TRUE),
('management_fee', 'expense', FALSE),
('repair', 'expense', FALSE),
('deposit', 'deposit', FALSE),
('misc', 'expense', FALSE);


-- ============================================================================
-- PART C: ETL - LOAD DATA FROM STAGING TO ODS
-- ============================================================================

-- Load Landlords
INSERT INTO landlord (
    landlord_reg_number, 
    landlord_name, 
    landlord_email, 
    landlord_phone, 
    landlord_address
)
SELECT DISTINCT 
    landlord_reg_number, 
    landlord_name, 
    landlord_email, 
    landlord_phone, 
    landlord_address
FROM statements_raw
WHERE landlord_reg_number IS NOT NULL;

-- Load Property Types
INSERT INTO property_type (property_type_name)
SELECT DISTINCT property_type
FROM statements_raw
WHERE property_type IS NOT NULL
ON DUPLICATE KEY UPDATE property_type_name = VALUES(property_type_name);

--  Load Properties
INSERT INTO property (
    landlord_id, 
    property_type_id, 
    property_alias, 
    property_address, 
    property_city, 
    property_postcode, 
    property_bedrooms
)
SELECT DISTINCT
    l.landlord_id,
    pt.property_type_id,
    sr.property_alias,
    sr.property_address,
    sr.property_city,
    sr.property_postcode,
    sr.property_bedrooms
FROM statements_raw sr
JOIN landlord l ON sr.landlord_reg_number = l.landlord_reg_number
LEFT JOIN property_type pt ON sr.property_type = pt.property_type_name
WHERE sr.property_alias IS NOT NULL;

--  Load Tenants
INSERT INTO tenant (
    tenant_first_name,
    tenant_last_name,
    tenant_email, 
    tenant_phone
)
SELECT DISTINCT 
    tenant_first_name, 
    tenant_last_name, 
    tenant_email, 
    tenant_phone
FROM statements_raw
WHERE tenant_email IS NOT NULL;

--  Load Agency Agents
INSERT INTO agency_agent (
    agent_name, 
    agent_role, 
    agent_email
)
SELECT DISTINCT 
    assigned_agent_name, 
    assigned_agent_role, 
    assigned_agent_email
FROM statements_raw
WHERE assigned_agent_email IS NOT NULL;
SELECT CONCAT('Agents loaded: ', COUNT(*)) AS status FROM agency_agent;

--  Load Contractors
INSERT INTO contractor (
    contractor_company, 
    contractor_contact, 
    contractor_phone,
    contractor_service_type
)
SELECT DISTINCT 
    contractor_company, 
    contractor_contact, 
    contractor_phone,
    contractor_service_type
FROM statements_raw
WHERE contractor_company IS NOT NULL;

--  Load Bank Accounts
INSERT INTO bank_account (
    landlord_id, 
    bank_account_name, 
    bank_name, 
    bank_account_number, 
    bank_sort_code
)
SELECT DISTINCT
    l.landlord_id, 
    sr.bank_account_name, 
    sr.bank_name, 
    CAST(sr.bank_account_number AS CHAR(20)),
    sr.bank_sort_code
FROM statements_raw sr
JOIN landlord l ON sr.landlord_reg_number = l.landlord_reg_number
WHERE sr.bank_account_number IS NOT NULL
GROUP BY l.landlord_id, sr.bank_account_name, sr.bank_name, 
         sr.bank_account_number, sr.bank_sort_code;

--  Load Leases
INSERT INTO lease (
    property_id, 
    tenant_id, 
    lease_start_date, 
    lease_end_date, 
    monthly_rent, 
    deposit_amount
)
SELECT DISTINCT
    p.property_id,
    t.tenant_id,
    CASE 
        WHEN sr.lease_start_date IS NULL THEN NULL
        WHEN sr.lease_start_date < '1900-01-01' THEN NULL
        ELSE sr.lease_start_date 
    END,
    CASE 
        WHEN sr.lease_end_date IS NULL THEN NULL
        WHEN sr.lease_end_date < '1900-01-01' THEN NULL
        ELSE sr.lease_end_date 
    END,
    sr.lease_monthly_rent,
    sr.lease_deposit_amount
FROM statements_raw sr
JOIN property p ON sr.property_alias = p.property_alias
JOIN tenant t ON sr.tenant_email = t.tenant_email
WHERE sr.tenant_email IS NOT NULL;

-- Load Property Assignments
INSERT INTO property_assignment (
    property_id, 
    agent_id, 
    assignment_start_date,
    is_current
)
SELECT DISTINCT
    p.property_id,
    a.agent_id,
    CURRENT_DATE,
    TRUE
FROM statements_raw sr
JOIN property p ON sr.property_alias = p.property_alias
JOIN agency_agent a ON sr.assigned_agent_email = a.agent_email
WHERE sr.assigned_agent_email IS NOT NULL;

-- Load Statements
INSERT INTO statement (
    statement_id, 
    property_id, 
    lease_id,
    statement_date, 
    period_start, 
    period_end,
    total_amount, 
    pay_date, 
    note
)
SELECT
    sr.statement_id,
    p.property_id,
    le.lease_id,
    CASE 
        WHEN sr.statement_date IS NULL THEN NULL
        WHEN sr.statement_date < '1900-01-01' THEN NULL
        ELSE sr.statement_date 
    END,
    CASE 
        WHEN sr.period_start IS NULL THEN NULL
        WHEN sr.period_start < '1900-01-01' THEN NULL
        ELSE sr.period_start 
    END,
    CASE 
        WHEN sr.period_end IS NULL THEN NULL
        WHEN sr.period_end < '1900-01-01' THEN NULL
        ELSE sr.period_end 
    END,
    sr.total,
    CASE 
        WHEN sr.pay_date IS NULL THEN NULL
        WHEN sr.pay_date < '1900-01-01' THEN NULL
        ELSE sr.pay_date 
    END,
    sr.note
FROM statements_raw sr
JOIN property p ON sr.property_alias = p.property_alias
LEFT JOIN tenant t ON sr.tenant_email = t.tenant_email
LEFT JOIN lease le ON le.property_id = p.property_id 
                   AND le.tenant_id = t.tenant_id;

--  Load Statement Charges ( columns to rows)

-- Rent charges
INSERT INTO statement_charge (statement_id, charge_type_id, amount, description)
SELECT 
    statement_id, 
    (SELECT charge_type_id FROM charge_type WHERE charge_type_name = 'rent'),
    rent,
    'Monthly rent'
FROM statements_raw 
WHERE rent IS NOT NULL AND rent != 0;

-- Management fee charges
INSERT INTO statement_charge (statement_id, charge_type_id, amount, description)
SELECT 
    statement_id,
    (SELECT charge_type_id FROM charge_type WHERE charge_type_name = 'management_fee'),
    management_fee,
    'Agency management fee'
FROM statements_raw 
WHERE management_fee IS NOT NULL AND management_fee != 0;

-- Repair charges (linked to contractor)
INSERT INTO statement_charge (statement_id, charge_type_id, contractor_id, amount, description)
SELECT 
    sr.statement_id,
    (SELECT charge_type_id FROM charge_type WHERE charge_type_name = 'repair'),
    c.contractor_id,
    sr.repair,
    CONCAT('Repair - ', COALESCE(sr.contractor_service_type, 'General'))
FROM statements_raw sr
LEFT JOIN contractor c ON sr.contractor_company = c.contractor_company
WHERE sr.repair IS NOT NULL AND sr.repair != 0;

-- Deposit charges
INSERT INTO statement_charge (statement_id, charge_type_id, amount, description)
SELECT 
    statement_id,
    (SELECT charge_type_id FROM charge_type WHERE charge_type_name = 'deposit'),
    deposit,
    'Deposit transaction'
FROM statements_raw 
WHERE deposit IS NOT NULL AND deposit != 0;

-- Miscellaneous charges
INSERT INTO statement_charge (statement_id, charge_type_id, amount, description)
SELECT 
    statement_id,
    (SELECT charge_type_id FROM charge_type WHERE charge_type_name = 'misc'),
    misc,
    'Miscellaneous charge'
FROM statements_raw 
WHERE misc IS NOT NULL AND misc != 0;

SELECT CONCAT('Statement charges loaded: ', COUNT(*)) AS status FROM statement_charge;


SELECT '02-C: ETL from staging to ODS complete' AS status;


-- ============================================================================
-- PART D: DATA QUALITY FIXES
-- ============================================================================

-- Fix invalid dates (ensure within date dimension range 2020-2030)
UPDATE statement
SET statement_date = COALESCE(
    CASE 
        WHEN statement_date BETWEEN '2020-01-01' AND '2030-12-31' 
        THEN statement_date 
        ELSE NULL 
    END,
    CASE 
        WHEN period_end BETWEEN '2020-01-01' AND '2030-12-31' 
        THEN period_end 
        ELSE NULL 
    END,
    CASE 
        WHEN period_start BETWEEN '2020-01-01' AND '2030-12-31' 
        THEN period_start 
        ELSE NULL 
    END,
    CASE 
        WHEN pay_date BETWEEN '2020-01-01' AND '2030-12-31' 
        THEN pay_date 
        ELSE NULL 
    END,
    '2020-01-01'
)
WHERE statement_date IS NULL
   OR statement_date NOT BETWEEN '2020-01-01' AND '2030-12-31';

SELECT '02-D: Data quality fixes applied' AS status;


-- ============================================================================
-- PART E: VERIFICATION
-- ============================================================================

SELECT '=== ODS Data Load Summary ===' AS report;

SELECT 'landlord' AS table_name, COUNT(*) AS row_count FROM landlord
UNION ALL SELECT 'property', COUNT(*) FROM property
UNION ALL SELECT 'tenant', COUNT(*) FROM tenant
UNION ALL SELECT 'agency_agent', COUNT(*) FROM agency_agent
UNION ALL SELECT 'contractor', COUNT(*) FROM contractor
UNION ALL SELECT 'lease', COUNT(*) FROM lease
UNION ALL SELECT 'statement', COUNT(*) FROM statement
UNION ALL SELECT 'statement_charge', COUNT(*) FROM statement_charge;

SELECT '02: ODS creation and data load complete!' AS status;

