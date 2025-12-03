-- ============================================================================
-- LANDLORD STATEMENT DATA WAREHOUSE 
-- ============================================================================

-- Step 1: Create the Database
-- ============================================================================
DROP DATABASE IF EXISTS landlord_dw;
CREATE DATABASE landlord_dw;
USE landlord_dw;

-- ============================================================================
-- PART A: OPERATIONAL DATA STORE (ODS) - 3NF Normalized Tables
-- ============================================================================
-- These tables store the clean, normalized data

-- ----------------------------------------------------------------------------
--  ENTITY TABLES 
-- ----------------------------------------------------------------------------

-- Landlord Table - Property owners
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

-- Bank Account Table - Landlord's bank details 
CREATE TABLE bank_account (
    bank_account_id INT AUTO_INCREMENT PRIMARY KEY,
    landlord_id INT NOT NULL,
    bank_account_name VARCHAR(100),
    bank_name VARCHAR(100),
    bank_account_number VARCHAR(20),
    bank_sort_code VARCHAR(20),
    is_primary BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (landlord_id) references landlord(landlord_id),
    INDEX idx_bank_landlord (landlord_id)
);

-- Property Type Table - Types of properties (Flat, Terraced, etc.)
CREATE TABLE property_type (
    property_type_id INT AUTO_INCREMENT PRIMARY KEY,
    property_type_name VARCHAR(50) NOT NULL UNIQUE
);

-- Property Table - Individual rental properties
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

-- Tenant Table - Renters
CREATE TABLE tenant (
    tenant_id INT AUTO_INCREMENT PRIMARY KEY,
    tenant_first_name VARCHAR(50),
    tenant_last_name VARCHAR(50),
    tenant_email VARCHAR(100),
    tenant_phone VARCHAR(20),
    INDEX idx_tenant_name (tenant_last_name, tenant_first_name)
);

-- Agency Agent Table - Staff members of the letting agency
CREATE TABLE agency_agent (
    agent_id INT AUTO_INCREMENT PRIMARY KEY,
    agent_name VARCHAR(100) NOT NULL,
    agent_role VARCHAR(50),
    agent_email VARCHAR(100),
    is_active BOOLEAN DEFAULT TRUE,
    INDEX idx_agent_name (agent_name)
);

-- Contractor Table - Service providers (plumbers, electricians, etc.)
CREATE TABLE contractor (
    contractor_id INT AUTO_INCREMENT PRIMARY KEY,
    contractor_company VARCHAR(100),
    contractor_contact VARCHAR(100),
    contractor_phone VARCHAR(20),
    contractor_service_type VARCHAR(50),
    INDEX idx_contractor_service (contractor_service_type)
);

-- Charge Type Table - Types of charges (rent, repairs, fees, etc.)
CREATE TABLE charge_type (
    charge_type_id INT AUTO_INCREMENT PRIMARY KEY,
    charge_type_name VARCHAR(50) NOT NULL UNIQUE,
    charge_category VARCHAR(50), 
    is_income BOOLEAN DEFAULT FALSE
);

-- ----------------------------------------------------------------------------
-- RELATIONSHIP/TRANSACTION TABLES
-- ----------------------------------------------------------------------------

-- Lease Table - Links Property to Tenant with lease terms
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

-- Statement Charge Table - Individual line items on statements
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
-- PART B: DATA WAREHOUSE - STAR SCHEMA (Dimensional Model)
-- ============================================================================
-- These tables are for analytical queries

-- ----------------------------------------------------------------------------
-- DIMENSION TABLES
-- ----------------------------------------------------------------------------

-- Date Dimension - For time-based analysis
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,  
    full_date DATE NOT NULL UNIQUE,
    day_of_week TINYINT,
    day_name VARCHAR(10),
    day_of_month TINYINT,
    day_of_year SMALLINT,
    week_of_year TINYINT,
    month_number TINYINT,
    month_name VARCHAR(10),
    quarter TINYINT,
    year SMALLINT,
    is_weekend BOOLEAN,
    is_month_end BOOLEAN,
    INDEX idx_date_year_month (year, month_number),
    INDEX idx_date_quarter (year, quarter)
);

-- Property Dimension - Denormalized property information
CREATE TABLE dim_property (
    property_key INT AUTO_INCREMENT PRIMARY KEY,
    property_id INT NOT NULL,  -- Natural key from ODS
    landlord_reg_number VARCHAR(50),
    landlord_name VARCHAR(100),
    property_alias VARCHAR(100),
    property_type VARCHAR(50),
    property_address VARCHAR(255),
    property_city VARCHAR(100),
    property_postcode VARCHAR(20),
    property_bedrooms INT,
    current_agent_name VARCHAR(100),
    current_agent_role VARCHAR(50),
    -- SCD Type 2 
    effective_date DATE,
    expiry_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    INDEX idx_dim_property_id (property_id),
    INDEX idx_dim_property_landlord (landlord_name),
    INDEX idx_dim_property_city (property_city)
);

-- Landlord Dimension
CREATE TABLE dim_landlord (
    landlord_key INT AUTO_INCREMENT PRIMARY KEY,
    landlord_id INT NOT NULL,
    landlord_reg_number VARCHAR(50),
    landlord_name VARCHAR(100),
    landlord_email VARCHAR(100),
    landlord_city VARCHAR(100),  -- Extracted from address
    property_count INT DEFAULT 0,
    effective_date DATE,
    expiry_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    INDEX idx_dim_landlord_id (landlord_id),
    INDEX idx_dim_landlord_name (landlord_name)
);

-- Tenant Dimension
CREATE TABLE dim_tenant (
    tenant_key INT AUTO_INCREMENT PRIMARY KEY,
    tenant_id INT NOT NULL,
    tenant_full_name VARCHAR(100),
    tenant_email VARCHAR(100),
    effective_date DATE,
    expiry_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    INDEX idx_dim_tenant_id (tenant_id)
);

-- Agent Dimension
CREATE TABLE dim_agent (
    agent_key INT AUTO_INCREMENT PRIMARY KEY,
    agent_id INT NOT NULL,
    agent_name VARCHAR(100),
    agent_role VARCHAR(50),
    agent_email VARCHAR(100),
    is_current BOOLEAN DEFAULT TRUE,
    INDEX idx_dim_agent_id (agent_id)
);

-- Charge Type Dimension
CREATE TABLE dim_charge_type (
    charge_type_key INT AUTO_INCREMENT PRIMARY KEY,
    charge_type_id INT NOT NULL,
    charge_type_name VARCHAR(50),
    charge_category VARCHAR(50),
    is_income BOOLEAN,
    INDEX idx_dim_charge_type_id (charge_type_id)
);

-- ----------------------------------------------------------------------------
-- FACT TABLES
-- ----------------------------------------------------------------------------

-- Main Fact Table: Statement Charges (grain = one charge line item)
CREATE TABLE fact_statement_charge (
    fact_id INT AUTO_INCREMENT PRIMARY KEY,
    -- Foreign keys to dimensions
    date_key INT NOT NULL,
    property_key INT NOT NULL,
    landlord_key INT NOT NULL,
    tenant_key INT,
    agent_key INT,
    charge_type_key INT NOT NULL,
    -- Degenerate dimensions
    statement_id INT NOT NULL,
    -- Measures
    charge_amount DECIMAL(10,2),
    -- For analysis convenience
    is_income BOOLEAN,
    statement_period_days INT,
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (property_key) REFERENCES dim_property(property_key),
    FOREIGN KEY (landlord_key) REFERENCES dim_landlord(landlord_key),
    FOREIGN KEY (tenant_key) REFERENCES dim_tenant(tenant_key),
    FOREIGN KEY (agent_key) REFERENCES dim_agent(agent_key),
    FOREIGN KEY (charge_type_key) REFERENCES dim_charge_type(charge_type_key),
    INDEX idx_fact_date (date_key),
    INDEX idx_fact_property (property_key),
    INDEX idx_fact_landlord (landlord_key),
    INDEX idx_fact_charge_type (charge_type_key),
    INDEX idx_fact_statement (statement_id)
);

-- Snapshot Fact Table: Monthly Statement Summary (grain = one statement)
CREATE TABLE fact_statement_summary (
    fact_id INT AUTO_INCREMENT PRIMARY KEY,
    date_key INT NOT NULL,
    property_key INT NOT NULL,
    landlord_key INT NOT NULL,
    tenant_key INT,
    agent_key INT,
    statement_id INT NOT NULL,
    -- Measures
    total_rent DECIMAL(10,2) DEFAULT 0,
    total_management_fee DECIMAL(10,2) DEFAULT 0,
    total_repair DECIMAL(10,2) DEFAULT 0,
    total_deposit DECIMAL(10,2) DEFAULT 0,
    total_misc DECIMAL(10,2) DEFAULT 0,
    net_to_landlord DECIMAL(10,2) DEFAULT 0,
    agency_earnings DECIMAL(10,2) DEFAULT 0,
    statement_period_days INT,
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (property_key) REFERENCES dim_property(property_key),
    FOREIGN KEY (landlord_key) REFERENCES dim_landlord(landlord_key),
    INDEX idx_fact_summary_date (date_key),
    INDEX idx_fact_summary_property (property_key),
    INDEX idx_fact_summary_landlord (landlord_key)
);


-- ============================================================================
-- PART C: POPULATE REFERENCE DATA
-- ============================================================================

-- Insert charge types
INSERT INTO charge_type (charge_type_name, charge_category, is_income) VALUES
('rent', 'income', TRUE),
('management_fee', 'expense', FALSE),
('repair', 'expense', FALSE),
('deposit', 'deposit', FALSE),
('misc', 'expense', FALSE);

-- Insert property types
INSERT INTO property_type (property_type_name) VALUES
('Flat'),
('Terraced'),
('Semi-Detached'),
('Detached'),
('Bungalow');

-- Populate date dimension (2020-2030)
DELIMITER //
CREATE PROCEDURE populate_date_dimension()
BEGIN
    DECLARE v_date DATE;
    DECLARE v_end_date DATE;
    
    SET v_date = '2020-01-01';
    SET v_end_date = '2030-12-31';
    
    WHILE v_date <= v_end_date DO
        INSERT INTO dim_date (
            date_key, full_date, day_of_week, day_name, day_of_month,
            day_of_year, week_of_year, month_number, month_name,
            quarter, year, is_weekend, is_month_end
        ) VALUES (
            YEAR(v_date) * 10000 + MONTH(v_date) * 100 + DAY(v_date),
            v_date,
            DAYOFWEEK(v_date),
            DAYNAME(v_date),
            DAY(v_date),
            DAYOFYEAR(v_date),
            WEEK(v_date),
            MONTH(v_date),
            MONTHNAME(v_date),
            QUARTER(v_date),
            YEAR(v_date),
            DAYOFWEEK(v_date) IN (1, 7),
            v_date = LAST_DAY(v_date)
        );
        SET v_date = DATE_ADD(v_date, INTERVAL 1 DAY);
    END WHILE;
END //
DELIMITER ;

CALL populate_date_dimension();

-- Insert dimension for charge types
INSERT INTO dim_charge_type (charge_type_id, charge_type_name, charge_category, is_income)
SELECT charge_type_id, charge_type_name, charge_category, is_income
FROM charge_type;


-- ============================================================================
-- PART D: ETL STORED PROCEDURES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- D1: Load ODS from Staging (simulated - you would load from your CSV/staging)
-- ----------------------------------------------------------------------------

DELIMITER //

-- Procedure to load landlord data
CREATE PROCEDURE etl_load_landlord(
    IN p_reg_number VARCHAR(50),
    IN p_name VARCHAR(100),
    IN p_email VARCHAR(100),
    IN p_phone VARCHAR(20),
    IN p_address VARCHAR(255)
)
BEGIN
    INSERT INTO landlord (landlord_reg_number, landlord_name, landlord_email, landlord_phone, landlord_address)
    VALUES (p_reg_number, p_name, p_email, p_phone, p_address)
    ON DUPLICATE KEY UPDATE
        landlord_name = VALUES(landlord_name),
        landlord_email = VALUES(landlord_email),
        landlord_phone = VALUES(landlord_phone),
        landlord_address = VALUES(landlord_address);
END //

-- Procedure to load property data
CREATE PROCEDURE etl_load_property(
    IN p_landlord_reg VARCHAR(50),
    IN p_alias VARCHAR(100),
    IN p_type VARCHAR(50),
    IN p_address VARCHAR(255),
    IN p_city VARCHAR(100),
    IN p_postcode VARCHAR(20),
    IN p_bedrooms INT
)
BEGIN
    DECLARE v_landlord_id INT;
    DECLARE v_type_id INT;
    
    SELECT landlord_id INTO v_landlord_id FROM landlord WHERE landlord_reg_number = p_landlord_reg;
    SELECT property_type_id INTO v_type_id FROM property_type WHERE property_type_name = p_type;
    
    INSERT INTO property (landlord_id, property_type_id, property_alias, property_address, property_city, property_postcode, property_bedrooms)
    SELECT v_landlord_id, v_type_id, p_alias, p_address, p_city, p_postcode, p_bedrooms
    WHERE NOT EXISTS (
        SELECT 1 FROM property WHERE property_alias = p_alias AND landlord_id = v_landlord_id
    );
END //

-- Procedure to load tenant data
CREATE PROCEDURE etl_load_tenant(
    IN p_first_name VARCHAR(50),
    IN p_last_name VARCHAR(50),
    IN p_email VARCHAR(100),
    IN p_phone VARCHAR(20)
)
BEGIN
    INSERT INTO tenant (tenant_first_name, tenant_last_name, tenant_email, tenant_phone)
    SELECT p_first_name, p_last_name, p_email, p_phone
    WHERE NOT EXISTS (
        SELECT 1 FROM tenant WHERE tenant_email = p_email
    );
END //

-- Procedure to load agent data
CREATE PROCEDURE etl_load_agent(
    IN p_name VARCHAR(100),
    IN p_role VARCHAR(50),
    IN p_email VARCHAR(100)
)
BEGIN
    INSERT INTO agency_agent (agent_name, agent_role, agent_email)
    SELECT p_name, p_role, p_email
    WHERE NOT EXISTS (
        SELECT 1 FROM agency_agent WHERE agent_email = p_email
    );
END //

-- Procedure to load contractor data
CREATE PROCEDURE etl_load_contractor(
    IN p_company VARCHAR(100),
    IN p_contact VARCHAR(100),
    IN p_phone VARCHAR(20),
    IN p_service_type VARCHAR(50)
)
BEGIN
    INSERT INTO contractor (contractor_company, contractor_contact, contractor_phone, contractor_service_type)
    SELECT p_company, p_contact, p_phone, p_service_type
    WHERE p_company IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM contractor WHERE contractor_company = p_company
    );
END //

DELIMITER ;

-- ----------------------------------------------------------------------------
-- D2: Load Dimensions from ODS
-- ----------------------------------------------------------------------------

DELIMITER //

CREATE PROCEDURE etl_load_dim_landlord()
BEGIN
    INSERT INTO dim_landlord (landlord_id, landlord_reg_number, landlord_name, landlord_email, landlord_city, property_count, effective_date, is_current)
    SELECT 
        l.landlord_id,
        l.landlord_reg_number,
        l.landlord_name,
        l.landlord_email,
        SUBSTRING_INDEX(l.landlord_address, ',', -1) as landlord_city,
        (SELECT COUNT(*) FROM property p WHERE p.landlord_id = l.landlord_id) as property_count,
        CURRENT_DATE,
        TRUE
    FROM landlord l
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_landlord dl WHERE dl.landlord_id = l.landlord_id AND dl.is_current = TRUE
    );
END //

CREATE PROCEDURE etl_load_dim_property()
BEGIN
    INSERT INTO dim_property (
        property_id, landlord_reg_number, landlord_name, property_alias, 
        property_type, property_address, property_city, property_postcode, 
        property_bedrooms, current_agent_name, current_agent_role,
        effective_date, is_current
    )
    SELECT 
        p.property_id,
        l.landlord_reg_number,
        l.landlord_name,
        p.property_alias,
        pt.property_type_name,
        p.property_address,
        p.property_city,
        p.property_postcode,
        p.property_bedrooms,
        a.agent_name,
        a.agent_role,
        CURRENT_DATE,
        TRUE
    FROM property p
    JOIN landlord l ON p.landlord_id = l.landlord_id
    LEFT JOIN property_type pt ON p.property_type_id = pt.property_type_id
    LEFT JOIN property_assignment pa ON p.property_id = pa.property_id AND pa.is_current = TRUE
    LEFT JOIN agency_agent a ON pa.agent_id = a.agent_id
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_property dp WHERE dp.property_id = p.property_id AND dp.is_current = TRUE
    );
END //

CREATE PROCEDURE etl_load_dim_tenant()
BEGIN
    INSERT INTO dim_tenant (tenant_id, tenant_full_name, tenant_email, effective_date, is_current)
    SELECT 
        t.tenant_id,
        CONCAT(t.tenant_first_name, ' ', t.tenant_last_name),
        t.tenant_email,
        CURRENT_DATE,
        TRUE
    FROM tenant t
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_tenant dt WHERE dt.tenant_id = t.tenant_id AND dt.is_current = TRUE
    );
END //

CREATE PROCEDURE etl_load_dim_agent()
BEGIN
    INSERT INTO dim_agent (agent_id, agent_name, agent_role, agent_email, is_current)
    SELECT agent_id, agent_name, agent_role, agent_email, TRUE
    FROM agency_agent
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_agent da WHERE da.agent_id = agency_agent.agent_id AND da.is_current = TRUE
    );
END //

DELIMITER ;

-- ----------------------------------------------------------------------------
-- D3: Load Fact Tables
-- ----------------------------------------------------------------------------

DELIMITER //

CREATE PROCEDURE etl_load_fact_statement_charge()
BEGIN
    -- Load individual charge facts
    INSERT INTO fact_statement_charge (
        date_key, property_key, landlord_key, tenant_key, agent_key,
        charge_type_key, statement_id, charge_amount, is_income, statement_period_days
    )
    SELECT 
        YEAR(s.statement_date) * 10000 + MONTH(s.statement_date) * 100 + DAY(s.statement_date) as date_key,
        dp.property_key,
        dl.landlord_key,
        dt.tenant_key,
        da.agent_key,
        dct.charge_type_key,
        s.statement_id,
        sc.amount,
        dct.is_income,
        DATEDIFF(s.period_end, s.period_start) as statement_period_days
    FROM statement s
    JOIN statement_charge sc ON s.statement_id = sc.statement_id
    JOIN property p ON s.property_id = p.property_id
    JOIN dim_property dp ON p.property_id = dp.property_id AND dp.is_current = TRUE
    JOIN landlord l ON p.landlord_id = l.landlord_id
    JOIN dim_landlord dl ON l.landlord_id = dl.landlord_id AND dl.is_current = TRUE
    LEFT JOIN lease le ON s.lease_id = le.lease_id
    LEFT JOIN tenant t ON le.tenant_id = t.tenant_id
    LEFT JOIN dim_tenant dt ON t.tenant_id = dt.tenant_id AND dt.is_current = TRUE
    LEFT JOIN property_assignment pa ON p.property_id = pa.property_id AND pa.is_current = TRUE
    LEFT JOIN dim_agent da ON pa.agent_id = da.agent_id AND da.is_current = TRUE
    JOIN charge_type ct ON sc.charge_type_id = ct.charge_type_id
    JOIN dim_charge_type dct ON ct.charge_type_id = dct.charge_type_id
    WHERE NOT EXISTS (
        SELECT 1 FROM fact_statement_charge f 
        WHERE f.statement_id = s.statement_id AND f.charge_type_key = dct.charge_type_key
    );
END //

CREATE PROCEDURE etl_load_fact_statement_summary()
BEGIN
    -- Load aggregated statement facts
    INSERT INTO fact_statement_summary (
        date_key, property_key, landlord_key, tenant_key, agent_key,
        statement_id, total_rent, total_management_fee, total_repair,
        total_deposit, total_misc, net_to_landlord, agency_earnings,
        statement_period_days
    )
    SELECT 
        YEAR(s.statement_date) * 10000 + MONTH(s.statement_date) * 100 + DAY(s.statement_date) as date_key,
        dp.property_key,
        dl.landlord_key,
        dt.tenant_key,
        da.agent_key,
        s.statement_id,
        COALESCE(SUM(CASE WHEN ct.charge_type_name = 'rent' THEN sc.amount ELSE 0 END), 0) as total_rent,
        COALESCE(SUM(CASE WHEN ct.charge_type_name = 'management_fee' THEN sc.amount ELSE 0 END), 0) as total_management_fee,
        COALESCE(SUM(CASE WHEN ct.charge_type_name = 'repair' THEN sc.amount ELSE 0 END), 0) as total_repair,
        COALESCE(SUM(CASE WHEN ct.charge_type_name = 'deposit' THEN sc.amount ELSE 0 END), 0) as total_deposit,
        COALESCE(SUM(CASE WHEN ct.charge_type_name = 'misc' THEN sc.amount ELSE 0 END), 0) as total_misc,
        s.total_amount as net_to_landlord,
        COALESCE(SUM(CASE WHEN ct.charge_type_name = 'management_fee' THEN sc.amount ELSE 0 END), 0) as agency_earnings,
        DATEDIFF(s.period_end, s.period_start) as statement_period_days
    FROM statement s
    LEFT JOIN statement_charge sc ON s.statement_id = sc.statement_id
    LEFT JOIN charge_type ct ON sc.charge_type_id = ct.charge_type_id
    JOIN property p ON s.property_id = p.property_id
    JOIN dim_property dp ON p.property_id = dp.property_id AND dp.is_current = TRUE
    JOIN landlord l ON p.landlord_id = l.landlord_id
    JOIN dim_landlord dl ON l.landlord_id = dl.landlord_id AND dl.is_current = TRUE
    LEFT JOIN lease le ON s.lease_id = le.lease_id
    LEFT JOIN tenant t ON le.tenant_id = t.tenant_id
    LEFT JOIN dim_tenant dt ON t.tenant_id = dt.tenant_id AND dt.is_current = TRUE
    LEFT JOIN property_assignment pa ON p.property_id = pa.property_id AND pa.is_current = TRUE
    LEFT JOIN dim_agent da ON pa.agent_id = da.agent_id AND da.is_current = TRUE
    WHERE NOT EXISTS (
        SELECT 1 FROM fact_statement_summary f WHERE f.statement_id = s.statement_id
    )
    GROUP BY s.statement_id, dp.property_key, dl.landlord_key, dt.tenant_key, 
             da.agent_key, s.statement_date, s.total_amount, s.period_start, s.period_end;
END //

-- Master ETL procedure to run all loads
CREATE PROCEDURE etl_run_full_load()
BEGIN
    -- Load dimensions
    CALL etl_load_dim_landlord();
    CALL etl_load_dim_property();
    CALL etl_load_dim_tenant();
    CALL etl_load_dim_agent();
    
    -- Load facts
    CALL etl_load_fact_statement_charge();
    CALL etl_load_fact_statement_summary();
    
    SELECT 'ETL Full Load Complete' as status;
END //

DELIMITER ;


-- ============================================================================
-- PART E: ANALYTICAL QUERIES (Answer Your Business Questions)
-- ============================================================================

-- Q1: Rent and profit per property per year
-- How much rent and profit did each property generate over the past years?
CREATE VIEW vw_property_annual_performance AS
SELECT 
    dp.property_alias,
    dp.landlord_name,
    dp.property_city,
    dd.year,
    SUM(fs.total_rent) as total_rent,
    SUM(fs.total_management_fee) as total_fees,
    SUM(fs.total_repair) as total_repairs,
    SUM(fs.net_to_landlord) as net_profit,
    COUNT(DISTINCT fs.statement_id) as statement_count
FROM fact_statement_summary fs
JOIN dim_property dp ON fs.property_key = dp.property_key
JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY dp.property_alias, dp.landlord_name, dp.property_city, dd.year
ORDER BY dd.year DESC, net_profit DESC;

-- Q2: Total management fees (agency earnings)
-- What was the total management fee collected by the agency?
CREATE VIEW vw_agency_earnings AS
SELECT 
    dd.year,
    dd.quarter,
    dd.month_name,
    SUM(fs.agency_earnings) as total_management_fees,
    COUNT(DISTINCT fs.statement_id) as statements_processed,
    SUM(fs.total_rent) as total_rent_collected
FROM fact_statement_summary fs
JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY dd.year, dd.quarter, dd.month_number, dd.month_name
ORDER BY dd.year, dd.quarter, dd.month_number;

-- Q3: Properties with high repair expenses (anomaly detection)
-- Which properties had unusually high repair expenses?
CREATE VIEW vw_repair_analysis AS
SELECT 
    dp.property_alias,
    dp.landlord_name,
    dp.property_city,
    dd.year,
    SUM(fs.total_repair) as total_repairs,
    SUM(fs.total_rent) as total_rent,
    ROUND(SUM(fs.total_repair) / NULLIF(SUM(fs.total_rent), 0) * 100, 2) as repair_to_rent_ratio,
    COUNT(DISTINCT CASE WHEN fs.total_repair > 0 THEN fs.statement_id END) as repair_incidents
FROM fact_statement_summary fs
JOIN dim_property dp ON fs.property_key = dp.property_key
JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY dp.property_alias, dp.landlord_name, dp.property_city, dd.year
HAVING SUM(fs.total_repair) > 0
ORDER BY total_repairs DESC;

-- Q4: Monthly revenue trends
CREATE VIEW vw_monthly_trends AS
SELECT 
    dd.year,
    dd.month_number,
    dd.month_name,
    SUM(fs.total_rent) as total_rent,
    SUM(fs.total_management_fee) as total_fees,
    SUM(fs.net_to_landlord) as net_to_landlords,
    COUNT(DISTINCT dp.property_key) as active_properties
FROM fact_statement_summary fs
JOIN dim_date dd ON fs.date_key = dd.date_key
JOIN dim_property dp ON fs.property_key = dp.property_key
GROUP BY dd.year, dd.month_number, dd.month_name
ORDER BY dd.year, dd.month_number;

-- Q5: Landlord performance summary
CREATE VIEW vw_landlord_performance AS
SELECT 
    dl.landlord_name,
    dl.landlord_reg_number,
    dl.property_count,
    COUNT(DISTINCT fs.statement_id) as total_statements,
    SUM(fs.total_rent) as total_rent_collected,
    SUM(fs.net_to_landlord) as total_profit,
    SUM(fs.total_repair) as total_repairs,
    ROUND(AVG(fs.net_to_landlord), 2) as avg_monthly_profit
FROM fact_statement_summary fs
JOIN dim_landlord dl ON fs.landlord_key = dl.landlord_key
GROUP BY dl.landlord_key, dl.landlord_name, dl.landlord_reg_number, dl.property_count
ORDER BY total_profit DESC;

-- Q6: Fee analysis by property
-- How do fees vary by property or landlord?
CREATE VIEW vw_fee_analysis AS
SELECT 
    dp.property_alias,
    dl.landlord_name,
    dd.year,
    SUM(fs.total_rent) as total_rent,
    SUM(fs.total_management_fee) as total_fees,
    ROUND(SUM(fs.total_management_fee) / NULLIF(SUM(fs.total_rent), 0) * 100, 2) as fee_percentage
FROM fact_statement_summary fs
JOIN dim_property dp ON fs.property_key = dp.property_key
JOIN dim_landlord dl ON fs.landlord_key = dl.landlord_key
JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY dp.property_alias, dl.landlord_name, dd.year
ORDER BY dd.year, fee_percentage DESC;

-- Q7: Year-over-year comparison
CREATE VIEW vw_yoy_comparison AS
SELECT 
    curr.year as current_year,
    curr.total_rent as current_rent,
    prev.total_rent as previous_rent,
    ROUND((curr.total_rent - COALESCE(prev.total_rent, 0)) / NULLIF(prev.total_rent, 0) * 100, 2) as rent_growth_pct,
    curr.total_fees as current_fees,
    prev.total_fees as previous_fees,
    curr.net_profit as current_profit,
    prev.net_profit as previous_profit
FROM (
    SELECT 
        dd.year,
        SUM(fs.total_rent) as total_rent,
        SUM(fs.total_management_fee) as total_fees,
        SUM(fs.net_to_landlord) as net_profit
    FROM fact_statement_summary fs
    JOIN dim_date dd ON fs.date_key = dd.date_key
    GROUP BY dd.year
) curr
LEFT JOIN (
    SELECT 
        dd.year,
        SUM(fs.total_rent) as total_rent,
        SUM(fs.total_management_fee) as total_fees,
        SUM(fs.net_to_landlord) as net_profit
    FROM fact_statement_summary fs
    JOIN dim_date dd ON fs.date_key = dd.date_key
    GROUP BY dd.year
) prev ON curr.year = prev.year + 1
ORDER BY curr.year;

-- Q8: Seasonal maintenance patterns
CREATE VIEW vw_seasonal_maintenance AS
SELECT 
    dd.month_name,
    dd.month_number,
    AVG(fs.total_repair) as avg_repair_cost,
    SUM(fs.total_repair) as total_repair_cost,
    COUNT(CASE WHEN fs.total_repair > 0 THEN 1 END) as repair_count
FROM fact_statement_summary fs
JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY dd.month_name, dd.month_number
ORDER BY dd.month_number;


-- ============================================================================
-- PART G: VERIFICATION QUERIES
-- ============================================================================

-- Check loaded data
SELECT 'Landlords' as entity, COUNT(*) as count FROM landlord
UNION ALL SELECT 'Properties', COUNT(*) FROM property
UNION ALL SELECT 'Tenants', COUNT(*) FROM tenant
UNION ALL SELECT 'Agents', COUNT(*) FROM agency_agent
UNION ALL SELECT 'Contractors', COUNT(*) FROM contractor
UNION ALL SELECT 'Statements', COUNT(*) FROM statement
UNION ALL SELECT 'Statement Charges', COUNT(*) FROM statement_charge
UNION ALL SELECT 'Dim Date Records', COUNT(*) FROM dim_date
UNION ALL SELECT 'Dim Property', COUNT(*) FROM dim_property
UNION ALL SELECT 'Dim Landlord', COUNT(*) FROM dim_landlord
UNION ALL SELECT 'Fact Statement Charge', COUNT(*) FROM fact_statement_charge
UNION ALL SELECT 'Fact Statement Summary', COUNT(*) FROM fact_statement_summary;

-- Test analytical views
SELECT '=== Property Annual Performance ===' as report;
SELECT * FROM vw_property_annual_performance LIMIT 10;

SELECT '=== Agency Earnings ===' as report;
SELECT * FROM vw_agency_earnings LIMIT 10;

SELECT '=== Landlord Performance ===' as report;
SELECT * FROM vw_landlord_performance;


-- ============================================================================
-- END OF SCRIPT
-- ============================================================================
-- 
-- NEXT STEPS:
-- 1. Load your full dataset from the spreadsheet using the ETL procedures
-- 2. Schedule regular ETL runs to keep the warehouse updated
-- 3. Create additional views/reports as needed
-- 4. Consider adding more dimensions (e.g., dim_contractor, dim_location)
-- 5. Implement data quality checks and auditing
-- ============================================================================
