-- ============================================================================
-- 3: create data warehouse and analysis(star schema)
-- ============================================================================
-- Execution  Step 3
-- Purpose: Create star schema (dimensions + facts) and load data for analytics
-- ============================================================================

use landlord_dw;

-- ============================================================================
-- PART A: CREATE DIMENSION TABLES
-- ============================================================================

-- Date Dimension - For time-based analysis
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,                    -- YYYYMMDD format
    full_date DATE NOT NULL UNIQUE,
    day_of_week TINYINT,                         -- 1-7
    day_name VARCHAR(10),                        -- Monday, Tuesday...
    day_of_month TINYINT,                        -- 1-31
    day_of_year SMALLINT,                        -- 1-366
    week_of_year TINYINT,                        -- 1-53
    month_number TINYINT,                        -- 1-12
    month_name VARCHAR(10),                      -- January, February...
    quarter TINYINT,                             -- 1-4
    year SMALLINT,                               -- 2020, 2021...
    is_weekend BOOLEAN,
    is_month_end BOOLEAN,
    INDEX idx_date_year_month (year, month_number),
    INDEX idx_date_quarter (year, quarter)
);

-- Property Dimension 
CREATE TABLE dim_property (
    property_key INT AUTO_INCREMENT PRIMARY KEY,
    property_id INT NOT NULL,                    -- Natural key from ODS
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
    -- SCD Type 2 fields
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
    landlord_city VARCHAR(100),
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



-- ============================================================================
-- PART B: CREATE FACT TABLES
-- ============================================================================

-- Fact Table: Statement Charges (grain = one charge line item)
CREATE TABLE fact_statement_charge (
    fact_id INT AUTO_INCREMENT PRIMARY KEY,
    -- Dimension foreign keys
    date_key INT NOT NULL,
    property_key INT NOT NULL,
    landlord_key INT NOT NULL,
    tenant_key INT,
    agent_key INT,
    charge_type_key INT NOT NULL,
    -- Degenerate dimension
    statement_id INT NOT NULL,
    -- Measures
    charge_amount DECIMAL(10,2),
    is_income BOOLEAN,
    statement_period_days INT,
    -- Foreign key constraints
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (property_key) REFERENCES dim_property(property_key),
    FOREIGN KEY (landlord_key) REFERENCES dim_landlord(landlord_key),
    FOREIGN KEY (tenant_key) REFERENCES dim_tenant(tenant_key),
    FOREIGN KEY (agent_key) REFERENCES dim_agent(agent_key),
    FOREIGN KEY (charge_type_key) REFERENCES dim_charge_type(charge_type_key),
    -- Indexes
    INDEX idx_fact_date (date_key),
    INDEX idx_fact_property (property_key),
    INDEX idx_fact_landlord (landlord_key),
    INDEX idx_fact_charge_type (charge_type_key),
    INDEX idx_fact_statement (statement_id)
);


-- Fact Table: Statement Summary (grain = one statement)
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
    -- Foreign keys and indexes
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (property_key) REFERENCES dim_property(property_key),
    FOREIGN KEY (landlord_key) REFERENCES dim_landlord(landlord_key),
    INDEX idx_fact_summary_date (date_key),
    INDEX idx_fact_summary_property (property_key),
    INDEX idx_fact_summary_landlord (landlord_key)
);


-- ============================================================================
-- PART C: POPULATE DATE DIMENSION (2020-2030)
-- ============================================================================

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

-- Execute population
CALL populate_date_dimension();


-- ============================================================================
-- PART D: ETL STORED PROCEDURES - LOAD DIMENSIONS
-- ============================================================================

DELIMITER //

-- Load Charge Type Dimension
CREATE PROCEDURE etl_load_dim_charge_type()
BEGIN
    INSERT INTO dim_charge_type (charge_type_id, charge_type_name, charge_category, is_income)
    SELECT charge_type_id, charge_type_name, charge_category, is_income
    FROM charge_type
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_charge_type dc WHERE dc.charge_type_id = charge_type.charge_type_id
    );
END //

-- Load Landlord Dimension
CREATE PROCEDURE etl_load_dim_landlord()
BEGIN
    INSERT INTO dim_landlord (
        landlord_id, landlord_reg_number, landlord_name, 
        landlord_email, landlord_city, property_count, 
        effective_date, is_current
    )
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
        SELECT 1 FROM dim_landlord dl 
        WHERE dl.landlord_id = l.landlord_id AND dl.is_current = TRUE
    );
END //

-- Load Property Dimension
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
        SELECT 1 FROM dim_property dp 
        WHERE dp.property_id = p.property_id AND dp.is_current = TRUE
    );
END //

-- Load Tenant Dimension
CREATE PROCEDURE etl_load_dim_tenant()
BEGIN
    INSERT INTO dim_tenant (
        tenant_id, tenant_full_name, tenant_email, 
        effective_date, is_current
    )
    SELECT 
        t.tenant_id,
        CONCAT(t.tenant_first_name, ' ', t.tenant_last_name),
        t.tenant_email,
        CURRENT_DATE,
        TRUE
    FROM tenant t
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_tenant dt 
        WHERE dt.tenant_id = t.tenant_id AND dt.is_current = TRUE
    );
END //

-- Load Agent Dimension
CREATE PROCEDURE etl_load_dim_agent()
BEGIN
    INSERT INTO dim_agent (
        agent_id, agent_name, agent_role, agent_email, is_current
    )
    SELECT agent_id, agent_name, agent_role, agent_email, TRUE
    FROM agency_agent
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_agent da 
        WHERE da.agent_id = agency_agent.agent_id AND da.is_current = TRUE
    );
END //

DELIMITER ;


-- ============================================================================
-- PART E: ETL STORED PROCEDURES - LOAD FACT TABLES
-- ============================================================================

DELIMITER //

-- Load Statement Charge Fact Table
CREATE PROCEDURE etl_load_fact_statement_charge()
BEGIN
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

-- Load Statement Summary Fact Table
CREATE PROCEDURE etl_load_fact_statement_summary()
BEGIN
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

-- Master ETL Procedure - Run all loads
CREATE PROCEDURE etl_run_full_load()
BEGIN
    -- Load dimensions first
    CALL etl_load_dim_charge_type();
    CALL etl_load_dim_landlord();
    CALL etl_load_dim_property();
    CALL etl_load_dim_tenant();
    CALL etl_load_dim_agent();
    
    -- Load facts
    CALL etl_load_fact_statement_charge();
    CALL etl_load_fact_statement_summary();
    
    SELECT 'ETL Complete: Data warehouse loaded successfully' as status;
END //

DELIMITER ;



-- ============================================================================
-- PART F: EXECUTE ETL TO LOAD DATA WAREHOUSE
-- ============================================================================

CALL etl_run_full_load();


-- ============================================================================
-- PART G: CREATE ANALYTICAL VIEWS
-- ============================================================================

-- Q1: Property annual performance - rent and profit per property per year
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

-- Q2: Agency earnings - total management fees collected
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

-- Q3: Repair analysis - properties with high repair expenses
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

SELECT '03-G: Analytical views created' AS status;


-- ============================================================================
-- PART H: VERIFICATION QUERIES
-- ============================================================================

SELECT '=== Data Warehouse Load Summary ===' AS report;

SELECT 'dim_date' AS table_name, COUNT(*) AS row_count FROM dim_date
UNION ALL SELECT 'dim_landlord', COUNT(*) FROM dim_landlord
UNION ALL SELECT 'dim_property', COUNT(*) FROM dim_property
UNION ALL SELECT 'dim_tenant', COUNT(*) FROM dim_tenant
UNION ALL SELECT 'dim_agent', COUNT(*) FROM dim_agent
UNION ALL SELECT 'dim_charge_type', COUNT(*) FROM dim_charge_type
UNION ALL SELECT 'fact_statement_charge', COUNT(*) FROM fact_statement_charge
UNION ALL SELECT 'fact_statement_summary', COUNT(*) FROM fact_statement_summary;

-- Test analytical views
SELECT '=== Property Annual Performance (Top 5) ===' AS report;
SELECT * FROM vw_property_annual_performance LIMIT 5;

SELECT '=== Agency Earnings ===' AS report;
SELECT * FROM vw_agency_earnings LIMIT 5;

SELECT '=== Landlord Performance ===' AS report;
SELECT * FROM vw_landlord_performance LIMIT 5;

SELECT '03: Data Warehouse creation complete!' AS final_status;

-- ============================================================================
-- COMPLETE! Data warehouse is ready for analytical queries
-- ============================================================================
-- 
-- Available Analytical Views:
--   - vw_property_annual_performance  Property yearly performance
--   - vw_agency_earnings              Agency management fee income
--   - vw_repair_analysis              Repair cost analysis
--   - vw_monthly_trends               Monthly revenue trends
--   - vw_landlord_performance         Landlord performance summary
--   - vw_fee_analysis                 Fee analysis by property
--   - vw_yoy_comparison               Year-over-year comparison
--   - vw_seasonal_maintenance         Seasonal maintenance patterns
--
-- ============================================================================
