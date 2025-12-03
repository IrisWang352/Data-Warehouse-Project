USE landlord_dw;
SET FOREIGN_KEY_CHECKS = 0;

TRUNCATE TABLE fact_statement_charge;
TRUNCATE TABLE fact_statement_summary;

TRUNCATE TABLE statement_charge;
TRUNCATE TABLE statement;
TRUNCATE TABLE lease;
TRUNCATE TABLE property_assignment;
TRUNCATE TABLE bank_account;

TRUNCATE TABLE contractor;
TRUNCATE TABLE agency_agent;
TRUNCATE TABLE tenant;
TRUNCATE TABLE property;
TRUNCATE TABLE landlord;

SET FOREIGN_KEY_CHECKS = 1;

-- landlord
INSERT INTO landlord (landlord_reg_number, landlord_name, landlord_email, landlord_phone, landlord_address)
SELECT DISTINCT
  landlord_reg_number,
  landlord_name,
  landlord_email,
  landlord_phone,
  landlord_address
FROM statements_raw
WHERE landlord_reg_number IS NOT NULL;

-- property type
INSERT IGNORE INTO property_type (property_type_name)
SELECT DISTINCT property_type
FROM statements_raw
WHERE property_type IS NOT NULL;

-- property
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
JOIN landlord l
  ON l.landlord_reg_number = sr.landlord_reg_number
LEFT JOIN property_type pt
  ON pt.property_type_name = sr.property_type;
  
 -- tenant
INSERT INTO tenant (tenant_first_name, tenant_last_name, tenant_email, tenant_phone)
SELECT DISTINCT
  tenant_first_name,
  tenant_last_name,
  tenant_email,
  tenant_phone
FROM statements_raw
WHERE tenant_email IS NOT NULL;

-- agency_agent
INSERT INTO agency_agent (agent_name, agent_role, agent_email)
SELECT DISTINCT
  assigned_agent_name,
  assigned_agent_role,
  assigned_agent_email
FROM statements_raw
WHERE assigned_agent_email IS NOT NULL;

-- contractor
INSERT INTO contractor (contractor_company, contractor_contact, contractor_phone, contractor_service_type)
SELECT DISTINCT
  contractor_company,
  contractor_contact,
  contractor_phone,
  contractor_service_type
FROM statements_raw
WHERE contractor_company IS NOT NULL;

-- bank account
INSERT INTO bank_account (landlord_id, bank_account_name, bank_name, bank_account_number, bank_sort_code)
SELECT DISTINCT
  l.landlord_id,
  sr.bank_account_name,
  sr.bank_name,
  sr.bank_account_number,
  sr.bank_sort_code
FROM statements_raw sr
JOIN landlord l
  ON l.landlord_reg_number = sr.landlord_reg_number
WHERE sr.bank_account_number IS NOT NULL;

-- lease
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

  -- 处理 lease_start_date：'' 或 '0000-00-00' 都当成 NULL，其它转成 DATE
  CASE
    WHEN CAST(sr.lease_start_date AS CHAR) IN ('', '0000-00-00') THEN NULL
    ELSE STR_TO_DATE(CAST(sr.lease_start_date AS CHAR), '%Y-%m-%d')
  END AS lease_start_date,

  -- 处理 lease_end_date：'' 或 '0000-00-00' 都当成 NULL，其它转成 DATE
  CASE
    WHEN CAST(sr.lease_end_date AS CHAR) IN ('', '0000-00-00') THEN NULL
    ELSE STR_TO_DATE(CAST(sr.lease_end_date AS CHAR), '%Y-%m-%d')
  END AS lease_end_date,

  sr.lease_monthly_rent,
  sr.lease_deposit_amount
FROM statements_raw sr
JOIN property p
  ON p.property_alias = sr.property_alias
JOIN tenant t
  ON t.tenant_email = sr.tenant_email;
SELECT COUNT(*) FROM lease;
SELECT lease_start_date, lease_end_date
FROM lease
LIMIT 10;


  -- property_assignment
INSERT INTO property_assignment (
  property_id,
  agent_id,
  assignment_start_date,
  is_current
)
SELECT DISTINCT
  p.property_id,
  a.agent_id,
  COALESCE(sr.lease_start_date, sr.statement_date, '2020-01-01') AS assignment_start_date,
  TRUE
FROM statements_raw sr
JOIN property p
  ON p.property_alias = sr.property_alias
JOIN agency_agent a
  ON a.agent_email = sr.assigned_agent_email;
  
  -- statement
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
SELECT DISTINCT
  sr.statement_id,
  p.property_id,
  le.lease_id,

  -- statement_date
  CASE
    WHEN CAST(sr.statement_date AS CHAR) IN ('', '0000-00-00') THEN NULL
    ELSE STR_TO_DATE(CAST(sr.statement_date AS CHAR), '%Y-%m-%d')
  END AS statement_date,

  -- period_start
  CASE
    WHEN CAST(sr.period_start AS CHAR) IN ('', '0000-00-00') THEN NULL
    ELSE STR_TO_DATE(CAST(sr.period_start AS CHAR), '%Y-%m-%d')
  END AS period_start,

  -- period_end
  CASE
    WHEN CAST(sr.period_end AS CHAR) IN ('', '0000-00-00') THEN NULL
    ELSE STR_TO_DATE(CAST(sr.period_end AS CHAR), '%Y-%m-%d')
  END AS period_end,

  sr.total AS total_amount,

  -- pay_date
  CASE
    WHEN CAST(sr.pay_date AS CHAR) IN ('', '0000-00-00') THEN NULL
    ELSE STR_TO_DATE(CAST(sr.pay_date AS CHAR), '%Y-%m-%d')
  END AS pay_date,

  sr.note
FROM statements_raw sr
JOIN property p
  ON p.property_alias = sr.property_alias
LEFT JOIN tenant t
  ON t.tenant_email = sr.tenant_email
LEFT JOIN lease le
  ON le.property_id = p.property_id
 AND le.tenant_id = t.tenant_id
ON DUPLICATE KEY UPDATE
  statement_date = VALUES(statement_date),
  period_start   = VALUES(period_start),
  period_end     = VALUES(period_end),
  total_amount   = VALUES(total_amount),
  pay_date       = VALUES(pay_date),
  note           = VALUES(note);
 
 -- statement_charge
 -- RENT
INSERT INTO statement_charge (statement_id, charge_type_id, contractor_id, amount, description)
SELECT
  sr.statement_id,
  ct.charge_type_id,
  NULL,
  sr.rent,
  'rent'
FROM statements_raw sr
JOIN charge_type ct
  ON ct.charge_type_name = 'rent'
WHERE sr.rent IS NOT NULL AND sr.rent <> 0;

-- MANAGEMENT FEE
INSERT INTO statement_charge (statement_id, charge_type_id, contractor_id, amount, description)
SELECT
  sr.statement_id,
  ct.charge_type_id,
  NULL,
  sr.management_fee,
  'management fee'
FROM statements_raw sr
JOIN charge_type ct
  ON ct.charge_type_name = 'management_fee'
WHERE sr.management_fee IS NOT NULL AND sr.management_fee <> 0;

-- REPAIR（including contractor）
INSERT INTO statement_charge (statement_id, charge_type_id, contractor_id, amount, description)
SELECT
  sr.statement_id,
  ct.charge_type_id,
  c.contractor_id,
  sr.repair,
  sr.contractor_service_type
FROM statements_raw sr
JOIN charge_type ct
  ON ct.charge_type_name = 'repair'
LEFT JOIN contractor c
  ON c.contractor_company = sr.contractor_company
WHERE sr.repair IS NOT NULL AND sr.repair <> 0;

-- DEPOSIT
INSERT INTO statement_charge (statement_id, charge_type_id, contractor_id, amount, description)
SELECT
  sr.statement_id,
  ct.charge_type_id,
  NULL,
  sr.deposit,
  'deposit'
FROM statements_raw sr
JOIN charge_type ct
  ON ct.charge_type_name = 'deposit'
WHERE sr.deposit IS NOT NULL AND sr.deposit <> 0;

-- MISC
INSERT INTO statement_charge (statement_id, charge_type_id, contractor_id, amount, description)
SELECT
  sr.statement_id,
  ct.charge_type_id,
  c.contractor_id,
  sr.misc,
  'misc'
FROM statements_raw sr
JOIN charge_type ct
  ON ct.charge_type_name = 'misc'
LEFT JOIN contractor c
  ON c.contractor_company = sr.contractor_company
WHERE sr.misc IS NOT NULL AND sr.misc <> 0;


USE landlord_dw;

UPDATE statement
SET statement_date = COALESCE(
        statement_date,           -- 原本有就用原本的
        period_end,               -- 没有就用账单结束日
        period_start,             -- 还没有就用账单开始日
        pay_date,                 -- 再没有就用付款日
        '2020-01-01'              -- 最后兜底：给一个仓库里的起始日期
    )
    
WHERE statement_date IS NULL
   OR statement_date NOT BETWEEN '2020-01-01' AND '2030-12-31';
   
   SELECT
  COUNT(*) AS bad_dates
FROM statement
WHERE statement_date IS NULL
   OR statement_date NOT BETWEEN '2020-01-01' AND '2030-12-31';

USE landlord_dw;
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE fact_statement_charge;
TRUNCATE TABLE fact_statement_summary;
SET FOREIGN_KEY_CHECKS = 1;

CALL etl_run_full_load();



-- ============================================================================
-- RUN ETL TO POPULATE DIMENSIONS AND FACTS
-- ============================================================================

CALL etl_run_full_load();

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

SELECT 'Data Load Complete' as status;

SELECT 'Record Counts:' as info;
SELECT 'Landlords' as entity, COUNT(*) as count FROM landlord
UNION ALL SELECT 'Properties', COUNT(*) FROM property
UNION ALL SELECT 'Tenants', COUNT(*) FROM tenant
UNION ALL SELECT 'Agents', COUNT(*) FROM agency_agent
UNION ALL SELECT 'Contractors', COUNT(*) FROM contractor
UNION ALL SELECT 'Statements', COUNT(*) FROM statement
UNION ALL SELECT 'Statement Charges', COUNT(*) FROM statement_charge
UNION ALL SELECT 'Dim Property', COUNT(*) FROM dim_property
UNION ALL SELECT 'Dim Landlord', COUNT(*) FROM dim_landlord
UNION ALL SELECT 'Fact Statement Summary', COUNT(*) FROM fact_statement_summary;

USE landlord_dw;

-- 原始/3NF 层
SELECT COUNT(*) AS landlords       FROM landlord;
SELECT COUNT(*) AS properties      FROM property;
SELECT COUNT(*) AS tenants         FROM tenant;
SELECT COUNT(*) AS agents          FROM agency_agent;
SELECT COUNT(*) AS contractors     FROM contractor;
SELECT COUNT(*) AS leases          FROM lease;
SELECT COUNT(*) AS assignments     FROM property_assignment;
SELECT COUNT(*) AS statements      FROM statement;
SELECT COUNT(*) AS statement_lines FROM statement_charge;

-- DW 层
SELECT COUNT(*) AS dim_property_cnt    FROM dim_property;
SELECT COUNT(*) AS dim_landlord_cnt    FROM dim_landlord;
SELECT COUNT(*) AS dim_tenant_cnt      FROM dim_tenant;
SELECT COUNT(*) AS dim_agent_cnt       FROM dim_agent;
SELECT COUNT(*) AS fact_summary_cnt    FROM fact_statement_summary;
SELECT COUNT(*) AS fact_charge_cnt     FROM fact_statement_charge;



  


 



