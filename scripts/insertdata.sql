USE landlord_dw;
SHOW TABLES;

DROP TABLE IF EXISTS statements_raw;
CREATE TABLE statements_raw (
  statement_id           INT,
  statement_date         DATE,
  period_start           DATE,
  period_end             DATE,
  landlord_reg_number    VARCHAR(50),
  landlord_name          VARCHAR(255),
  landlord_email         VARCHAR(255),
  landlord_phone         VARCHAR(50),
  landlord_address       VARCHAR(255),
  bank_account_name      VARCHAR(255),
  bank_name              VARCHAR(255),
  bank_account_number    BIGINT,
  bank_sort_code         VARCHAR(20),
  property_alias         VARCHAR(255),
  property_type          VARCHAR(50),
  property_address       VARCHAR(255),
  property_city          VARCHAR(100),
  property_postcode      VARCHAR(20),
  property_bedrooms      INT,
  tenant_first_name      VARCHAR(100),
  tenant_last_name       VARCHAR(100),
  tenant_email           VARCHAR(255),
  tenant_phone           VARCHAR(50),
  lease_start_date       DATE,
  lease_end_date         DATE,
  lease_monthly_rent     DECIMAL(10,2),
  lease_deposit_amount   DECIMAL(10,2),
  assigned_agent_name    VARCHAR(255),
  assigned_agent_role    VARCHAR(100),
  assigned_agent_email   VARCHAR(255),
  rent                   DECIMAL(10,2),
  management_fee         DECIMAL(10,2),
  repair                 DECIMAL(10,2),
  deposit                DECIMAL(10,2),
  misc                   DECIMAL(10,2),
  total                  DECIMAL(10,2),
  contractor_company     VARCHAR(255),
  contractor_contact     VARCHAR(255),
  contractor_phone       VARCHAR(50),
  contractor_service_type VARCHAR(100),
  note                   TEXT,
  pay_date               DATE
);

SELECT COUNT(*) FROM statements_raw;


