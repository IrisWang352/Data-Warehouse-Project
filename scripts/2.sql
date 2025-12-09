-- 2: ETL (staging table to ODS)
-- Purpose: Create ODS tables (3NF normalization) + ETL from staging

use landlord_dw;
-- PART A: create ODS tables (3NF Normalized)

-- ENTITY TABLES:

-- Landlord 
create table landlord (
    landlord_id int auto_increment primary key,
    landlord_reg_number varchar(50) not null unique,
    landlord_name varchar(100) not null,
    landlord_email varchar(100),
    landlord_phone varchar(20),
    landlord_address varchar(255),
    created_date date default (current_date),
    is_active boolean default true,
    index idx_landlord_reg (landlord_reg_number)
);

-- Bank Account 
create table bank_account (
    bank_account_id int auto_increment primary key,
    landlord_id int not null,
    bank_account_name varchar(100),
    bank_name varchar(100),
    bank_account_number varchar(20),
    bank_sort_code varchar(20),
    is_primary boolean default true,
    foreign key (landlord_id) references landlord(landlord_id),
    index idx_bank_landlord (landlord_id));
    
-- Property Type 
create table property_type (
    property_type_id int auto_increment primary key,
    property_type_name varchar(50) not null unique);
    
-- Property 
create table property (
    property_id int auto_increment primary key,
    landlord_id int not null,
    property_type_id int,
    property_alias varchar(100),
    property_address varchar(255),
    property_city varchar(100),
    property_postcode varchar(20),
    property_bedrooms int,
    is_active boolean default true,
    foreign key (landlord_id) references landlord(landlord_id),
    foreign key (property_type_id) references property_type(property_type_id),
    index idx_property_landlord (landlord_id),
    index idx_property_postcode (property_postcode)
);

-- Tenant 
create table tenant (
    tenant_id int auto_increment primary key,
    tenant_first_name varchar(50),
    tenant_last_name varchar(50),
    tenant_email varchar(100),
    tenant_phone varchar(20),
    index idx_tenant_name (tenant_last_name, tenant_first_name));
-- Agency Agent 
create table agency_agent (
    agent_id int auto_increment primary key,
    agent_name varchar(100) not null,
    agent_role varchar(50),
    agent_email varchar(100),
    is_active boolean default true,
    index idx_agent_name (agent_name));
    
-- Contractor (after fix)
CREATE TABLE service_type (
    service_type_id   INT AUTO_INCREMENT PRIMARY KEY,
    service_type_name VARCHAR(50) NOT NULL UNIQUE
);
CREATE TABLE contractor (
    contractor_id        INT AUTO_INCREMENT PRIMARY KEY,
    contractor_company   VARCHAR(100),
    contractor_contact   VARCHAR(100),
    contractor_phone     VARCHAR(20)
);
CREATE TABLE contractor_service (
    contractor_id    INT,
    service_type_id  INT,
    PRIMARY KEY (contractor_id, service_type_id),
    FOREIGN KEY (contractor_id)   REFERENCES contractor(contractor_id),
    FOREIGN KEY (service_type_id) REFERENCES service_type(service_type_id)
);


-- Charge Type 
create table charge_type (
    charge_type_id int auto_increment primary key,
    charge_type_name varchar(50) not null unique,
    charge_category varchar(50),
    is_income boolean default false);

-- TRANSACTION TABLES
-- Lease Table (property and tenant)
create table lease (
    lease_id int auto_increment primary key,
    property_id int not null,
    tenant_id int not null,
    lease_start_date date,
    lease_end_date date,
    monthly_rent decimal(10,2),
    deposit_amount decimal(10,2),
    is_active boolean default true,
    foreign key (property_id) references property(property_id),
    foreign key (tenant_id) references tenant(tenant_id),
    index idx_lease_property (property_id),
    index idx_lease_dates (lease_start_date, lease_end_date)
);

-- Property Assignment Table (property and agent)
create table property_assignment (
    assignment_id int auto_increment primary key,
    property_id int not null,
    agent_id int not null,
    assignment_start_date date,
    assignment_end_date date,
    is_current boolean default true,
    foreign key (property_id) references property(property_id),
    foreign key (agent_id) references agency_agent(agent_id),
    index idx_assignment_property (property_id),
    index idx_assignment_agent (agent_id)
);

-- Statement Table - Monthly landlord statements
create table statement (
    statement_id int primary key,
    property_id int not null,
    lease_id int,
    statement_date date,
    period_start date,
    period_end date,
    total_amount decimal(10,2),
    pay_date date,
    note text,
    foreign key (property_id) references property(property_id),
    foreign key (lease_id) references lease(lease_id),
    index idx_statement_property (property_id),
    index idx_statement_date (statement_date),
    index idx_statement_period (period_start, period_end)
);

-- Statement Charge Table 
create table statement_charge (
   charge_id int auto_increment primary key,
   statement_id int not null,
   charge_type_id int not null,
   contractor_id int,
   amount decimal(10,2),
   description text,
   foreign key (statement_id) references statement(statement_id),
   foreign key (charge_type_id) references charge_type(charge_type_id),
   foreign key (contractor_id) references contractor(contractor_id),
   index idx_charge_statement (statement_id),
   index idx_charge_type (charge_type_id)
);


-- PART B: INSERT REFERENCE DATA

-- Charge types
insert into charge_type (charge_type_name, charge_category, is_income) values
('rent', 'income', true),('management_fee', 'expense', false),
('repair', 'expense', false),('deposit', 'deposit', false),('misc', 'expense', false);

select *
from charge_type;

-- PART C: ETL - LOAD DATA FROM STAGING TO ODS

-- Load Landlords
insert into landlord (landlord_reg_number, landlord_name, landlord_email, landlord_phone, landlord_address)
select distinct landlord_reg_number, landlord_name, landlord_email, landlord_phone, landlord_address from statements_raw
where landlord_reg_number is not null;

select *
from landlord;

-- Load Property Types
insert into property_type (property_type_name)
select distinct property_type from statements_raw
where property_type is not null
on duplicate key update property_type_name = values(property_type_name);

select *
from property_type;

--  Load Properties
insert into property (landlord_id, property_type_id, property_alias,property_address, property_city, property_postcode, property_bedrooms)
select distinct
    l.landlord_id,
    pt.property_type_id,
    sr.property_alias,
    sr.property_address,
    sr.property_city,
    sr.property_postcode,
    sr.property_bedrooms
from statements_raw sr
join landlord l on sr.landlord_reg_number = l.landlord_reg_number
left join property_type pt on sr.property_type = pt.property_type_name
where sr.property_alias is not null;

select *
from property;

--  Load Tenants
insert into tenant (tenant_first_name, tenant_last_name, tenant_email, tenant_phone)
select distinct tenant_first_name, tenant_last_name, tenant_email, tenant_phone
from statements_raw
where tenant_email is not null;

select *
from tenant;

--  Load Agency Agents
insert into agency_agent (agent_name, agent_role, agent_email)
select distinct assigned_agent_name, assigned_agent_role, assigned_agent_email
from statements_raw
where assigned_agent_email is not null;

select * from agency_agent;

--  Load Contractors(after fix)
INSERT INTO service_type (service_type_name)
SELECT DISTINCT contractor_service_type FROM statements_raw
WHERE contractor_service_type IS NOT NULL
ON DUPLICATE KEY UPDATE service_type_name = VALUES(service_type_name);

SELECT * FROM service_type;

INSERT INTO contractor (contractor_company, contractor_contact, contractor_phone)
SELECT DISTINCT
    contractor_company,contractor_contact,contractor_phone
FROM statements_raw
WHERE contractor_company IS NOT NULL;

select * from contractor;

INSERT IGNORE INTO contractor_service (contractor_id, service_type_id)
SELECT DISTINCT c.contractor_id,st.service_type_id
FROM statements_raw sr
JOIN contractor c ON sr.contractor_company = c.contractor_company
JOIN service_type st ON sr.contractor_service_type = st.service_type_name
WHERE sr.contractor_company IS NOT NULL
  AND sr.contractor_service_type IS NOT NULL;

SELECT * FROM contractor_service;


--  Load Bank Accounts
insert into bank_account (landlord_id, bank_account_name, bank_name, bank_account_number, bank_sort_code)
select distinct
    l.landlord_id,
    sr.bank_account_name,
    sr.bank_name,
    cast(sr.bank_account_number as char(20)),
    sr.bank_sort_code
from statements_raw sr
join landlord l on sr.landlord_reg_number = l.landlord_reg_number
where sr.bank_account_number is not null;
select * from bank_account;

--  Load Leases
insert into lease (property_id, tenant_id, lease_start_date, lease_end_date, monthly_rent, deposit_amount)
select distinct
    p.property_id,
    t.tenant_id,
    case when sr.lease_start_date < '1900-01-01' then null else sr.lease_start_date end,
    case when sr.lease_end_date   < '1900-01-01' then null else sr.lease_end_date end,
    sr.lease_monthly_rent,
    sr.lease_deposit_amount
from statements_raw sr
join property p on sr.property_alias = p.property_alias
join tenant t on sr.tenant_email = t.tenant_email;

select * from  lease;

-- Load Property Assignments
insert into property_assignment (property_id, agent_id, assignment_start_date, is_current)
select distinct
    p.property_id,
    a.agent_id,
    current_date,
    true
from statements_raw sr
join property p on sr.property_alias = p.property_alias
join agency_agent a on sr.assigned_agent_email = a.agent_email;

select * from property_assignment;


-- Load Statements
insert into statement (
    statement_id, property_id, lease_id,
    statement_date, period_start, period_end,
    total_amount, pay_date, note
)
select
    sr.statement_id,
    p.property_id,
    le.lease_id,
    case when sr.statement_date < '1900-01-01' then null else sr.statement_date end,
    case when sr.period_start < '1900-01-01' then null else sr.period_start end,
    case when sr.period_end   < '1900-01-01' then null else sr.period_end end,
    sr.total,
    case when sr.pay_date < '1900-01-01' then null else sr.pay_date end,
    sr.note
from statements_raw sr
join property p on sr.property_alias = p.property_alias
left join tenant t on sr.tenant_email = t.tenant_email
left join lease le on le.property_id = p.property_id and le.tenant_id = t.tenant_id;

select * from statement;

--  Load Statement Charges (columns to rows)

-- Rent charges
insert into statement_charge (statement_id, charge_type_id, amount, description)
select
    statement_id,
    (select charge_type_id from charge_type where charge_type_name = 'rent'),
    rent,'Monthly rent'
from statements_raw
where rent is not null and rent != 0;


-- Management fee charges
insert into statement_charge (statement_id, charge_type_id, amount, description)
select
    statement_id,
    (select charge_type_id from charge_type where charge_type_name = 'management_fee'),
    management_fee,
    'Agency management fee'
from statements_raw
where management_fee is not null and management_fee != 0;

-- Repair charges (linked to contractor)
INSERT INTO statement_charge (statement_id, charge_type_id, contractor_id, amount, description)
SELECT
    sr.statement_id,
    (SELECT charge_type_id FROM charge_type WHERE charge_type_name = 'repair'),
    c.contractor_id,
    sr.repair,
    CONCAT('Repair - ', COALESCE(st.service_type_name, 'General'))
FROM statements_raw sr
LEFT JOIN contractor c
    ON sr.contractor_company = c.contractor_company
LEFT JOIN service_type st
    ON sr.contractor_service_type = st.service_type_name
WHERE sr.repair IS NOT NULL AND sr.repair != 0;


-- Deposit charges
insert into statement_charge (statement_id, charge_type_id, amount, description)
select
    statement_id,
    (select charge_type_id from charge_type where charge_type_name = 'deposit'),
    deposit,
    'Deposit Transaction'
from statements_raw
where deposit is not null and deposit!= 0;

-- Miscellaneous charges
insert into statement_charge (statement_id, charge_type_id, amount, description)
select
    statement_id,
    (select charge_type_id from charge_type where charge_type_name = 'misc'),
    misc,
    'Miscellaneous Charge'
from statements_raw
where misc is not null and misc!= 0;

select * from statement_charge;

#first not work then need to add this:
-- Fix invalid dates (ensure within date dimension range 2020-2030)
update statement
set statement_date = coalesce(
    case when statement_date between '2020-01-01' and '2030-12-31' then statement_date end,
    case when period_end between '2020-01-01' and '2030-12-31' then period_end end,
    case when period_start between '2020-01-01' and '2030-12-31' then period_start end,
    case when pay_date between '2020-01-01' and '2030-12-31' then pay_date end,
    '2020-01-01'
)
where statement_date is null
   or statement_date not between '2020-01-01' and '2030-12-31';

