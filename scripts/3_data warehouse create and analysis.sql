-- 3: create data warehouse and analysis(star schema)
-- Purpose: Create star schema (dimensions + facts) and load data for analytics

use landlord_dw;

-- part a. dimension tables

-- date dimension
create table dim_date (
    date_key        int primary key,         
    full_date       date not null unique,
    day_of_week     tinyint,
    day_name        varchar(10),
    day_of_month    tinyint,
    day_of_year     smallint,
    week_of_year    tinyint,
    month_number    tinyint,
    month_name      varchar(10),
    quarter         tinyint,
    year            smallint,
    is_weekend      boolean,
    is_month_end    boolean,
    index idx_date_year_month (year, month_number)
);

-- property dimension (scd type 2: is_current + effective/expiry dates)
create table dim_property (
    property_key        int auto_increment primary key,
    property_id         int not null,
    landlord_name       varchar(100),
    property_alias      varchar(100),
    property_type       varchar(50),
    property_address    varchar(255),
    property_city       varchar(100),
    property_postcode   varchar(20),
    property_bedrooms   int,
    current_agent_name  varchar(100),
    current_agent_role  varchar(50),
    effective_date      date,
    expiry_date         date,
    is_current          boolean default true,
    index idx_dim_property_id   (property_id),
    index idx_dim_property_city (property_city)
);

-- landlord dimension
create table dim_landlord (
    landlord_key        int auto_increment primary key,
    landlord_id         int not null,
    landlord_reg_number varchar(50),
    landlord_name       varchar(100),
    landlord_email      varchar(100),
    landlord_city       varchar(100),
    property_count      int default 0,
    effective_date      date,
    expiry_date        date,
    is_current     boolean default true,
    index idx_dim_landlord_id (landlord_id)
);

-- tenant dimension
create table dim_tenant (
    tenant_key        int auto_increment primary key,
    tenant_id         int not null,
    tenant_full_name  varchar(100),
    tenant_email      varchar(100),
    effective_date    date,
    expiry_date       date,
    is_current        boolean default true,
    index idx_dim_tenant_id (tenant_id)
);

-- agent dimension
create table dim_agent (
    agent_key    int auto_increment primary key,
    agent_id     int not null,
    agent_name   varchar(100),
    agent_role   varchar(50),
    agent_email  varchar(100),
    is_current   boolean default true,
    index idx_dim_agent_id (agent_id)
);

-- charge type dimension
create table dim_charge_type (
    charge_type_key     int auto_increment primary key,
    charge_type_id      int not null,
    charge_type_name    varchar(50),
    charge_category     varchar(50),
    is_income           boolean,
    index idx_dim_charge_type_id (charge_type_id)
);

-- part b. fact tables

-- transcation fact_statement_charge table: individual charge per line 
create table fact_statement_charge (
    fact_id               int auto_increment primary key,
    date_key              int not null,
    property_key          int not null,
    landlord_key          int not null,
    tenant_key            int,
    agent_key             int,
    charge_type_key       int not null,
    statement_id          int not null,
    charge_amount       decimal(10,2),
    is_income           boolean,
    statement_period_days int,
    foreign key (date_key)        references dim_date(date_key),
    foreign key (property_key)    references dim_property(property_key),
    foreign key (landlord_key)    references dim_landlord(landlord_key),
    foreign key (tenant_key)      references dim_tenant(tenant_key),
    foreign key (agent_key)       references dim_agent(agent_key),
    foreign key (charge_type_key) references dim_charge_type(charge_type_key),
    index idx_fact_date        (date_key),
    index idx_fact_property    (property_key),
    index idx_fact_landlord    (landlord_key),
    index idx_fact_charge_type (charge_type_key),
    index idx_fact_statement   (statement_id)
);

-- snapshot fact_statement_summary_table: one record per statement (monthly summary)
create table fact_statement_summary (
    fact_id                 int auto_increment primary key,
    date_key                int not null,
    property_key            int not null,
    landlord_key            int not null,
    tenant_key              int,
    agent_key               int,
    statement_id            int not null,
    total_rent              decimal(10,2) default 0,
    total_management_fee    decimal(10,2) default 0,
    total_repair            decimal(10,2) default 0,
    total_deposit           decimal(10,2) default 0,
    total_misc              decimal(10,2) default 0,
    net_to_landlord         decimal(10,2) default 0,
    agency_earnings         decimal(10,2) default 0,
    statement_period_days   int,
    foreign key (date_key)       references dim_date(date_key),
    foreign key (property_key)   references dim_property(property_key),
    foreign key (landlord_key)   references dim_landlord(landlord_key),
    index idx_fact_summary_date     (date_key),
    index idx_fact_summary_property (property_key),
    index idx_fact_summary_landlord (landlord_key)
);

-- part c. populate date dimension (2020–2030)
delimiter //
create procedure populate_date_dimension()
begin
    declare d date default '2020-01-01';
    while d <= '2030-12-31' do
        insert into dim_date (date_key, full_date, day_of_week, day_name, day_of_month,day_of_year, week_of_year, month_number, month_name,quarter, year, is_weekend, is_month_end)
        values (
            year(d)*10000 + month(d)*100 + day(d),
            d,
            dayofweek(d),
            dayname(d),
            day(d),
            dayofyear(d),
            week(d),
            month(d),
            monthname(d),
            quarter(d),
            year(d),
            dayofweek(d) in (1,7),
            d = last_day(d)
        );
        set d = date_add(d, interval 1 day);
    end while;
end //
delimiter ;

call populate_date_dimension();

-- part d. dimension ETL procedures
delimiter //


create procedure etl_load_dim_charge_type()
begin
    insert into dim_charge_type (charge_type_id, charge_type_name, charge_category, is_income)
    select c.charge_type_id, c.charge_type_name, c.charge_category, c.is_income
    from charge_type c
    where not exists (
        select 1 from dim_charge_type d
        where d.charge_type_id = c.charge_type_id
    );
end //

create procedure etl_load_dim_landlord()
begin
    insert into dim_landlord (
        landlord_id, landlord_reg_number, landlord_name,
        landlord_email, landlord_city, property_count,
        effective_date, is_current
    )
    select
        l.landlord_id,
        l.landlord_reg_number,
        l.landlord_name,
        l.landlord_email,
        substring_index(l.landlord_address, ',', -1) as landlord_city,
        (select count(*) from property p where p.landlord_id = l.landlord_id),
        current_date,
        true
    from landlord l
    where not exists (
        select 1
        from dim_landlord d
        where d.landlord_id = l.landlord_id
          and d.is_current = true
    );
end //

create procedure etl_load_dim_property()
begin
    insert into dim_property (
        property_id, landlord_name, property_alias,
        property_type, property_address, property_city,
        property_postcode, property_bedrooms,
        current_agent_name, current_agent_role,
        effective_date, is_current
    )
    select
        p.property_id,
        l.landlord_name,
        p.property_alias,
        pt.property_type_name,
        p.property_address,
        p.property_city,
        p.property_postcode,
        p.property_bedrooms,
        a.agent_name,
        a.agent_role,
        current_date,
        true
    from property p
    join landlord l on p.landlord_id = l.landlord_id
    left join property_type pt on p.property_type_id = pt.property_type_id
    left join property_assignment pa on p.property_id = pa.property_id and pa.is_current = true
    left join agency_agent a on pa.agent_id = a.agent_id
    where not exists (
        select 1
        from dim_property d
        where d.property_id = p.property_id
          and d.is_current = true
    );
end //

create procedure etl_load_dim_tenant()
begin
    insert into dim_tenant (
        tenant_id, tenant_full_name, tenant_email,
        effective_date, is_current
    )
    select
        t.tenant_id,
        concat(t.tenant_first_name, ' ', t.tenant_last_name),
        t.tenant_email,
        current_date,
        true
    from tenant t
    where not exists (
        select 1
        from dim_tenant d
        where d.tenant_id = t.tenant_id
          and d.is_current = true
    );
end //

create procedure etl_load_dim_agent()
begin
    insert into dim_agent (
        agent_id, agent_name, agent_role, agent_email, is_current
    )
    select
        a.agent_id,
        a.agent_name,
        a.agent_role,
        a.agent_email,
        true
    from agency_agent a
    where not exists (
        select 1
        from dim_agent d
        where d.agent_id = a.agent_id
          and d.is_current = true
    );
end //

delimiter ;

-- part e. fact etl procedures
delimiter //

create procedure etl_load_fact_statement_charge()
begin
    insert into fact_statement_charge (
        date_key, property_key, landlord_key, tenant_key, agent_key,
        charge_type_key, statement_id, charge_amount, is_income, statement_period_days
    )
    select
        year(s.statement_date)*10000 + month(s.statement_date)*100 + day(s.statement_date),
        dp.property_key,
        dl.landlord_key,
        dt.tenant_key,
        da.agent_key,
        dct.charge_type_key,
        s.statement_id,
        sc.amount,
        dct.is_income,
        datediff(s.period_end, s.period_start)
    from statement s
    join statement_charge sc on s.statement_id = sc.statement_id
    join property p on s.property_id = p.property_id
    join dim_property dp on p.property_id = dp.property_id and dp.is_current = true
    join landlord l on p.landlord_id = l.landlord_id
    join dim_landlord dl on l.landlord_id = dl.landlord_id and dl.is_current = true
    left join lease le on s.lease_id = le.lease_id
    left join tenant t on le.tenant_id = t.tenant_id
    left join dim_tenant dt on t.tenant_id = dt.tenant_id and dt.is_current = true
    left join property_assignment pa on p.property_id = pa.property_id and pa.is_current = true
    left join dim_agent da on pa.agent_id = da.agent_id and da.is_current = true
    join charge_type ct on sc.charge_type_id = ct.charge_type_id
    join dim_charge_type dct on ct.charge_type_id = dct.charge_type_id
    where not exists (
        select 1 from fact_statement_charge f
        where f.statement_id = s.statement_id
          and f.charge_type_key = dct.charge_type_key
    );
end //

create procedure etl_load_fact_statement_summary()
begin
    insert into fact_statement_summary (
        date_key, property_key, landlord_key, tenant_key, agent_key,
        statement_id, total_rent, total_management_fee, total_repair,
        total_deposit, total_misc, net_to_landlord, agency_earnings, statement_period_days
    )
    select
        year(s.statement_date)*10000 + month(s.statement_date)*100 + day(s.statement_date),
        dp.property_key,
        dl.landlord_key,
        dt.tenant_key,
        da.agent_key,
        s.statement_id,
        coalesce(sum(case when ct.charge_type_name = 'rent' then sc.amount else 0 end), 0),
        coalesce(sum(case when ct.charge_type_name = 'management_fee' then sc.amount else 0 end), 0),
        coalesce(sum(case when ct.charge_type_name = 'repair' then sc.amount else 0 end), 0),
        coalesce(sum(case when ct.charge_type_name = 'deposit' then sc.amount else 0 end), 0),
        coalesce(sum(case when ct.charge_type_name = 'misc' then sc.amount else 0 end), 0),
        s.total_amount,
        coalesce(sum(case when ct.charge_type_name = 'management_fee' then sc.amount else 0 end), 0),
        datediff(s.period_end, s.period_start)
    from statement s
    left join statement_charge sc on s.statement_id = sc.statement_id
    left join charge_type ct on sc.charge_type_id = ct.charge_type_id
    join property p on s.property_id = p.property_id
    join dim_property dp on p.property_id = dp.property_id and dp.is_current = true
    join landlord l on p.landlord_id = l.landlord_id
    join dim_landlord dl on l.landlord_id = dl.landlord_id and dl.is_current = true
    left join lease le on s.lease_id = le.lease_id
    left join tenant t on le.tenant_id = t.tenant_id
    left join dim_tenant dt on t.tenant_id = dt.tenant_id and dt.is_current = true
    left join property_assignment pa on p.property_id = pa.property_id and pa.is_current = true
    left join dim_agent da on pa.agent_id = da.agent_id and da.is_current = true
    where not exists (
        select 1 from fact_statement_summary f
        where f.statement_id = s.statement_id
    )
    group by
        s.statement_id,
        dp.property_key,
        dl.landlord_key,
        dt.tenant_key,
        da.agent_key,
        s.statement_date,
        s.total_amount,
        s.period_start,
        s.period_end;
end //

-- master etl runner
create procedure etl_run_full_load()
begin
    call etl_load_dim_charge_type();
    call etl_load_dim_landlord();
    call etl_load_dim_property();
    call etl_load_dim_tenant();
    call etl_load_dim_agent();
    call etl_load_fact_statement_charge();
    call etl_load_fact_statement_summary();
end //

delimiter ;

-- run once 
call etl_run_full_load();

-- part f. analytical views

-- 1. annual property performance
create view vw_property_annual_performance as
select
    dp.property_alias,
    dp.landlord_name,
    dp.property_city,
    dd.year,
    sum(fs.total_rent)             as total_rent,
    sum(fs.total_management_fee)   as total_fees,
    sum(fs.total_repair)           as total_repairs,
    sum(fs.net_to_landlord)        as net_profit,
    count(distinct fs.statement_id) as statement_count
from fact_statement_summary fs
join dim_property dp on fs.property_key = dp.property_key
join dim_date dd on fs.date_key = dd.date_key
group by dp.property_alias, dp.landlord_name, dp.property_city, dd.year
order by dd.year desc, net_profit desc;

-- 2. agency earnings (management fee trends)
create view vw_agency_earnings as
select
    dd.year,
    dd.quarter,
    dd.month_name,
    sum(fs.agency_earnings)        as total_management_fees,
    count(distinct fs.statement_id) as statements_processed,
    sum(fs.total_rent)             as total_rent_collected
from fact_statement_summary fs
join dim_date dd on fs.date_key = dd.date_key
group by dd.year, dd.quarter, dd.month_number, dd.month_name
order by dd.year, dd.quarter, dd.month_number;

-- 3. repair analysis
create view vw_repair_analysis as
select
    dp.property_alias,
    dp.landlord_name,
    dp.property_city,
    dd.year,
    sum(fs.total_repair) as total_repairs,
    sum(fs.total_rent)   as total_rent,
    round(sum(fs.total_repair)/nullif(sum(fs.total_rent),0)*100,2) as repair_to_rent_ratio,
    count(case when fs.total_repair > 0 then 1 end) as repair_incidents
from fact_statement_summary fs
join dim_property dp on fs.property_key = dp.property_key
join dim_date dd on fs.date_key = dd.date_key
group by dp.property_alias, dp.landlord_name, dp.property_city, dd.year
having sum(fs.total_repair) > 0
order by total_repairs desc;

-- 4. monthly revenue and activity trends
create view vw_monthly_trends as
select
    dd.year,
    dd.month_number,
    dd.month_name,
    sum(fs.total_rent)           as total_rent,
    sum(fs.total_management_fee) as total_fees,
    sum(fs.net_to_landlord)      as net_to_landlords,
    count(distinct dp.property_key) as active_properties
from fact_statement_summary fs
join dim_date dd on fs.date_key = dd.date_key
join dim_property dp on fs.property_key = dp.property_key
group by dd.year, dd.month_number, dd.month_name
order by dd.year, dd.month_number;

-- 5. landlord performance summary
create view vw_landlord_performance as
select
    dl.landlord_name,
    dl.landlord_reg_number,
    dl.property_count,
    count(distinct fs.statement_id) as total_statements,
    sum(fs.total_rent)             as total_rent_collected,
    sum(fs.net_to_landlord)        as total_profit,
    sum(fs.total_repair)           as total_repairs,
    round(avg(fs.net_to_landlord),2) as avg_monthly_profit
from fact_statement_summary fs
join dim_landlord dl on fs.landlord_key = dl.landlord_key
group by dl.landlord_key, dl.landlord_name, dl.landlord_reg_number, dl.property_count
order by total_profit desc;

-- 6. fee analysis (management fee %)
create view vw_fee_analysis as
select
    dp.property_alias,
    dl.landlord_name,
    dd.year,
    sum(fs.total_rent)           as total_rent,
    sum(fs.total_management_fee) as total_fees,
    round(sum(fs.total_management_fee)/nullif(sum(fs.total_rent),0)*100,2)
        as fee_percentage
from fact_statement_summary fs
join dim_property dp on fs.property_key = dp.property_key
join dim_landlord dl on fs.landlord_key = dl.landlord_key
join dim_date dd on fs.date_key = dd.date_key
group by dp.property_alias, dl.landlord_name, dd.year
order by dd.year, fee_percentage desc;

-- 7. year-over-year comparison
create view vw_yoy_comparison as
select
    curr.year as current_year,
    curr.total_rent as current_rent,
    prev.total_rent as previous_rent,
    round((curr.total_rent - coalesce(prev.total_rent,0)) / nullif(prev.total_rent,0) * 100, 2) as rent_growth_pct,
    curr.total_fees as current_fees,
    prev.total_fees as previous_fees,
    curr.net_profit as current_profit,
    prev.net_profit as previous_profit
from (
    select
        dd.year,
        sum(fs.total_rent)           as total_rent,
        sum(fs.total_management_fee) as total_fees,
        sum(fs.net_to_landlord)      as net_profit
    from fact_statement_summary fs
    join dim_date dd on fs.date_key = dd.date_key
    group by dd.year
) curr
left join (
    select dd.year,
        sum(fs.total_rent)           as total_rent,
        sum(fs.total_management_fee) as total_fees,
        sum(fs.net_to_landlord)      as net_profit
    from fact_statement_summary fs
    join dim_date dd on fs.date_key = dd.date_key
    group by dd.year
) prev on curr.year = prev.year + 1
order by curr.year;

-- 8. seasonal maintenance trends
create view vw_seasonal_maintenance as
select
    dd.month_name,dd.month_number,
    avg(fs.total_repair) as avg_repair_cost,
    sum(fs.total_repair) as total_repair_cost,
    count(case when fs.total_repair > 0 then 1 end) as repair_count
from fact_statement_summary fs
join dim_date dd on fs.date_key = dd.date_key
group by dd.month_name, dd.month_number
order by dd.month_number;

