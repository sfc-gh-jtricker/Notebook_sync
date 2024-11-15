/* Run the start to recreate the tablesfor a simple lineage demo */

use role sysadmin ; 
create schema if not exists frostbyte_tasty_bytes_v2.lineage ; 

use schema frostbyte_tasty_bytes_v2.lineage ; 

CREATE OR REPLACE table menu_item_cogs_and_price
    AS
SELECT DISTINCT
    r.menu_item_id,
    ip.start_date,
    ip.end_date,
    SUM(ip.unit_price * r.unit_quantity) 
        OVER (PARTITION BY r.menu_item_id, ip.start_date, ip.end_date)
            AS cost_of_menu_item_usd,
    mp.sales_price_usd
FROM frostbyte_tasty_bytes_v2.raw_supply_chain.item i
JOIN frostbyte_tasty_bytes_v2.raw_supply_chain.recipe r
    ON i.item_id = r.item_id
JOIN frostbyte_tasty_bytes_v2.raw_supply_chain.item_prices ip
    ON ip.item_id = r.item_id
JOIN frostbyte_tasty_bytes_v2.raw_supply_chain.menu_prices mp
    ON mp.menu_item_id = r.menu_item_id
    AND mp.start_date = ip.start_date
ORDER BY r.menu_item_id, ip.start_date;


create or replace tag salesdates comment = ' Dates relevant to sales information' ; 

alter table menu_item_cogs_and_price modify column START_DATE set tag
  salesdates = 'Date menu item was initially available';

alter table menu_item_cogs_and_price modify column END_DATE set tag
  salesdates = 'Date menu item was last available';

  create or replace table menu_icap_start_summary as 
  select start_date, menu_item_id, sum(cost_of_menu_item_usd) SUM_COST,avg(sales_price_usd) AVG_PRICE
  from menu_item_cogs_and_price 
  group by 1, 2 ; 

  create or replace table menu_icap_end_summary as 
  select end_date, menu_item_id, sum(cost_of_menu_item_usd) SUM_COST,avg(sales_price_usd) AVG_PRICE
  from menu_item_cogs_and_price 
  group by 1, 2 ; 

  create or replace table menu_icap_end_summary_dev clone menu_icap_end_summary ; 

  create or replace table menu_icap_start_cost as 
  select start_date, menu_item_id, sum_cost
  from menu_icap_start_summary ; 


create or replace table micgs_ctas1 as 
select menu_item_id, datediff(Month, end_date,start_date) elapsed,
cost_of_menu_item_usd,sales_price_usd
from menu_item_cogs_and_price; 

create or replace view menu_item_cogs_and_price_recent
as 
select * from menu_item_cogs_and_price 
where end_date >= '2023-01-01' ; 

create or replace view menu_item_cogs_and_price_fresh
as 
select * from menu_item_cogs_and_price_recent 
where end_date >= '2023-12-01' ; 

create or replace dynamic table menu_items_summary
target_lag = '24 hours'
warehouse=util_wh
refresh_mode = auto
initialize = on_create 
as
select menu_item_id, time_slice(start_date,1 ,'Month') trade_month,
avg(cost_of_menu_item_usd) AVG_COST,
max(cost_of_menu_item_usd) MAX_COST,
min(cost_of_menu_item_usd) MIN_COST,
avg(sales_price_usd) AVG_SALE,
max(sales_price_usd) MAX_SALE,
min(sales_price_usd) min_SALE
from FROSTBYTE_TASTY_BYTES_V2.LINEAGE.MENU_ITEM_COGS_AND_PRICE
group by 1,2 ; 

create table menu_insert_test like menu_icap_end_summary ; 

create table menu_insert_test2 like menu_insert_test ; 

insert into menu_insert_test2 select * from menu_icap_end_summary ; 



-- END OF LINEAGE SETUP

GRANT VIEW LINEAGE on account TO ROLE sysadmin ; 
