CREATE OR REPLACE DYNAMIC TABLE {{db}}.{{schema}}.product
  TARGET_LAG = '1 day'
  WAREHOUSE = {{dyn_wh}}
  REFRESH_MODE = auto
  INITIALIZE = on_create
  AS
    SELECT end_date,menu_item_id FROM {{db}}.{{schema}}.staging_table; */ 