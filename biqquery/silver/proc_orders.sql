CREATE OR REPLACE PROCEDURE dw_silver.proc_ORDERS()
BEGIN
  -- Truncate the target table
  EXECUTE IMMEDIATE '''
    TRUNCATE TABLE `dwbq-445617.dw_silver.orders`
  ''';

  -- Insert data into the target table
  EXECUTE IMMEDIATE '''
  insert into `dwbq-445617`.dw_silver.orders (order_id, customer_id, employee_id, order_date, required_date, shipped_date, ship_via, freight, ship_name, ship_address, ship_city, ship_region, ship_postal_code, ship_country)
  SELECT cast(order_id as int64) as order_id, 
      customer_id, 
      cast(employee_id as int64) as employee_id, 
      CAST(COALESCE(SUBSTR(order_date,1,10),'1901-01-01') as date)  AS order_date, 
      CAST(COALESCE(SUBSTR(required_date,1,10),'1901-01-01') as date)  AS required_date, 
      CAST(COALESCE(SUBSTR(shipped_date,1,10),'1901-01-01') as date)  AS shipped_date, 
      cast(ship_via as int64) ship_via, 
      ROUND(freight,2) AS freight, 
      UPPER(ship_name) AS ship_name, 
      UPPER(ship_address) AS ship_address, 
      UPPER(ship_city) AS ship_city, 
      COALESCE(UPPER(ship_region),'NA') AS ship_region, 
      ship_postal_code, 
      UPPER(ship_country) AS ship_country
  FROM `dwbq-445617`.dw_bronze.ORDERS
  ''';
END;

CALL dw_silver.proc_ORDERS();
