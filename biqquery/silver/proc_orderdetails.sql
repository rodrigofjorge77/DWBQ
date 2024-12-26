CREATE OR REPLACE PROCEDURE dw_silver.proc_ORDER_DETAILS()
BEGIN
  -- Truncate the target table
  EXECUTE IMMEDIATE '''
    TRUNCATE TABLE `dwbq-445617.dw_silver.order_details`
  ''';

  -- Insert data into the target table
  EXECUTE IMMEDIATE '''
    insert into `dwbq-445617`.dw_silver.order_details(order_id, product_id, unit_price, quantity, discount)
    SELECT CAST(order_id AS INT64) order_id, 
        CAST(product_id AS INT64) AS product_id, 
        unit_price, 
        CAST(quantity AS INT64) AS quantity, 
        discount
    FROM `dwbq-445617`.dw_bronze.ORDER_DETAILS
  ''';
END;

CALL dw_silver.proc_ORDER_DETAILS();
