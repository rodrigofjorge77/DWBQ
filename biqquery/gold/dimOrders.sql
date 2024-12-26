CREATE OR REPLACE PROCEDURE dw_gold.dim_ORDERS()
BEGIN
  -- Truncate the target table
  EXECUTE IMMEDIATE '''
    TRUNCATE TABLE `dwbq-445617`.dw_gold.DIM_ORDERS
  ''';

  EXECUTE IMMEDIATE '''
    TRUNCATE TABLE `dwbq-445617`.dw_gold.SHIPPERS
  ''';

  -- Insert data into the target table
  EXECUTE IMMEDIATE '''
  insert into `dwbq-445617`.dw_gold.DIM_ORDERS (SK_ORDERS, ORDER_ID, PRODUCT_ID, UNIT_PRICE, REQUIRED_DATE, SHIPPED_DATE, SHIP_VIA, SHIP_NAME, SHIP_ADDRESS, SHIP_CITY, SHIP_REGION, SHIP_POSTAL_CODE, SHIP_COUNTRY)
  SELECT  ROW_NUMBER() OVER () AS SK_ORDERS, 
          OD.ORDER_ID, 
          OT.PRODUCT_ID ,
          OT.UNIT_PRICE,
          OD.REQUIRED_DATE, 
          OD.SHIPPED_DATE, 
          OD.SHIP_VIA, 
          OD.SHIP_NAME, 
          OD.SHIP_ADDRESS, 
          OD.SHIP_CITY, 
          OD.SHIP_REGION, 
          OD.SHIP_POSTAL_CODE, 
          OD.SHIP_COUNTRY
      FROM `dwbq-445617`.dw_silver.orders OD,
        `dwbq-445617`.dw_silver.order_details OT
      WHERE OT.ORDER_ID  = OD.ORDER_ID 
      ORDER BY 1,2;
  ''';

  EXECUTE IMMEDIATE '''
  INSERT INTO `dwbq-445617`.dw_gold.SHIPPERS (SHIPPER_ID, COMPANY_NAME, PHONE)
  SELECT SHIPPER_ID, COMPANY_NAME, PHONE
  FROM `dwbq-445617`.dw_silver.shippers ;
  ''';

END;

CALL dw_gold.dim_ORDERS()
