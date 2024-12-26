CREATE OR REPLACE PROCEDURE dw_gold.fato_ORDERS()
BEGIN
  -- Truncate the target table
  EXECUTE IMMEDIATE '''
    TRUNCATE TABLE `dwbq-445617`.dw_gold.FATO_ORDERS
  ''';

  -- Insert data into the target table
  EXECUTE IMMEDIATE '''
    INSERT INTO `dwbq-445617`.dw_gold.FATO_ORDERS (SK_ORDERS, SK_CUSTOMER, SK_EMPLOYEE, SK_DATA, SK_PRODUCT, FREIGHT, QUANTITY, DISCOUNT)
    SELECT DO.SK_ORDERS, 
        COALESCE(DC.SK_CUSTOMER,-1),
        COALESCE(DE.SK_EMPLOYEE,-1),  
        DT.SK_DATA, 
        COALESCE(DP.SK_PRODUCT,-1),
        ROUND((OD.FREIGHT / count(*) OVER (PARTITION BY OD.ORDER_ID)) ,2) FREIGHT,
        OT.QUANTITY,
        OT.DISCOUNT 
    FROM `dwbq-445617`.dw_silver.orders OD,
        `dwbq-445617`.dw_gold.DIM_ORDERS DO,
        `dwbq-445617`.dw_gold.DIM_TEMPO DT,
        `dwbq-445617`.dw_silver.order_details OT,
        `dwbq-445617`.dw_gold.DIM_CUSTOMERS DC,
        `dwbq-445617`.dw_gold.DIM_EMPLOYEES  DE,
        `dwbq-445617`.dw_gold.DIM_PRODUCTS DP
    WHERE DO.ORDER_ID          = OD.ORDER_ID 
    AND   DT.DATA 	           = OD.ORDER_DATE 
    AND   OT.ORDER_ID 		   = DO.ORDER_ID 
    AND   OT.PRODUCT_ID 	   = DO.PRODUCT_ID
    AND   DC.CUSTOMER_ID 	   = OD.CUSTOMER_ID
    AND   DE.EMPLOYEE_ID 	   = OD.EMPLOYEE_ID 
    AND   DP.PRODUCT_ID 	   = DO.PRODUCT_ID ;
  ''';
END;

CALL dw_gold.fato_ORDERS()
