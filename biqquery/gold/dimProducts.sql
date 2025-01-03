CREATE OR REPLACE PROCEDURE dw_gold.dim_PRODUCTS()
BEGIN
  -- Truncate the target table
  EXECUTE IMMEDIATE '''
    TRUNCATE TABLE `dwbq-445617`.dw_gold.DIM_PRODUCTS
  ''';

  EXECUTE IMMEDIATE '''
    TRUNCATE TABLE `dwbq-445617`.dw_gold.CATEGORIES
  ''';

  EXECUTE IMMEDIATE '''
    TRUNCATE TABLE `dwbq-445617`.dw_gold.SUPPLIERS
  ''';

  -- Insert data into the target table
  EXECUTE IMMEDIATE '''
   INSERT INTO `dwbq-445617`.dw_gold.DIM_PRODUCTS (SK_PRODUCT, PRODUCT_ID, PRODUCT_NAME, SUPPLIER_ID, SUPPLIER_NAME, CATEGORY_ID, QUANTITY_PER_UNIT, UNIT_PRICE, UNITS_IN_STOCK, UNITS_ON_ORDER, REORDER_LEVEL, DISCONTINUED)      
   SELECT ROW_NUMBER() OVER () AS SK_PRODUCT, P.PRODUCT_ID, P.PRODUCT_NAME, P.SUPPLIER_ID, S.COMPANY_NAME,  P.CATEGORY_ID, P.QUANTITY_PER_UNIT, P.UNIT_PRICE, P.UNITS_IN_STOCK, P.UNITS_ON_ORDER, P.REORDER_LEVEL, P.DISCONTINUED
    FROM `dwbq-445617`.dw_silver.products P ,
         `dwbq-445617`.dw_silver.suppliers S
    WHERE S.SUPPLIER_ID  = P.SUPPLIER_ID ;
  ''';

   EXECUTE IMMEDIATE '''
  INSERT INTO `dwbq-445617`.dw_gold.CATEGORIES (CATEGORY_ID, CATEGORY_NAME, DESCRIPTION)
  SELECT CATEGORY_ID, CATEGORY_NAME, DESCRIPTION
  FROM `dwbq-445617`.dw_silver.categories ;
  ''';

   EXECUTE IMMEDIATE '''
  INSERT INTO `dwbq-445617`.dw_gold.SUPPLIERS (SUPPLIER_ID, COMPANY_NAME, CONTACT_NAME, CONTACT_TITLE, ADDRESS, CITY, REGION, POSTAL_CODE, COUNTRY, PHONE, FAX, HOMEPAGE)
  SELECT SUPPLIER_ID, COMPANY_NAME, CONTACT_NAME, CONTACT_TITLE, ADDRESS, CITY, REGION, POSTAL_CODE, COUNTRY, PHONE, FAX, HOMEPAGE
  FROM `dwbq-445617`.dw_silver.suppliers ;
  ''';

END;

CALL dw_gold.dim_PRODUCTS()
