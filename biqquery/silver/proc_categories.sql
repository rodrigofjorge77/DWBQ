CREATE OR REPLACE PROCEDURE dw_silver.proc_CATEGORIES()
BEGIN
  -- Truncate the target table
  EXECUTE IMMEDIATE '''
    TRUNCATE TABLE `dwbq-445617.dw_silver.categories`
  ''';

  -- Insert data into the target table
  EXECUTE IMMEDIATE '''
    INSERT INTO `dwbq-445617.dw_silver.categories` (CATEGORY_ID, DESCRIPTION, CATEGORY_NAME)
    SELECT CAST(CATEGORY_ID AS INT64) AS CATEGORY_ID, 
           UPPER(DESCRIPTION), 
           UPPER(CATEGORY_NAME)
    FROM `dwbq-445617.dw_bronze.CATEGORIES`
  ''';
END;

CALL dw_silver.proc_CATEGORIES();
