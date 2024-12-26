CREATE OR REPLACE PROCEDURE dw_silver.proc_SHIPPERS()
BEGIN
  -- Truncate the target table
  EXECUTE IMMEDIATE '''
    TRUNCATE TABLE `dwbq-445617.dw_silver.shippers`
  ''';

  -- Insert data into the target table
  EXECUTE IMMEDIATE '''
  INSERT INTO `dwbq-445617`.dw_silver.shippers (shipper_id, company_name, phone)
  SELECT CAST(shipper_id AS INT64) AS shipper_id, 
      UPPER(company_name) AS company_name, 
      phone
  FROM `dwbq-445617`.dw_bronze.SHIPPERS
  ''';
END;

CALL dw_silver.proc_SHIPPERS();
