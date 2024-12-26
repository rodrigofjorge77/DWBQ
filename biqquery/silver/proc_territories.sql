CREATE OR REPLACE PROCEDURE dw_silver.proc_TERRITORIES()
BEGIN
  -- Truncate the target table
  EXECUTE IMMEDIATE '''
    TRUNCATE TABLE `dwbq-445617.dw_silver.territories`
  ''';

  -- Insert data into the target table
  EXECUTE IMMEDIATE '''
  INSERT INTO `dwbq-445617`.dw_silver.territories (territory_id, territory_description, region_id)
  SELECT territory_id, 
      UPPER(territory_description) AS territory_description, 
      CAST(region_id AS INT64) AS region_id 
  FROM `dwbq-445617`.dw_bronze.TERRITORIES
  ''';
END;

CALL dw_silver.proc_TERRITORIES();
