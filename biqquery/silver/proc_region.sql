CREATE OR REPLACE PROCEDURE dw_silver.proc_REGION()
BEGIN
  -- Truncate the target table
  EXECUTE IMMEDIATE '''
    TRUNCATE TABLE `dwbq-445617.dw_silver.region`
  ''';

  -- Insert data into the target table
  EXECUTE IMMEDIATE '''
  insert into `dwbq-445617`.dw_silver.region (region_id, region_description)
  SELECT cast(region_id as int64) as region_id, 
      UPPER(region_description) AS region_description
  FROM `dwbq-445617`.dw_bronze.REGION
  ''';
END;

CALL dw_silver.proc_REGION();
