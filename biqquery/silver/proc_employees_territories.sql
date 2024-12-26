CREATE OR REPLACE PROCEDURE dw_silver.proc_EMPLOYEE_TERRITORIES()
BEGIN
  -- Truncate the target table
  EXECUTE IMMEDIATE '''
    TRUNCATE TABLE `dwbq-445617.dw_silver.employee_territories`
  ''';

  -- Insert data into the target table
  EXECUTE IMMEDIATE '''
    INSERT INTO `dwbq-445617.dw_silver.employee_territories` (employee_id, territory_id)
    SELECT CAST(employee_id AS INT64) AS employee_id, 
        territory_id
    FROM `dwbq-445617`.dw_bronze.EMPLOYEE_TERRITORIES
  ''';
END;

CALL dw_silver.proc_EMPLOYEE_TERRITORIES();
