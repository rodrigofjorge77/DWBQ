CREATE OR REPLACE PROCEDURE dw_silver.proc_EMPLOYEES()
BEGIN
  -- Truncate the target table
  EXECUTE IMMEDIATE '''
    TRUNCATE TABLE `dwbq-445617.dw_silver.employees`
  ''';

  -- Insert data into the target table
  EXECUTE IMMEDIATE '''
    INSERT INTO `dwbq-445617.dw_silver.employees` (employee_id, last_name, first_name, title, title_of_courtesy, birth_date, hire_date, address, city, region, postal_code, country, home_phone, extension, photo, notes, reports_to, photo_path)
    SELECT CAST(employee_id as INT64) AS employee_id, 
        UPPER(last_name) AS last_name, 
        UPPER(first_name) AS first_name, 
        UPPER(title) AS title, 
        UPPER(title_of_courtesy) AS title_of_courtesy, 
        CAST(SUBSTR(birth_date, 1, 10) as date) as birth_date, 
        CAST(SUBSTR(hire_date, 1, 10) as date)  as hire_date, 
        UPPER(address) as address, 
        UPPER(city) as city, 
        COALESCE(UPPER(region),'NA') as region, 
        UPPER(postal_code) AS postal_code, 
        UPPER(country) AS country, 
        home_phone, 
        extension, 
        null, 
        UPPER(notes) AS notes, 
        COALESCE(cast(reports_to AS INT64),-1) reports_to, 
        photo_path
    FROM `dwbq-445617`.dw_bronze.EMPLOYEES
  ''';
END;

CALL dw_silver.proc_EMPLOYEES();
