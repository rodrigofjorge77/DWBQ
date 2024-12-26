CREATE OR REPLACE PROCEDURE dw_gold.dim_EMPLOYEES()
BEGIN
  -- Truncate the target table
  EXECUTE IMMEDIATE '''
    TRUNCATE TABLE `dwbq-445617`.dw_gold.DIM_EMPLOYEES
  ''';

  EXECUTE IMMEDIATE '''
    TRUNCATE TABLE `dwbq-445617`.dw_gold.TERRITORIES
  ''';

  EXECUTE IMMEDIATE '''
    TRUNCATE TABLE `dwbq-445617`.dw_gold.EMPLOYEE_TERRITORIES
  ''';

  EXECUTE IMMEDIATE '''
    TRUNCATE TABLE `dwbq-445617`.dw_gold.REGION
  ''';

  -- Insert data into the target table
  EXECUTE IMMEDIATE '''
    INSERT INTO `dwbq-445617`.dw_gold.DIM_EMPLOYEES (SK_EMPLOYEE, EMPLOYEE_ID, LAST_NAME, FIRST_NAME, TITLE, TITLE_OF_COURTESY, BIRTH_DATE, HIRE_DATE, ADDRESS, CITY, REGION, POSTAL_CODE, COUNTRY, HOME_PHONE, EXTENSION, NOTES, REPORTS_TO, PHOTO_PATH)
    SELECT ROW_NUMBER() OVER () AS SK_EMPLOYEE, 
           employee_id, last_name, first_name, title, title_of_courtesy, birth_date, hire_date, address, city, region, postal_code, country, home_phone, extension,  notes, reports_to, photo_path
    FROM `dwbq-445617`.dw_silver.employees;    
  ''';

  -- Insert data into the target table
  EXECUTE IMMEDIATE '''
  insert into `dwbq-445617`.dw_gold.TERRITORIES (territory_id, territory_description, region_id)
  SELECT territory_id, territory_description, region_id
  FROM `dwbq-445617`.dw_silver.territories;   
  ''';

  -- Insert data into the target table
  EXECUTE IMMEDIATE '''
  insert into `dwbq-445617`.dw_gold.EMPLOYEE_TERRITORIES (employee_id, territory_id) 
  SELECT employee_id, territory_id
  FROM `dwbq-445617`.dw_silver.employee_territories ;  
  ''';

  EXECUTE IMMEDIATE '''
  insert into `dwbq-445617`.dw_gold.REGION (region_id, region_description)
  SELECT region_id, region_description
  FROM `dwbq-445617`.dw_silver.region ;
  ''';

END;

CALL dw_gold.dim_EMPLOYEES()
