CREATE OR REPLACE PROCEDURE dw_silver.proc_CUSTOMERS()
BEGIN
  -- Truncate the target table
  EXECUTE IMMEDIATE '''
    TRUNCATE TABLE `dwbq-445617.dw_silver.customers`
  ''';

  -- Insert data into the target table
  EXECUTE IMMEDIATE '''
    INSERT INTO `dwbq-445617.dw_silver.customers` (customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax)
    SELECT UPPER(customer_id) AS customer_id, 
        UPPER(company_name) AS company_name, 
        UPPER(contact_name) AS contact_name, 
        UPPER(contact_title) AS contact_title, 
        UPPER(address) AS address, 
        UPPER(city) AS city, 
        COALESCE(UPPER(region),'NA') AS region, 
        COALESCE(UPPER(postal_code),'NA') AS postal_code, 
        UPPER(country) AS country, 
        COALESCE(phone,'NA') AS phone, 
        COALESCE(fax,'NA') AS fax
    FROM `dwbq-445617`.dw_bronze.CUSTOMERS
  ''';
END;

CALL dw_silver.proc_CUSTOMERS();
