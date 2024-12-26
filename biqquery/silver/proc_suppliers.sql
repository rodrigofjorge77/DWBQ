CREATE OR REPLACE PROCEDURE dw_silver.proc_SUPPLIERS()
BEGIN
  -- Truncate the target table
  EXECUTE IMMEDIATE '''
    TRUNCATE TABLE `dwbq-445617.dw_silver.suppliers`
  ''';

  -- Insert data into the target table
  EXECUTE IMMEDIATE '''
  INSERT INTO `dwbq-445617`.dw_silver.suppliers (supplier_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax, homepage)
  SELECT CAST(supplier_id AS INT64) AS supplier_id, 
      UPPER(company_name) AS company_name, 
      UPPER(contact_name) AS contact_name, 
      UPPER(contact_title) AS contact_title, 
      UPPER(address) AS ADDRESS , 
      UPPER(city) AS CITY , 
      COALESCE(UPPER(region),'NA') AS REGION , 
      postal_code, 
      UPPER(country) AS COUNTRY , 
      phone, 
      fax, 
      homepage
  FROM `dwbq-445617`.dw_bronze.SUPPLIERS;
  ''';
END;

CALL dw_silver.proc_SUPPLIERS();
