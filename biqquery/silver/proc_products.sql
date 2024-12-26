CREATE OR REPLACE PROCEDURE dw_silver.proc_PRODUCTS()
BEGIN
  -- Truncate the target table
  EXECUTE IMMEDIATE '''
    TRUNCATE TABLE `dwbq-445617.dw_silver.products`
  ''';

  -- Insert data into the target table
  EXECUTE IMMEDIATE '''
  insert into `dwbq-445617`.dw_silver.products (product_id, product_name, supplier_id, category_id, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued)
  SELECT cast(product_id as int64) as product_id, 
      upper(product_name) as product_name, 
      cast(supplier_id as int64) as supplier_id, 
      cast(category_id as int64) as category_id, 
      upper(quantity_per_unit) as quantity_per_unit, 
      round(unit_price,2) unit_price, 
      cast(units_in_stock as int64) as units_in_stock, 
      cast(units_on_order as int64) as units_on_order, 
      cast(reorder_level as int64) as reorder_level, 
      CASE 
            WHEN coalesce(discontinued,0) = 0 THEN FALSE
            WHEN coalesce(discontinued,0) = 1 THEN TRUE
        END AS discontinued	   
  FROM `dwbq-445617`.dw_bronze.PRODUCTS
  ''';
END;

CALL dw_silver.proc_PRODUCTS();
