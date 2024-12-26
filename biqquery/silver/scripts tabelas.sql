CREATE OR REPLACE TABLE dwbq-445617.dw_silver.categories (
    category_id INT64 NOT NULL,
    category_name STRING NOT NULL,
    description STRING
);

CREATE OR REPLACE TABLE dwbq-445617.dw_silver.customers (
    customer_id STRING NOT NULL,
    company_name STRING NOT NULL,
    contact_name STRING,
    contact_title STRING,
    address STRING,
    city STRING,
    region STRING,
    postal_code STRING,
    country STRING,
    phone STRING,
    fax STRING
);

CREATE OR REPLACE TABLE dwbq-445617.dw_silver.employees (
    employee_id INT64 NOT NULL,
    last_name STRING NOT NULL,
    first_name STRING NOT NULL,
    title STRING,
    title_of_courtesy STRING,
    birth_date DATE,
    hire_date DATE,
    address STRING,
    city STRING,
    region STRING,
    postal_code STRING,
    country STRING,
    home_phone STRING,
    extension STRING,
    photo BYTES,
    notes STRING,
    reports_to INT64,
    photo_path STRING
);

CREATE OR REPLACE TABLE dwbq-445617.dw_silver.employee_territories (
    employee_id INT64 NOT NULL,
    territory_id STRING NOT NULL
);

CREATE OR REPLACE TABLE dwbq-445617.dw_silver.order_details (
    order_id INT64 NOT NULL,
    product_id INT64 NOT NULL,
    unit_price FLOAT64 NOT NULL,
    quantity INT64 NOT NULL,
    discount FLOAT64 NOT NULL
);

CREATE OR REPLACE TABLE dwbq-445617.dw_silver.orders (
    order_id INT64 NOT NULL,
    customer_id STRING,
    employee_id INT64,
    order_date DATE,
    required_date DATE,
    shipped_date DATE,
    ship_via INT64,
    freight FLOAT64,
    ship_name STRING,
    ship_address STRING,
    ship_city STRING,
    ship_region STRING,
    ship_postal_code STRING,
    ship_country STRING
);

CREATE OR REPLACE TABLE dwbq-445617.dw_silver.products (
    product_id INT64 NOT NULL,
    product_name STRING NOT NULL,
    supplier_id INT64,
    category_id INT64,
    quantity_per_unit STRING,
    unit_price FLOAT64,
    units_in_stock INT64,
    units_on_order INT64,
    reorder_level INT64,
    discontinued BOOL NOT NULL
);

CREATE OR REPLACE TABLE dwbq-445617.dw_silver.region (
    region_id INT64 NOT NULL,
    region_description STRING NOT NULL
);

CREATE OR REPLACE TABLE dwbq-445617.dw_silver.shippers (
    shipper_id INT64 NOT NULL,
    company_name STRING NOT NULL,
    phone STRING
);

CREATE OR REPLACE TABLE dwbq-445617.dw_silver.suppliers (
    supplier_id INT64 NOT NULL,
    company_name STRING NOT NULL,
    contact_name STRING,
    contact_title STRING,
    address STRING,
    city STRING,
    region STRING,
    postal_code STRING,
    country STRING,
    phone STRING,
    fax STRING,
    homepage STRING
);

CREATE OR REPLACE TABLE dwbq-445617.dw_silver.territories (
    territory_id STRING NOT NULL,
    territory_description STRING NOT NULL,
    region_id INT64 NOT NULL
);
