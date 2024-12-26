CREATE  OR REPLACE TABLE  dwbq-445617.dw_gold.`DIM_CUSTOMERS` (
    SK_CUSTOMER INT64  NOT NULL,
    CUSTOMER_ID STRING ,
    COMPANY_NAME STRING,
    CONTACT_NAME STRING,
    CONTACT_TITLE STRING,
    ADDRESS STRING,
    CITY STRING,
    REGION STRING,
    POSTAL_CODE STRING,
    COUNTRY STRING,
    PHONE STRING,
    FAX STRING
);

CREATE OR REPLACE TABLE dwbq-445617.dw_gold.`DIM_EMPLOYEES` (
    SK_EMPLOYEE INT64 NOT NULL,
    EMPLOYEE_ID INT64 ,
    LAST_NAME STRING ,
    FIRST_NAME STRING ,
    TITLE STRING,
    TITLE_OF_COURTESY STRING,
    BIRTH_DATE DATE,
    HIRE_DATE DATE,
    ADDRESS STRING,
    CITY STRING,
    REGION STRING,
    POSTAL_CODE STRING,
    COUNTRY STRING,
    HOME_PHONE STRING,
    EXTENSION STRING,
    NOTES STRING,
    REPORTS_TO INT64,
    PHOTO_PATH STRING
);

CREATE OR REPLACE TABLE dwbq-445617.dw_gold.`DIM_ORDERS` (
    SK_ORDERS INT64 NOT NULL,
    ORDER_ID INT64 ,
    PRODUCT_ID INT64,
    UNIT_PRICE FLOAT64,
    REQUIRED_DATE DATE,
    SHIPPED_DATE DATE,
    SHIP_VIA INT64,
    SHIP_NAME STRING,
    SHIP_ADDRESS STRING,
    SHIP_CITY STRING,
    SHIP_REGION STRING,
    SHIP_POSTAL_CODE STRING,
    SHIP_COUNTRY STRING
);

CREATE OR REPLACE TABLE dwbq-445617.dw_gold.`SHIPPERS` (
    SHIPPER_ID INT64 NOT NULL,
    COMPANY_NAME STRING,
    PHONE STRING
);

CREATE OR REPLACE TABLE dwbq-445617.dw_gold.`DIM_PRODUCTS` (
    SK_PRODUCT INT64 NOT NULL,
    PRODUCT_ID INT64 ,
    PRODUCT_NAME STRING ,
    SUPPLIER_ID INT64,
    SUPPLIER_NAME STRING,
    CATEGORY_ID INT64,
    QUANTITY_PER_UNIT STRING,
    UNIT_PRICE FLOAT64,
    UNITS_IN_STOCK INT64,
    UNITS_ON_ORDER INT64,
    REORDER_LEVEL INT64,
    DISCONTINUED BOOL
);

CREATE OR REPLACE TABLE dwbq-445617.dw_gold.`CATEGORIES` (
    CATEGORY_ID INT64 NOT NULL,
    CATEGORY_NAME STRING ,
    DESCRIPTION STRING
);

CREATE OR REPLACE TABLE dwbq-445617.dw_gold.`SUPPLIERS` (
    SUPPLIER_ID INT64 NOT NULL,
    COMPANY_NAME STRING ,
    CONTACT_NAME STRING,
    CONTACT_TITLE STRING,
    ADDRESS STRING,
    CITY STRING,
    REGION STRING,
    POSTAL_CODE STRING,
    COUNTRY STRING,
    PHONE STRING,
    FAX STRING,
    HOMEPAGE STRING
);

CREATE OR REPLACE TABLE dwbq-445617.dw_gold.`DIM_TEMPO` (
    SK_DATA INT64 NOT NULL,
    DATA DATE NOT NULL,
    ANO INT64 NOT NULL,
    MES INT64 NOT NULL,
    DIA INT64 NOT NULL,
    DIA_SEMANA INT64 NOT NULL,
    DIA_ANO INT64 NOT NULL,
    ANO_BISSEXTO STRING NOT NULL,
    DIA_UTIL STRING NOT NULL,
    FIM_SEMANA STRING NOT NULL,
    FERIADO STRING NOT NULL,
    PRE_FERIADO STRING NOT NULL,
    POS_FERIADO STRING NOT NULL,
    NOME_FERIADO STRING,
    NOME_DIA_SEMANA STRING NOT NULL,
    NOME_DIA_SEMANA_ABREV STRING NOT NULL,
    NOME_MES STRING NOT NULL,
    NOME_MES_ABREV STRING NOT NULL,
    QUINZENA INT64 NOT NULL,
    BIMESTRE INT64 NOT NULL,
    TRIMESTRE INT64 NOT NULL,
    SEMESTRE INT64 NOT NULL,
    NR_SEMANA_MES INT64 NOT NULL,
    NR_SEMANA_ANO INT64 NOT NULL,
    DATA_POR_EXTENSO STRING NOT NULL,
    EVENTO STRING
);

CREATE OR REPLACE TABLE dwbq-445617.dw_gold.`FATO_ORDERS` (
    SK_ORDERS INT64 NOT NULL,
    SK_CUSTOMER INT64 NOT NULL,
    SK_EMPLOYEE INT64 NOT NULL,
    SK_DATA INT64 NOT NULL,
    SK_PRODUCT INT64 NOT NULL,
    FREIGHT FLOAT64,
    QUANTITY INT64,
    DISCOUNT FLOAT64
);

CREATE OR REPLACE TABLE dwbq-445617.dw_gold.EMPLOYEE_TERRITORIES (
    employee_id INT64 NOT NULL,
    territory_id STRING NOT NULL
);

CREATE OR REPLACE TABLE dwbq-445617.dw_gold.REGION (
    region_id INT64 NOT NULL,
    region_description STRING 
);

CREATE OR REPLACE TABLE dwbq-445617.dw_gold.TERRITORIES (
    territory_id STRING NOT NULL,
    territory_description STRING ,
    region_id INT64 NOT NULL
);
