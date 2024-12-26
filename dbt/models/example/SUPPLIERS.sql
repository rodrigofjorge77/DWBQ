
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (

    SELECT SUPPLIER_ID, COMPANY_NAME, CONTACT_NAME, CONTACT_TITLE, FAX, CITY, PHONE, REGION, ADDRESS, COUNTRY, HOMEPAGE, POSTAL_CODE
    FROM dwbq-445617.dw_bronze.SUPPLIERS

)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
