WITH stg_customer AS (
    SELECT
        customerid,
        personid,
        storeid
    FROM
        {{ source(
            'sales',
            'customer'
        ) }}
),
stg_person AS (
    SELECT
        businessentityid,
        CONCAT(COALESCE(firstname, ''), ' ', COALESCE(middlename, ''), ' ', COALESCE(lastname, '')) AS fullname
    FROM
        {{ source(
            'person',
            'person'
        ) }}
),
stg_store AS (
    SELECT
        businessentityid AS storebusinessentityid,
        NAME
    FROM
        {{ source(
            'sales',
            'store'
        ) }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['stg_customer.customerid']) }} AS customer_key,
    stg_customer.customerid,
    stg_person.businessentityid,
    stg_person.fullname,
    stg_store.storebusinessentityid,
    stg_store.name
FROM
    stg_customer
    LEFT JOIN stg_person
    ON stg_customer.personid = stg_person.businessentityid
    LEFT JOIN stg_store
    ON stg_customer.storeid = stg_store.storebusinessentityid
