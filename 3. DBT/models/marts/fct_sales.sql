WITH stg_salesorderheader AS (
    SELECT
        salesorderid,
        customerid,
        creditcardid,
        shiptoaddressid,
        status AS order_status,
        CAST(
            orderdate AS DATE
        ) AS orderdate
    FROM
        {{ source(
            'sales',
            'salesorderheader'
        ) }}
),
stg_salesorderdetail AS (
    SELECT
        salesorderid,
        salesorderdetailid,
        productid,
        orderqty,
        unitprice,
        unitprice * orderqty AS revenue
    FROM
        {{ source(
            'sales',
            'salesorderdetail'
        ) }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['stg_salesorderdetail.salesorderid', 'salesorderdetailid']) }} AS sales_key,
    {{ dbt_utils.generate_surrogate_key(['productid']) }} AS product_key,
    {{ dbt_utils.generate_surrogate_key(['customerid']) }} AS customer_key,
    {{ dbt_utils.generate_surrogate_key(['creditcardid']) }} AS creditcard_key,
    {{ dbt_utils.generate_surrogate_key(['shiptoaddressid']) }} AS ship_address_key,
    {{ dbt_utils.generate_surrogate_key(['order_status']) }} AS order_status_key,
    {{ dbt_utils.generate_surrogate_key(['orderdate']) }} AS order_date_key,
    stg_salesorderdetail.salesorderid,
    stg_salesorderdetail.salesorderdetailid,
    stg_salesorderdetail.unitprice,
    stg_salesorderdetail.orderqty,
    stg_salesorderdetail.revenue
FROM
    stg_salesorderdetail
    INNER JOIN stg_salesorderheader
    ON stg_salesorderdetail.salesorderid = stg_salesorderheader.salesorderid
