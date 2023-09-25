WITH stg_salesorderheader AS (
    SELECT
        DISTINCT creditcardid
    FROM
        {{ source(
            'sales',
            'salesorderheader'
        ) }}
    WHERE
        creditcardid IS NOT NULL
),
stg_creditcard AS (
    SELECT
        *
    FROM
        {{ source(
            'sales',
            'creditcard'
        ) }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(['stg_salesorderheader.creditcardid']) }} AS creditcard_key,
    stg_salesorderheader.creditcardid,
    stg_creditcard.cardtype
FROM
    stg_salesorderheader
    LEFT JOIN stg_creditcard
    ON stg_salesorderheader.creditcardid = stg_creditcard.creditcardid
