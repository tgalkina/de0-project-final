CREATE TABLE IF NOT EXISTS STV2025061618__STAGING.currencies (
    date_update TIMESTAMP,
    currency_code INT,
    currency_code_with INT,
    currency_with_div NUMERIC(5,3)
)
ORDER BY date_update, currency_code
SEGMENTED BY HASH(date_update, currency_code) ALL NODES;