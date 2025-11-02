CREATE TABLE IF NOT EXISTS STV2025061618__STAGING.transactions (
    operation_id VARCHAR(60),
    account_number_from INT,
    account_number_to INT,
    currency_code INT,
    country VARCHAR(50),
    status VARCHAR(20),
    transaction_type VARCHAR(30),
    amount INT,
    transaction_dt TIMESTAMP
)
ORDER BY transaction_dt, operation_id
SEGMENTED BY HASH(transaction_dt, operation_id) ALL NODES;