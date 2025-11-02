INSERT INTO STV2025061618__STAGING.transactions 
(operation_id, account_number_from, account_number_to, currency_code, country, status, transaction_type, amount, transaction_dt)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)