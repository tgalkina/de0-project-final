INSERT INTO STV2025061618__DWH.global_metrics
SELECT 
    DATE(t.transaction_dt) as date_update,
    t.currency_code as currency_from,
    SUM(
        CASE 
            WHEN t.currency_code = 420 THEN t.amount
            ELSE t.amount / c.currency_with_div
        END
    ) as amount_total,
    
    COUNT(t.operation_id) as cnt_transactions,
    ROUND(COUNT(t.operation_id) * 1.0 / COUNT(DISTINCT t.account_number_from), 2) as avg_transactions_per_account,
    COUNT(DISTINCT t.account_number_from) as cnt_accounts_make_transactions
    
FROM STV2025061618__STAGING.transactions t
LEFT JOIN STV2025061618__STAGING.currencies c 
    ON t.currency_code = c.currency_code 
    AND c.currency_code_with = 420
    AND DATE(t.transaction_dt) = DATE(c.date_update)
    
WHERE t.account_number_from > 0
    AND t.account_number_to > 0
    AND t.status = 'done'
    AND t.transaction_type NOT IN ('authorization', 'authorization_commission')
    AND DATE(t.transaction_dt) = '{target_date}'
    
GROUP BY DATE(t.transaction_dt), t.currency_code;