SELECT 
    operation_id, 
    account_number_from, 
    account_number_to, 
    currency_code, 
    country, 
    status, 
    transaction_type, 
    amount, 
    transaction_dt
FROM public.transactions 
WHERE transaction_dt::DATE = '{target_date}'