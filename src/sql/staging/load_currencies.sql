SELECT 
    date_update, 
    currency_code, 
    currency_code_with, 
    currency_with_div
FROM public.currencies 
WHERE date_update::DATE = '{target_date}'