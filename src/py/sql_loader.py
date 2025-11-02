import os

def load_sql_query(file_path, **params):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    full_path = os.path.join(base_dir, file_path)
    
    with open(full_path, 'r', encoding='utf-8') as file:
        sql_content = file.read()
    
    for key, value in params.items():
        sql_content = sql_content.replace(f'{{{key}}}', str(value))
    
    return sql_content