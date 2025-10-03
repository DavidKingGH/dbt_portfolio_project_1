select * 
from {{ get_parquet_path('bronze', 'dim_items') }}

limit 100