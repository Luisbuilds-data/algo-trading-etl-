select *
from {{ source('lakehouse', 'benchmark_prices') }}
