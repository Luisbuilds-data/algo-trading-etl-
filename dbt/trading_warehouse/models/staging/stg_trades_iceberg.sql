select *
from {{ source('lakehouse', 'trades') }}
