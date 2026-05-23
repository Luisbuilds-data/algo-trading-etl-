select *
from {{ source('lakehouse', 'wazuh_alerts') }}
