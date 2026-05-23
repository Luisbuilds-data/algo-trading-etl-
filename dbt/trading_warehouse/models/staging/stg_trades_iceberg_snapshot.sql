select *
from iceberg_scan(
    's3://cle-portfolio-etl/iceberg/trades/metadata/current.metadata.json',
    snapshot_from_id={{ var('snapshot_id_trades') }}
)
