import dagster as dg

from gazette.bronze import assets_bronze
from gazette.partitions import partitions_quotidiennes

job_quotidien = dg.define_asset_job(
    "gazette_quotidienne",
    selection=dg.AssetSelection.groups("bronze"),
    partitions_def=partitions_quotidiennes,
)

# 6 h, heure de Paris (fuseau repris de la partition) : deux heures de marge
# avant l'échéance de 8 h pour les relances et un éventuel rejeu manuel.
schedule_quotidien = dg.build_schedule_from_partitioned_job(
    job_quotidien,
    hour_of_day=6,
    minute_of_hour=0,
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=assets_bronze,
    jobs=[job_quotidien],
    schedules=[schedule_quotidien],
)
