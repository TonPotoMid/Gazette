import dagster as dg

from gazette.bronze import assets_bronze
from gazette.silver.assets import assets_silver, controles_silver
from gazette.silver.declencheurs import declencheurs_silver
from gazette.themes.assets import assets_themes
from gazette.themes.declencheurs import declencheur_themes

job_quotidien = dg.define_asset_job(
    "gazette_quotidienne",
    selection=dg.AssetSelection.groups("bronze"),
)

# Le silver n'a pas d'heure : il dépend du bronze et se lance quand le job bronze se termine.
job_silver = dg.define_asset_job(
    "gazette_silver",
    selection=dg.AssetSelection.groups("silver"),
)

# Les thèmes non plus : ils se lancent quand le silver du jour a réussi.
job_themes = dg.define_asset_job(
    "gazette_themes",
    selection=dg.AssetSelection.groups("gold"),
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
    assets=[*assets_bronze, *assets_silver, *assets_themes],
    asset_checks=controles_silver,
    jobs=[job_quotidien, job_silver, job_themes],
    schedules=[schedule_quotidien],
    sensors=[
        *declencheurs_silver(job_quotidien, job_silver),
        declencheur_themes(job_silver, job_themes),
    ],
)
