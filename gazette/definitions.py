import dagster as dg

from gazette.bronze import assets_bronze
from gazette.planification import CRON_COLLECTE, TAG_COLLECTE, demande_collecte
from gazette.silver.assets import assets_silver, controles_silver
from gazette.silver.declencheurs import declencheurs_silver
from gazette.surveillance import capteurs_alertes

job_quotidien = dg.define_asset_job(
    "gazette_quotidienne",
    selection=dg.AssetSelection.groups("bronze"),
    # Une seule collecte à la fois (tag_concurrency_limits dans dagster.yaml) : deux runs en
    # parallèle liraient les mêmes articles « déjà connus » et écriraient des doublons.
    tags=TAG_COLLECTE,
)

# Le silver n'a pas d'heure : il dépend du bronze et se lance quand le job bronze se termine.
job_silver = dg.define_asset_job(
    "gazette_silver",
    selection=dg.AssetSelection.groups("silver"),
)

# Toutes les 30 minutes, heure de Paris. Chaque passage tombe dans la partition du jour et
# n'y ajoute que les articles jamais vus (le bronze ignore ceux déjà écrits).
schedule_collecte = dg.ScheduleDefinition(
    name="gazette_collecte_30min",
    job=job_quotidien,
    cron_schedule=CRON_COLLECTE,
    execution_timezone="Europe/Paris",
    execution_fn=demande_collecte,
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=[*assets_bronze, *assets_silver],
    asset_checks=controles_silver,
    jobs=[job_quotidien, job_silver],
    schedules=[schedule_collecte],
    sensors=[*declencheurs_silver(job_quotidien, job_silver), *capteurs_alertes],
)
