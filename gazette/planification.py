from zoneinfo import ZoneInfo

import dagster as dg

CRON_COLLECTE = "*/30 * * * *"
TAG_COLLECTE = {"gazette/collecte": "bronze"}
PARIS = ZoneInfo("Europe/Paris")


def demande_collecte(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    """La collecte de ce passage, rangée dans la partition du jour (date de Paris).

    Une partition est un jour, mais la collecte repasse toutes les 30 minutes : chaque passage
    ajoute à la même partition les articles parus depuis le précédent.
    """
    jour = context.scheduled_execution_time.astimezone(PARIS).date().isoformat()
    return dg.RunRequest(partition_key=jour)
