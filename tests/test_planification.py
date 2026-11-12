from datetime import datetime
from zoneinfo import ZoneInfo

import dagster as dg

from gazette import definitions
from gazette.planification import PARIS, demande_collecte


def contexte(quand: datetime) -> dg.ScheduleEvaluationContext:
    return dg.build_schedule_context(scheduled_execution_time=quand)


def test_la_collecte_vise_la_partition_du_jour_de_paris():
    demande = demande_collecte(contexte(datetime(2026, 11, 12, 14, 30, tzinfo=PARIS)))
    assert demande.partition_key == "2026-11-12"


def test_juste_apres_minuit_a_paris_la_collecte_change_de_partition():
    # 23 h 30 UTC le 12 = 00 h 30 à Paris le 13.
    demande = demande_collecte(contexte(datetime(2026, 11, 12, 23, 30, tzinfo=ZoneInfo("UTC"))))
    assert demande.partition_key == "2026-11-13"


def test_le_schedule_tourne_toutes_les_30_minutes_a_l_heure_de_paris():
    schedule = definitions.schedule_collecte
    assert schedule.cron_schedule == "*/30 * * * *"
    assert schedule.execution_timezone == "Europe/Paris"
    assert schedule.default_status == dg.DefaultScheduleStatus.RUNNING
    assert schedule.job_name == "gazette_quotidienne"
    assert definitions.job_quotidien.tags["gazette/collecte"] == "bronze"
