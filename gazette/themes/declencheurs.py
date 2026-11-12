from collections.abc import Mapping

import dagster as dg


def demande_themes(tags: Mapping[str, str], run_id: str) -> dg.RunRequest | dg.SkipReason:
    """Les thèmes du jour que le silver vient de nettoyer ; rien si le run n'a pas de partition."""
    partition = tags.get("dagster/partition")
    if not partition:
        return dg.SkipReason("Run du silver sans partition : pas de thèmes à calculer.")
    return dg.RunRequest(run_key=f"themes-{partition}-{run_id}", partition_key=partition)


def declencheur_themes(job_silver, job_themes) -> dg.SensorDefinition:
    """Lance le regroupement dès que le silver a réussi : bronze → silver → thèmes, sans main.

    Seulement sur un succès : sans articles propres à jour, les thèmes du jour seraient faux,
    et ceux du dernier run réussi restent en base.
    """

    @dg.run_status_sensor(
        name="themes_apres_silver",
        run_status=dg.DagsterRunStatus.SUCCESS,
        monitored_jobs=[job_silver],
        request_job=job_themes,
        default_status=dg.DefaultSensorStatus.RUNNING,
    )
    def _declencheur(context: dg.RunStatusSensorContext):
        return demande_themes(context.dagster_run.tags, context.dagster_run.run_id)

    return _declencheur
