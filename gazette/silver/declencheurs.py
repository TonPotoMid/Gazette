from collections.abc import Mapping

import dagster as dg


def demande_silver(tags: Mapping[str, str], run_id: str) -> dg.RunRequest | dg.SkipReason:
    """Le silver du jour que le bronze vient de traiter ; rien si le run n'a pas de partition."""
    partition = tags.get("dagster/partition")
    if not partition:
        return dg.SkipReason("Run du bronze sans partition : rien à traiter côté silver.")
    return dg.RunRequest(run_key=f"silver-{partition}-{run_id}", partition_key=partition)


def declencheurs_silver(job_bronze, job_silver) -> list[dg.SensorDefinition]:
    """Lance le silver dès que le job bronze se termine, réussi ou non.

    Un run bronze échoue dès qu'une seule source échoue : sans le capteur « échec », une source
    morte empêcherait de traiter les sept autres.
    """

    def construire(statut: dg.DagsterRunStatus) -> dg.SensorDefinition:
        @dg.run_status_sensor(
            name=f"silver_apres_bronze_{statut.value.lower()}",
            run_status=statut,
            monitored_jobs=[job_bronze],
            request_job=job_silver,
            default_status=dg.DefaultSensorStatus.RUNNING,
        )
        def _declencheur(context: dg.RunStatusSensorContext):
            return demande_silver(context.dagster_run.tags, context.dagster_run.run_id)

        return _declencheur

    return [construire(dg.DagsterRunStatus.SUCCESS), construire(dg.DagsterRunStatus.FAILURE)]
