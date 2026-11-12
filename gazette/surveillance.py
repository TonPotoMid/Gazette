import os

import dagster as dg
from dagster import DagsterEvent, DagsterEventType, DagsterInstance

from gazette import alertes

LIMITE_ERREUR = 300  # caractères d'un message d'erreur dans l'alerte


def url_dagster() -> str:
    return os.environ.get("GAZETTE_DAGSTER_URL", "http://localhost:3000").rstrip("/")


def etapes_en_echec(evenements: list[DagsterEvent]) -> list[tuple[str, str]]:
    """(étape, cause) pour chaque étape en échec : la cause racine, sur une ligne."""
    etapes = []
    for evenement in evenements:
        erreur = evenement.event_specific_data.error
        while erreur is not None and erreur.cause is not None:
            erreur = erreur.cause
        texte = (erreur.message if erreur else "").strip()
        premiere_ligne = texte.splitlines()[0] if texte else "erreur inconnue"
        etapes.append((evenement.step_key or "?", premiere_ligne[:LIMITE_ERREUR]))
    return etapes


def controles_en_echec(instance: DagsterInstance, run_id: str) -> list[str]:
    """Les contrôles de qualité faux d'un run : « asset · contrôle : résumé »."""
    lignes = []
    for entree in instance.all_logs(run_id, of_type=DagsterEventType.ASSET_CHECK_EVALUATION):
        evaluation = entree.dagster_event.event_specific_data
        if evaluation.passed:
            continue
        resume = evaluation.metadata.get("resume")
        ligne = f"{'/'.join(evaluation.asset_key.path)} · {evaluation.check_name}"
        lignes.append(f"{ligne} : {resume.value}" if resume is not None else ligne)
    return lignes


@dg.run_failure_sensor(name="alerte_run_en_echec", default_status=dg.DefaultSensorStatus.RUNNING)
def alerte_run_en_echec(context: dg.RunFailureSensorContext):
    """Prévient dès qu'un run de la gazette (bronze ou silver) échoue."""
    run = context.dagster_run
    message = alertes.message_run_en_echec(
        run.job_name,
        run.tags.get("dagster/partition"),
        run.run_id,
        etapes_en_echec(context.get_step_failure_events()),
        url_dagster(),
    )
    if not alertes.envoyer(message):
        context.log.warning("Alerte de run en échec non envoyée (voir les journaux).")


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    name="alerte_controles_en_echec",
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def alerte_controles_en_echec(context: dg.RunStatusSensorContext):
    """Prévient quand un run s'est bien déroulé mais qu'un contrôle de qualité est faux.

    Un contrôle faux ne fait pas échouer le run : sans ce capteur, personne ne le verrait.
    """
    run = context.dagster_run
    controles = controles_en_echec(context.instance, run.run_id)
    if not controles:
        return
    message = alertes.message_controles_en_echec(
        run.job_name, run.tags.get("dagster/partition"), run.run_id, controles, url_dagster()
    )
    if not alertes.envoyer(message):
        context.log.warning("Alerte de contrôle en échec non envoyée (voir les journaux).")


capteurs_alertes = [alerte_run_en_echec, alerte_controles_en_echec]
