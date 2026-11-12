import dagster as dg
from dagster import DagsterEventType

from gazette import alertes, definitions, surveillance
from gazette.silver import assets as silver_assets
from gazette.silver import controles


@dg.asset
def asset_casse():
    raise ValueError("la collecte a explosé : cause racine")


def lancer_silver(tmp_path, monkeypatch, instance, taux_max=0.8):
    from tests.test_silver import bronze_du_jour

    monkeypatch.setattr(silver_assets, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(silver_assets, "RAPPORTS_DIR", tmp_path / "rapports")
    monkeypatch.setattr(controles, "TAUX_REJET_MAX", taux_max)
    bronze_du_jour(tmp_path / "data")
    job = definitions.defs.resolve_job_def("gazette_silver")
    return job.execute_in_process(partition_key="2026-11-12", instance=instance)


def run_en_echec(instance):
    resultat = dg.materialize(
        [asset_casse], instance=instance, partition_key=None, raise_on_error=False
    )
    return resultat.dagster_run


def contexte_d_echec(instance, run):
    return dg.build_run_status_sensor_context(
        sensor_name="alerte_run_en_echec",
        dagster_instance=instance,
        dagster_run=run,
        dagster_event=evenements(instance, run.run_id, DagsterEventType.RUN_FAILURE)[0],
    ).for_run_failure()


def evenements(instance, run_id, type_):
    return [e.dagster_event for e in instance.all_logs(run_id, of_type=type_)]


# --- Les contrôles faux d'un run ---------------------------------------------------------------


def test_controles_en_echec_est_vide_quand_tout_passe(tmp_path, monkeypatch):
    with dg.DagsterInstance.ephemeral() as instance:
        resultat = lancer_silver(tmp_path, monkeypatch, instance)

        assert surveillance.controles_en_echec(instance, resultat.run_id) == []


def test_controles_en_echec_donne_le_controle_et_son_resume(tmp_path, monkeypatch):
    with dg.DagsterInstance.ephemeral() as instance:
        resultat = lancer_silver(tmp_path, monkeypatch, instance, taux_max=0.01)

        lignes = surveillance.controles_en_echec(instance, resultat.run_id)

    assert (
        resultat.success
    )  # le run est réussi : sans ce capteur, personne ne verrait le contrôle faux
    assert lignes == [
        "silver/articles_propres · taux_de_rejet_par_source : au-dessus de 1.0% : le_monde 50.0%"
    ]


# --- Les étapes en échec -------------------------------------------------------------------------


def test_etapes_en_echec_donne_la_cause_racine_sur_une_ligne():
    with dg.DagsterInstance.ephemeral() as instance:
        run = run_en_echec(instance)
        etapes = surveillance.etapes_en_echec(
            evenements(instance, run.run_id, DagsterEventType.STEP_FAILURE)
        )

    assert etapes == [("asset_casse", "ValueError: la collecte a explosé : cause racine")]


# --- Les capteurs, de bout en bout ---------------------------------------------------------------


def test_le_capteur_de_run_en_echec_envoie_une_alerte(monkeypatch):
    envoyes = []
    monkeypatch.setattr(
        alertes, "envoyer", lambda message, url=None: envoyes.append(message) or True
    )
    monkeypatch.setenv("GAZETTE_DAGSTER_URL", "http://dagster.test:3000/")

    with dg.DagsterInstance.ephemeral() as instance:
        run = run_en_echec(instance)
        contexte = contexte_d_echec(instance, run)
        surveillance.alerte_run_en_echec(contexte)

    [message] = envoyes
    assert message.startswith("🚨 **Gazette : run en échec**")
    assert "`asset_casse` : ValueError: la collecte a explosé" in message
    assert message.endswith(f"http://dagster.test:3000/runs/{run.run_id}")


def test_le_capteur_de_controles_envoie_une_alerte_seulement_si_un_controle_est_faux(
    tmp_path, monkeypatch
):
    envoyes = []
    monkeypatch.setattr(
        alertes, "envoyer", lambda message, url=None: envoyes.append(message) or True
    )

    def evaluer(instance, resultat):
        contexte = dg.build_run_status_sensor_context(
            sensor_name="alerte_controles_en_echec",
            dagster_instance=instance,
            dagster_run=instance.get_run_by_id(resultat.run_id),
            dagster_event=evenements(instance, resultat.run_id, DagsterEventType.RUN_SUCCESS)[0],
        )
        surveillance.alerte_controles_en_echec(contexte)

    with dg.DagsterInstance.ephemeral() as instance:
        evaluer(instance, lancer_silver(tmp_path, monkeypatch, instance))
        assert envoyes == []  # tout est vrai : pas d'alerte

        evaluer(instance, lancer_silver(tmp_path, monkeypatch, instance, taux_max=0.01))

    [message] = envoyes
    assert message.startswith("⚠️ **Gazette : contrôle de qualité en échec**")
    assert "Job `gazette_silver`, jour 2026-11-12" in message
    assert "taux_de_rejet_par_source : au-dessus de 1.0% : le_monde 50.0%" in message


def test_une_alerte_qui_ne_part_pas_ne_casse_pas_le_capteur(monkeypatch):
    monkeypatch.setattr(alertes, "envoyer", lambda message, url=None: False)

    with dg.DagsterInstance.ephemeral() as instance:
        run = run_en_echec(instance)
        contexte = contexte_d_echec(instance, run)

        surveillance.alerte_run_en_echec(contexte)  # ne lève rien


def test_les_capteurs_d_alerte_sont_dans_les_definitions_et_actifs():
    capteurs = {s.name: s for s in definitions.defs.sensors}

    for nom in ("alerte_run_en_echec", "alerte_controles_en_echec"):
        assert capteurs[nom].default_status == dg.DefaultSensorStatus.RUNNING
