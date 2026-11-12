from datetime import UTC, datetime, timedelta

import dagster as dg
import pytest

from gazette import definitions
from gazette.silver import assets as silver_assets
from gazette.silver import controles
from gazette.silver.article import Article
from gazette.silver.passage import Passage
from gazette.silver.regles import Rejet
from gazette.silver.tables import ecrire_tables

JOUR = "2026-11-12"
RECU = datetime(2026, 11, 12, 6, 0, tzinfo=UTC)


def propre(
    source="le_monde", titre="Un titre ordinaire sans rien de particulier", **champs
) -> dict:
    return {
        "source": source,
        "titre": titre,
        "publie_le": RECU - timedelta(hours=1),
        "recupere_le": RECU,
        **champs,
    }


def rejete(source="le_monde") -> dict:
    return {"source": source, "regle": "R10_doublon_approx"}


# --- Les quatre faits, sur des lignes de tables ------------------------------------------------


def test_aucune_date_future_passe_sur_des_dates_passees_ou_absentes():
    verdict = controles.aucune_date_future([propre(), propre(publie_le=None)], [])

    assert verdict.passe
    assert verdict.metriques == {"nb_dates_futures": 0, "nb_articles_verifies": 2}


def test_aucune_date_future_echoue_et_cite_un_exemple():
    futur = propre(
        source="sud_ouest", titre="Un article daté de demain", publie_le=RECU + timedelta(days=1)
    )

    verdict = controles.aucune_date_future([propre(), futur], [])

    assert not verdict.passe
    assert verdict.metriques["nb_dates_futures"] == 1
    assert "sud_ouest « Un article daté de demain »" in verdict.resume


def test_aucun_titre_vide():
    assert controles.aucun_titre_vide([propre()], []).passe

    verdict = controles.aucun_titre_vide([propre(), propre(source="liberation", titre="  ")], [])

    assert not verdict.passe
    assert verdict.metriques["nb_titres_vides"] == 1
    assert "liberation" in verdict.resume


def test_taux_de_rejet_passe_sous_le_seuil_et_donne_le_taux_de_chaque_source():
    propres = [propre("le_monde")] * 9 + [propre("liberation")] * 6
    rejets = [rejete("le_monde"), rejete("liberation"), rejete("liberation")]

    verdict = controles.taux_de_rejet_par_source(propres, rejets)

    assert verdict.passe
    assert verdict.metriques["taux_par_source"] == {"le_monde": 0.1, "liberation": 0.25}
    assert (verdict.metriques["source_max"], verdict.metriques["taux_max"]) == ("liberation", 0.25)
    assert "maximum : liberation 25.0%" in verdict.resume


def test_taux_de_rejet_echoue_au_dessus_du_seuil_et_nomme_la_source():
    propres = [propre("la_depeche")] + [propre("le_monde")] * 9
    rejets = [rejete("la_depeche")] * 9  # 9 rejetés sur 10 : 90 %

    verdict = controles.taux_de_rejet_par_source(propres, rejets)

    assert not verdict.passe
    assert verdict.resume == "au-dessus de 80.0% : la_depeche 90.0%"


def test_taux_de_rejet_egal_au_seuil_passe():
    propres = [propre()] * 2
    rejets = [rejete()] * 8  # 80 % pile : « ne dépasse pas »

    assert controles.taux_de_rejet_par_source(propres, rejets).passe


def test_taux_de_rejet_suit_le_seuil_reglable(monkeypatch):
    monkeypatch.setattr(controles, "TAUX_REJET_MAX", 0.01)

    verdict = controles.taux_de_rejet_par_source([propre()] * 9, [rejete()])

    assert not verdict.passe and verdict.metriques["seuil"] == 0.01


def test_taux_de_rejet_sans_article_ne_dit_rien():
    assert controles.taux_de_rejet_par_source([], []).passe


def test_au_moins_un_article_entre():
    assert controles.au_moins_un_article_entre([propre()], []).passe
    assert controles.au_moins_un_article_entre([], [rejete()]).passe

    verdict = controles.au_moins_un_article_entre([], [])

    assert not verdict.passe and verdict.metriques == {"nb_entres": 0}


# --- Sur les tables réellement écrites ---------------------------------------------------------


def article(source, lien, titre, **champs) -> Article:
    base = {
        "jour": JOUR,
        "extrait": f"Extrait de {source} {lien}",
        "rubriques": (),
        "publie_le": RECU - timedelta(hours=1),
        "recupere_le": RECU,
        "fichier_brut": "bronze/x.parquet",
        "rang_brut": 0,
    }
    return Article(source=source, lien=lien, titre=titre, **{**base, **champs})


def ecrire_un_jour(racine, gardes, rejets=()):
    passage = Passage(JOUR, [*gardes, *(r.article for r in rejets)], list(gardes), list(rejets))
    ecrire_tables(racine, passage)


def test_verifier_lit_les_tables_ecrites_par_silver(tmp_path):
    gardes = [article("le_monde", "https://m/1", "Le gouvernement présente son budget annuel")]
    quiz = article("la_depeche", "https://d/1", "QUIZ. Testez vos connaissances en histoire")
    ecrire_un_jour(tmp_path, gardes, [Rejet(quiz, "R04_jeux")])

    verdict = controles.verifier("taux_de_rejet_par_source", tmp_path, JOUR)

    assert not verdict.passe  # la_depeche : 1 rejeté sur 1
    assert verdict.metriques["taux_par_source"] == {"la_depeche": 1.0, "le_monde": 0.0}
    assert controles.verifier("aucune_date_future", tmp_path, JOUR).passe


def test_verifier_sans_tables_echoue_franchement(tmp_path):
    with pytest.raises(FileNotFoundError):
        controles.verifier("aucun_titre_vide", tmp_path, JOUR)


# --- Dans Dagster : les contrôles tournent après le silver, dans le même job ---------------------


def lancer_silver(tmp_path, monkeypatch, bronze_vide=False):
    from tests.test_silver import bronze_du_jour

    monkeypatch.setattr(silver_assets, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(silver_assets, "RAPPORTS_DIR", tmp_path / "rapports")
    if not bronze_vide:
        bronze_du_jour(tmp_path / "data")
    job = definitions.defs.resolve_job_def("gazette_silver")
    with dg.DagsterInstance.ephemeral() as instance:
        return job.execute_in_process(partition_key=JOUR, instance=instance)


def verdicts(resultat) -> dict[str, dg.AssetCheckEvaluation]:
    return {e.check_name: e for e in resultat.get_asset_check_evaluations()}


def test_le_job_silver_lance_les_quatre_controles_apres_l_asset(tmp_path, monkeypatch):
    resultat = lancer_silver(tmp_path, monkeypatch)

    assert resultat.success
    evaluations = verdicts(resultat)
    assert set(evaluations) == set(controles.CONTROLES)
    assert all(e.passed for e in evaluations.values())
    assert (
        evaluations["taux_de_rejet_par_source"].metadata["resume"].value.startswith("aucune source")
    )
    ordre = [e.step_key for e in resultat.get_step_success_events()]
    assert ordre[0] == "silver_articles"  # l'asset d'abord, les contrôles ensuite


def test_un_taux_trop_haut_fait_echouer_le_controle_sans_casser_le_run(tmp_path, monkeypatch):
    monkeypatch.setattr(controles, "TAUX_REJET_MAX", 0.01)  # le_monde : 1 rejeté sur 2

    resultat = lancer_silver(tmp_path, monkeypatch)

    assert (
        resultat.success
    )  # le fait est faux, mais le run s'est déroulé : c'est l'alerte qui prévient
    evaluation = verdicts(resultat)["taux_de_rejet_par_source"]
    assert not evaluation.passed
    assert evaluation.severity == dg.AssetCheckSeverity.ERROR
    assert "le_monde" in evaluation.metadata["resume"].value


def test_un_jour_sans_bronze_est_signale_par_un_controle(tmp_path, monkeypatch):
    resultat = lancer_silver(tmp_path, monkeypatch, bronze_vide=True)

    assert resultat.success
    evaluations = verdicts(resultat)
    assert not evaluations["au_moins_un_article_entre"].passed
    assert evaluations["aucune_date_future"].passed and evaluations["aucun_titre_vide"].passed
