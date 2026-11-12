from datetime import UTC, datetime, timedelta

import dagster as dg
import pyarrow as pa
import pyarrow.parquet as pq

from gazette.silver import assets as silver_assets
from gazette.silver import declencheurs, rapport, regles
from gazette.silver.article import Article
from gazette.silver.lecture import lire_bronze
from gazette.silver.passage import traiter_jour
from gazette.silver.tables import chemin_table, ecrire_tables

RECU = datetime(2026, 11, 12, 6, 0, tzinfo=UTC)
JOUR = "2026-11-12"


def ecrire_bronze(racine, jour, source, articles, nom="une"):
    """Un fichier bronze : `articles` est une liste de (titre, lien, extrait)."""
    dossier = racine / "bronze" / f"date={jour}" / source
    dossier.mkdir(parents=True, exist_ok=True)
    n = len(articles)
    table = pa.table(
        {
            "source": [source] * n,
            "recu_le": pa.array([RECU] * n, pa.timestamp("us", "UTC")),
            "rang": pa.array(range(n), pa.int32()),
            "title": [a[0] for a in articles],
            "link": [a[1] for a in articles],
            "summary": [a[2] for a in articles],
            "published": ["Thu, 12 Nov 2026 05:00:00 +0100"] * n,
            "tags": pa.array([[]] * n, pa.list_(pa.string())),
        }
    )
    pq.write_table(table, dossier / f"{nom}.parquet")


def bronze_du_jour(racine):
    ecrire_bronze(
        racine,
        JOUR,
        "le_monde",
        [
            ("Le gouvernement présente son budget annuel", "https://m/1", "Un extrait du Monde."),
            ("Horoscope du jour : Bélier et Taureau", "https://m/2", "Prévisions pour la semaine."),
        ],
    )
    ecrire_bronze(
        racine,
        JOUR,
        "liberation",
        [("Un séisme de magnitude six secoue la côte", "https://l/1", "Un extrait de Libération.")],
    )


def lignes_table(racine, nom, jour=JOUR):
    return pq.read_table(chemin_table(racine, nom, jour)).to_pylist()


# --- Lecture et traitement d'un jour ------------------------------------------------------------


def test_lire_bronze_un_jour_donne_le_fichier_et_le_rang(tmp_path):
    bronze_du_jour(tmp_path)
    ecrire_bronze(
        tmp_path, "2026-11-11", "le_monde", [("Un titre de la veille", "https://m/0", "x")]
    )

    lignes = lire_bronze(tmp_path, JOUR)

    assert sorted(x["link"] for x in lignes) == ["https://l/1", "https://m/1", "https://m/2"]
    assert {x["jour"] for x in lignes} == {JOUR}
    seconde = next(x for x in lignes if x["link"] == "https://m/2")
    assert seconde["fichier"] == f"bronze/date={JOUR}/le_monde/une.parquet"
    assert seconde["rang"] == 1
    assert len(lire_bronze(tmp_path)) == 4  # sans jour : tous les jours


def test_traiter_jour_separe_gardes_et_rejets(tmp_path):
    bronze_du_jour(tmp_path)

    passage = traiter_jour(tmp_path, JOUR)

    assert len(passage.entres) == 3
    assert {a.lien for a in passage.gardes} == {"https://m/1", "https://l/1"}
    assert [(r.article.lien, r.regle) for r in passage.rejets] == [("https://m/2", "R02_horoscope")]


def test_traiter_un_jour_sans_bronze(tmp_path):
    passage = traiter_jour(tmp_path, JOUR)

    assert (passage.entres, passage.gardes, passage.rejets) == ([], [], [])


# --- Les tables ------------------------------------------------------------------------------


def test_tables_gardent_la_tracabilite_et_la_regle_de_rejet(tmp_path):
    bronze_du_jour(tmp_path)

    chemins = ecrire_tables(tmp_path, traiter_jour(tmp_path, JOUR))

    assert (
        chemins["articles_propres"]
        == tmp_path / "silver/articles_propres/date=2026-11-12/articles_propres.parquet"
    )
    propres = {x["lien"]: x for x in lignes_table(tmp_path, "articles_propres")}
    assert set(propres) == {"https://m/1", "https://l/1"}
    assert propres["https://m/1"]["fichier_brut"] == f"bronze/date={JOUR}/le_monde/une.parquet"
    assert propres["https://m/1"]["rang_brut"] == 0
    assert propres["https://m/1"]["publie_le"] == datetime(2026, 11, 12, 4, 0, tzinfo=UTC)
    [rejet] = lignes_table(tmp_path, "rejets")
    assert (rejet["lien"], rejet["regle"], rejet["rang_brut"]) == (
        "https://m/2",
        "R02_horoscope",
        1,
    )
    assert rejet["doublon_de_lien"] is None


def test_rejets_gardent_l_exemplaire_conserve_pour_un_doublon(tmp_path):
    titre = "Un immeuble s'effondre à New York et fait deux morts"
    ecrire_bronze(tmp_path, JOUR, "le_figaro", [(titre, "https://f/1", "Extrait du Figaro.")])
    ecrire_bronze(tmp_path, JOUR, "liberation", [(titre + " !", "https://l/1", "Extrait de Libé.")])

    ecrire_tables(tmp_path, traiter_jour(tmp_path, JOUR))

    [rejet] = lignes_table(tmp_path, "rejets")
    assert rejet["regle"] == "R09_doublon_exact"
    assert (rejet["doublon_de_source"], rejet["doublon_de_lien"]) == ("le_figaro", "https://f/1")


def test_un_jour_vide_ecrit_quand_meme_des_tables_vides(tmp_path):
    ecrire_tables(tmp_path, traiter_jour(tmp_path, JOUR))

    assert lignes_table(tmp_path, "articles_propres") == []
    assert lignes_table(tmp_path, "rejets") == []
    schema = pq.read_schema(chemin_table(tmp_path, "rejets", JOUR))
    assert "regle" in schema.names and "doublon_de_lien" in schema.names


def test_relancer_un_jour_ne_cree_pas_de_doublon(tmp_path):
    bronze_du_jour(tmp_path)
    ecrire_tables(tmp_path, traiter_jour(tmp_path, JOUR))
    premiere = lignes_table(tmp_path, "articles_propres"), lignes_table(tmp_path, "rejets")

    ecrire_tables(tmp_path, traiter_jour(tmp_path, JOUR))

    assert (
        lignes_table(tmp_path, "articles_propres"),
        lignes_table(tmp_path, "rejets"),
    ) == premiere
    assert not list((tmp_path / "silver").rglob("*.tmp"))  # pas de fichier temporaire oublié


def test_relancer_un_jour_prend_en_compte_un_nouveau_fichier_bronze(tmp_path):
    bronze_du_jour(tmp_path)
    ecrire_tables(tmp_path, traiter_jour(tmp_path, JOUR))
    ecrire_bronze(
        tmp_path,
        JOUR,
        "sud_ouest",
        [("Une nouvelle information arrivée plus tard", "https://s/1", "Extrait Sud Ouest.")],
        nom="plus_tard",
    )

    ecrire_tables(tmp_path, traiter_jour(tmp_path, JOUR))

    assert {x["lien"] for x in lignes_table(tmp_path, "articles_propres")} == {
        "https://m/1",
        "https://l/1",
        "https://s/1",
    }


# --- Le rapport ------------------------------------------------------------------------------


def article(source, lien, titre, **champs):
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


def test_rapport_dit_entres_gardes_rejetes_par_regle_et_par_source(tmp_path):
    bronze_du_jour(tmp_path)

    texte = rapport.construire_rapport(traiter_jour(tmp_path, JOUR), ["le_monde", "liberation"])

    assert texte.startswith("# Rapport de qualité du 2026-11-12")
    assert "| 3 | 2 | 1 | 33.3% |" in texte  # entrés, gardés, rejetés, taux
    assert "| le_monde | 2 | 1 | 1 | 50.0% |" in texte
    assert "| liberation | 1 | 1 | 0 | 0.0% |" in texte
    assert "| R02_horoscope | 1 | 0 | 1 |" in texte  # une ligne par règle, une colonne par source
    assert "| R01_publi | 0 | 0 | 0 |" in texte  # les règles sans rejet figurent aussi


def test_rapport_classe_les_cinq_rejets_les_plus_frequents_et_montre_la_part_de_la_source():
    entres = [
        article("la_depeche", f"https://d/{i}", f"Quiz numéro {i} sur la culture") for i in range(4)
    ]
    entres += [article("le_monde", "https://m/1", "Horoscope du jour : Bélier")]
    entres += [
        article("le_monde", f"https://m/{i}", f"Un article ordinaire numéro {i} ici")
        for i in range(2, 6)
    ]
    resultat = regles.appliquer_regles(entres)
    from gazette.silver.passage import Passage

    texte = rapport.construire_rapport(
        Passage(JOUR, entres, resultat.gardes, resultat.rejets), ["la_depeche", "le_monde"]
    )

    bloc = texte.split("## Les 5 rejets les plus fréquents")[1].split("##")[0]
    lignes = [
        ligne for ligne in bloc.splitlines() if ligne.startswith("| R0") or ligne.startswith("| R1")
    ]
    assert lignes[0] == "| R04_jeux | la_depeche | 4 | 100.0% |"  # une règle trop gourmande se voit
    assert lignes[1] == "| R02_horoscope | le_monde | 1 | 20.0% |"


def test_rapport_limite_les_exemples_et_cite_l_exemplaire_garde_d_un_doublon():
    entres = [
        article("le_monde", f"https://m/{i}", f"Quiz numéro {i} sur la culture") for i in range(5)
    ]
    original = article("le_figaro", "https://f/1", "Le même titre exactement ici")
    copie = article("liberation", "https://l/1", "Le même titre exactement ici !", publie_le=RECU)
    resultat = regles.appliquer_regles([*entres, original, copie])
    from gazette.silver.passage import Passage

    texte = rapport.construire_rapport(
        Passage(JOUR, [*entres, original, copie], resultat.gardes, resultat.rejets), []
    )

    jeux = texte.split("### R04_jeux (5)")[1].split("###")[0]
    assert jeux.count("\n- ") == rapport.EXEMPLES_PAR_REGLE
    assert (
        "- liberation : « Le même titre exactement ici ! » "
        "(doublon de le_figaro : « Le même titre exactement ici »)" in texte
    )


def test_rapport_sans_rejet_et_sans_article():
    from gazette.silver.passage import Passage

    propre = article("le_monde", "https://m/1", "Un article ordinaire sans rien de particulier")
    sans_rejet = rapport.construire_rapport(Passage(JOUR, [propre], [propre], []), ["le_monde"])
    vide = rapport.construire_rapport(Passage(JOUR, [], [], []), ["le_monde"])

    assert "Aucun rejet." in sans_rejet and "## Exemples de rejets" not in sans_rejet
    assert "| 0 | 0 | 0 | 0.0% |" in vide
    assert "Aucun article collecté ce jour-là." in vide


def test_rapport_echappe_les_barres_verticales_des_titres():
    from gazette.silver.passage import Passage

    entre = article("le_monde", "https://m/1", "Quiz | culture générale pour tous")
    resultat = regles.appliquer_regles([entre])

    texte = rapport.construire_rapport(Passage(JOUR, [entre], [], resultat.rejets), ["le_monde"])

    assert "Quiz \\| culture" in texte


def test_ecrire_rapport_nomme_le_fichier_par_jour_et_le_remplace(tmp_path):
    bronze_du_jour(tmp_path)
    passage = traiter_jour(tmp_path, JOUR)

    premier = rapport.ecrire_rapport(tmp_path / "rapports", passage, ["le_monde", "liberation"])
    contenu = premier.read_text(encoding="utf-8")
    second = rapport.ecrire_rapport(tmp_path / "rapports", passage, ["le_monde", "liberation"])

    assert premier == second == tmp_path / "rapports" / "qualite-2026-11-12.md"
    assert second.read_text(encoding="utf-8") == contenu
    assert [p.name for p in (tmp_path / "rapports").iterdir()] == ["qualite-2026-11-12.md"]


# --- L'asset Dagster -------------------------------------------------------------------------


def test_asset_silver_ecrit_les_tables_et_le_rapport(tmp_path, monkeypatch):
    monkeypatch.setattr(silver_assets, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(silver_assets, "RAPPORTS_DIR", tmp_path / "rapports")
    bronze_du_jour(tmp_path / "data")

    resultat = dg.materialize([silver_assets.silver_articles], partition_key=JOUR)

    assert resultat.success
    propres = resultat.asset_materializations_for_node("silver_articles")
    metadonnees = {m.asset_key: m.metadata for m in propres}
    meta = metadonnees[silver_assets.CLE_ARTICLES_PROPRES]
    assert (meta["nb_entres"].value, meta["nb_gardes"].value, meta["nb_rejetes"].value) == (3, 2, 1)
    assert meta["rejets_par_regle"].value == {"R02_horoscope": 1}
    assert set(metadonnees) == {silver_assets.CLE_ARTICLES_PROPRES, silver_assets.CLE_REJETS}
    assert len(lignes_table(tmp_path / "data", "articles_propres")) == 2
    assert (tmp_path / "rapports" / "qualite-2026-11-12.md").exists()


def test_asset_silver_sur_un_jour_sans_bronze_reussit_avec_des_sorties_vides(tmp_path, monkeypatch):
    monkeypatch.setattr(silver_assets, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(silver_assets, "RAPPORTS_DIR", tmp_path / "rapports")

    resultat = dg.materialize([silver_assets.silver_articles], partition_key=JOUR)

    assert resultat.success
    assert lignes_table(tmp_path / "data", "articles_propres") == []
    assert "Aucun article collecté" in (tmp_path / "rapports" / "qualite-2026-11-12.md").read_text(
        encoding="utf-8"
    )


# --- Le déclenchement après le bronze ----------------------------------------------------------


def test_demande_silver_reprend_la_partition_du_run_bronze():
    demande = declencheurs.demande_silver({"dagster/partition": JOUR}, "abc123")

    assert isinstance(demande, dg.RunRequest)
    assert demande.partition_key == JOUR
    assert demande.run_key == f"silver-{JOUR}-abc123"


def test_demande_silver_ignore_un_run_sans_partition():
    assert isinstance(declencheurs.demande_silver({}, "abc123"), dg.SkipReason)


def test_les_definitions_relient_silver_au_bronze_et_le_declenchent_apres_lui():
    from gazette import definitions

    graphe = definitions.defs.resolve_asset_graph()
    amont = graphe.get(silver_assets.CLE_ARTICLES_PROPRES).parent_keys
    assert amont == set(silver_assets.CLES_BRONZE) and len(amont) == 8

    capteurs = {s.name: s for s in definitions.defs.sensors}
    declencheurs = {"silver_apres_bronze_success", "silver_apres_bronze_failure"}
    assert declencheurs <= set(capteurs)
    assert all(
        capteurs[nom].default_status == dg.DefaultSensorStatus.RUNNING for nom in declencheurs
    )
    assert {j.name for j in definitions.defs.jobs} == {"gazette_quotidienne", "gazette_silver"}
