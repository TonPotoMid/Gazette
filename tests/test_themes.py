import json
from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from gazette.silver.tables import SCHEMA_ARTICLES_PROPRES, chemin_table, ecrire_remplacant
from gazette.themes import declencheurs, runs
from gazette.themes.base import connexion
from gazette.themes.fenetre import lire_fenetre
from gazette.themes.parametres import charger_parametres
from gazette.themes.production import chemin_base, produire_themes
from gazette.themes.regroupement import BRUIT, mesurer, mots_du_texte, nommer, regrouper, renumeroter

JOUR = "2026-11-12"
VEILLE = "2026-11-11"
RECU = datetime(2026, 11, 12, 5, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def parametres():
    return charger_parametres()


# --- Le fichier de paramètres ---------------------------------------------------------------


def test_les_parametres_viennent_du_fichier(parametres):
    assert parametres.fenetre_heures == 24
    assert parametres.regroupement.algorithme == "hdbscan"
    assert parametres.modele.revision != "REVISION_A_FIGER"  # version du modèle figée
    assert "les" in parametres.nommage.mots_vides
    json.dumps(parametres.en_dict())  # enregistrable tel quel dans le run


def test_un_algorithme_inconnu_est_refuse(tmp_path):
    texte = Path(charger_parametres().fichier).read_text(encoding="utf-8")
    fichier = tmp_path / "clustering.yaml"
    fichier.write_text(texte.replace("algorithme: hdbscan", "algorithme: kmeans"), encoding="utf-8")
    with pytest.raises(ValueError, match="kmeans"):
        charger_parametres(fichier)


# --- La fenêtre de 24 h ----------------------------------------------------------------------


def ecrire_silver(racine, jour, articles):
    """`articles` : liste de (source, lien, titre, extrait, publie_le)."""
    lignes = [
        {
            "source": s,
            "jour": jour,
            "lien": lien,
            "titre": titre,
            "extrait": extrait,
            "rubriques": [],
            "publie_le": publie,
            "recupere_le": RECU if jour == JOUR else RECU.replace(day=11),
            "fichier_brut": "bronze/x.parquet",
            "rang_brut": i,
        }
        for i, (s, lien, titre, extrait, publie) in enumerate(articles)
    ]
    table = pa.Table.from_pylist(lignes, schema=SCHEMA_ARTICLES_PROPRES)
    tampon = pa.BufferOutputStream()
    pq.write_table(table, tampon)
    ecrire_remplacant(chemin_table(racine, "articles_propres", jour), tampon.getvalue().to_pybytes())


def h(jour, heure):
    return datetime.fromisoformat(f"{jour}T{heure:02d}:00:00+00:00")


def test_la_fenetre_garde_les_24_h_avant_la_collecte(tmp_path):
    ecrire_silver(
        tmp_path,
        VEILLE,
        [
            ("le_monde", "https://m/vieux", "Trop vieux pour la fenêtre", "x", h(VEILLE, 4)),
            ("le_monde", "https://m/hier", "Publié hier soir", "x", h(VEILLE, 20)),
        ],
    )
    ecrire_silver(
        tmp_path,
        JOUR,
        [
            ("liberation", "https://l/1", "Publié ce matin", "x", h(JOUR, 4)),
            ("liberation", "https://l/2", "Sans date de publication", "x", None),
        ],
    )

    fenetre = lire_fenetre(tmp_path, JOUR, 24)

    assert fenetre.fin == RECU and fenetre.debut == h(VEILLE, 5)
    assert [a.lien for a in fenetre.articles] == ["https://m/hier", "https://l/1", "https://l/2"]
    assert fenetre.hash_entree == lire_fenetre(tmp_path, JOUR, 24).hash_entree  # rejouable
    assert fenetre.hash_entree != lire_fenetre(tmp_path, JOUR, 1).hash_entree


def test_une_fenetre_sans_silver_du_jour_est_vide(tmp_path):
    ecrire_silver(tmp_path, VEILLE, [("le_monde", "https://m/1", "Hier", "x", h(VEILLE, 20))])
    assert lire_fenetre(tmp_path, JOUR, 24).articles == []


# --- Regrouper, nommer, mesurer --------------------------------------------------------------


def nuages(tailles, dimension=16, graine=0):
    """Des points serrés autour de centres éloignés, normalisés : un nuage = un groupe attendu."""
    rng = np.random.default_rng(graine)
    points, attendu = [], []
    for k, n in enumerate(tailles):
        centre = rng.normal(size=dimension) * 5
        points.append(centre + rng.normal(scale=0.1, size=(n, dimension)))
        attendu += [k] * n
    v = np.vstack(points)
    return v / np.linalg.norm(v, axis=1, keepdims=True), np.array(attendu)


def test_hdbscan_retrouve_les_nuages_sans_qu_on_donne_leur_nombre(parametres):
    vecteurs, attendu = nuages([8, 5, 4])

    etiquettes = regrouper(vecteurs, parametres.regroupement)

    assert sorted(set(etiquettes) - {BRUIT}) == [0, 1, 2]
    for k in range(3):  # chaque nuage tombe dans un seul groupe
        assert len(set(etiquettes[attendu == k]) - {BRUIT}) == 1
    assert list(etiquettes[:8]) == [0] * 8  # le plus gros groupe porte le numéro 0
    assert list(regrouper(vecteurs, parametres.regroupement)) == list(etiquettes)  # déterministe


def test_trop_peu_d_articles_donne_du_bruit(parametres):
    vecteurs, _ = nuages([2])
    assert list(regrouper(vecteurs, parametres.regroupement)) == [BRUIT, BRUIT]
    assert list(regrouper(np.zeros((0, 4)), parametres.regroupement)) == []


def test_renumeroter_par_taille_puis_par_apparition():
    # 2 est le plus gros ; 5 et 7 sont à égalité, 5 apparaît en premier.
    assert list(renumeroter(np.array([5, -1, 2, 2, 2, 7, 7, 5]))) == [1, -1, 0, 0, 0, 2, 2, 1]


def test_les_mots_d_un_texte_sans_mots_vides(parametres):
    mots = mots_du_texte("L'école de Brest ouvre aujourd'hui, 2026 : les élèves", parametres.nommage)
    assert mots == ["école", "brest", "ouvre", "élèves"]


def test_nommer_par_les_mots_les_plus_discriminants(parametres):
    textes = [
        "Séisme en Italie : Naples secouée",
        "Italie, un séisme près de Naples",
        "Le séisme italien fait des dégâts",
        "Le budget de la France présenté",
        "Budget : la France vise moins de déficit",
        "Ce texte est dans le bruit",
    ]
    noms = nommer(textes, np.array([0, 0, 0, 1, 1, BRUIT]), parametres.nommage)

    assert noms[0][0] == "séisme"
    assert set(noms[1][:2]) == {"budget", "france"}
    assert "bruit" not in {m for n in noms.values() for m in n}
    assert all(len(n) <= parametres.nommage.nb_mots for n in noms.values())


def test_mesurer():
    vecteurs, attendu = nuages([6, 4])
    etiquettes = attendu.copy()
    etiquettes[-1] = BRUIT

    m = mesurer(vecteurs, etiquettes)

    assert (m.nb_articles, m.nb_themes, m.taille_mediane, m.taille_max) == (10, 2, 4.5, 6)
    assert m.part_bruit == 0.1
    assert 0.9 < m.silhouette <= 1
    assert mesurer(vecteurs, np.full(10, BRUIT)).silhouette is None


# --- Un run complet, tracé -------------------------------------------------------------------


class EncodeurParMot:
    """Vecteur = présence de quelques mots : assez pour former des groupes sans vrai modèle.

    Les articles d'actualité partagent une composante « épisode » : les trois sujets sont plus
    proches entre eux que de l'article à part, qui doit finir dans le bruit.
    """

    nom = "par-mot"
    revision = "test"
    MOTS = ["séisme", "italie", "budget", "déficit", "football", "match", "épisode", "à part"]

    def encoder(self, textes):
        v = np.array(
            [[1.0 if m in t.lower() else 0.0 for m in self.MOTS] for t in textes],
            dtype=np.float32,
        )
        return v / np.linalg.norm(v, axis=1, keepdims=True)


def silver_a_trois_sujets(racine):
    sujets = {
        "seisme": "Séisme en Italie, épisode {}",
        "budget": "Budget et déficit, épisode {}",
        "foot": "Football : le match, épisode {}",
    }
    articles = [
        (source, f"https://{s}/{i}", modele.format(i), "Extrait.", h(JOUR, 3))
        for s, modele in sujets.items()
        for i, source in enumerate(["le_monde", "liberation", "franceinfo", "sud_ouest"])
    ]
    articles.append(("le_figaro", "https://bruit/1", "Un article à part", "Rien à voir.", h(JOUR, 3)))
    ecrire_silver(racine, JOUR, articles)


def lire(base, requete, *args):
    with connexion(base) as cnx:
        return cnx.execute(requete, args).fetchall()


def test_un_run_ecrit_les_themes_et_sa_fiche(tmp_path, parametres):
    silver_a_trois_sujets(tmp_path)

    resultat = produire_themes(tmp_path, JOUR, parametres, EncodeurParMot(), "dagster-123")

    base = chemin_base(tmp_path)
    assert resultat.metriques.nb_themes == 3
    assert {tuple(t.mots_cles[:1]) for t in resultat.themes} <= {
        ("séisme",), ("italie",), ("budget",), ("déficit",), ("football",), ("match",), ("épisode",),
        ("numéro",),
    }
    [run] = lire(
        base,
        "SELECT statut, lance_par, dagster_run_id, nb_articles, hash_entree, modele, "
        "modele_revision, parametres, metriques, artefacts FROM runs",
    )
    statut, lance_par, dagster_id, nb, hash_entree, modele, revision, params, metriques, artefacts = run
    assert (statut, lance_par, dagster_id, nb) == ("reussi", "dagster", "dagster-123", 13)
    assert (modele, revision) == ("par-mot", "test")
    assert len(hash_entree) == 64
    assert json.loads(params)["regroupement"]["taille_min_groupe"] == 3
    m = json.loads(metriques)
    assert m["nb_themes"] == 3 and m["part_bruit"] == round(1 / 13, 4)
    assert m["silhouette"] is not None and m["nb_vecteurs_calcules"] == 13
    assert json.loads(artefacts)["articles_themes"] == 13

    assert lire(base, "SELECT count(*), sum(nb_articles) FROM themes") == [(3, 12)]
    assert lire(base, "SELECT theme_id FROM articles_themes WHERE lien = 'https://bruit/1'") == [(None,)]
    assert lire(base, "SELECT DISTINCT nb_sources FROM themes") == [(4,)]


def test_les_themes_en_base_sont_ceux_du_dernier_run_reussi(tmp_path, parametres):
    silver_a_trois_sujets(tmp_path)
    premier = produire_themes(tmp_path, JOUR, parametres, EncodeurParMot())
    second = produire_themes(tmp_path, JOUR, parametres, EncodeurParMot())

    base = chemin_base(tmp_path)
    assert lire(base, "SELECT DISTINCT run_id FROM themes_du_jour") == [(second.run_id,)]
    assert lire(base, "SELECT count(DISTINCT run_id) FROM themes") == [(2,)]  # l'historique reste
    assert second.nb_vecteurs_calcules == 0 and second.nb_vecteurs_en_cache == 13
    [(h1,), (h2,)] = lire(base, "SELECT hash_entree FROM runs ORDER BY debut")
    assert h1 == h2  # même entrée, même empreinte

    run, themes = runs.themes_en_base(base, JOUR)
    assert run["run_id"] == second.run_id != premier.run_id
    assert [t["nb_articles"] for t in themes] == [4, 4, 4]


def test_un_run_qui_echoue_reste_trace_et_ne_remplace_pas_les_themes(tmp_path, parametres):
    silver_a_trois_sujets(tmp_path)
    bon = produire_themes(tmp_path, JOUR, parametres, EncodeurParMot())

    class EncodeurEnPanne(EncodeurParMot):
        revision = "panne"

        def encoder(self, textes):
            raise RuntimeError("modèle introuvable")

    with pytest.raises(RuntimeError):
        produire_themes(tmp_path, JOUR, parametres, EncodeurEnPanne())

    base = chemin_base(tmp_path)
    assert lire(base, "SELECT statut, erreur FROM runs WHERE statut = 'echoue'") == [
        ("echoue", "RuntimeError: modèle introuvable")
    ]
    assert lire(base, "SELECT DISTINCT run_id FROM themes_du_jour") == [(bon.run_id,)]


def test_un_jour_sans_articles_donne_un_run_sans_theme(tmp_path, parametres):
    resultat = produire_themes(tmp_path, JOUR, parametres, EncodeurParMot())

    assert resultat.nb_articles == 0 and resultat.themes == []
    assert lire(chemin_base(tmp_path), "SELECT statut, nb_articles FROM runs") == [("reussi", 0)]


# --- Dans le pipeline -------------------------------------------------------------------------


def test_demande_themes_reprend_la_partition_du_run_silver():
    demande = declencheurs.demande_themes({"dagster/partition": JOUR}, "abc123")
    assert isinstance(demande, dg.RunRequest)
    assert demande.partition_key == JOUR
    assert demande.run_key == f"themes-{JOUR}-abc123"
    assert isinstance(declencheurs.demande_themes({}, "abc123"), dg.SkipReason)


def test_le_pipeline_enchaine_silver_puis_themes():
    from gazette.definitions import defs

    graphe = defs.resolve_asset_graph()
    cle = dg.AssetKey(["gold", "themes"])
    assert dg.AssetKey(["silver", "articles_propres"]) in graphe.get(cle).parent_keys
    capteur = defs.resolve_sensor_def("themes_apres_silver")
    assert capteur.job_name == "gazette_themes"
    assert defs.resolve_job_def("gazette_themes")
