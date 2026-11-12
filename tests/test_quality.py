from datetime import UTC, date, datetime

import pyarrow as pa
import pyarrow.parquet as pq

from gazette import quality
from gazette.silver import regles


def ecrire_bronze(racine, jour: str, source: str, nb_articles: int, nom: str = "une") -> None:
    dossier = racine / "bronze" / f"date={jour}" / source
    dossier.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"title": ["t"] * nb_articles}), dossier / f"{nom}.parquet")


def test_compter_bronze_additionne_les_fichiers_par_jour_et_source(tmp_path):
    ecrire_bronze(tmp_path, "2026-10-19", "le_monde", 3)
    ecrire_bronze(tmp_path, "2026-10-19", "le_monde", 2, nom="autre")
    ecrire_bronze(tmp_path, "2026-10-19", "liberation", 5)
    ecrire_bronze(tmp_path, "2026-10-21", "le_monde", 4)

    assert quality.compter_bronze(tmp_path) == {
        date(2026, 10, 19): {"le_monde": 5, "liberation": 5},
        date(2026, 10, 21): {"le_monde": 4},
    }


def test_compter_bronze_sans_donnees(tmp_path):
    assert quality.compter_bronze(tmp_path) == {}
    assert "n'a jamais tourné" in quality.rapport({}, ["le_monde"], date(2026, 10, 19))


def test_plages_regroupe_les_jours_consecutifs():
    jours = [date(2026, 10, 20), date(2026, 10, 22), date(2026, 10, 23), date(2026, 10, 24)]

    assert quality.plages(jours) == "2026-10-20, 2026-10-22 → 2026-10-24"


def test_rapport_dit_si_la_collecte_a_tourne_et_combien_par_source(tmp_path):
    ecrire_bronze(tmp_path, "2026-10-19", "le_monde", 3)
    ecrire_bronze(tmp_path, "2026-10-19", "liberation", 5)
    ecrire_bronze(tmp_path, "2026-10-22", "le_monde", 4)
    comptes = quality.compter_bronze(tmp_path)

    texte = quality.rapport(comptes, ["le_monde", "liberation"], date(2026, 10, 22))

    assert "Jours collectés : 2 sur 4 attendus (du 2026-10-19 au 2026-10-22)" in texte
    assert "Jours sans collecte : 2026-10-20 → 2026-10-21 (2 jours)" in texte
    assert "Collecte du jour (2026-10-22) : oui, 1 sources sur 2 (rien pour : liberation)" in texte
    assert "le_monde" in texte and "total" in texte
    ligne_monde = next(ligne for ligne in texte.splitlines() if ligne.startswith("le_monde"))
    assert ligne_monde.split() == ["le_monde", "3", "4", "7"]


def test_rapport_signale_l_absence_de_collecte_du_jour(tmp_path):
    ecrire_bronze(tmp_path, "2026-10-19", "le_monde", 3)

    texte = quality.rapport(quality.compter_bronze(tmp_path), ["le_monde"], date(2026, 10, 21))

    assert "Collecte du jour (2026-10-21) : non, aucun article" in texte
    assert "Jours collectés : 1 sur 3 attendus" in texte


def ecrire_articles(racine, jour: str, source: str, titres: list[str]) -> None:
    """Un vrai fichier bronze (toutes les colonnes lues par le rapport)."""
    dossier = racine / "bronze" / f"date={jour}" / source
    dossier.mkdir(parents=True, exist_ok=True)
    n = len(titres)
    table = pa.table(
        {
            "source": [source] * n,
            "recu_le": pa.array(
                [datetime(2026, 11, 12, 6, tzinfo=UTC)] * n, pa.timestamp("us", "UTC")
            ),
            "rang": pa.array(range(n), pa.int32()),
            "title": titres,
            "link": [f"https://exemple.fr/{source}/{i}" for i in range(n)],
            "summary": [f"Un extrait de {source} numéro {i}." for i in range(n)],
            "published": ["Thu, 12 Nov 2026 05:00:00 +0100"] * n,
            "tags": pa.array([[]] * n, pa.list_(pa.string())),
        }
    )
    pq.write_table(table, dossier / "une.parquet")


def test_rapport_regles_compte_les_rejets_par_regle_et_par_source(tmp_path):
    ecrire_articles(
        tmp_path, "2026-11-12", "le_monde", ["Horoscope du jour : Bélier", "Un titre ordinaire"]
    )
    ecrire_articles(tmp_path, "2026-11-12", "liberation", ["Un autre titre ordinaire"])

    texte = quality.rapport_regles(quality.lire_bronze(tmp_path))

    lignes = texte.splitlines()
    assert lignes[0] == "Règles d'exclusion (3 articles : 2 gardés, 1 rejetés, 33.3%)"
    assert [ligne.split()[0] for ligne in lignes[1:11]] == [i for i, _ in regles.regles()]
    horoscope = next(ligne for ligne in lignes if ligne.startswith("R02_horoscope"))
    assert horoscope.split()[-3:] == ["1", "le_monde", "1"]
    assert "Sensibilité du seuil de ressemblance" in texte


def test_rapport_regles_cherche_les_doublons_jour_par_jour(tmp_path):
    for jour in ("2026-11-11", "2026-11-12"):
        ecrire_articles(tmp_path, jour, "le_monde", ["Le même titre exactement ici"])

    texte = quality.rapport_regles(quality.lire_bronze(tmp_path))

    assert texte.splitlines()[0] == "Règles d'exclusion (2 articles : 2 gardés, 0 rejetés, 0.0%)"


def test_main_affiche_le_bilan_et_les_regles_sans_rien_ecrire(tmp_path, capsys):
    ecrire_articles(
        tmp_path / "data", "2026-11-12", "le_monde", ["Un titre ordinaire", "Un autre titre"]
    )
    avant = sorted(tmp_path.rglob("*"))

    code = quality.main(["--data-dir", str(tmp_path / "data")])

    sortie = capsys.readouterr().out
    assert code == 0
    assert "Jours collectés" in sortie and "Règles d'exclusion (2 articles" in sortie
    assert sorted(tmp_path.rglob("*")) == avant


def test_main_sans_donnees_n_affiche_pas_les_regles(tmp_path, capsys):
    quality.main(["--data-dir", str(tmp_path)])

    sortie = capsys.readouterr().out
    assert "n'a jamais tourné" in sortie and "Règles d'exclusion" not in sortie
