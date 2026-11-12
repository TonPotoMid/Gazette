"""La base des thèmes : chaque run, ce qu'il a lu, ses réglages, ses mesures et ce qu'il a produit.

« Quel run a produit les thèmes en base ? » : chaque ligne de `themes` et `articles_themes`
porte le `run_id` qui l'a écrite, et la vue `themes_du_jour` ne montre que le dernier run réussi
de chaque jour.
"""

import json
import os
import subprocess
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from gazette.themes.base import connexion

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id          TEXT PRIMARY KEY,
    jour            TEXT NOT NULL,
    statut          TEXT NOT NULL CHECK (statut IN ('en_cours', 'reussi', 'echoue')),
    debut           TEXT NOT NULL,
    fin             TEXT,
    duree_s         REAL,
    erreur          TEXT,
    lance_par       TEXT,          -- « dagster » ou « manuel »
    dagster_run_id  TEXT,
    version_code    TEXT,
    -- données
    fenetre_debut   TEXT,
    fenetre_fin     TEXT,
    nb_articles     INTEGER,
    hash_entree     TEXT,
    -- modèle
    modele          TEXT,
    modele_revision TEXT,
    -- paramètres, métriques, artefacts (JSON)
    parametres      TEXT,
    metriques       TEXT,
    artefacts       TEXT
);

CREATE TABLE IF NOT EXISTS themes (
    run_id      TEXT NOT NULL REFERENCES runs (run_id) ON DELETE CASCADE,
    theme_id    INTEGER NOT NULL,
    jour        TEXT NOT NULL,
    nom         TEXT NOT NULL,
    mots_cles   TEXT NOT NULL,     -- JSON : liste des mots, du plus discriminant au moins
    nb_articles INTEGER NOT NULL,
    nb_sources  INTEGER NOT NULL,
    PRIMARY KEY (run_id, theme_id)
);

CREATE TABLE IF NOT EXISTS articles_themes (
    run_id   TEXT NOT NULL REFERENCES runs (run_id) ON DELETE CASCADE,
    source   TEXT NOT NULL,
    lien     TEXT NOT NULL,
    theme_id INTEGER,              -- NULL : l'article est dans le bruit
    titre    TEXT NOT NULL,
    PRIMARY KEY (run_id, source, lien)
);

CREATE INDEX IF NOT EXISTS runs_par_jour ON runs (jour, statut, fin);

CREATE VIEW IF NOT EXISTS dernier_run_du_jour AS
SELECT jour, run_id FROM (
    SELECT jour, run_id,
           ROW_NUMBER() OVER (PARTITION BY jour ORDER BY fin DESC, debut DESC) AS rang
    FROM runs WHERE statut = 'reussi'
) WHERE rang = 1;

CREATE VIEW IF NOT EXISTS themes_du_jour AS
SELECT t.* FROM themes t JOIN dernier_run_du_jour d USING (run_id);
"""


def initialiser(chemin: Path) -> None:
    with connexion(chemin) as cnx:
        cnx.executescript(SCHEMA)


def maintenant() -> datetime:
    return datetime.now(UTC)


def version_du_code() -> str | None:
    """Le commit du code qui tourne : variable d'environnement (image Docker), sinon git."""
    if version := os.environ.get("GAZETTE_VERSION_CODE"):
        return version
    try:
        return subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
            cwd=Path(__file__).resolve().parent,
            timeout=5,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None


@dataclass(frozen=True)
class Theme:
    theme_id: int
    nom: str
    mots_cles: list[str]
    nb_articles: int
    nb_sources: int


@dataclass(frozen=True)
class Affectation:
    source: str
    lien: str
    theme_id: int | None
    titre: str


class Run:
    """Un run tracé : ouvert « en_cours » dès le départ, fermé « reussi » ou « echoue »."""

    def __init__(self, chemin: Path, jour: str, dagster_run_id: str | None = None):
        initialiser(chemin)
        self.chemin = chemin
        self.jour = jour
        self.run_id = uuid.uuid4().hex
        self.debut = maintenant()
        with connexion(chemin) as cnx:
            cnx.execute(
                "INSERT INTO runs (run_id, jour, statut, debut, lance_par, dagster_run_id, "
                "version_code) VALUES (?, ?, 'en_cours', ?, ?, ?, ?)",
                (
                    self.run_id,
                    jour,
                    self.debut.isoformat(),
                    "dagster" if dagster_run_id else "manuel",
                    dagster_run_id,
                    version_du_code(),
                ),
            )

    def _duree(self, fin: datetime) -> float:
        return round((fin - self.debut).total_seconds(), 3)

    def reussir(
        self,
        *,
        donnees: dict,
        modele: str,
        modele_revision: str,
        parametres: dict,
        metriques: dict,
        artefacts: dict,
        themes: list[Theme],
        affectations: list[Affectation],
    ) -> None:
        """Écrit les thèmes, les affectations et la fiche du run d'un seul coup (une transaction)."""
        fin = maintenant()
        with connexion(self.chemin) as cnx:
            cnx.executemany(
                "INSERT INTO themes VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        self.run_id,
                        t.theme_id,
                        self.jour,
                        t.nom,
                        json.dumps(t.mots_cles, ensure_ascii=False),
                        t.nb_articles,
                        t.nb_sources,
                    )
                    for t in themes
                ],
            )
            cnx.executemany(
                "INSERT INTO articles_themes VALUES (?, ?, ?, ?, ?)",
                [(self.run_id, a.source, a.lien, a.theme_id, a.titre) for a in affectations],
            )
            cnx.execute(
                "UPDATE runs SET statut = 'reussi', fin = ?, duree_s = ?, fenetre_debut = ?, "
                "fenetre_fin = ?, nb_articles = ?, hash_entree = ?, modele = ?, "
                "modele_revision = ?, parametres = ?, metriques = ?, artefacts = ? "
                "WHERE run_id = ?",
                (
                    fin.isoformat(),
                    self._duree(fin),
                    donnees["fenetre_debut"],
                    donnees["fenetre_fin"],
                    donnees["nb_articles"],
                    donnees["hash_entree"],
                    modele,
                    modele_revision,
                    json.dumps(parametres, ensure_ascii=False),
                    json.dumps(metriques, ensure_ascii=False),
                    json.dumps(artefacts, ensure_ascii=False),
                    self.run_id,
                ),
            )

    def echouer(self, erreur: BaseException) -> None:
        fin = maintenant()
        with connexion(self.chemin) as cnx:
            cnx.execute(
                "UPDATE runs SET statut = 'echoue', fin = ?, duree_s = ?, erreur = ? "
                "WHERE run_id = ?",
                (fin.isoformat(), self._duree(fin), f"{type(erreur).__name__}: {erreur}", self.run_id),
            )
