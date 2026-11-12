import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import numpy as np

from gazette.themes.base import connexion
from gazette.themes.parametres import ParametresModele


class Encodeur(Protocol):
    """Ce dont la vectorisation a besoin : un nom, une révision, et des textes → vecteurs."""

    nom: str
    revision: str

    def encoder(self, textes: Sequence[str]) -> np.ndarray: ...


class ModeleSentenceTransformers:
    """Un modèle sentence-transformers, chargé au premier appel, sur CPU."""

    def __init__(self, parametres: ParametresModele):
        self.nom = parametres.nom
        self.revision = parametres.revision
        self._parametres = parametres
        self._modele = None

    def encoder(self, textes: Sequence[str]) -> np.ndarray:
        if self._modele is None:
            from sentence_transformers import SentenceTransformer

            self._modele = SentenceTransformer(self.nom, revision=self.revision, device="cpu")
        vecteurs = self._modele.encode(
            list(textes),
            batch_size=self._parametres.taille_lot,
            normalize_embeddings=self._parametres.normaliser,
            show_progress_bar=False,
            convert_to_numpy=True,
        )
        return np.asarray(vecteurs, dtype=np.float32)


def texte_a_vectoriser(titre: str, extrait: str, champs: Sequence[str] = ("titre", "extrait")) -> str:
    valeurs = {"titre": titre, "extrait": extrait}
    return "\n".join(valeurs[c] for c in champs if valeurs[c])


def cle_texte(texte: str) -> str:
    return hashlib.sha256(texte.encode("utf-8")).hexdigest()


class CacheVecteurs:
    """Les vecteurs déjà calculés, par (modèle, révision, empreinte du texte), dans SQLite.

    La clé est le texte et non le lien : un article dont le texte change est revectorisé,
    et un changement de modèle ou de révision ne réutilise jamais d'anciens vecteurs.
    """

    def __init__(self, chemin: Path):
        chemin.parent.mkdir(parents=True, exist_ok=True)
        self.chemin = chemin
        with self._connexion() as cnx:
            cnx.execute(
                """CREATE TABLE IF NOT EXISTS vecteurs (
                    modele TEXT NOT NULL,
                    revision TEXT NOT NULL,
                    cle TEXT NOT NULL,
                    dimension INTEGER NOT NULL,
                    vecteur BLOB NOT NULL,
                    PRIMARY KEY (modele, revision, cle)
                )"""
            )

    def _connexion(self):
        return connexion(self.chemin)

    def lire(self, modele: str, revision: str, cles: Sequence[str]) -> dict[str, np.ndarray]:
        trouves: dict[str, np.ndarray] = {}
        with self._connexion() as cnx:
            uniques = list(dict.fromkeys(cles))
            for i in range(0, len(uniques), 500):  # limite de variables d'une requête SQLite
                lot = uniques[i : i + 500]
                trous = ",".join("?" * len(lot))
                for cle, dim, blob in cnx.execute(
                    f"SELECT cle, dimension, vecteur FROM vecteurs "
                    f"WHERE modele = ? AND revision = ? AND cle IN ({trous})",
                    [modele, revision, *lot],
                ):
                    trouves[cle] = np.frombuffer(blob, dtype=np.float32, count=dim)
        return trouves

    def ecrire(self, modele: str, revision: str, vecteurs: dict[str, np.ndarray]) -> None:
        with self._connexion() as cnx:
            cnx.executemany(
                "INSERT OR REPLACE INTO vecteurs VALUES (?, ?, ?, ?, ?)",
                [
                    (modele, revision, cle, v.shape[0], v.astype(np.float32).tobytes())
                    for cle, v in vecteurs.items()
                ],
            )


@dataclass(frozen=True)
class Vectorisation:
    vecteurs: np.ndarray  # une ligne par texte, dans l'ordre reçu
    nb_calcules: int
    nb_en_cache: int


def vectoriser(textes: Sequence[str], encodeur: Encodeur, cache: CacheVecteurs) -> Vectorisation:
    """Les vecteurs des textes : ceux du cache, et les autres calculés puis mis en cache."""
    cles = [cle_texte(t) for t in textes]
    connus = cache.lire(encodeur.nom, encodeur.revision, cles)
    a_calculer = {c: t for c, t in zip(cles, textes) if c not in connus}  # un texte répété : une fois
    if a_calculer:
        nouveaux = encodeur.encoder(list(a_calculer.values()))
        calcules = dict(zip(a_calculer, nouveaux))
        cache.ecrire(encodeur.nom, encodeur.revision, calcules)
        connus = {**connus, **calcules}
    if not textes:
        return Vectorisation(np.zeros((0, 0), dtype=np.float32), 0, 0)
    vecteurs = np.vstack([connus[c] for c in cles]).astype(np.float32)
    return Vectorisation(vecteurs, len(a_calculer), len(set(cles)) - len(a_calculer))
