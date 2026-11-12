import math
import re
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np
from sklearn.cluster import HDBSCAN
from sklearn.metrics import silhouette_score

from gazette.themes.parametres import ParametresNommage, ParametresRegroupement

BRUIT = -1


def regrouper(vecteurs: np.ndarray, parametres: ParametresRegroupement) -> np.ndarray:
    """Un numéro de groupe par vecteur (HDBSCAN), -1 pour le bruit.

    Les groupes sont renumérotés du plus gros au plus petit (0 = le plus gros), puis par
    première apparition : la numérotation ne dépend que des données.
    """
    n = len(vecteurs)
    if n < max(parametres.taille_min_groupe, 2):
        return np.full(n, BRUIT, dtype=int)
    etiquettes = HDBSCAN(
        min_cluster_size=parametres.taille_min_groupe,
        min_samples=parametres.echantillons_min,
        metric=parametres.metrique,
        cluster_selection_method=parametres.methode_selection,
        copy=True,
    ).fit_predict(vecteurs)
    return renumeroter(etiquettes)


def renumeroter(etiquettes: np.ndarray) -> np.ndarray:
    tailles = Counter(int(e) for e in etiquettes if e != BRUIT)
    premiere = {}
    for i, e in enumerate(etiquettes):
        premiere.setdefault(int(e), i)
    ordre = sorted(tailles, key=lambda e: (-tailles[e], premiere[e]))
    nouveau = {ancien: i for i, ancien in enumerate(ordre)}
    return np.array([nouveau.get(int(e), BRUIT) for e in etiquettes], dtype=int)


# --- Nommer : les mots les plus discriminants de chaque groupe (c-TF-IDF) --------------------


def mots_du_texte(texte: str, parametres: ParametresNommage) -> list[str]:
    mots = re.findall(r"[^\W\d_]+(?:['’-][^\W\d_]+)*", texte.lower())
    mots = [m.split("'")[-1].split("’")[-1] for m in mots]  # « l'école » → « école »
    return [m for m in mots if len(m) >= parametres.longueur_min_mot and m not in parametres.mots_vides]


def nommer(
    textes: Sequence[str], etiquettes: np.ndarray, parametres: ParametresNommage
) -> dict[int, list[str]]:
    """Pour chaque groupe, ses `nb_mots` mots au plus fort c-TF-IDF.

    Chaque groupe est vu comme un seul document. Un mot pèse d'autant plus qu'il est fréquent
    dans le groupe (tf) et rare dans les autres (idf = log(1 + A / fréquence totale du mot),
    A = nombre moyen de mots par groupe). À égalité, l'ordre alphabétique départage.
    """
    comptes: dict[int, Counter] = {}
    for texte, e in zip(textes, etiquettes):
        if e != BRUIT:
            comptes.setdefault(int(e), Counter()).update(mots_du_texte(texte, parametres))
    if not comptes:
        return {}
    total_par_mot = Counter()
    for c in comptes.values():
        total_par_mot.update(c)
    moyenne = sum(total_par_mot.values()) / len(comptes)

    noms = {}
    for e, c in comptes.items():
        taille = sum(c.values()) or 1
        score = {m: (n / taille) * math.log(1 + moyenne / total_par_mot[m]) for m, n in c.items()}
        noms[e] = sorted(score, key=lambda m: (-score[m], m))[: parametres.nb_mots]
    return noms


# --- Mesurer --------------------------------------------------------------------------------


@dataclass(frozen=True)
class Metriques:
    nb_articles: int
    nb_themes: int
    taille_mediane: float | None
    taille_max: int | None
    part_bruit: float | None
    silhouette: float | None  # cosinus, sur les articles classés ; None s'il y a moins de 2 thèmes


def mesurer(vecteurs: np.ndarray, etiquettes: np.ndarray) -> Metriques:
    n = len(etiquettes)
    classes = etiquettes != BRUIT
    tailles = sorted(Counter(int(e) for e in etiquettes[classes]).values())
    nb_themes = len(tailles)
    silhouette = None
    if 2 <= nb_themes < int(classes.sum()):
        silhouette = round(
            float(silhouette_score(vecteurs[classes], etiquettes[classes], metric="cosine")), 4
        )
    return Metriques(
        nb_articles=n,
        nb_themes=nb_themes,
        taille_mediane=float(np.median(tailles)) if tailles else None,
        taille_max=tailles[-1] if tailles else None,
        part_bruit=round(float((~classes).sum()) / n, 4) if n else None,
        silhouette=silhouette,
    )
