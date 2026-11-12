import os
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow.parquet as pq

from gazette.silver.tables import chemin_table

# Réglable pour tester l'alerte : GAZETTE_TAUX_REJET_MAX=0.01 fait échouer le contrôle des taux.
TAUX_REJET_MAX = float(os.environ.get("GAZETTE_TAUX_REJET_MAX", "0.8"))
EXEMPLES = 3


@dataclass(frozen=True)
class Verdict:
    nom: str
    passe: bool
    resume: str  # une phrase lisible, reprise telle quelle dans l'alerte
    metriques: dict = field(default_factory=dict)


def _pct(taux: float) -> str:
    return f"{taux:.1%}"


def aucune_date_future(propres: list[dict], rejets: list[dict]) -> Verdict:
    futures = [x for x in propres if x["publie_le"] and x["publie_le"] > x["recupere_le"]]
    metriques = {"nb_dates_futures": len(futures), "nb_articles_verifies": len(propres)}
    if not futures:
        return Verdict(
            "aucune_date_future", True, "aucune date de publication dans le futur", metriques
        )
    exemples = ", ".join(f"{x['source']} « {x['titre'][:50]} »" for x in futures[:EXEMPLES])
    return Verdict(
        "aucune_date_future",
        False,
        f"{len(futures)} article(s) daté(s) du futur : {exemples}",
        metriques,
    )


def aucun_titre_vide(propres: list[dict], rejets: list[dict]) -> Verdict:
    vides = [x for x in propres if not x["titre"].strip()]
    metriques = {"nb_titres_vides": len(vides), "nb_articles_verifies": len(propres)}
    if not vides:
        return Verdict("aucun_titre_vide", True, "aucun titre vide", metriques)
    sources = ", ".join(sorted({x["source"] for x in vides}))
    return Verdict(
        "aucun_titre_vide", False, f"{len(vides)} titre(s) vide(s) ({sources})", metriques
    )


def taux_de_rejet_par_source(propres: list[dict], rejets: list[dict]) -> Verdict:
    """Aucune source ne doit avoir plus de TAUX_REJET_MAX de ses articles rejetés."""
    entres = Counter(x["source"] for x in propres) + Counter(x["source"] for x in rejets)
    rejetes = Counter(x["source"] for x in rejets)
    taux = {source: rejetes[source] / n for source, n in sorted(entres.items())}
    metriques = {
        "taux_par_source": {s: round(t, 4) for s, t in taux.items()},
        "seuil": TAUX_REJET_MAX,
    }
    if not taux:
        return Verdict("taux_de_rejet_par_source", True, "aucun article à évaluer", metriques)
    source_max = max(taux, key=lambda s: taux[s])
    metriques |= {"taux_max": round(taux[source_max], 4), "source_max": source_max}
    depassent = {s: t for s, t in taux.items() if t > TAUX_REJET_MAX}
    if not depassent:
        resume = (
            f"aucune source au-dessus de {_pct(TAUX_REJET_MAX)} "
            f"(maximum : {source_max} {_pct(taux[source_max])})"
        )
        return Verdict("taux_de_rejet_par_source", True, resume, metriques)
    detail = ", ".join(f"{s} {_pct(t)}" for s, t in depassent.items())
    return Verdict(
        "taux_de_rejet_par_source",
        False,
        f"au-dessus de {_pct(TAUX_REJET_MAX)} : {detail}",
        metriques,
    )


def au_moins_un_article_entre(propres: list[dict], rejets: list[dict]) -> Verdict:
    entres = len(propres) + len(rejets)
    metriques = {"nb_entres": entres}
    if entres:
        return Verdict("au_moins_un_article_entre", True, f"{entres} articles entrés", metriques)
    return Verdict(
        "au_moins_un_article_entre", False, "aucun article dans le bronze de ce jour", metriques
    )


CONTROLES: dict[str, Callable[[list[dict], list[dict]], Verdict]] = {
    f.__name__: f
    for f in (
        aucune_date_future,
        aucun_titre_vide,
        taux_de_rejet_par_source,
        au_moins_un_article_entre,
    )
}


def verifier(nom: str, racine: Path, jour: str) -> Verdict:
    """Lance un contrôle sur les tables silver déjà écrites pour ce jour (les données réelles)."""
    propres = pq.read_table(chemin_table(racine, "articles_propres", jour)).to_pylist()
    rejets = pq.read_table(chemin_table(racine, "rejets", jour)).to_pylist()
    return CONTROLES[nom](propres, rejets)
