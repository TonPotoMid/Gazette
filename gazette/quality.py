import argparse
import os
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from gazette.bronze.sources import charger_sources
from gazette.silver import regles
from gazette.silver.article import Article, depuis_bronze
from gazette.silver.lecture import lire_bronze

SEUILS_SENSIBILITE = (0.8, 0.7, 0.6)


def compter_bronze(racine: Path) -> dict[date, dict[str, int]]:
    """Articles écrits dans le bronze, par jour puis par source (slug)."""
    comptes: dict[date, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for fichier in (racine / "bronze").glob("date=*/*/*.parquet"):
        jour = date.fromisoformat(fichier.parts[-3].removeprefix("date="))
        comptes[jour][fichier.parts[-2]] += pq.ParquetFile(fichier).metadata.num_rows
    return {jour: dict(par_source) for jour, par_source in sorted(comptes.items())}


def plages(jours: list[date]) -> str:
    """Les jours consécutifs sont regroupés : '2026-10-20, 2026-10-22 → 2026-11-11'."""
    groupes: list[list[date]] = []
    for jour in jours:
        if groupes and jour - groupes[-1][-1] == timedelta(days=1):
            groupes[-1].append(jour)
        else:
            groupes.append([jour])
    return ", ".join(
        g[0].isoformat() if len(g) == 1 else f"{g[0].isoformat()} → {g[-1].isoformat()}"
        for g in groupes
    )


def synthese_collecte(
    comptes: dict[date, dict[str, int]], slugs: list[str], aujourdhui: date
) -> tuple[list[str], list[str]]:
    """Les trois lignes « a-t-elle tourné ? » et la liste des sources (connues + inattendues)."""
    premier, dernier = min(comptes), max(max(comptes), aujourdhui)
    attendus = [premier + timedelta(days=n) for n in range((dernier - premier).days + 1)]
    manquants = [jour for jour in attendus if jour not in comptes]
    inconnues = sorted({s for par_source in comptes.values() for s in par_source} - set(slugs))
    sources = slugs + inconnues

    lignes = [
        f"Jours collectés : {len(comptes)} sur {len(attendus)} attendus "
        f"(du {premier} au {dernier})",
        f"Jours sans collecte : {plages(manquants) if manquants else 'aucun'} "
        f"({len(manquants)} jours)",
    ]
    du_jour = comptes.get(aujourdhui, {})
    if du_jour:
        actives = [s for s in sources if du_jour.get(s)]
        absentes = [s for s in sources if s not in actives]
        lignes.append(
            f"Collecte du jour ({aujourdhui}) : oui, {len(actives)} sources sur {len(sources)}"
            + (f" (rien pour : {', '.join(absentes)})" if absentes else "")
        )
    else:
        lignes.append(f"Collecte du jour ({aujourdhui}) : non, aucun article")
    return lignes, sources


def rapport(comptes: dict[date, dict[str, int]], slugs: list[str], aujourdhui: date) -> str:
    """Le bilan de collecte en texte, pour le terminal."""
    if not comptes:
        return "Aucun article dans le bronze : la collecte n'a jamais tourné ici."

    resume, sources = synthese_collecte(comptes, slugs, aujourdhui)
    jours = list(comptes)
    largeur = max(len(s) for s in [*sources, "total"])
    lignes = [*resume, "", "Articles par source et par jour"]
    lignes.append("source".ljust(largeur) + "".join(f"  {j}" for j in jours) + "    total")
    for source in sources:
        valeurs = [comptes[j].get(source, 0) for j in jours]
        lignes.append(
            source.ljust(largeur)
            + "".join(f"  {v:>{len(str(j))}}" for v, j in zip(valeurs, jours, strict=True))
            + f"  {sum(valeurs):>7}"
        )
    totaux = [sum(comptes[j].values()) for j in jours]
    lignes.append(
        "total".ljust(largeur)
        + "".join(f"  {t:>{len(str(j))}}" for t, j in zip(totaux, jours, strict=True))
        + f"  {sum(totaux):>7}"
    )
    return "\n".join(lignes)


def articles_par_jour(lignes: list[dict]) -> dict[str, list[Article]]:
    """Les articles du bronze, normalisés, rangés par jour de collecte (une partition silver)."""
    par_jour: dict[str, list[Article]] = defaultdict(list)
    for ligne in lignes:
        par_jour[ligne["jour"]].append(depuis_bronze(ligne))
    return dict(sorted(par_jour.items()))


def rapport_regles(lignes: list[dict], ordre_sources: tuple[str, ...] = ()) -> str:
    """Les règles d'exclusion appliquées au bronze, jour par jour : rejets par règle et source."""
    par_jour = articles_par_jour(lignes)
    rejets = [
        rejet
        for articles in par_jour.values()
        for rejet in regles.appliquer_regles(articles, ordre_sources).rejets
    ]
    total = len(lignes)
    taux = len(rejets) / total if total else 0
    out = [
        f"Règles d'exclusion ({total} articles : {total - len(rejets)} gardés, "
        f"{len(rejets)} rejetés, {taux:.1%})"
    ]
    nom_max = max(len(nom) for _, nom in regles.regles())
    for identifiant, nom in regles.regles():
        concernes = [r for r in rejets if r.regle == identifiant]
        par_source = ", ".join(
            f"{s} {n}" for s, n in Counter(r.article.source for r in concernes).most_common()
        )
        out.append(
            f"{identifiant:<20}  {nom.ljust(nom_max)}  {len(concernes):>4}  {par_source}".rstrip()
        )

    out += ["", "Sensibilité du seuil de ressemblance entre deux titres (part de mots en commun)"]
    out.append("seuil  groupes  articles  sources max")
    for seuil in SEUILS_SENSIBILITE:
        groupes = [
            [articles[p] for p in groupe]
            for articles in par_jour.values()
            for groupe in regles.titres_proches(articles, seuil)
        ]
        sources_max = max((len({a.source for a in g}) for g in groupes), default=0)
        out.append(
            f"{seuil:>5.0%}  {len(groupes):>7}  {sum(len(g) for g in groupes):>8}  "
            f"{sources_max:>11}"
        )
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    parseur = argparse.ArgumentParser(
        description="Bilan de collecte et règles d'exclusion sur le bronze (affichage seulement)"
    )
    parseur.add_argument(
        "--data-dir",
        type=Path,
        default=Path(os.environ.get("GAZETTE_DATA_DIR", "data")),
        help="racine des données (par défaut $GAZETTE_DATA_DIR, sinon ./data)",
    )
    args = parseur.parse_args(argv)

    aujourdhui = datetime.now(ZoneInfo("Europe/Paris")).date()
    slugs = [source.slug for source in charger_sources()]
    comptes = compter_bronze(args.data_dir)
    print(rapport(comptes, slugs, aujourdhui))
    if comptes:
        print()
        print(rapport_regles(lire_bronze(args.data_dir), tuple(slugs)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
