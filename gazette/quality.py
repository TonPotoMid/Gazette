import argparse
import os
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from gazette import pollueurs
from gazette.bronze.sources import charger_sources

COLONNES = ["source", "recu_le", "title", "link", "summary", "published", "tags"]


def compter_bronze(racine: Path) -> dict[date, dict[str, int]]:
    """Articles écrits dans le bronze, par jour puis par source (slug)."""
    comptes: dict[date, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for fichier in (racine / "bronze").glob("date=*/*/*.parquet"):
        jour = date.fromisoformat(fichier.parts[-3].removeprefix("date="))
        comptes[jour][fichier.parts[-2]] += pq.ParquetFile(fichier).metadata.num_rows
    return {jour: dict(par_source) for jour, par_source in sorted(comptes.items())}


def lire_bronze(racine: Path) -> list[dict]:
    """Tous les articles du bronze, avec leur jour de collecte."""
    lignes = []
    for fichier in sorted((racine / "bronze").glob("date=*/*/*.parquet")):
        jour = fichier.parts[-3].removeprefix("date=")
        for ligne in pq.read_table(fichier, columns=COLONNES).to_pylist():
            lignes.append({**ligne, "jour": jour})
    return lignes


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


def rapport_pollueurs(lignes: list[dict]) -> str:
    """Les dix pollueurs comptés sur le bronze, en texte, pour le terminal."""
    mesures = pollueurs.mesurer(lignes)
    largeur = max(len(m.nom) for m in mesures)
    out = [f"Les dix pollueurs ({len(lignes)} articles)"]
    for m in mesures:
        par_source = ", ".join(
            f"{s} {n}" for s, n in Counter(x["source"] for x in m.lignes).most_common()
        )
        groupes = f"({len(m.groupes)} groupes) " if m.groupes else ""
        detail = f"{groupes}{par_source}"
        out.append(f"{m.numero:>2}  {m.nom.ljust(largeur)}  {len(m.lignes):>4}  {detail}".rstrip())

    out += ["", "Sensibilité du seuil de ressemblance entre deux titres (part de mots en commun)"]
    out.append("seuil  groupes  articles  sources max")
    out += [
        f"{seuil:>5.0%}  {groupes:>7}  {articles:>8}  {max_sources:>11}"
        for seuil, groupes, articles, max_sources in pollueurs.sensibilite(lignes)
    ]
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    parseur = argparse.ArgumentParser(
        description="Bilan de collecte et dix pollueurs du bronze (affichage seulement)"
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
        print(rapport_pollueurs(lire_bronze(args.data_dir)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
