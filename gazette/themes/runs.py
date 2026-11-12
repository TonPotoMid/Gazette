"""Lire les runs de regroupement et les thèmes qu'ils ont produits.

    python -m gazette.themes.runs                 # les 10 derniers runs
    python -m gazette.themes.runs --jour 2026-11-12   # les thèmes en base pour ce jour, et leur run
"""

import argparse
import json
import os
import sys
from pathlib import Path

from gazette.themes.base import connexion
from gazette.themes.production import chemin_base


def derniers_runs(base: Path, nombre: int = 10) -> list[dict]:
    with connexion(base) as cnx:
        cnx.row_factory = lambda c, r: {d[0]: v for d, v in zip(c.description, r)}
        return cnx.execute(
            "SELECT run_id, jour, statut, debut, duree_s, lance_par, nb_articles, hash_entree, "
            "modele, modele_revision, metriques, erreur FROM runs ORDER BY debut DESC LIMIT ?",
            (nombre,),
        ).fetchall()


def themes_en_base(base: Path, jour: str) -> tuple[dict | None, list[dict]]:
    """Le run qui a produit les thèmes en base pour `jour`, et ces thèmes."""
    with connexion(base) as cnx:
        cnx.row_factory = lambda c, r: {d[0]: v for d, v in zip(c.description, r)}
        run = cnx.execute(
            "SELECT r.* FROM runs r JOIN dernier_run_du_jour d USING (run_id) WHERE d.jour = ?",
            (jour,),
        ).fetchone()
        themes = cnx.execute(
            "SELECT theme_id, nom, nb_articles, nb_sources FROM themes_du_jour "
            "WHERE jour = ? ORDER BY theme_id",
            (jour,),
        ).fetchall()
    return run, themes


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")  # console Windows : accents lisibles
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--jour", help="AAAA-MM-JJ : les thèmes en base et le run qui les a produits")
    parser.add_argument("--nombre", type=int, default=10)
    args = parser.parse_args()
    base = chemin_base(Path(os.environ.get("GAZETTE_DATA_DIR", "data")))
    if not base.exists():
        raise SystemExit(f"Pas de base : {base}")

    if args.jour:
        run, themes = themes_en_base(base, args.jour)
        if run is None:
            raise SystemExit(f"Aucun run réussi pour le {args.jour}.")
        print(f"Run {run['run_id']} ({run['lance_par']}, Dagster {run['dagster_run_id']})")
        print(f"  fin {run['fin']}, {run['nb_articles']} articles, entrée {run['hash_entree'][:12]}")
        print(f"  modèle {run['modele']}@{run['modele_revision']}")
        print(f"  métriques {run['metriques']}")
        for t in themes:
            print(f"  [{t['theme_id']:>2}] {t['nom']}  ({t['nb_articles']} art., {t['nb_sources']} src.)")
        return

    for r in derniers_runs(base, args.nombre):
        m = json.loads(r["metriques"] or "{}")
        print(
            f"{r['debut'][:19]}  {r['jour']}  {r['statut']:<8} {r['run_id'][:8]}  "
            f"{r['nb_articles'] or 0:>4} art.  {m.get('nb_themes', '-'):>3} thèmes  "
            f"bruit {m.get('part_bruit', '-')}  silhouette {m.get('silhouette', '-')}"
            + (f"  ERREUR {r['erreur']}" if r["erreur"] else "")
        )


if __name__ == "__main__":
    main()
