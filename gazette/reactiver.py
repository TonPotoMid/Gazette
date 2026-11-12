"""Réveille une source mise en sommeil après trois échecs permanents de suite.

python -m gazette.reactiver <slug> [flux]
docker compose exec code python -m gazette.reactiver le_monde
"""

import argparse
import os
import sys
from pathlib import Path

from gazette.bronze.etat import reactiver


def main(argv: list[str] | None = None, racine: Path | None = None) -> int:
    parseur = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parseur.add_argument("slug", help="source à réactiver (colonne slug de sources.csv)")
    parseur.add_argument("flux", nargs="?", help="un seul flux ; par défaut tous ceux de la source")
    args = parseur.parse_args(argv)

    racine = racine or Path(os.environ.get("GAZETTE_DATA_DIR", "data"))
    reveilles = reactiver(racine, args.slug, args.flux)
    if not reveilles:
        print(f"Rien à réactiver pour {args.slug} : aucun flux connu.", file=sys.stderr)
        return 1
    print(f"Réactivés : {', '.join(reveilles)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
