from pathlib import Path

import pyarrow.parquet as pq

COLONNES = ["source", "recu_le", "rang", "title", "link", "summary", "published", "tags"]


def lire_bronze(racine: Path, jour: str | None = None) -> list[dict]:
    """Les lignes du bronze, d'un jour (AAAA-MM-JJ) ou de tous, avec leur fichier d'origine.

    L'ordre est celui des fichiers triés : relire le même bronze donne la même liste.
    """
    partition = f"date={jour}" if jour else "date=*"
    lignes = []
    for fichier in sorted((racine / "bronze").glob(f"{partition}/*/*.parquet")):
        jour_du_fichier = fichier.parts[-3].removeprefix("date=")
        chemin = fichier.relative_to(racine).as_posix()
        for ligne in pq.read_table(fichier, columns=COLONNES).to_pylist():
            lignes.append({**ligne, "jour": jour_du_fichier, "fichier": chemin})
    return lignes
