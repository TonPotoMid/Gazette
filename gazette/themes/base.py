import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def connexion(chemin: Path) -> Iterator[sqlite3.Connection]:
    """Une connexion SQLite : validée en sortie (annulée sur erreur), puis fermée."""
    chemin.parent.mkdir(parents=True, exist_ok=True)
    cnx = sqlite3.connect(chemin)
    try:
        cnx.execute("PRAGMA foreign_keys = ON")
        with cnx:
            yield cnx
    finally:
        cnx.close()
