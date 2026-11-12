from dataclasses import dataclass
from pathlib import Path

from gazette.silver import regles
from gazette.silver.article import Article, depuis_bronze
from gazette.silver.lecture import lire_bronze


@dataclass(frozen=True)
class Passage:
    """Un jour de silver : ce qui est entré, ce qui est gardé, ce qui est rejeté et pourquoi."""

    jour: str
    entres: list[Article]
    gardes: list[Article]
    rejets: list[regles.Rejet]


def traiter_jour(racine: Path, jour: str, ordre_sources: tuple[str, ...] = ()) -> Passage:
    """Normalise le bronze d'un jour et lui applique les règles d'exclusion."""
    entres = [depuis_bronze(ligne) for ligne in lire_bronze(racine, jour)]
    resultat = regles.appliquer_regles(entres, ordre_sources)
    return Passage(jour, entres, resultat.gardes, resultat.rejets)
