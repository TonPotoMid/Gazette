"""Lecture de sources.csv : un journal par slug, un ou plusieurs flux RSS par journal."""

import csv
import os
from dataclasses import dataclass
from pathlib import Path

SOURCES_CSV = Path(
    os.environ.get("GAZETTE_SOURCES_CSV", Path(__file__).resolve().parents[2] / "docs" / "sources.csv")
)


@dataclass(frozen=True)
class Flux:
    nom: str
    url: str


@dataclass(frozen=True)
class Source:
    slug: str
    nom: str
    type: str
    region: str
    flux: tuple[Flux, ...]


def charger_sources(chemin: Path = SOURCES_CSV) -> list[Source]:
    lignes_par_slug: dict[str, list[dict[str, str]]] = {}
    with open(chemin, newline="", encoding="utf-8") as f:
        for ligne in csv.DictReader(f):
            ligne = {cle: valeur.strip() for cle, valeur in ligne.items()}
            lignes_par_slug.setdefault(ligne["slug"], []).append(ligne)

    return [
        Source(
            slug=slug,
            nom=lignes[0]["nom"],
            type=lignes[0]["type"],
            region=lignes[0]["region"],
            flux=tuple(Flux(nom=l["flux"], url=l["url"]) for l in lignes),
        )
        for slug, lignes in lignes_par_slug.items()
    ]
