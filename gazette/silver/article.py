from dataclasses import dataclass
from datetime import datetime

from gazette.silver.normalisation import borner_extrait, date_en_utc, nettoyer_texte


@dataclass(frozen=True)
class Article:
    """Un article du bronze, normalisé : ce que les règles d'exclusion lisent."""

    source: str
    jour: str  # jour de collecte (la partition), AAAA-MM-JJ
    lien: str
    titre: str
    extrait: str
    rubriques: tuple[str, ...]
    publie_le: datetime | None  # UTC ; None si la date manque ou est illisible
    recupere_le: datetime


def depuis_bronze(ligne: dict) -> Article:
    """Normalise une ligne du bronze : texte nettoyé, extrait borné, dates en UTC."""
    return Article(
        source=ligne["source"],
        jour=ligne["jour"],
        lien=(ligne["link"] or "").strip(),
        titre=nettoyer_texte(ligne["title"]),
        extrait=borner_extrait(nettoyer_texte(ligne["summary"])),
        rubriques=tuple(
            rubrique for rubrique in map(nettoyer_texte, ligne["tags"] or []) if rubrique
        ),
        publie_le=date_en_utc(ligne["published"]),
        recupere_le=ligne["recu_le"],
    )
