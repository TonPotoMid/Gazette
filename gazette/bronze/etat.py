"""État des flux entre deux runs : un petit fichier JSON par flux, pas de base de données.

    $GAZETTE_DATA_DIR/etat/<slug>/<flux>.json

Seuls les échecs permanents (404, 410...) comptent : après SEUIL_SOMMEIL échecs de suite, le flux
n'est plus contacté tant que quelqu'un ne l'a pas réactivé (python -m gazette.reactiver).
Une erreur transitoire (503, timeout...) ne change rien : elle se règle par les relances.
"""

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path

SEUIL_SOMMEIL = 3


@dataclass
class EtatFlux:
    actif: bool = True
    echecs_consecutifs: int = 0
    derniere_reussite: str | None = None
    derniere_erreur: str | None = None


def chemin_etat(racine: Path, slug: str, flux: str) -> Path:
    return racine / "etat" / slug / f"{flux}.json"


def lire_etat(racine: Path, slug: str, flux: str) -> EtatFlux:
    """L'état du flux ; un flux jamais vu est actif, sans échec."""
    try:
        return EtatFlux(**json.loads(chemin_etat(racine, slug, flux).read_text(encoding="utf-8")))
    except FileNotFoundError:
        return EtatFlux()


def ecrire_etat(racine: Path, slug: str, flux: str, etat: EtatFlux) -> None:
    """Remplacement atomique : un run interrompu ne laisse jamais un fichier à moitié écrit."""
    chemin = chemin_etat(racine, slug, flux)
    chemin.parent.mkdir(parents=True, exist_ok=True)
    temporaire = chemin.with_suffix(".json.tmp")
    temporaire.write_text(json.dumps(asdict(etat), ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporaire, chemin)


def noter_reussite(etat: EtatFlux, quand: str) -> None:
    etat.echecs_consecutifs = 0
    etat.derniere_reussite = quand
    etat.derniere_erreur = None


def noter_echec_permanent(etat: EtatFlux, erreur: str) -> bool:
    """Compte l'échec ; renvoie True si le flux vient de passer en sommeil."""
    etat.echecs_consecutifs += 1
    etat.derniere_erreur = erreur
    if etat.actif and etat.echecs_consecutifs >= SEUIL_SOMMEIL:
        etat.actif = False
        return True
    return False


def reactiver(racine: Path, slug: str, flux: str | None = None) -> list[str]:
    """Remet en service les flux connus de la source (ou le seul `flux`) ; renvoie leurs noms."""
    dossier = racine / "etat" / slug
    noms = [flux] if flux else sorted(f.stem for f in dossier.glob("*.json"))
    reveilles = []
    for nom in noms:
        if not chemin_etat(racine, slug, nom).exists():
            continue
        etat = lire_etat(racine, slug, nom)
        etat.actif, etat.echecs_consecutifs = True, 0
        ecrire_etat(racine, slug, nom, etat)
        reveilles.append(nom)
    return reveilles
