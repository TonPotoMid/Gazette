import os
from dataclasses import asdict, dataclass
from pathlib import Path

import yaml

RACINE_PROJET = Path(__file__).resolve().parents[2]
CHEMIN_PAR_DEFAUT = Path(
    os.environ.get("GAZETTE_CLUSTERING_CONFIG", RACINE_PROJET / "config" / "clustering.yaml")
)


@dataclass(frozen=True)
class ParametresModele:
    nom: str
    revision: str
    taille_lot: int
    normaliser: bool


@dataclass(frozen=True)
class ParametresRegroupement:
    algorithme: str
    taille_min_groupe: int
    echantillons_min: int
    metrique: str
    methode_selection: str


@dataclass(frozen=True)
class ParametresNommage:
    nb_mots: int
    longueur_min_mot: int
    mots_vides: frozenset[str]


@dataclass(frozen=True)
class Parametres:
    """Les réglages d'un run de regroupement, tels que lus dans `config/clustering.yaml`."""

    fenetre_heures: int
    champs_texte: tuple[str, ...]
    modele: ParametresModele
    regroupement: ParametresRegroupement
    nommage: ParametresNommage
    fichier: str

    def en_dict(self) -> dict:
        """Ce que le run enregistre : sérialisable en JSON, mots vides comptés plutôt que listés."""
        d = asdict(self)
        d["nommage"]["mots_vides"] = len(self.nommage.mots_vides)
        d["champs_texte"] = list(self.champs_texte)
        return d


def _lire_mots_vides(chemin: Path) -> frozenset[str]:
    lignes = chemin.read_text(encoding="utf-8").splitlines()
    return frozenset(m.strip().lower() for m in lignes if m.strip() and not m.startswith("#"))


def charger_parametres(chemin: Path | str = CHEMIN_PAR_DEFAUT) -> Parametres:
    chemin = Path(chemin)
    brut = yaml.safe_load(chemin.read_text(encoding="utf-8"))
    algorithme = brut["regroupement"]["algorithme"]
    if algorithme != "hdbscan":
        raise ValueError(f"Algorithme de regroupement inconnu : {algorithme!r} (attendu : hdbscan)")
    champs = tuple(brut["texte"]["champs"])
    if not champs or set(champs) - {"titre", "extrait"}:
        raise ValueError(f"Champs de texte invalides : {champs!r} (titre et/ou extrait)")

    nommage = dict(brut["nommage"])
    mots_vides = Path(nommage.pop("mots_vides"))
    if not mots_vides.is_absolute():
        mots_vides = RACINE_PROJET / mots_vides
    return Parametres(
        fenetre_heures=int(brut["fenetre_heures"]),
        champs_texte=champs,
        modele=ParametresModele(**brut["modele"]),
        regroupement=ParametresRegroupement(**brut["regroupement"]),
        nommage=ParametresNommage(**nommage, mots_vides=_lire_mots_vides(mots_vides)),
        fichier=str(chemin),
    )
