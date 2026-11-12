import itertools
import re
from collections import defaultdict
from dataclasses import dataclass, field

from gazette.silver.normalisation import date_en_utc, reparer_encodage

SEUIL_QUASI_DOUBLON = 0.8
SEUILS_SENSIBILITE = (0.8, 0.7, 0.6)
MIN_CARACTERES_TITRE = 12

PUBLI_LIEN = r"/partenaires?/|/publi[-_]|/brand[-_]content|/sponsoris"
PUBLI_TEXTE = r"\bcontenus? partenaires?\b|\bpublireportage|\bpublir[ée]dactionnel"
HOROSCOPE = r"\bhoroscope\b|\bzodiaque\b|\bastrologie\b"
PROGRAMME_TV = r"\bprogrammes? (tv|télé)\b|\bce soir (à|sur) la (télé|tv)\b"
JEUX_TEXTE = r"\bquiz\b|\bjeux?[- ]concours\b|\bgagnez\b|\bmots? (croisés|fléchés)\b|\bsudoku\b"
JEUX_LIEN = r"/jeux?/|espace-jeux"
AGENCE = r"\bafp\b|\breuters\b|\bassociated press\b|\bbelga\b|\bavec agences?\b"


@dataclass
class Mesure:
    numero: int
    nom: str
    lignes: list[dict] = field(default_factory=list)
    groupes: list[list[dict]] | None = None  # pour les doublons : les lignes rangées par groupe


def normaliser_titre(titre: str | None) -> str:
    """Minuscules, sans ponctuation ni espaces insécables : ’ et ' deviennent identiques."""
    return re.sub(r"\W+", " ", (titre or "").lower()).strip()


def mots(titre: str | None) -> set[str]:
    return {mot for mot in normaliser_titre(titre).split() if len(mot) > 2}


def jaccard(a: set[str], b: set[str]) -> float:
    return len(a & b) / len(a | b) if a and b else 0.0


def groupes_quasi_doublons(lignes: list[dict], seuil: float) -> list[list[dict]]:
    """Titres proches (Jaccard sur les mots) le même jour dans des sources différentes."""
    parent = list(range(len(lignes)))

    def racine(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    par_jour: dict[str, list[int]] = defaultdict(list)
    for i, ligne in enumerate(lignes):
        par_jour[ligne["jour"]].append(i)
    ensembles = [mots(ligne["title"]) for ligne in lignes]
    for indices in par_jour.values():
        for a, b in itertools.combinations(indices, 2):
            if lignes[a]["source"] != lignes[b]["source"] and (
                jaccard(ensembles[a], ensembles[b]) >= seuil
            ):
                parent[racine(a)] = racine(b)

    groupes: dict[int, list[dict]] = defaultdict(list)
    for i, ligne in enumerate(lignes):
        groupes[racine(i)].append(ligne)
    return [g for g in groupes.values() if len(g) > 1]


def _texte(ligne: dict) -> str:
    return " ".join([ligne["title"] or "", ligne["summary"] or "", *(ligne["tags"] or [])])


def _cherche(motif: str, texte: str) -> bool:
    return re.search(motif, texte, re.IGNORECASE) is not None


def encodage_casse(texte: str | None) -> bool:
    """Vrai pour « Ã© » à la place de « é » : un texte que `reparer_encodage` change.

    Les accents normaux (â, é, ç…) ne déclenchent rien. Le caractère de remplacement � l'est aussi.
    """
    texte = texte or ""
    return "\ufffd" in texte or reparer_encodage(texte) != texte


def mesurer(lignes: list[dict]) -> list[Mesure]:
    """Les dix pollueurs, dans l'ordre de la consigne."""
    sources_par_titre: dict[str, set[str]] = defaultdict(set)
    lignes_par_titre: dict[str, list[dict]] = defaultdict(list)
    for ligne in lignes:
        titre = normaliser_titre(ligne["title"])
        if titre:
            sources_par_titre[titre].add(ligne["source"])
            lignes_par_titre[titre].append(ligne)
    titre_commun = {t for t, sources in sources_par_titre.items() if len(sources) > 1}
    titres_repetes = {t for t, ls in lignes_par_titre.items() if len(ls) > 1}
    quasi = groupes_quasi_doublons(lignes, SEUIL_QUASI_DOUBLON)

    def date_future(ligne: dict) -> bool:
        date = date_en_utc(ligne["published"])
        return date is not None and date > ligne["recu_le"]

    return [
        Mesure(
            1,
            "Publi-reportage, contenu partenaire",
            [
                x
                for x in lignes
                if _cherche(PUBLI_LIEN, x["link"] or "") or _cherche(PUBLI_TEXTE, _texte(x))
            ],
        ),
        Mesure(
            2,
            "Horoscope",
            [x for x in lignes if _cherche(HOROSCOPE, _texte(x) + " " + (x["link"] or ""))],
        ),
        Mesure(
            3,
            "Programme TV",
            [x for x in lignes if _cherche(PROGRAMME_TV, _texte(x))],
        ),
        Mesure(
            4,
            "Jeux et concours",
            [
                x
                for x in lignes
                if _cherche(JEUX_TEXTE, x["title"] or "") or _cherche(JEUX_LIEN, x["link"] or "")
            ],
        ),
        Mesure(
            5,
            "Dépêche d'agence reprise",
            [
                x
                for x in lignes
                if _cherche(AGENCE, _texte(x)) or normaliser_titre(x["title"]) in titre_commun
            ],
        ),
        Mesure(
            6,
            "Titre vide ou trop court",
            [x for x in lignes if len((x["title"] or "").strip()) < MIN_CARACTERES_TITRE],
        ),
        Mesure(
            7,
            "Date dans le futur",
            [x for x in lignes if date_future(x)],
        ),
        Mesure(
            8,
            "Encodage cassé",
            [x for x in lignes if encodage_casse(x["title"]) or encodage_casse(x["summary"])],
        ),
        Mesure(
            9,
            "Doublon exact",
            [x for x in lignes if normaliser_titre(x["title"]) in titres_repetes],
        ),
        Mesure(
            10,
            "Doublon approximatif",
            [x for groupe in quasi for x in groupe],
            groupes=quasi,
        ),
    ]


def sensibilite(lignes: list[dict]) -> list[tuple[float, int, int, int]]:
    """Par seuil de Jaccard : groupes, articles concernés, plus grand nombre de sources."""
    resultat = []
    for seuil in SEUILS_SENSIBILITE:
        groupes = groupes_quasi_doublons(lignes, seuil)
        resultat.append(
            (
                seuil,
                len(groupes),
                sum(len(g) for g in groupes),
                max((len({x["source"] for x in g}) for g in groupes), default=0),
            )
        )
    return resultat
