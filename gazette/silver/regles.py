import re
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from gazette.silver.article import Article

# --- Réglages : les motifs et les seuils, au même endroit, lisibles. ---------------------------

LONGUEUR_MIN_TITRE = 12
SEUIL_DOUBLON_APPROX = 0.8  # part de mots en commun entre deux titres (Jaccard sur les mots)
LONGUEUR_MIN_MOT = 3  # les mots plus courts (« de », « la »…) ne comptent pas dans la comparaison

PUBLI_LIEN = r"/partenaires?/|/publi[-_]|/brand[-_]content|/sponsoris"
PUBLI_TEXTE = r"\bcontenus? partenaires?\b|\bpublireportage|\bpublir[ée]dactionnel"
HOROSCOPE = r"\bhoroscope\b|\bzodiaque\b|\bastrologie\b"
PROGRAMME_TV = r"\bprogrammes? (tv|télé)\b|\bce soir (à|sur) la (télé|tv)\b"
JEUX_TEXTE = r"\bquiz\b|\bjeux?[- ]concours\b|\bgagnez\b|\bmots? (croisés|fléchés)\b|\bsudoku\b"
JEUX_LIEN = r"/jeux?/|espace-jeux"
ENCODAGE_IRREPARABLE = r"�|Ã[\u0080-¿]|â€"  # « � », « Ã© », « â€™ » encore présents

# --- Les outils de comparaison. -----------------------------------------------------------------


def comparable(texte: str) -> str:
    """Minuscules, sans ponctuation ni espaces multiples : ’ et ' deviennent identiques."""
    return re.sub(r"\W+", " ", texte.lower()).strip()


def mots(titre: str) -> set[str]:
    return {m for m in comparable(titre).split() if len(m) >= LONGUEUR_MIN_MOT}


def jaccard(a: set[str], b: set[str]) -> float:
    return len(a & b) / len(a | b) if a and b else 0.0


def _texte(article: Article) -> str:
    return " ".join([article.titre, article.extrait, *article.rubriques])


def _cherche(motif: str, texte: str) -> bool:
    return re.search(motif, texte, re.IGNORECASE) is not None


# --- Les règles qui regardent un article seul. -------------------------------------------------


def est_publi(article: Article) -> bool:
    return _cherche(PUBLI_LIEN, article.lien) or _cherche(PUBLI_TEXTE, _texte(article))


def est_horoscope(article: Article) -> bool:
    return _cherche(HOROSCOPE, _texte(article) + " " + article.lien)


def est_programme_tv(article: Article) -> bool:
    return _cherche(PROGRAMME_TV, _texte(article))


def est_jeu(article: Article) -> bool:
    return _cherche(JEUX_TEXTE, article.titre) or _cherche(JEUX_LIEN, article.lien)


def a_un_titre_vide(article: Article) -> bool:
    return len(article.titre.strip()) < LONGUEUR_MIN_TITRE


def est_dans_le_futur(article: Article) -> bool:
    return article.publie_le is not None and article.publie_le > article.recupere_le


def a_un_encodage_irreparable(article: Article) -> bool:
    return re.search(ENCODAGE_IRREPARABLE, article.titre + " " + article.extrait) is not None


# --- Les règles qui comparent les articles entre eux : elles renvoient des groupes de positions. -


def meme_extrait(articles: list[Article]) -> list[list[int]]:
    """Le même extrait dans au moins deux sources : une dépêche reprise telle quelle."""
    par_extrait: dict[str, list[int]] = defaultdict(list)
    for position, article in enumerate(articles):
        if cle := comparable(article.extrait):
            par_extrait[cle].append(position)
    return [
        groupe for groupe in par_extrait.values() if len({articles[p].source for p in groupe}) > 1
    ]


def meme_titre(articles: list[Article]) -> list[list[int]]:
    """Le même titre (à la casse, la ponctuation et les espaces près)."""
    par_titre: dict[str, list[int]] = defaultdict(list)
    for position, article in enumerate(articles):
        if cle := comparable(article.titre):
            par_titre[cle].append(position)
    return [groupe for groupe in par_titre.values() if len(groupe) > 1]


def titres_proches(articles: list[Article], seuil: float = SEUIL_DOUBLON_APPROX) -> list[list[int]]:
    """Des titres presque identiques dans des sources différentes (à comparer sur un seul jour)."""
    parent = list(range(len(articles)))

    def racine(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    ensembles = [mots(article.titre) for article in articles]
    for a in range(len(articles)):
        for b in range(a + 1, len(articles)):
            if articles[a].source != articles[b].source and (
                jaccard(ensembles[a], ensembles[b]) >= seuil
            ):
                parent[racine(a)] = racine(b)

    groupes: dict[int, list[int]] = defaultdict(list)
    for position in range(len(articles)):
        groupes[racine(position)].append(position)
    return [groupe for groupe in groupes.values() if len(groupe) > 1]


@dataclass(frozen=True)
class RegleArticle:
    identifiant: str
    nom: str
    ecarte: Callable[[Article], bool]


@dataclass(frozen=True)
class RegleDoublon:
    identifiant: str
    nom: str
    groupes: Callable[[list[Article]], list[list[int]]]


REGLES_ARTICLE = (
    RegleArticle("R01_publi", "Publi-reportage, contenu partenaire", est_publi),
    RegleArticle("R02_horoscope", "Horoscope", est_horoscope),
    RegleArticle("R03_programme_tv", "Programme TV", est_programme_tv),
    RegleArticle("R04_jeux", "Jeux et concours", est_jeu),
    RegleArticle("R06_titre_vide", "Titre vide ou trop court", a_un_titre_vide),
    RegleArticle("R07_date_future", "Date dans le futur", est_dans_le_futur),
    RegleArticle("R08_encodage", "Encodage irréparable", a_un_encodage_irreparable),
)
REGLES_DOUBLON = (
    RegleDoublon("R05_agence_reprise", "Dépêche d'agence reprise", meme_extrait),
    RegleDoublon("R09_doublon_exact", "Doublon exact", meme_titre),
    RegleDoublon("R10_doublon_approx", "Doublon approximatif", titres_proches),
)


def regles() -> list[tuple[str, str]]:
    """(identifiant, nom) des dix règles, dans l'ordre R01 à R10."""
    return sorted((r.identifiant, r.nom) for r in (*REGLES_ARTICLE, *REGLES_DOUBLON))


# --- L'application. -----------------------------------------------------------------------------


@dataclass(frozen=True)
class Rejet:
    article: Article
    regle: str  # identifiant de la première règle qui l'écarte
    doublon_de: Article | None = None  # pour un doublon : l'exemplaire gardé


@dataclass(frozen=True)
class Resultat:
    gardes: list[Article]
    rejets: list[Rejet]


def appliquer_regles(articles: list[Article], ordre_sources: tuple[str, ...] = ()) -> Resultat:
    """Sépare les articles gardés des rejetés. À appeler sur les articles d'un seul jour.

    Les règles sur un article seul passent d'abord, puis les comparaisons sur ce qui reste (un
    doublon se cherche entre articles valides). Un article n'est rejeté qu'une fois, par la
    première règle qui l'écarte. Dans un groupe de doublons, on garde le premier publié ; à
    égalité, la source la plus haute dans `ordre_sources` (par défaut, l'ordre alphabétique).
    """
    rang_source = {source: rang for rang, source in enumerate(ordre_sources)}
    sans_date = datetime.max.replace(tzinfo=UTC)

    def a_garder(article: Article) -> tuple:
        return (
            article.publie_le is None,
            article.publie_le or sans_date,
            rang_source.get(article.source, len(rang_source)),
            article.source,
            article.lien,
        )

    rejets: dict[int, Rejet] = {}
    for regle in REGLES_ARTICLE:
        for position, article in enumerate(articles):
            if position not in rejets and regle.ecarte(article):
                rejets[position] = Rejet(article, regle.identifiant)

    for regle in REGLES_DOUBLON:
        restants = [p for p in range(len(articles)) if p not in rejets]
        for groupe in regle.groupes([articles[p] for p in restants]):
            membres = [restants[k] for k in groupe]
            garde = min(membres, key=lambda p: a_garder(articles[p]))
            for position in membres:
                if position != garde:
                    rejets[position] = Rejet(articles[position], regle.identifiant, articles[garde])

    return Resultat(
        gardes=[a for p, a in enumerate(articles) if p not in rejets],
        rejets=[rejets[p] for p in sorted(rejets)],
    )
