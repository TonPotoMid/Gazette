import email.utils
import html
import re
from datetime import UTC, datetime

LONGUEUR_MAX_EXTRAIT = 300
ELLIPSE = "…"

BALISES_DE_BLOC = r"<\s*/?\s*(?:br|p|div|li|ul|ol|h[1-6]|tr|td|table|blockquote)\b[^>]*>"
COMMENTAIRE_HTML = r"<!--.*?-->"
BALISE_HTML = r"</?[a-zA-Z][^>]*>"
CARACTERES_INVISIBLES = "[​﻿]"


def reparer_encodage(texte: str) -> str:
    """« Ã© » redevient « é » : un texte UTF-8 qui a été lu comme du cp1252 ou du latin-1.

    Un texte normal ne change pas : ses accents ne forment pas de l'UTF-8 valide une fois
    relus ainsi. Un texte à moitié cassé, ou avec des caractères hors cp1252, est laissé tel quel.
    """
    for codage in ("cp1252", "latin-1"):
        try:
            return texte.encode(codage).decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            continue
    return texte


def retirer_html(texte: str) -> str:
    """Retire les balises et les commentaires, garde le texte.

    Les balises de bloc (<p>, <br>, <li>…) deviennent une espace, pour ne pas coller deux
    paragraphes ; les balises dans une phrase (<b>, <a>…) disparaissent sans laisser d'espace.
    Un « < » dans une phrase (« 3 < 5 ») n'est pas une balise et reste.
    """
    texte = re.sub(COMMENTAIRE_HTML, "", texte, flags=re.DOTALL)
    texte = re.sub(BALISES_DE_BLOC, " ", texte, flags=re.IGNORECASE)
    return re.sub(BALISE_HTML, "", texte)


def decoder_entites(texte: str) -> str:
    """`&amp;` devient `&`, `&#39;` devient `'`, `&eacute;` devient `é`."""
    return html.unescape(texte)


def normaliser_espaces(texte: str) -> str:
    """Une seule espace entre les mots, rien aux bords.

    Espaces insécables, tabulations et retours à la ligne comptent comme des espaces ;
    les caractères de largeur nulle disparaissent.
    """
    texte = re.sub(CARACTERES_INVISIBLES, "", texte)
    return re.sub(r"\s+", " ", texte).strip()


def date_en_utc(texte: str | None) -> datetime | None:
    """La date d'un flux (RSS `Thu, 12 Nov 2026 05:00:00 +0100` ou ISO 8601) en UTC.

    None si la date manque ou est illisible. Une date sans fuseau est lue comme étant en UTC.
    """
    if not texte:
        return None
    try:
        date = email.utils.parsedate_to_datetime(texte)
    except (TypeError, ValueError):
        try:
            date = datetime.fromisoformat(texte)
        except ValueError:
            return None
    return date.astimezone(UTC) if date.tzinfo else date.replace(tzinfo=UTC)


def borner_extrait(texte: str, longueur: int = LONGUEUR_MAX_EXTRAIT) -> str:
    """Coupe un extrait trop long à la fin d'un mot et ajoute « … » (le total reste ≤ longueur)."""
    if len(texte) <= longueur:
        return texte
    limite = longueur - len(ELLIPSE)
    coupe = texte[:limite]
    if texte[limite] != " " and " " in coupe:  # on ne coupe pas un mot en deux
        coupe = coupe.rsplit(" ", 1)[0]
    return coupe.rstrip(" ,;:-–—") + ELLIPSE


def nettoyer_texte(texte: str | None) -> str:
    """Un titre ou un extrait propre : encodage, HTML, entités, puis espaces, dans cet ordre."""
    if not texte:
        return ""
    return normaliser_espaces(decoder_entites(retirer_html(reparer_encodage(texte))))
