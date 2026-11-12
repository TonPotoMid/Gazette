import hashlib
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq

from gazette.silver.tables import chemin_table

COLONNES = ["source", "jour", "lien", "titre", "extrait", "publie_le", "recupere_le"]


@dataclass(frozen=True)
class ArticleATraiter:
    source: str
    lien: str
    titre: str
    extrait: str
    date: datetime  # publication, ou réception si la date de publication manque


@dataclass(frozen=True)
class Fenetre:
    """Les articles propres d'une fenêtre de temps, et de quoi la reconnaître."""

    jour: str
    debut: datetime | None
    fin: datetime | None
    articles: list[ArticleATraiter]
    jours_lus: list[str]

    @property
    def hash_entree(self) -> str:
        """Empreinte des articles en entrée : deux runs de même hash ont lu les mêmes données."""
        h = hashlib.sha256()
        for a in self.articles:
            h.update("\x1f".join([a.source, a.lien, a.titre, a.extrait]).encode("utf-8"))
            h.update(b"\x1e")
        return h.hexdigest()


def _lire_silver(racine: Path, jour: str) -> list[dict]:
    chemin = chemin_table(racine, "articles_propres", jour)
    if not chemin.exists():
        return []
    return pq.read_table(chemin, columns=COLONNES).to_pylist()


def lire_fenetre(racine: Path, jour: str, heures: int) -> Fenetre:
    """Les articles propres publiés dans les `heures` qui précèdent la collecte du `jour`.

    La fin de la fenêtre est la dernière réception du jour (et non l'heure du run) : rejouer
    un jour relit exactement la même fenêtre. Un article sans date de publication est daté de
    sa réception. La collecte étant quotidienne, le jour et la veille suffisent pour 24 h.
    """
    veille = (date.fromisoformat(jour) - timedelta(days=1)).isoformat()
    du_jour = _lire_silver(racine, jour)
    if not du_jour:
        return Fenetre(jour, None, None, [], [jour, veille])

    fin = max(ligne["recupere_le"] for ligne in du_jour)
    debut = fin - timedelta(hours=heures)
    vus: set[tuple[str, str]] = set()
    articles = []
    for ligne in [*_lire_silver(racine, veille), *du_jour]:
        quand = ligne["publie_le"] or ligne["recupere_le"]
        cle = (ligne["source"], ligne["lien"])
        if debut < quand <= fin and cle not in vus:
            vus.add(cle)
            articles.append(
                ArticleATraiter(ligne["source"], ligne["lien"], ligne["titre"], ligne["extrait"], quand)
            )
    articles.sort(key=lambda a: (a.source, a.lien))
    return Fenetre(jour, debut, fin, articles, [veille, jour])
