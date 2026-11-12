import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from gazette.silver.article import Article
from gazette.silver.passage import Passage
from gazette.silver.regles import Rejet

HORODATAGE = pa.timestamp("us", tz="UTC")

SCHEMA_ARTICLES_PROPRES = pa.schema(
    [
        ("source", pa.string()),
        ("jour", pa.string()),
        ("lien", pa.string()),
        ("titre", pa.string()),
        ("extrait", pa.string()),
        ("rubriques", pa.list_(pa.string())),
        ("publie_le", HORODATAGE),
        ("recupere_le", HORODATAGE),
        ("fichier_brut", pa.string()),
        ("rang_brut", pa.int32()),
    ]
)
SCHEMA_REJETS = (
    SCHEMA_ARTICLES_PROPRES.append(pa.field("regle", pa.string()))
    .append(pa.field("doublon_de_source", pa.string()))
    .append(pa.field("doublon_de_lien", pa.string()))
)


def ecrire_remplacant(chemin: Path, contenu: bytes) -> None:
    """Écrit le fichier d'un bloc : un passage interrompu ne laisse pas un fichier à moitié écrit.

    Contrairement au bronze, le silver se recalcule : relancer un jour remplace ses fichiers.
    """
    chemin.parent.mkdir(parents=True, exist_ok=True)
    temporaire = chemin.with_name(chemin.name + ".tmp")
    temporaire.write_bytes(contenu)
    os.replace(temporaire, chemin)


def _ligne_article(article: Article) -> dict:
    return {
        "source": article.source,
        "jour": article.jour,
        "lien": article.lien,
        "titre": article.titre,
        "extrait": article.extrait,
        "rubriques": list(article.rubriques),
        "publie_le": article.publie_le,
        "recupere_le": article.recupere_le,
        "fichier_brut": article.fichier_brut,
        "rang_brut": article.rang_brut,
    }


def _ligne_rejet(rejet: Rejet) -> dict:
    gardee = rejet.doublon_de
    return {
        **_ligne_article(rejet.article),
        "regle": rejet.regle,
        "doublon_de_source": gardee.source if gardee else None,
        "doublon_de_lien": gardee.lien if gardee else None,
    }


def _en_parquet(table: pa.Table) -> bytes:
    tampon = pa.BufferOutputStream()
    pq.write_table(table, tampon, compression="zstd")
    return tampon.getvalue().to_pybytes()


def chemin_table(racine: Path, nom: str, jour: str) -> Path:
    return racine / "silver" / nom / f"date={jour}" / f"{nom}.parquet"


def ecrire_tables(racine: Path, passage: Passage) -> dict[str, Path]:
    """Écrit articles_propres et rejets du jour (même vides), en remplaçant un passage précédent."""
    contenus = {
        "articles_propres": pa.Table.from_pylist(
            [_ligne_article(a) for a in passage.gardes], schema=SCHEMA_ARTICLES_PROPRES
        ),
        "rejets": pa.Table.from_pylist(
            [_ligne_rejet(r) for r in passage.rejets], schema=SCHEMA_REJETS
        ),
    }
    chemins = {}
    for nom, table in contenus.items():
        chemins[nom] = chemin_table(racine, nom, passage.jour)
        ecrire_remplacant(chemins[nom], _en_parquet(table))
    return chemins
