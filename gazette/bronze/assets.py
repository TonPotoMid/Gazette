"""Couche bronze : réponses RSS brutes, compressées et jamais réécrites.

Un asset par journal, pour que l'échec d'une source n'empêche pas les autres
de tourner et que chaque source ait ses propres relances et son propre check.

Arborescence produite :
    $GAZETTE_DATA_DIR/bronze/date=AAAA-MM-JJ/<slug>/<flux>_<horodatage UTC>.xml.gz
    $GAZETTE_DATA_DIR/bronze/date=AAAA-MM-JJ/<slug>/<flux>_<horodatage UTC>.json   (contexte HTTP)
"""

import gzip
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import dagster as dg
import feedparser
import requests

from gazette.bronze.sources import Flux, Source, charger_sources
from gazette.partitions import partitions_quotidiennes

DATA_DIR = Path(os.environ.get("GAZETTE_DATA_DIR", "data"))
USER_AGENT = "GazetteBot/0.1 (projet etudiant M2Ops Efrei; lecture des flux RSS publics)"
TIMEOUT_S = 30
CHECK_AU_MOINS_UN_ARTICLE = "au_moins_un_article"

relances = dg.RetryPolicy(max_retries=3, delay=30, backoff=dg.Backoff.EXPONENTIAL)


def ecrire_immuable(chemin: Path, contenu: bytes) -> None:
    """Écrit un fichier qui ne doit jamais exister déjà : le bronze ne se réécrit pas."""
    chemin.parent.mkdir(parents=True, exist_ok=True)
    with open(chemin, "xb") as f:
        f.write(contenu)


def collecter_flux(session: requests.Session, flux: Flux, dossier: Path) -> dict:
    reponse = session.get(flux.url, timeout=TIMEOUT_S)
    reponse.raise_for_status()
    recu_le = datetime.now(timezone.utc)

    nb_articles = len(feedparser.parse(reponse.content).entries)
    base = dossier / f"{flux.nom}_{recu_le:%Y%m%dT%H%M%S%fZ}"
    contexte = {
        "flux": flux.nom,
        "url": flux.url,
        "url_finale": reponse.url,
        "statut_http": reponse.status_code,
        "content_type": reponse.headers.get("Content-Type"),
        "etag": reponse.headers.get("ETag"),
        "last_modified": reponse.headers.get("Last-Modified"),
        "recu_le": recu_le.isoformat(),
        "octets": len(reponse.content),
        "nb_articles": nb_articles,
    }
    ecrire_immuable(Path(f"{base}.xml.gz"), gzip.compress(reponse.content))
    ecrire_immuable(
        Path(f"{base}.json"),
        json.dumps(contexte, ensure_ascii=False, indent=2).encode("utf-8"),
    )
    return {**contexte, "fichier": f"{base}.xml.gz"}


def construire_asset_bronze(source: Source) -> dg.AssetsDefinition:
    cle = dg.AssetKey(["bronze", source.slug])

    @dg.asset(
        key=cle,
        group_name="bronze",
        partitions_def=partitions_quotidiennes,
        retry_policy=relances,
        description=f"Flux RSS bruts de {source.nom} ({source.type}, {source.region}), gzip, immuables.",
        metadata={"journal": source.nom, "type": source.type, "region": source.region},
        check_specs=[
            dg.AssetCheckSpec(
                CHECK_AU_MOINS_UN_ARTICLE,
                asset=cle,
                description="La source a renvoyé au moins un article.",
            )
        ],
    )
    def _bronze(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        date = context.partition_key
        if date != datetime.now(ZoneInfo("Europe/Paris")).date().isoformat():
            context.log.warning(
                f"Partition {date} rejouée un autre jour : un flux RSS ne donne que ses "
                "articles du moment, les fichiers collectés reflètent donc le flux actuel."
            )

        dossier = DATA_DIR / "bronze" / f"date={date}" / source.slug
        debut = time.monotonic()
        collectes, echecs = [], []
        with requests.Session() as session:
            session.headers["User-Agent"] = USER_AGENT
            for flux in source.flux:
                try:
                    collectes.append(collecter_flux(session, flux, dossier))
                except requests.RequestException as erreur:
                    context.log.warning(f"Flux {flux.nom} ({flux.url}) en échec : {erreur}")
                    echecs.append(f"{flux.nom} : {erreur}")
        duree_s = round(time.monotonic() - debut, 2)

        if not collectes:
            raise dg.Failure(
                description=f"Aucun flux de {source.nom} n'a répondu.",
                metadata={"flux_en_echec": echecs, "duree_s": duree_s},
            )

        nb_articles = sum(c["nb_articles"] for c in collectes)
        context.log.info(f"{source.nom} : {nb_articles} articles en {duree_s} s")
        return dg.MaterializeResult(
            metadata={
                "nb_articles": nb_articles,
                "nb_flux_ok": len(collectes),
                "nb_flux_en_echec": len(echecs),
                "flux_en_echec": echecs,
                "duree_s": duree_s,
                "octets": sum(c["octets"] for c in collectes),
                "fichiers": [c["fichier"] for c in collectes],
            },
            check_results=[
                dg.AssetCheckResult(
                    check_name=CHECK_AU_MOINS_UN_ARTICLE,
                    passed=nb_articles > 0,
                    severity=dg.AssetCheckSeverity.ERROR,
                    metadata={"nb_articles": nb_articles},
                )
            ],
        )

    return _bronze


assets_bronze = [construire_asset_bronze(source) for source in charger_sources()]
