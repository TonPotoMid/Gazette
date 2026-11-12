import os
from collections import Counter
from pathlib import Path

import dagster as dg

from gazette.bronze.sources import charger_sources
from gazette.partitions import partitions_quotidiennes
from gazette.silver import controles
from gazette.silver.passage import traiter_jour
from gazette.silver.rapport import ecrire_rapport
from gazette.silver.tables import ecrire_tables

DATA_DIR = Path(os.environ.get("GAZETTE_DATA_DIR", "data"))
RAPPORTS_DIR = Path(os.environ.get("GAZETTE_RAPPORTS_DIR", "rapports"))

CLE_ARTICLES_PROPRES = dg.AssetKey(["silver", "articles_propres"])
CLE_REJETS = dg.AssetKey(["silver", "rejets"])

SOURCES = [source.slug for source in charger_sources()]
CLES_BRONZE = [dg.AssetKey(["bronze", slug]) for slug in SOURCES]


@dg.multi_asset(
    name="silver_articles",
    specs=[
        dg.AssetSpec(
            CLE_ARTICLES_PROPRES,
            deps=CLES_BRONZE,
            group_name="silver",
            description="Articles normalisés qui ont passé les règles d'exclusion, par jour.",
        ),
        dg.AssetSpec(
            CLE_REJETS,
            deps=CLES_BRONZE,
            group_name="silver",
            description="Articles écartés par une règle d'exclusion (R01_publi…), par jour.",
        ),
    ],
    partitions_def=partitions_quotidiennes,
)
def silver_articles(context: dg.AssetExecutionContext):
    jour = context.partition_key
    passage = traiter_jour(DATA_DIR, jour, tuple(SOURCES))
    chemins = ecrire_tables(DATA_DIR, passage)
    rapport = ecrire_rapport(RAPPORTS_DIR, passage, SOURCES)

    nb_entres, nb_rejetes = len(passage.entres), len(passage.rejets)
    par_regle = dict(sorted(Counter(r.regle for r in passage.rejets).items()))
    if not nb_entres:
        context.log.warning(f"Aucun article dans le bronze du {jour} : tables et rapport vides.")
    context.log.info(
        f"{jour} : {nb_entres} entrés, {len(passage.gardes)} gardés, {nb_rejetes} rejetés"
    )

    commun = {
        "nb_entres": nb_entres,
        "nb_gardes": len(passage.gardes),
        "nb_rejetes": nb_rejetes,
        "taux_rejet": round(nb_rejetes / nb_entres, 4) if nb_entres else 0.0,
        "rejets_par_regle": par_regle,
        "rapport": str(rapport),
    }
    yield dg.MaterializeResult(
        asset_key=CLE_ARTICLES_PROPRES,
        metadata={**commun, "fichier": str(chemins["articles_propres"])},
    )
    yield dg.MaterializeResult(
        asset_key=CLE_REJETS, metadata={**commun, "fichier": str(chemins["rejets"])}
    )


DESCRIPTIONS_CONTROLES = {
    "aucune_date_future": "Aucun article propre n'est daté d'après sa réception.",
    "aucun_titre_vide": "Aucun article propre n'a un titre vide.",
    "taux_de_rejet_par_source": (
        f"Aucune source n'a plus de {controles.TAUX_REJET_MAX:.0%} de ses articles rejetés."
    ),
    "au_moins_un_article_entre": "Le bronze du jour contenait au moins un article.",
}


def _construire_controle(nom: str) -> dg.AssetChecksDefinition:
    @dg.asset_check(
        asset=CLE_ARTICLES_PROPRES,
        name=nom,
        description=DESCRIPTIONS_CONTROLES[nom],
    )
    def _controle(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
        # Un contrôle partitionné est encore en préversion dans Dagster : le jour vient du run.
        jour = context.run.tags["dagster/partition"]
        verdict = controles.verifier(nom, DATA_DIR, jour)
        return dg.AssetCheckResult(
            passed=verdict.passe,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={"resume": verdict.resume, **verdict.metriques},
        )

    return _controle


assets_silver = [silver_articles]
controles_silver = [_construire_controle(nom) for nom in controles.CONTROLES]
