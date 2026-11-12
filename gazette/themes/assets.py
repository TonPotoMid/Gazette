import os
from pathlib import Path

import dagster as dg

from gazette.partitions import partitions_quotidiennes
from gazette.silver.assets import CLE_ARTICLES_PROPRES
from gazette.themes.embeddings import ModeleSentenceTransformers
from gazette.themes.parametres import charger_parametres
from gazette.themes.production import chemin_base, produire_themes

DATA_DIR = Path(os.environ.get("GAZETTE_DATA_DIR", "data"))

CLE_THEMES = dg.AssetKey(["gold", "themes"])


@dg.asset(
    key=CLE_THEMES,
    group_name="gold",
    partitions_def=partitions_quotidiennes,
    # La fenêtre de 24 h déborde sur la veille : le silver du jour et celui de la veille.
    deps=[
        dg.AssetDep(
            CLE_ARTICLES_PROPRES,
            partition_mapping=dg.TimeWindowPartitionMapping(start_offset=-1, end_offset=0),
        )
    ],
    description=(
        "Thèmes des articles propres des dernières 24 h : vecteurs (en cache), HDBSCAN, "
        "noms par c-TF-IDF. Tables themes et articles_themes, chaque run tracé dans runs."
    ),
)
def themes(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    jour = context.partition_key
    # Relu à chaque run : changer config/clustering.yaml change le run suivant, sans redéploiement.
    parametres = charger_parametres()
    resultat = produire_themes(
        DATA_DIR, jour, parametres, ModeleSentenceTransformers(parametres.modele), context.run_id
    )
    m = resultat.metriques
    if not resultat.nb_articles:
        context.log.warning(f"Aucun article propre dans la fenêtre du {jour} : aucun thème.")
    context.log.info(
        f"{jour} : {resultat.nb_articles} articles, {m.nb_themes} thèmes, bruit {m.part_bruit}, "
        f"{resultat.nb_vecteurs_calcules} vecteurs calculés, "
        f"{resultat.nb_vecteurs_en_cache} repris du cache (run {resultat.run_id})"
    )
    return dg.MaterializeResult(
        metadata={
            "run_id": resultat.run_id,
            "nb_articles": resultat.nb_articles,
            "nb_themes": m.nb_themes,
            "taille_mediane": m.taille_mediane,
            "part_bruit": m.part_bruit,
            "silhouette": m.silhouette,
            "nb_vecteurs_calcules": resultat.nb_vecteurs_calcules,
            "nb_vecteurs_en_cache": resultat.nb_vecteurs_en_cache,
            "modele": f"{parametres.modele.nom}@{parametres.modele.revision}",
            "themes": dg.MetadataValue.md(
                "\n".join(f"- **{t.nom}** ({t.nb_articles} articles, {t.nb_sources} sources)"
                          for t in resultat.themes) or "_aucun_"
            ),
            "base": str(chemin_base(DATA_DIR)),
        }
    )


assets_themes = [themes]
