import time
from dataclasses import asdict, dataclass
from pathlib import Path

from gazette.themes.embeddings import CacheVecteurs, Encodeur, texte_a_vectoriser, vectoriser
from gazette.themes.fenetre import lire_fenetre
from gazette.themes.parametres import Parametres
from gazette.themes.regroupement import BRUIT, Metriques, mesurer, nommer, regrouper
from gazette.themes.suivi import Affectation, Run, Theme


def chemin_base(racine: Path) -> Path:
    return racine / "gold" / "gazette.db"


def chemin_cache(racine: Path) -> Path:
    return racine / "cache" / "vecteurs.sqlite"


@dataclass(frozen=True)
class Resultat:
    run_id: str
    jour: str
    nb_articles: int
    themes: list[Theme]
    metriques: Metriques
    nb_vecteurs_calcules: int
    nb_vecteurs_en_cache: int
    durees_s: dict[str, float]


def produire_themes(
    racine: Path,
    jour: str,
    parametres: Parametres,
    encodeur: Encodeur,
    dagster_run_id: str | None = None,
) -> Resultat:
    """Un run complet et tracé : fenêtre de 24 h → vecteurs (cache) → groupes → noms → base.

    Le run est enregistré « en_cours » avant tout calcul ; s'il échoue, il reste en base avec
    son erreur, et les thèmes du dernier run réussi restent ceux du jour.
    """
    run = Run(chemin_base(racine), jour, dagster_run_id)
    try:
        durees = {}
        t = time.perf_counter()
        fenetre = lire_fenetre(racine, jour, parametres.fenetre_heures)
        articles = fenetre.articles
        textes = [texte_a_vectoriser(a.titre, a.extrait, parametres.champs_texte) for a in articles]
        durees["lecture"] = time.perf_counter() - t

        t = time.perf_counter()
        vecto = vectoriser(textes, encodeur, CacheVecteurs(chemin_cache(racine)))
        durees["vectorisation"] = time.perf_counter() - t

        t = time.perf_counter()
        etiquettes = regrouper(vecto.vecteurs, parametres.regroupement)
        noms = nommer(textes, etiquettes, parametres.nommage)
        metriques = mesurer(vecto.vecteurs, etiquettes)
        durees["regroupement"] = time.perf_counter() - t

        themes = [
            Theme(
                theme_id=e,
                nom=" · ".join(noms.get(e, [])) or f"thème {e}",
                mots_cles=noms.get(e, []),
                nb_articles=int((etiquettes == e).sum()),
                nb_sources=len({a.source for a, x in zip(articles, etiquettes) if x == e}),
            )
            for e in sorted(set(int(x) for x in etiquettes) - {BRUIT})
        ]
        affectations = [
            Affectation(a.source, a.lien, None if e == BRUIT else int(e), a.titre)
            for a, e in zip(articles, etiquettes)
        ]
        durees = {k: round(v, 3) for k, v in durees.items()}
        run.reussir(
            donnees={
                "fenetre_debut": fenetre.debut.isoformat() if fenetre.debut else None,
                "fenetre_fin": fenetre.fin.isoformat() if fenetre.fin else None,
                "nb_articles": len(articles),
                "hash_entree": fenetre.hash_entree,
            },
            modele=encodeur.nom,
            modele_revision=encodeur.revision,
            parametres=parametres.en_dict(),
            metriques={
                **asdict(metriques),
                "nb_vecteurs_calcules": vecto.nb_calcules,
                "nb_vecteurs_en_cache": vecto.nb_en_cache,
                "durees_s": durees,
            },
            artefacts={
                "base": str(chemin_base(racine)),
                "themes": len(themes),
                "articles_themes": len(affectations),
                "jours_silver_lus": fenetre.jours_lus,
                "cache_vecteurs": str(chemin_cache(racine)),
            },
            themes=themes,
            affectations=affectations,
        )
    except BaseException as erreur:
        run.echouer(erreur)
        raise
    return Resultat(
        run.run_id,
        jour,
        len(articles),
        themes,
        metriques,
        vecto.nb_calcules,
        vecto.nb_en_cache,
        durees,
    )
