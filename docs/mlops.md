# MLOps · La gazette

Le regroupement des articles par thèmes : le modèle, les paramètres, et comment savoir **quel run a produit les thèmes en base**.

## Ce que fait un run

```
articles propres (24 h) ──embed──▶ vecteurs (cache) ──HDBSCAN──▶ groupes + bruit ──c-TF-IDF──▶ thèmes nommés, en base
```

| Étape | Ce qui se passe | Code |
| --- | --- | --- |
| Fenêtre | Les articles propres publiés dans les 24 h qui précèdent la dernière réception du jour. Un article sans date de publication est daté de sa réception. On lit le silver du jour et celui de la veille | `gazette/themes/fenetre.py` |
| Vectoriser | Titre + extrait → un vecteur de norme 1. Un texte déjà vectorisé par le même modèle (même révision) est repris du cache, sans recalcul | `gazette/themes/embeddings.py` |
| Regrouper | HDBSCAN : le nombre de thèmes vient des données, et les articles isolés vont dans le bruit | `gazette/themes/regroupement.py` |
| Nommer | Les mots les plus discriminants du groupe (c-TF-IDF, mots vides retirés) | `gazette/themes/regroupement.py` |
| Écrire et tracer | Thèmes, affectations et fiche du run, écrits dans une seule transaction | `gazette/themes/suivi.py` |

Le run est lancé **par le pipeline** : le capteur `themes_apres_silver` lance le job `gazette_themes` dès que le job silver du jour réussit (bronze à 6 h → silver → thèmes). On ne le lance pas à la main. Pour rejouer un jour, on matérialise la partition `gold/themes` depuis l'interface Dagster.

## Le modèle

| | |
| --- | --- |
| Modèle | [`sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2`](https://huggingface.co/sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2) |
| Version | Révision Hugging Face figée dans `config/clustering.yaml` (`modele.revision`) |
| Taille | ~120 M de paramètres, vecteurs de dimension 384 |
| Où il tourne | Sur CPU, dans le conteneur `code`. Quelques secondes pour quelques centaines d'articles |
| Pourquoi | Multilingue (les titres sont en français), léger, pensé pour la similarité de phrases courtes. Aucun coût, aucune clé d'API |
| Où il est | Téléchargé **à la construction de l'image** (`python -m gazette.themes.telecharger`) dans `/opt/models`. Les runs tournent hors ligne (`HF_HUB_OFFLINE=1`) |

Pas d'entraînement ni de fine-tuning : le modèle est utilisé tel quel.

**Changer de modèle** : modifier `modele.nom` et `modele.revision`, puis reconstruire l'image (`docker compose up -d --build`). Les vecteurs en cache sont indexés par modèle et révision, donc un nouveau modèle ne réutilise jamais les anciens vecteurs.

## Les paramètres

Tous les réglages sont dans [`config/clustering.yaml`](../config/clustering.yaml), et aucun dans le code. Le fichier est relu à chaque run : le dossier `config/` est monté dans le conteneur, il n'y a rien à reconstruire. Chaque run garde une copie des paramètres qu'il a lus (colonne `runs.parametres`).

| Paramètre | Valeur | Effet |
| --- | --- | --- |
| `fenetre_heures` | 24 | La fenêtre de temps des articles regroupés |
| `texte.champs` | `[titre, extrait]` | Ce qui est vectorisé |
| `modele.taille_lot` | 64 | Textes encodés par lot (vitesse seulement, pas le résultat) |
| `modele.normaliser` | `true` | Vecteurs de norme 1 : la distance euclidienne suit la similarité cosinus |
| `regroupement.algorithme` | `hdbscan` | Le seul accepté |
| `regroupement.taille_min_groupe` | 3 | Un thème compte au moins 3 articles |
| `regroupement.echantillons_min` | 2 | Plus haut : plus d'articles classés en bruit, des groupes plus sûrs |
| `regroupement.metrique` | `euclidean` | Distance entre vecteurs normalisés |
| `regroupement.methode_selection` | `eom` | `eom` donne des groupes stables, `leaf` des groupes plus fins et plus nombreux |
| `nommage.nb_mots` | 4 | Mots dans le nom d'un thème |
| `nommage.longueur_min_mot` | 3 | Les mots plus courts sont ignorés |
| `nommage.mots_vides` | `config/mots_vides_fr.txt` | Les mots exclus des noms |

**Pourquoi HDBSCAN** (famille 2, « elle trouve le nombre seule ») : le nombre de sujets change chaque jour, et le fixer à l'avance (famille 1, KMeans) forcerait des articles isolés dans des thèmes qui ne sont pas les leurs. HDBSCAN les laisse dans le bruit, qu'on mesure (`part_bruit`). Une chaîne tout-en-un (famille 3, type BERTopic) aurait fait la même chose avec une dépendance plus lourde et moins de prise sur chaque étape.

## Les tables

Base SQLite `data/gold/gazette.db` (le dossier `data/` est monté dans les conteneurs).

| Table | Une ligne par | Contenu |
| --- | --- | --- |
| `runs` | run | Voir ci-dessous |
| `themes` | thème d'un run | `run_id`, `theme_id` (0 = le plus gros), `jour`, `nom`, `mots_cles` (JSON), `nb_articles`, `nb_sources` |
| `articles_themes` | article d'un run | `run_id`, `source`, `lien` (la clé de `articles_propres`), `theme_id` (`NULL` = bruit), `titre` |
| vue `dernier_run_du_jour` | jour | Le dernier run **réussi** de chaque jour |
| vue `themes_du_jour` | thème | Les thèmes de ce run : **les thèmes en base** |

Les runs précédents restent en base : on peut comparer deux runs du même jour.

Le cache des vecteurs est à part : `data/cache/vecteurs.sqlite`, avec pour clé `(modèle, révision, sha256 du texte)`. Il n'est jamais purgé automatiquement, et on peut le supprimer sans risque (tout sera revectorisé).

## Un run tracé : la table `runs`

| Ce qu'on trace | Colonnes |
| --- | --- |
| Identité | `run_id`, `jour` (la partition), `statut` (`en_cours`, `reussi`, `echoue`), `erreur`, `debut`, `fin`, `duree_s` |
| Qui l'a lancé | `lance_par` (`dagster` ou `manuel`), `dagster_run_id` (le lien vers le run dans l'interface Dagster), `version_code` (commit git) |
| Données | `fenetre_debut`, `fenetre_fin`, `nb_articles`, `hash_entree` (sha256 des articles lus : même hash = mêmes données) |
| Modèle | `modele`, `modele_revision` |
| Paramètres | `parametres` (JSON, la copie du fichier) |
| Métriques | `metriques` (JSON) : `nb_themes`, `taille_mediane`, `taille_max`, `part_bruit`, `silhouette` (cosinus, sur les articles classés), `nb_vecteurs_calcules`, `nb_vecteurs_en_cache`, `durees_s` |
| Artefacts | `artefacts` (JSON) : la base, le nombre de lignes écrites dans `themes` et `articles_themes`, les jours de silver lus, le cache |

Le run est inscrit `en_cours` avant tout calcul. S'il échoue, il reste en base avec son erreur, et les thèmes du jour restent ceux du dernier run réussi. Les métriques principales apparaissent aussi dans les métadonnées de l'asset `gold/themes` dans Dagster, avec le `run_id`.

On a choisi une table plutôt qu'un outil de suivi (MLflow) : elle n'ajoute aucun service à héberger, et les thèmes et leur run sont dans la même base.

## Où lire les runs

En ligne de commande (dans le conteneur : `docker compose exec code python -m gazette.themes.runs`) :

```bash
python -m gazette.themes.runs                    # les 10 derniers runs, avec leurs métriques
python -m gazette.themes.runs --jour 2026-11-12  # les thèmes en base pour ce jour, et le run qui les a produits
```

En SQL (`sqlite3 data/gold/gazette.db`) :

```sql
-- Quel run a produit les thèmes en base ?
SELECT r.run_id, r.dagster_run_id, r.fin, r.modele, r.modele_revision, r.hash_entree, r.metriques
FROM dernier_run_du_jour d JOIN runs r USING (run_id)
WHERE d.jour = '2026-11-12';

-- Les thèmes du jour et leurs articles
SELECT t.nom, a.source, a.titre
FROM themes_du_jour t JOIN articles_themes a USING (run_id, theme_id)
WHERE t.jour = '2026-11-12'
ORDER BY t.theme_id;

-- L'historique des runs d'un jour
SELECT run_id, statut, debut, json_extract(metriques, '$.nb_themes') AS nb_themes,
       json_extract(metriques, '$.part_bruit') AS bruit, json_extract(metriques, '$.silhouette') AS silhouette
FROM runs WHERE jour = '2026-11-12' ORDER BY debut;
```

Dans Dagster (http://localhost:3000) : l'asset `gold/themes`, partition du jour. Ses métadonnées donnent le `run_id`, les métriques et la liste des thèmes.

## Reproductibilité

Les mêmes données d'entrée (`hash_entree`), le même modèle (`modele_revision`) et les mêmes paramètres (`parametres`) donnent les mêmes thèmes :

- la fenêtre se cale sur la dernière réception du jour, pas sur l'heure du run ;
- les articles sont triés par `(source, lien)` avant d'être vectorisés ;
- HDBSCAN est déterministe, et les thèmes sont numérotés par taille puis par ordre d'apparition ;
- en cas d'égalité, les noms sont départagés par ordre alphabétique.

## À relire (TD 6)

Ce qui est bon, ce qui est absurde, et une hypothèse sur pourquoi, sur deux journées :

- *[journée 1]*
- *[journée 2]*
