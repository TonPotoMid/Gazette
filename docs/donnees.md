# Données · La gazette

Le contrat des données : ce qu'on stocke, où, sous quelle forme, à quelle fraîcheur et pour combien de temps.

## Ce qu'on a le droit de garder

On ne collecte que ce que les éditeurs exposent aux machines : leurs flux RSS publics. Aucune page HTML n'est récupérée.

| On republie | On ne stocke pas |
| --- | --- |
| Le titre | Le texte intégral des articles |
| Un extrait court (300 caractères au plus) | Les images |
| Le lien vers l'original | Les commentaires |
| La date de publication | Les données sur les lecteurs (il n'y en a aucune) |
| La source (le journal) | La signature des auteurs dans ce qu'on republie |

La signature d'un auteur n'existe que dans le bronze, tel que le flux la donne (seul Libération la renseigne aujourd'hui). Elle n'est reprise ni dans le silver ni dans la gazette. Le bronze a une rétention courte (voir plus bas).

## Les couches

| Couche | Où | Format | Écriture |
| --- | --- | --- | --- |
| Bronze | `data/bronze/date=AAAA-MM-JJ/<source>/<flux>_<horodatage>.parquet`, avec un `.json` lisible à côté | Parquet (zstd) | Immuable : un fichier n'est jamais réécrit. Un article déjà collecté pour `(source, lien)` n'est pas réécrit |
| Silver, articles propres | `data/silver/articles_propres/date=AAAA-MM-JJ/articles_propres.parquet` | Parquet (zstd) | Recalculée : relancer un jour remplace ses fichiers |
| Silver, rejets | `data/silver/rejets/date=AAAA-MM-JJ/rejets.parquet` | Parquet (zstd) | Idem |
| Rapport de qualité | `rapports/qualite-AAAA-MM-JJ.md` | Markdown | Un par jour, remplacé si le jour est relancé |
| État des flux | `data/etat/<source>/<flux>.json` | JSON | Mis à jour à chaque collecte |

`data/` et `rapports/` sont les dossiers montés dans les conteneurs.

## Schéma

### Bronze

Une ligne par article, champs RSS tels que le flux les donne (aucun nettoyage).

| Champ | Contenu |
| --- | --- |
| `source` | Identifiant du journal (colonne `slug` de `sources.csv`) |
| `flux` | Nom du flux |
| `url_flux` | Adresse du flux |
| `recu_le` | Moment de la réponse du flux (UTC) |
| `rang` | Position de l'article dans le flux au moment de la collecte |
| `id` | Identifiant (`guid`) donné par le flux |
| `title`, `link`, `summary` | Titre, lien et extrait du flux, tels quels (l'extrait peut contenir du HTML) |
| `published`, `updated` | Dates du flux, en texte, avec leur fuseau d'origine |
| `author` | Signature donnée par le flux, vide chez la plupart des journaux |
| `tags` | Rubriques du flux |

Chaque fichier garde aussi, dans ses métadonnées, la réponse XML complète du flux (`gazette.xml_brut`) et le contexte HTTP de l'appel (`gazette.http` : adresse, statut, type de contenu, `ETag`, `Last-Modified`, taille).

### Silver : `articles_propres`

Une ligne par article gardé. Clé naturelle : `(source, lien)`.

| Champ | Contenu |
| --- | --- |
| `source` | Identifiant du journal |
| `jour` | Jour de collecte (AAAA-MM-JJ), pas le jour de publication |
| `lien` | Lien vers l'article original |
| `titre` | Titre nettoyé (HTML, entités, encodage et espaces normalisés) |
| `extrait` | Extrait nettoyé, 300 caractères au plus (coupé à la fin d'un mot, avec « … ») |
| `rubriques` | Rubriques du flux |
| `publie_le` | Date de publication en UTC ; vide si la date manque ou est illisible |
| `recupere_le` | Moment de la réception en UTC |
| `fichier_brut` | Fichier bronze d'origine, relatif à `data/` |
| `rang_brut` | `rang` de l'article dans ce fichier |

### Silver : `rejets`

Les mêmes champs que `articles_propres`, plus :

| Champ | Contenu |
| --- | --- |
| `regle` | La règle qui a écarté l'article (`R01_publi`… `R10_doublon_approx`, voir `qualite.md`) |
| `doublon_de_source`, `doublon_de_lien` | Pour un doublon : la source et le lien de l'article conservé, sinon vides |

## Fraîcheur

- Le bronze est collecté **une fois par jour, à 6 h 00 (heure de Paris)**, pour la date du jour.
- Le silver est calculé **dès que le job bronze se termine**, sans heure propre. Les contrôles de qualité tournent juste après.
- Objectif : les données du jour sont prêtes **avant 8 h**.
- Un jour est une partition : on peut le rejouer, et le silver du jour est alors recalculé à l'identique à partir du bronze.
- Un flux RSS ne donne que les articles du moment : rejouer un jour passé collecte le flux actuel, pas celui de la date rejouée.

## Rétention

| Donnée | Gardée | Ensuite |
| --- | --- | --- |
| Bronze (fichiers bruts) | 90 jours | supprimé |
| Silver (`articles_propres`, `rejets`) | 1 an | supprimé |
| Rapports de qualité | 1 an | supprimé |
| Éditions de la gazette (à venir) | 30 jours en ligne | déplacées vers un stockage froid (archive compressée) |
| État des flux | tant que le flux existe | — |

Aucune purge n'est automatisée pour l'instant : ces durées sont celles à appliquer.

## Traçabilité

D'un article propre, on retrouve l'item brut, sa source et l'appel qui l'a rapporté :

```python
import json
import pyarrow.parquet as pq

racine = "data/"
propre = pq.read_table(
    racine + "silver/articles_propres/date=2026-11-12/articles_propres.parquet"
).to_pylist()[0]

fichier = racine + propre["fichier_brut"]
brut = next(
    ligne for ligne in pq.read_table(fichier).to_pylist() if ligne["rang"] == propre["rang_brut"]
)  # l'item brut : titre, extrait, auteur, dates telles que le flux les donnait
reponse_xml = pq.read_schema(fichier).metadata[b"gazette.xml_brut"].decode("utf-8")
appel_http = json.loads(pq.read_schema(fichier).metadata[b"gazette.http"])
```

Le `rang` du bronze est celui de l'article dans le flux, pas sa position dans le fichier (un fichier ne contient que les articles nouveaux) : on le cherche par valeur, pas par index.
