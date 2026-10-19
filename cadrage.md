# Note de cadrage · La gazette

> Projet M2Ops · M2 Data Engineering & IA · Efrei · 2026-2027

## Le besoin

Chaque matin, nous voulons savoir de quoi parle la presse française, nationale comme régionale, sans avoir à la lire nous-mêmes. Le pipeline récupère les titres et les extraits de 8 journaux. Il les range, les regroupe par thèmes et publie une « gazette » du jour. Il doit **tourner seul**, et nous devons pouvoir **prouver qu'il a tourné** : quand, combien d'articles, quelles sources ont échoué.

## Périmètre

| Dedans | Dehors |
|---|---|
| 4 titres de PQN et 4 de PQR (voir `sources.csv`) | Le texte intégral des articles, les images, les commentaires |
| Titre, extrait, lien, date et rubrique de chaque article | Tout scraping de pages HTML |
| Une exécution automatique par jour, le matin | Le temps réel et la collecte en continu |
| Le regroupement des articles du jour par thèmes | L'analyse de sentiment, le résumé d'articles |
| La surveillance : métriques, alertes, journaux | Une interface d'administration ou des comptes utilisateurs |
| L'infrastructure décrite en code | Une application mobile |

## Contraintes

- **Coût nul :** aucun service payant, seulement des outils open source ou des offres gratuites.
- **Données autorisées :** uniquement ce que les éditeurs exposent aux machines (flux RSS), dans le respect de leurs conditions d'utilisation.
- **Autonomie :** aucune machine personnelle ne doit rester allumée pour que le pipeline tourne.
- **Horaire :** la gazette doit être prête avant 8 h, heure de Paris.
- **Reproductibilité :** tout doit pouvoir être relancé à l'identique, en local comme en CI.

## Stack retenue et alternatives écartées

| Brique | Retenu | Écarté, et pourquoi |
|---|---|---|
| Récupérer | Flux **RSS** officiels lus avec Python `feedparser`, **API GDELT** en secours | Scraping HTML : interdit par la consigne. NewsAPI : offre gratuite limitée au développement, 100 requêtes par jour |
| Faire tourner | **GitHub Actions** planifié (cron), retries, relance manuelle avec une date en paramètre | Airflow ou Prefect auto-hébergés : il faut une machine allumée en permanence. AWS : payant |
| Stocker | **Architecture médaillon**. Bronze : fichiers `.xml.gz` immuables. Silver et gold : **SQLite** (`gazette.db`) sur une branche `data` | DuckDB + Parquet : pensé pour l'analytique à grande échelle, superflu à ce volume. Delta Lake / Iceberg : trop complexes pour quelques centaines d'articles par jour. PostgreSQL gratuit hébergé : quotas et mise en veille |
| Regrouper | **sentence-transformers** (modèle multilingue) + **HDBSCAN**, thèmes nommés par c-TF-IDF, avec TF-IDF + KMeans comme référence | TF-IDF seul : trop faible sur des textes courts (titres et chapôs), et il faut fixer le nombre de thèmes à l'avance |
| Surveiller | Logs JSON, table `runs`, **issue GitHub ouverte automatiquement** en cas d'échec, **Healthchecks.io** pour l'alerte « n'a pas tourné », tableau de bord sur GitHub Pages | Prometheus + Grafana : à héberger soi-même. CloudWatch : payant au-delà de l'offre gratuite, et lié à AWS |
| Infra en code | **Terraform** (provider GitHub) pour le dépôt, les secrets, les branches et Pages. Workflows YAML, Dockerfile et `docker-compose.yml` pour rejouer en local | OpenTofu : compatible mais moins répandu. CDK ou CloudFormation : liés à AWS |

## Risques

| Risque | Parade |
|---|---|
| Un flux RSS change d'URL ou disparaît | Alerte si une source renvoie 0 article. Repli sur GDELT filtré par domaine. Mise à jour de `sources.csv` |
| Le cron GitHub est retardé aux heures chargées | Lancement à 5 h UTC, qui laisse de la marge avant 8 h. Healthchecks alerte si aucun run n'a réussi |
| Le cron est en UTC, d'où un décalage d'une heure au changement d'heure | Le documenter et choisir une heure qui convient dans les deux cas |
| Le workflow est désactivé après 60 jours sans activité | Le commit quotidien sur la branche `data` maintient l'activité du dépôt. Healthchecks alerte si le pipeline s'arrête |
| `gazette.db` est binaire : Git stocke une copie complète à chaque commit | Un seul commit par jour. Taille surveillée, avec nettoyage de l'historique si besoin |
| Le fichier d'état Terraform contient des secrets | Il n'est jamais commité (`.gitignore`). Une seule personne lance `terraform apply` |
| Les CGU d'un éditeur interdisent la réutilisation | Lecture des CGU pour chaque source (colonne `conditions`). On ne garde que titre, extrait et lien vers l'original |
| Les thèmes sont peu lisibles | Comparaison avec la référence TF-IDF + KMeans et relecture manuelle d'un échantillon |

## Rôles dans l'équipe

| Rôle | Responsable |
|---|---|
| Sources et collecte (RSS, GDELT, `sources.csv`) | *[Nom]* |
| Orchestration et infra en code (GitHub Actions, Terraform) | *[Nom]* |
| Stockage et modèle de données (médaillon, SQLite) | *[Nom]* |
| Regroupement thématique (embeddings, HDBSCAN) | *[Nom]* |
| Surveillance et qualité (métriques, alertes, tableau de bord) | *[Nom]* |

Voir aussi : [`architecture.md`](architecture.md) · [`veille.md`](veille.md) · [`../sources.csv`](../sources.csv)
