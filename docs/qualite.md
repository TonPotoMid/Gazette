# Qualité des données · La gazette

## Les dix pollueurs

| # | Pollueur | Ce que c'est | Pourquoi on l'écarte |
| --- | --- | --- | --- |
| 1 | Publi-reportage, contenu partenaire | Article fourni ou payé par un annonceur, présenté comme contenu partenaire | Ce n'est pas de l'information éditoriale |
| 2 | Horoscope | Prévisions astrologiques du jour, signe par signe | Pas de l'actualité, et le même texte revient chaque jour |
| 3 | Programme TV | Grille ou sélection des émissions de la soirée | Contenu de service, sans lien avec l'actualité |
| 4 | Jeux et concours | Quiz, mots croisés, jeux-concours, tirages | Divertissement ou promotion, pas de l'information |
| 5 | Dépêche d'agence reprise | Texte d'une agence de presse (AFP, Reuters…) publié tel quel par plusieurs journaux | Le même contenu compte plusieurs fois, et ce n'est pas le travail du journal |
| 6 | Titre vide | Article dont le titre est vide ou réduit à quelques caractères | Impossible à lire, à comparer ou à regrouper |
| 7 | Date dans le futur | Date de publication postérieure au moment où l'article a été reçu | Date fausse ou mal lue (fuseau) : elle fausse le classement par jour |
| 8 | Encodage cassé | Texte dont les accents sont remplacés par des séquences comme `Ã©` | Illisible, et deux titres identiques ne se reconnaissent plus |
| 9 | Doublon exact | Même titre (à la casse, la ponctuation et les espaces près) qu'un autre article | Gonfle le volume et fausse les thèmes |
| 10 | Doublon approximatif | Même fait raconté par plusieurs journaux, avec des titres presque identiques | Un événement pèse autant que le nombre de journaux qui en parlent |

## Règles d'exclusion

Chaque règle écarte des articles vers `rejets`, avec son identifiant. Les motifs et les seuils sont dans `gazette/silver/regles.py`.

| Règle | Ce qu'elle écarte | Pourquoi | Exemple |
| --- | --- | --- | --- |
| `R01_publi` | Article dont l'adresse contient `/partenaires/`, `/publi-` ou `/sponsoris`, ou qui se présente comme « contenu partenaire » | Ce n'est pas de l'information éditoriale | Ouest-France, `/partenaires/espace-des-sciences/` : « Question de science. Comment a-t-on percé les secrets des anneaux de Saturne ? » |
| `R02_horoscope` | Article contenant les mots « horoscope », « zodiaque » ou « astrologie » | Pas de l'actualité, et le même texte revient chaque jour | « Horoscope du jeudi 12 novembre : Bélier, Taureau… » |
| `R03_programme_tv` | Article contenant « programme TV », « programme télé » ou « ce soir à la télé » | Contenu de service, sans lien avec l'actualité | « Programme TV de ce soir : le film de 21 h » |
| `R04_jeux` | Titre contenant « quiz », « jeu-concours », « gagnez », « mots croisés », « mots fléchés » ou « sudoku », ou adresse contenant `/jeux/` | Divertissement ou promotion, pas de l'information | Ouest-France : « QUIZ. Expressions, conjugaison, ponctuation… ne commettez plus les erreurs courantes ! » |
| `R05_agence_reprise` | Même extrait dans au moins deux sources le même jour | Dépêche d'agence reprise telle quelle : le même contenu compterait plusieurs fois | Deux journaux publient la même dépêche, mot pour mot, sous deux titres différents |
| `R06_titre_vide` | Titre vide ou de moins de 12 caractères | Impossible à lire, à comparer ou à regrouper | « Oui » |
| `R07_date_future` | Date de publication (UTC) postérieure au moment où l'article a été reçu | Date fausse ou mal lue : elle fausse le classement par jour | Article daté du 13 novembre reçu le 12 |
| `R08_encodage` | Titre ou extrait qui contient encore `�`, `Ã©` ou `â€™` une fois la normalisation passée | Illisible, et impossible à réparer automatiquement | « Ã©lection et Élection mélangées » (accents corrects et cassés dans le même texte) |
| `R09_doublon_exact` | Même titre qu'un autre article, à la casse, la ponctuation et les espaces près | Gonfle le volume et fausse les thèmes | « L’école ouvre ses portes » et « L'ÉCOLE ouvre ses portes ! » |
| `R10_doublon_approx` | Titre presque identique à celui d'un autre article d'une autre source, le même jour : au moins 80 % de mots en commun | Un événement pèse autant que le nombre de journaux qui en parlent | Franceinfo : « Un immeuble s'effondre à New York et fait au moins deux morts et 17 blessés » ; Le Figaro : « New York : un immeuble s'effondre à Brooklyn, au moins deux morts et 17 blessés » |

- Un article n'est rejeté qu'une fois par la première règle qui l'écarte. Les règles qui regardent un article seul passent d'abord (R01 à R04, R06 à R08) puis les comparaisons (R05, R09, R10) entre articles valides d'un même jour.
- Dans un groupe de doublons, on garde l'article publié en premier (à égalité, la source la plus haute dans `sources.csv`). Les autres vont dans `rejets` avec la règle.