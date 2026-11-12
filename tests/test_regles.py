from datetime import UTC, datetime, timedelta

import pytest

from gazette.silver import regles
from gazette.silver.article import Article

RECU = datetime(2026, 11, 12, 6, 0, tzinfo=UTC)


def article(**champs) -> Article:
    """Un article propre ; son extrait par défaut est unique (sinon R05 les lie)."""
    base = {
        "source": "le_monde",
        "jour": "2026-11-12",
        "lien": "https://exemple.fr/societe/un-article",
        "titre": "Un titre ordinaire sans rien de particulier",
        "rubriques": (),
        "publie_le": RECU - timedelta(hours=1),
        "recupere_le": RECU,
    }
    base.update(champs)
    base.setdefault("extrait", f"Extrait de {base['source']} {base['lien']} {base['titre']}")
    return Article(**base)


def rejete_par(un_article: Article) -> str | None:
    """L'identifiant de la règle qui écarte cet article quand il est seul, sinon None."""
    rejets = regles.appliquer_regles([un_article]).rejets
    return rejets[0].regle if rejets else None


# --- Les règles sur un article seul : elles écartent le cas, et laissent passer les faux positifs.


def test_un_article_ordinaire_est_garde():
    assert rejete_par(article()) is None


@pytest.mark.parametrize(
    "cas",
    [
        article(lien="https://www.ouest-france.fr/partenaires/espace-des-sciences/x"),
        article(extrait="Contenu partenaire proposé par une marque"),
        article(lien="https://exemple.fr/publi-reportage/un-article"),
    ],
)
def test_r01_publi(cas):
    assert rejete_par(cas) == "R01_publi"


def test_r01_ne_prend_pas_un_article_normal_pour_une_publi():
    assert (
        rejete_par(article(titre="Parcoursup publie ses résultats en pleine contestation")) is None
    )


@pytest.mark.parametrize(
    "titre",
    ["Horoscope du jeudi 12 novembre : Bélier, Taureau…", "Astrologie : la semaine du Lion"],
)
def test_r02_horoscope(titre):
    assert rejete_par(article(titre=titre)) == "R02_horoscope"


def test_r02_ne_prend_pas_catastrophique_pour_de_l_astrologie():
    assert rejete_par(article(titre="Un bilan catastrophique pour la saison de l'équipe")) is None


def test_r03_programme_tv():
    assert rejete_par(article(titre="Programme TV de ce soir : le film de 21 h")) == (
        "R03_programme_tv"
    )
    assert rejete_par(article(titre="Ce soir à la télé : le grand match de rugby")) == (
        "R03_programme_tv"
    )


def test_r03_ne_prend_pas_un_replay_pour_un_programme_tv():
    assert (
        rejete_par(article(titre="REPLAY. Manifestations : la soirée de violences à Paris")) is None
    )


@pytest.mark.parametrize(
    "cas",
    [
        article(titre="QUIZ. Testez vos connaissances sur la ponctuation"),
        article(titre="Jeu-concours : gagnez deux places pour le concert"),
        article(lien="https://www.ladepeche.fr/2025/10/16/lance-son-espace-jeux-a-vos-neurones"),
    ],
)
def test_r04_jeux(cas):
    assert rejete_par(cas) == "R04_jeux"


def test_r04_ne_prend_pas_les_jeux_olympiques_pour_un_jeu():
    assert rejete_par(article(titre="Les jeux olympiques de la jeunesse s'ouvrent à Dakar")) is None


@pytest.mark.parametrize("titre", ["", "   ", "Oui", "Onze chars!"])
def test_r06_titre_vide_ou_trop_court(titre):
    assert rejete_par(article(titre=titre)) == "R06_titre_vide"


def test_r06_garde_un_titre_de_la_longueur_minimale():
    assert rejete_par(article(titre="a" * regles.LONGUEUR_MIN_TITRE)) is None


def test_r07_date_future():
    assert rejete_par(article(publie_le=RECU + timedelta(minutes=1))) == "R07_date_future"


def test_r07_garde_une_date_passee_ou_absente():
    assert rejete_par(article(publie_le=RECU)) is None
    assert rejete_par(article(publie_le=None)) is None


@pytest.mark.parametrize(
    "cas",
    [
        article(titre="Un titre avec � dedans pour tester"),
        article(extrait="Ã©lection et Élection mélangées"),  # mélange : non réparable
        article(extrait="L’ancien ministre dâ€™État est mort"),
    ],
)
def test_r08_encodage_irreparable(cas):
    assert rejete_par(cas) == "R08_encodage"


def test_r08_garde_les_accents_normaux():
    texte = "Pâtes, bâtiment, âge, forêt, Noël, ça, où, Ça et Ñ ’ « » – 5 €"
    assert rejete_par(article(extrait=texte)) is None


# --- Les règles de comparaison : on garde le premier publié, on référence l'exemplaire gardé. ----


def test_r05_meme_extrait_dans_plusieurs_sources():
    premier = article(
        source="le_monde",
        lien="https://a/1",
        titre="Titre un pour le monde ici",
        extrait="Le texte exact de la dépêche reprise.",
        publie_le=RECU - timedelta(hours=3),
    )
    copie = article(
        source="liberation",
        lien="https://b/1",
        titre="Titre deux pour libération",
        extrait="Le texte exact de la dépêche reprise.",
        publie_le=RECU - timedelta(hours=2),
    )

    resultat = regles.appliquer_regles([copie, premier])

    assert resultat.gardes == [premier]
    assert [(r.article, r.regle, r.doublon_de) for r in resultat.rejets] == [
        (copie, "R05_agence_reprise", premier)
    ]


def test_r05_ne_touche_pas_deux_extraits_identiques_dans_la_meme_source():
    a = article(
        source="le_monde", lien="https://a/1", titre="Titre numéro un unique", extrait="Même texte."
    )
    b = article(
        source="le_monde",
        lien="https://a/2",
        titre="Titre numéro deux unique",
        extrait="Même texte.",
    )

    assert regles.appliquer_regles([a, b]).rejets == []


def test_r09_meme_titre_a_la_casse_et_la_ponctuation_pres():
    premier = article(
        source="le_monde",
        lien="https://a/1",
        titre="L’école ouvre ses portes",
        extrait="Extrait A.",
        publie_le=RECU - timedelta(hours=2),
    )
    copie = article(
        source="le_figaro",
        lien="https://b/1",
        titre="L'ÉCOLE ouvre ses portes !",
        extrait="Extrait B.",
        publie_le=RECU - timedelta(hours=1),
    )

    resultat = regles.appliquer_regles([copie, premier])

    assert resultat.gardes == [premier]
    assert resultat.rejets[0].regle == "R09_doublon_exact"
    assert resultat.rejets[0].doublon_de == premier


def test_r09_s_applique_aussi_dans_la_meme_source():
    a = article(source="le_monde", lien="https://a/1", titre="Le même titre exactement ici")
    b = article(source="le_monde", lien="https://a/2", titre="Le même titre exactement ici")

    assert [r.regle for r in regles.appliquer_regles([a, b]).rejets] == ["R09_doublon_exact"]


def test_r10_titres_presque_identiques_dans_des_sources_differentes():
    ny_figaro = article(
        source="le_figaro",
        lien="https://f/1",
        extrait="Extrait F.",
        titre="New York : un immeuble s'effondre à Brooklyn, deux morts et 17 blessés",
        publie_le=RECU - timedelta(hours=3),
    )
    ny_libe = article(
        source="liberation",
        lien="https://l/1",
        extrait="Extrait L.",
        titre="Un immeuble s’effondre à New York, deux morts et 17 blessés à Brooklyn",
        publie_le=RECU - timedelta(hours=2),
    )
    ny_franceinfo = article(
        source="franceinfo",
        lien="https://i/1",
        extrait="Extrait I.",
        titre="Brooklyn : un immeuble s'effondre à New York, deux morts et 17 blessés",
        publie_le=RECU - timedelta(hours=1),
    )
    autre = article(
        source="sud_ouest",
        lien="https://s/1",
        extrait="Extrait S.",
        titre="Le gouvernement présente son budget annuel devant les députés",
    )

    resultat = regles.appliquer_regles([ny_franceinfo, autre, ny_libe, ny_figaro])

    assert resultat.gardes == [autre, ny_figaro]  # dans l'ordre d'entrée
    assert {r.article.source for r in resultat.rejets} == {"liberation", "franceinfo"}
    assert all(r.regle == "R10_doublon_approx" for r in resultat.rejets)
    assert all(r.doublon_de == ny_figaro for r in resultat.rejets)


def test_r10_le_seuil_est_de_80_pour_cent_de_mots_en_commun():
    base = "alpha bravo charlie delta echo foxtrot golfe hotel india"  # 9 mots
    huit_sur_dix = article(source="a", lien="https://a/1", titre=base + " juliet")  # 9 / 10 = 90 %
    proche = article(source="b", lien="https://b/1", titre=base + " kilo")  # 9 / 11 ≈ 82 %
    limite = article(
        source="c",
        lien="https://c/1",
        titre="alpha bravo charlie delta echo foxtrot golfe lima mike november",
    )  # 7 / 12
    assert regles.SEUIL_DOUBLON_APPROX == 0.8

    resultat = regles.appliquer_regles([huit_sur_dix, proche, limite])

    assert [r.article for r in resultat.rejets] == [proche]
    assert limite in resultat.gardes


def test_r10_deux_titres_proches_dans_la_meme_source_ne_sont_pas_un_doublon():
    a = article(
        source="le_figaro",
        lien="https://a/1",
        titre="Budget 2027 : le gouvernement dévoile son plan annuel",
    )
    b = article(
        source="le_figaro",
        lien="https://a/2",
        titre="Budget 2027 : le gouvernement dévoile son plan annuel complet",
    )

    assert regles.appliquer_regles([a, b]).rejets == []


def test_premier_publie_puis_ordre_des_sources_en_cas_d_egalite():
    a = article(
        source="le_monde", lien="https://a/1", titre="Le même titre exactement ici", publie_le=RECU
    )
    b = article(
        source="liberation",
        lien="https://b/1",
        titre="Le même titre exactement ici",
        publie_le=RECU,
    )

    par_defaut = regles.appliquer_regles([b, a])
    ordonne = regles.appliquer_regles([a, b], ordre_sources=("liberation", "le_monde"))

    assert par_defaut.gardes == [a]  # alphabétique : le_monde avant liberation
    assert ordonne.gardes == [b]  # l'ordre de sources.csv


def test_un_article_sans_date_passe_apres_ceux_qui_en_ont():
    sans_date = article(
        source="le_monde", lien="https://a/1", titre="Le même titre exactement ici", publie_le=None
    )
    avec_date = article(
        source="liberation", lien="https://b/1", titre="Le même titre exactement ici"
    )

    assert regles.appliquer_regles([sans_date, avec_date]).gardes == [avec_date]


# --- L'ensemble. -----------------------------------------------------------------------------


def test_un_doublon_se_cherche_entre_articles_valides_seulement():
    # Le plus ancien est dans le futur : R07 l'écarte, donc l'autre n'est le doublon de personne.
    futur = article(
        source="le_monde",
        lien="https://a/1",
        titre="Le même titre exactement ici",
        publie_le=RECU + timedelta(hours=1),
    )
    normal = article(source="liberation", lien="https://b/1", titre="Le même titre exactement ici")

    resultat = regles.appliquer_regles([futur, normal])

    assert resultat.gardes == [normal]
    assert [(r.regle, r.doublon_de) for r in resultat.rejets] == [("R07_date_future", None)]


def test_un_article_n_est_rejete_qu_une_fois_par_la_premiere_regle():
    publi_et_titre_vide = article(titre="Oui", lien="https://exemple.fr/partenaires/x")

    assert rejete_par(publi_et_titre_vide) == "R01_publi"


def test_une_journee_complete_un_cas_par_regle():
    propres = [
        article(
            source="le_monde",
            lien="https://m/1",
            titre="Le gouvernement présente son budget annuel",
        ),
        article(
            source="liberation",
            lien="https://l/1",
            titre="Un séisme de magnitude six secoue la côte",
        ),
    ]
    polluants = {
        "R01_publi": article(
            lien="https://exemple.fr/partenaires/1", titre="Question de science sur les anneaux"
        ),
        "R02_horoscope": article(
            lien="https://exemple.fr/h", titre="Horoscope du jour : Bélier et Taureau"
        ),
        "R03_programme_tv": article(
            lien="https://exemple.fr/t", titre="Programme TV de ce soir : le film"
        ),
        "R04_jeux": article(
            lien="https://exemple.fr/j", titre="QUIZ. Testez vos connaissances en histoire"
        ),
        "R06_titre_vide": article(lien="https://exemple.fr/v", titre=""),
        "R07_date_future": article(
            lien="https://exemple.fr/f",
            titre="Un article daté de demain matin",
            publie_le=RECU + timedelta(days=1),
        ),
        "R08_encodage": article(
            lien="https://exemple.fr/e", titre="Ã©lection et Élection mélangées ici"
        ),
    }
    copie = article(
        source="franceinfo",
        lien="https://i/1",
        titre="Un séisme de magnitude six secoue la côte",
        publie_le=RECU - timedelta(minutes=10),
    )

    resultat = regles.appliquer_regles([*propres, *polluants.values(), copie])

    assert resultat.gardes == propres
    assert {r.article.lien: r.regle for r in resultat.rejets if r.article is not copie} == {
        a.lien: regle for regle, a in polluants.items()
    }
    assert [r.regle for r in resultat.rejets if r.article is copie] == ["R09_doublon_exact"]
    assert len(resultat.gardes) + len(resultat.rejets) == 10


def test_sans_articles():
    resultat = regles.appliquer_regles([])

    assert resultat.gardes == [] and resultat.rejets == []


def test_les_dix_regles_ont_des_identifiants_uniques_dans_l_ordre():
    identifiants = [identifiant for identifiant, _ in regles.regles()]

    assert identifiants == [
        "R01_publi",
        "R02_horoscope",
        "R03_programme_tv",
        "R04_jeux",
        "R05_agence_reprise",
        "R06_titre_vide",
        "R07_date_future",
        "R08_encodage",
        "R09_doublon_exact",
        "R10_doublon_approx",
    ]
