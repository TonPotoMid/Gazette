from datetime import UTC, datetime

from gazette import pollueurs

RECU = datetime(2026, 11, 12, 6, 0, tzinfo=UTC)


def ligne(**champs) -> dict:
    base = {
        "source": "le_monde",
        "jour": "2026-11-12",
        "recu_le": RECU,
        "title": "Un titre ordinaire sans rien de particulier",
        "link": "https://exemple.fr/societe/un-titre",
        "summary": "Un extrait ordinaire.",
        "published": "Thu, 12 Nov 2026 05:00:00 +0100",
        "tags": [],
    }
    return {**base, **champs}


def mesure(numero: int, lignes: list[dict]) -> pollueurs.Mesure:
    return pollueurs.mesurer(lignes)[numero - 1]


def test_dix_pollueurs_dans_l_ordre_de_la_consigne():
    mesures = pollueurs.mesurer([ligne()])

    assert [m.numero for m in mesures] == list(range(1, 11))
    assert all(not m.lignes for m in mesures)  # une ligne ordinaire ne pollue rien


def test_publi_par_segment_d_url_ou_texte():
    par_url = ligne(link="https://www.ouest-france.fr/partenaires/espace-des-sciences/x")
    par_texte = ligne(summary="Contenu partenaire proposé par une marque")

    assert mesure(1, [par_url, par_texte, ligne()]).lignes == [par_url, par_texte]


def test_horoscope_et_programme_tv_par_mots_entiers():
    horoscope = ligne(title="Horoscope du jour : Bélier, vos amours")
    catastrophe = ligne(title="Un bilan catastrophique pour la saison")  # contient « astro »
    tv = ligne(title="Programme TV de ce soir : le film de 21 h")

    assert mesure(2, [horoscope, catastrophe, tv]).lignes == [horoscope]
    assert mesure(3, [horoscope, catastrophe, tv]).lignes == [tv]


def test_jeux_et_concours():
    quiz = ligne(title="QUIZ. Testez vos connaissances")
    espace = ligne(link="https://www.ladepeche.fr/2025/10/16/lance-son-espace-jeux-a-vos-neurones")
    olympique = ligne(title="Les jeux olympiques de la jeunesse s'ouvrent")  # pas un jeu

    assert mesure(4, [quiz, espace, olympique]).lignes == [quiz, espace]


def test_depeche_d_agence_par_mention_ou_titre_repris_par_plusieurs_journaux():
    afp = ligne(summary="Selon l'AFP, le texte a été publié ce matin")
    a = ligne(source="le_monde", title="Le même titre exactement")
    b = ligne(source="liberation", title="Le même titre exactement")

    assert mesure(5, [afp, a, b, ligne()]).lignes == [afp, a, b]


def test_titre_vide_ou_trop_court():
    vide = ligne(title="   ")
    court = ligne(title="Oui")
    absent = ligne(title=None)

    assert mesure(6, [vide, court, absent, ligne()]).lignes == [vide, court, absent]


def test_date_future_se_compare_en_utc_quel_que_soit_le_fuseau():
    futur = ligne(published="Thu, 12 Nov 2026 08:00:00 +0100")  # 07 h UTC > reçu à 06 h UTC
    passe = ligne(published="Thu, 12 Nov 2026 06:30:00 +0100")  # 05 h 30 UTC
    illisible = ligne(published="demain matin")

    assert mesure(7, [futur, passe, illisible]).lignes == [futur]


def test_encodage_casse():
    casse = ligne(title="Ã©lection : le rÃ©sultat")
    apostrophe = ligne(summary="Dâ€™un cÃ´tÃ© Ã\u00a0 lâ€™autre")
    remplacement = ligne(title="Un titre avec � dedans")
    propre = ligne(title="Élection : le résultat")

    assert mesure(8, [casse, apostrophe, remplacement, propre]).lignes == [
        casse,
        apostrophe,
        remplacement,
    ]


def test_les_accents_normaux_ne_sont_pas_un_encodage_casse():
    # Régression : une recherche de « Â » insensible à la casse flaggait tout « â ».
    texte = "Pâtes, bâtiment, âge, forêt, ça, Noël, coût, où, Ça et Ñ ’ « » – 5 €"

    assert not pollueurs.encodage_casse(texte)
    assert not pollueurs.encodage_casse("")
    assert not pollueurs.encodage_casse(None)


def test_doublon_exact_ignore_apostrophes_espaces_et_casse():
    a = ligne(source="le_monde", title="L’école ouvre ses portes")
    b = ligne(source="franceinfo", title="L'ÉCOLE ouvre ses portes")

    assert mesure(9, [a, b, ligne()]).lignes == [a, b]


def test_doublon_approximatif_meme_jour_et_sources_differentes():
    a = ligne(source="le_figaro", title="New York : un immeuble s'effondre à Brooklyn, deux morts")
    b = ligne(source="liberation", title="Un immeuble s’effondre à New York, deux morts à Brooklyn")
    autre_jour = ligne(
        source="sud_ouest", jour="2026-11-13", title="Un immeuble s'effondre à New York, deux morts"
    )
    autre_sujet = ligne(source="sud_ouest", title="Le gouvernement présente son budget annuel")

    resultat = mesure(10, [a, b, autre_jour, autre_sujet])

    assert resultat.lignes == [a, b]
    assert len(resultat.groupes) == 1


def test_deux_titres_proches_dans_la_meme_source_ne_sont_pas_un_doublon():
    premier = ligne(
        source="le_figaro", title="Budget 2027 : le gouvernement dévoile son plan annuel"
    )
    second = ligne(
        source="le_figaro", title="Budget 2027 : le gouvernement dévoile son plan annuel complet"
    )

    assert mesure(10, [premier, second]).lignes == []


def test_sensibilite_plus_le_seuil_baisse_plus_il_y_a_de_groupes():
    a = ligne(
        source="le_monde", title="Pluie et inondations cinq départements du Sud-Est en vigilance"
    )
    b = ligne(
        source="liberation", title="Pluie et inondations cinq départements du Sud-Est touchés"
    )
    c = ligne(
        source="franceinfo",
        title="Inondations dans le Sud-Est : cinq départements en vigilance orange",
    )

    groupes_par_seuil = {seuil: g for seuil, g, _, _ in pollueurs.sensibilite([a, b, c])}

    assert groupes_par_seuil[0.8] <= groupes_par_seuil[0.7] <= groupes_par_seuil[0.6]
    assert groupes_par_seuil[0.6] == 1
