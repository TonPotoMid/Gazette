from datetime import UTC, datetime

import pytest

from gazette.silver import normalisation as n


def test_reparer_encodage_remet_les_accents():
    assert n.reparer_encodage("Ã©lection : le rÃ©sultat") == "élection : le résultat"
    assert n.reparer_encodage("Dâ€™un cÃ´tÃ© Ã  lâ€™autre") == "D’un côté à l’autre"
    assert n.reparer_encodage("Ã\u0081lvaro") == "Álvaro"  # octet 0x81 : seulement en latin-1


def test_reparer_encodage_ne_touche_pas_un_texte_normal():
    for texte in ("", "ASCII seul", "Pâtes, bâtiment, âge, forêt, Noël, ça", "5 € « oui » ’"):
        assert n.reparer_encodage(texte) == texte


def test_reparer_encodage_laisse_un_texte_a_moitie_casse():
    texte = "Ã©lection et Élection"  # accents normaux et cassés mélangés : on ne devine pas
    assert n.reparer_encodage(texte) == texte


def test_retirer_html_garde_le_texte():
    assert n.retirer_html('Un <b>texte</b> avec <a href="https://x.fr">lien</a>') == (
        "Un texte avec lien"
    )
    assert n.retirer_html("<img src='a.jpg'/>Légende") == "Légende"
    assert n.retirer_html("avant<!-- un commentaire -->après") == "avantaprès"


def test_retirer_html_ne_colle_pas_les_paragraphes():
    assert n.retirer_html("Un<br>deux<br/>trois") == "Un deux trois"
    assert n.retirer_html("<p>a</p><p>b</p>") == " a  b "
    assert n.retirer_html("Un mot<b>mot</b>e") == "Un motmote"  # balise dans un mot : pas d'espace


def test_retirer_html_ignore_un_inferieur_a_dans_une_phrase():
    assert n.retirer_html("3 < 5 et 7 > 2") == "3 < 5 et 7 > 2"


def test_decoder_entites():
    assert n.decoder_entites("Tom &amp; Jerry : l&#39;&eacute;t&eacute;") == "Tom & Jerry : l'été"
    assert n.decoder_entites("a&nbsp;b") == "a b"
    assert n.decoder_entites("rien à décoder") == "rien à décoder"


def test_normaliser_espaces():
    assert n.normaliser_espaces("  a  b\tc\n\nd e  ") == "a b c d e"
    assert n.normaliser_espaces("zéro​largeur﻿") == "zérolargeur"
    assert n.normaliser_espaces("") == ""
    assert n.normaliser_espaces(" \n ") == ""


@pytest.mark.parametrize(
    ("texte", "attendu"),
    [
        ("Thu, 12 Nov 2026 05:00:00 +0100", datetime(2026, 11, 12, 4, 0, tzinfo=UTC)),
        ("Mon, 19 Oct 2026 15:01:03 +0200", datetime(2026, 10, 19, 13, 1, 3, tzinfo=UTC)),
        ("Mon, 19 Oct 2026 14:55:53 GMT", datetime(2026, 10, 19, 14, 55, 53, tzinfo=UTC)),
        ("Mon, 19 Oct 2026 14:43:41 +0000", datetime(2026, 10, 19, 14, 43, 41, tzinfo=UTC)),
        ("2026-11-12T05:00:00+01:00", datetime(2026, 11, 12, 4, 0, tzinfo=UTC)),
        ("2026-11-12T05:00:00Z", datetime(2026, 11, 12, 5, 0, tzinfo=UTC)),
        ("2026-11-12T05:00:00", datetime(2026, 11, 12, 5, 0, tzinfo=UTC)),  # sans fuseau : UTC
    ],
)
def test_date_en_utc(texte, attendu):
    resultat = n.date_en_utc(texte)

    assert resultat == attendu
    assert resultat.utcoffset().total_seconds() == 0


@pytest.mark.parametrize("texte", [None, "", "demain matin", "32 Foo 2026"])
def test_date_en_utc_illisible_donne_none(texte):
    assert n.date_en_utc(texte) is None


def test_borner_extrait_ne_touche_pas_un_extrait_court():
    assert n.borner_extrait("Un extrait court.") == "Un extrait court."
    exact = "a" * n.LONGUEUR_MAX_EXTRAIT
    assert n.borner_extrait(exact) == exact


def test_borner_extrait_coupe_a_la_fin_d_un_mot():
    texte = " ".join(["mot"] * 200)

    resultat = n.borner_extrait(texte)

    assert len(resultat) <= n.LONGUEUR_MAX_EXTRAIT
    assert resultat.endswith("mot…")  # pas de mot coupé en deux, pas d'espace avant « … »
    assert texte.startswith(resultat.removesuffix("…"))


def test_borner_extrait_longueur_personnalisee_et_ponctuation():
    assert (
        n.borner_extrait("Une phrase, puis une autre phrase qui continue", 22)
        == "Une phrase, puis une…"
    )
    assert n.borner_extrait("aaaa, bbbb", 8) == "aaaa…"  # la virgule avant « … » est retirée


def test_borner_extrait_sans_espace_coupe_net():
    resultat = n.borner_extrait("x" * 500)

    assert len(resultat) == n.LONGUEUR_MAX_EXTRAIT
    assert resultat.endswith("…")


def test_nettoyer_texte_enchaine_les_transformations_dans_l_ordre():
    brut = "<p>Le&nbsp;Monde  &amp; Ã©lection</p>"

    assert n.nettoyer_texte(brut) == "Le Monde & élection"


def test_nettoyer_texte_sans_texte():
    assert n.nettoyer_texte(None) == ""
    assert n.nettoyer_texte("") == ""
    assert n.nettoyer_texte("   ") == ""


def test_nettoyer_texte_est_idempotent():
    brut = "<b>Un</b> titre&nbsp;: Ã©lection &amp; co"
    propre = n.nettoyer_texte(brut)

    assert n.nettoyer_texte(propre) == propre
