from datetime import UTC, datetime

from gazette.silver.article import depuis_bronze
from gazette.silver.normalisation import LONGUEUR_MAX_EXTRAIT

RECU = datetime(2026, 11, 12, 6, 0, tzinfo=UTC)


def ligne(**champs) -> dict:
    base = {
        "source": "le_monde",
        "jour": "2026-11-12",
        "recu_le": RECU,
        "title": "Un titre",
        "link": "https://exemple.fr/un-titre",
        "summary": "Un extrait.",
        "published": "Thu, 12 Nov 2026 05:00:00 +0100",
        "tags": [],
    }
    return {**base, **champs}


def test_depuis_bronze_normalise_les_champs():
    article = depuis_bronze(
        ligne(
            title="<b>Pluie</b> : cinq dÃ©partements &amp; Ã©lection ",
            summary="<p>Un   extrait</p>",
            link="  https://exemple.fr/a  ",
            tags=["Faits divers", " ", "Société "],
        )
    )

    assert article.titre == "Pluie : cinq départements & élection"
    assert article.extrait == "Un extrait"
    assert article.lien == "https://exemple.fr/a"
    assert article.rubriques == ("Faits divers", "Société")
    assert article.publie_le == datetime(2026, 11, 12, 4, 0, tzinfo=UTC)
    assert (article.source, article.jour, article.recupere_le) == ("le_monde", "2026-11-12", RECU)


def test_depuis_bronze_borne_l_extrait():
    article = depuis_bronze(ligne(summary="mot " * 200))

    assert len(article.extrait) <= LONGUEUR_MAX_EXTRAIT
    assert article.extrait.endswith("…")


def test_depuis_bronze_champs_absents():
    article = depuis_bronze(ligne(title=None, summary=None, link=None, published=None, tags=None))

    assert (article.titre, article.extrait, article.lien) == ("", "", "")
    assert article.rubriques == ()
    assert article.publie_le is None
