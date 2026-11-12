import numpy as np
import pytest

from gazette.themes.embeddings import (
    CacheVecteurs,
    ModeleSentenceTransformers,
    cle_texte,
    texte_a_vectoriser,
    vectoriser,
)
from gazette.themes.parametres import charger_parametres

# Dix articles, deux sujets bien séparés : un séisme et un budget.
DIX_ARTICLES = [
    ("Un séisme de magnitude 6 frappe le sud de l'Italie", "La secousse a été ressentie jusqu'à Naples."),
    ("Italie : fort tremblement de terre près de Naples", "Les habitants sont sortis dans la rue."),
    ("Séisme en Italie, les secours mobilisés", "Plusieurs bâtiments se sont effondrés en Campanie."),
    ("Tremblement de terre : le bilan s'alourdit en Italie", "Les pompiers fouillent les décombres."),
    ("Les images du séisme qui a secoué Naples", "Des fissures sur les façades du centre historique."),
    ("Le gouvernement présente son projet de budget 2027", "Le déficit public doit baisser l'an prochain."),
    ("Budget : Bercy promet 40 milliards d'économies", "Le ministre des Finances détaille son plan."),
    ("Projet de loi de finances : ce qui change pour les impôts", "Le barème de l'impôt sur le revenu est indexé."),
    ("Budget 2027 : les députés commencent l'examen du texte", "Le débat s'ouvre à l'Assemblée nationale."),
    ("Déficit : le Haut Conseil des finances publiques sceptique", "Les prévisions de croissance jugées optimistes."),
]


class EncodeurFactice:
    """Encode chaque texte par un vecteur tiré de son empreinte, et compte ce qu'on lui demande."""

    nom = "factice"
    revision = "v1"

    def __init__(self):
        self.textes_encodes: list[str] = []

    def encoder(self, textes):
        self.textes_encodes.extend(textes)
        return np.array(
            [np.random.default_rng(int(cle_texte(t)[:8], 16)).normal(size=8) for t in textes],
            dtype=np.float32,
        )


def test_texte_a_vectoriser_suit_les_champs_choisis():
    assert texte_a_vectoriser("Titre", "Extrait") == "Titre\nExtrait"
    assert texte_a_vectoriser("Titre", "", ("titre", "extrait")) == "Titre"
    assert texte_a_vectoriser("Titre", "Extrait", ("titre",)) == "Titre"


def test_un_article_n_est_vectorise_qu_une_fois(tmp_path):
    cache = CacheVecteurs(tmp_path / "cache.sqlite")
    encodeur = EncodeurFactice()

    premier = vectoriser(["a", "b", "a"], encodeur, cache)
    second = vectoriser(["b", "c", "a"], encodeur, cache)

    assert encodeur.textes_encodes == ["a", "b", "c"]  # « a » répété et « b » repris du cache
    assert (premier.nb_calcules, premier.nb_en_cache) == (2, 0)
    assert (second.nb_calcules, second.nb_en_cache) == (1, 2)
    np.testing.assert_array_equal(premier.vecteurs[0], premier.vecteurs[2])
    np.testing.assert_array_equal(premier.vecteurs[1], second.vecteurs[0])


def test_le_cache_survit_au_processus_et_distingue_les_modeles(tmp_path):
    chemin = tmp_path / "cache.sqlite"
    vectoriser(["a"], EncodeurFactice(), CacheVecteurs(chemin))

    relu = EncodeurFactice()
    vectoriser(["a"], relu, CacheVecteurs(chemin))  # nouvelle instance : relu sur disque
    autre_revision = EncodeurFactice()
    autre_revision.revision = "v2"
    vectoriser(["a"], autre_revision, CacheVecteurs(chemin))

    assert relu.textes_encodes == []
    assert autre_revision.textes_encodes == ["a"]


def test_vectoriser_rien(tmp_path):
    resultat = vectoriser([], EncodeurFactice(), CacheVecteurs(tmp_path / "cache.sqlite"))
    assert resultat.vecteurs.shape[0] == 0


@pytest.fixture(scope="module")
def vrai_modele():
    pytest.importorskip("sentence_transformers")
    return ModeleSentenceTransformers(charger_parametres().modele)


def test_dix_articles_avec_le_vrai_modele(tmp_path, vrai_modele):
    textes = [texte_a_vectoriser(t, e) for t, e in DIX_ARTICLES]
    cache = CacheVecteurs(tmp_path / "cache.sqlite")

    premier = vectoriser(textes, vrai_modele, cache)
    second = vectoriser(textes, vrai_modele, cache)

    v = premier.vecteurs
    assert v.shape[0] == 10 and v.shape[1] > 100
    np.testing.assert_allclose(np.linalg.norm(v, axis=1), 1.0, atol=1e-4)  # normalisés
    assert (second.nb_calcules, second.nb_en_cache) == (0, 10)
    np.testing.assert_array_equal(premier.vecteurs, second.vecteurs)

    # Chaque article est plus proche, en moyenne, de son sujet que de l'autre.
    similarites = v @ v.T
    for i in range(10):
        meme = [j for j in range(10) if j != i and (j < 5) == (i < 5)]
        autre = [j for j in range(10) if (j < 5) != (i < 5)]
        assert similarites[i, meme].mean() > similarites[i, autre].mean()
