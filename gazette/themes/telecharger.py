"""Télécharge le modèle d'embeddings de `config/clustering.yaml` dans le cache Hugging Face.

Appelé à la construction de l'image Docker : le modèle est dans l'image, les runs n'ont pas
besoin du réseau. `python -m gazette.themes.telecharger`
"""

from gazette.themes.embeddings import ModeleSentenceTransformers
from gazette.themes.parametres import charger_parametres

if __name__ == "__main__":
    parametres = charger_parametres()
    vecteurs = ModeleSentenceTransformers(parametres.modele).encoder(["essai"])
    print(f"{parametres.modele.nom}@{parametres.modele.revision} : dimension {vecteurs.shape[1]}")
