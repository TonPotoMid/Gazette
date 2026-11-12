import argparse
import logging
import os
import sys
from pathlib import Path

import requests

journal = logging.getLogger("gazette.alertes")

VARIABLE_WEBHOOK = "GAZETTE_ALERT_WEBHOOK_URL"
LIMITE_DISCORD = 2000  # caractères par message
TIMEOUT_S = 10


def charger_env(chemin: Path = Path(".env")) -> None:
    """Lit un fichier .env (CLE=valeur) sans écraser les variables déjà définies.

    Python ne lit pas .env tout seul : Docker Compose le fait pour les conteneurs, cette fonction
    sert aux commandes lancées à la main.
    """
    if not chemin.exists():
        return
    for ligne in chemin.read_text(encoding="utf-8").splitlines():
        ligne = ligne.strip()
        if not ligne or ligne.startswith("#") or "=" not in ligne:
            continue
        cle, valeur = ligne.split("=", 1)
        os.environ.setdefault(cle.strip(), valeur.strip().strip("\"'"))


def envoyer(message: str, url: str | None = None) -> bool:
    """Poste le message sur le webhook Discord ; renvoie False sans jamais lever d'exception.

    Une alerte qui échoue ne doit pas faire échouer le run qu'elle surveille. L'URL du webhook
    est un secret (quiconque la connaît peut écrire dans le salon) : elle n'est jamais journalisée,
    et on ne journalise pas le texte des erreurs réseau, qui la contient souvent.
    """
    url = url or os.environ.get(VARIABLE_WEBHOOK, "")
    if not url:
        journal.warning("Alerte non envoyée : %s n'est pas défini.", VARIABLE_WEBHOOK)
        return False
    if len(message) > LIMITE_DISCORD:
        message = message[: LIMITE_DISCORD - 1] + "…"
    try:
        reponse = requests.post(
            url,
            json={"content": message, "allowed_mentions": {"parse": []}},  # pas de @everyone
            timeout=TIMEOUT_S,
        )
    except requests.RequestException as erreur:
        journal.warning("Alerte non envoyée : %s.", type(erreur).__name__)
        return False
    if not reponse.ok:
        journal.warning("Alerte refusée par Discord (statut %s).", reponse.status_code)
        return False
    return True


def _lien(url_dagster: str, run_id: str) -> str:
    return f"{url_dagster}/runs/{run_id}"


def _contexte(job: str, partition: str | None) -> str:
    return f"Job `{job}`" + (f", jour {partition}" if partition else "")


def message_run_en_echec(
    job: str,
    partition: str | None,
    run_id: str,
    etapes: list[tuple[str, str]],
    url_dagster: str,
) -> str:
    """L'alerte d'un run en échec : les étapes en cause et un lien vers le run."""
    lignes = [f"🚨 **Gazette : run en échec**\n{_contexte(job, partition)}"]
    lignes += [f"- `{etape}` : {erreur}" for etape, erreur in etapes]
    lignes.append(_lien(url_dagster, run_id))
    return "\n".join(lignes)


def message_controles_en_echec(
    job: str,
    partition: str | None,
    run_id: str,
    controles: list[str],
    url_dagster: str,
) -> str:
    """L'alerte d'un run dont des contrôles de qualité sont faux."""
    lignes = [f"⚠️ **Gazette : contrôle de qualité en échec**\n{_contexte(job, partition)}"]
    lignes += [f"- {controle}" for controle in controles]
    lignes.append(_lien(url_dagster, run_id))
    return "\n".join(lignes)


def main(argv: list[str] | None = None) -> int:
    parseur = argparse.ArgumentParser(description="Alertes de la gazette (webhook Discord)")
    parseur.add_argument("--test", action="store_true", help="envoie un message de test")
    args = parseur.parse_args(argv)
    if not args.test:
        parseur.print_help()
        return 0

    charger_env()
    if not os.environ.get(VARIABLE_WEBHOOK):
        print(f"{VARIABLE_WEBHOOK} n'est pas défini (fichier .env à la racine du projet).")
        return 1
    if envoyer("✅ **Gazette : test d'alerte**\nSi tu lis ce message, le webhook fonctionne."):
        print("Message de test envoyé : regarde le salon Discord.")
        return 0
    print("Le message n'a pas pu être envoyé (webhook invalide, supprimé ou réseau coupé).")
    return 1


if __name__ == "__main__":
    sys.exit(main())
