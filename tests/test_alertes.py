import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from gazette import alertes

SECRET = "TOKEN-SECRET-12345"


class FauxDiscord:
    """Un serveur local qui joue le webhook : il garde les messages reçus et répond `statut`."""

    def __init__(self, statut: int = 204) -> None:
        self.statut = statut
        self.recus: list[dict] = []
        faux = self

        class Gestionnaire(BaseHTTPRequestHandler):
            def do_POST(self):
                longueur = int(self.headers["Content-Length"])
                faux.recus.append(json.loads(self.rfile.read(longueur)))
                self.send_response(faux.statut)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *args):
                pass

        self.http = ThreadingHTTPServer(("127.0.0.1", 0), Gestionnaire)
        self.url = f"http://127.0.0.1:{self.http.server_port}/api/webhooks/123/{SECRET}"
        threading.Thread(target=self.http.serve_forever, daemon=True).start()

    def arreter(self):
        self.http.shutdown()
        self.http.server_close()


@pytest.fixture
def discord():
    faux = FauxDiscord()
    yield faux
    faux.arreter()


# --- Les messages ------------------------------------------------------------------------------


def test_message_run_en_echec_donne_les_etapes_et_un_lien():
    message = alertes.message_run_en_echec(
        "gazette_quotidienne",
        "2026-11-12",
        "abc123",
        [("bronze__la_depeche", "Aucun flux de La Dépêche du Midi n'a répondu.")],
        "http://localhost:3000",
    )

    assert message.splitlines() == [
        "🚨 **Gazette : run en échec**",
        "Job `gazette_quotidienne`, jour 2026-11-12",
        "- `bronze__la_depeche` : Aucun flux de La Dépêche du Midi n'a répondu.",
        "http://localhost:3000/runs/abc123",
    ]


def test_message_controles_en_echec_liste_chaque_controle():
    controle = "silver/articles_propres · taux_de_rejet_par_source : ouest_france 10.0%"

    message = alertes.message_controles_en_echec(
        "gazette_silver", "2026-11-12", "abc123", [controle], "http://localhost:3000"
    )

    assert message.splitlines()[0] == "⚠️ **Gazette : contrôle de qualité en échec**"
    assert f"- {controle}" in message
    assert message.endswith("http://localhost:3000/runs/abc123")


def test_message_sans_partition():
    message = alertes.message_run_en_echec("job", None, "r1", [], "http://x")

    assert message.splitlines()[1] == "Job `job`"


# --- L'envoi -------------------------------------------------------------------------------------


def test_envoyer_poste_le_message_sans_jamais_mentionner_personne(discord):
    assert alertes.envoyer("Bonjour @everyone", url=discord.url) is True

    [recu] = discord.recus
    assert recu["content"] == "Bonjour @everyone"
    assert recu["allowed_mentions"] == {"parse": []}  # le @everyone d'un titre ne réveille personne


def test_envoyer_lit_l_url_dans_l_environnement(discord, monkeypatch):
    monkeypatch.setenv(alertes.VARIABLE_WEBHOOK, discord.url)

    assert alertes.envoyer("depuis l'environnement") is True
    assert discord.recus[0]["content"] == "depuis l'environnement"


def test_envoyer_coupe_un_message_trop_long_pour_discord(discord):
    assert alertes.envoyer("x" * 5000, url=discord.url)

    contenu = discord.recus[0]["content"]
    assert len(contenu) == alertes.LIMITE_DISCORD and contenu.endswith("…")


def test_envoyer_sans_url_ne_fait_rien_et_ne_plante_pas(monkeypatch, caplog):
    monkeypatch.delenv(alertes.VARIABLE_WEBHOOK, raising=False)

    with caplog.at_level(logging.WARNING, logger="gazette.alertes"):
        assert alertes.envoyer("message") is False

    assert alertes.VARIABLE_WEBHOOK in caplog.text


def test_envoyer_renvoie_false_si_discord_refuse_et_ne_journalise_pas_l_url(discord, caplog):
    discord.statut = 404  # webhook supprimé

    with caplog.at_level(logging.WARNING, logger="gazette.alertes"):
        assert alertes.envoyer("message", url=discord.url) is False

    assert "404" in caplog.text
    assert SECRET not in caplog.text


def test_envoyer_renvoie_false_si_le_reseau_est_coupe_et_ne_journalise_pas_l_url(caplog):
    url_morte = f"http://127.0.0.1:9/api/webhooks/123/{SECRET}"  # rien n'écoute sur ce port

    with caplog.at_level(logging.WARNING, logger="gazette.alertes"):
        assert alertes.envoyer("message", url=url_morte) is False

    assert "ConnectionError" in caplog.text
    assert SECRET not in caplog.text  # le texte d'une erreur réseau contient l'adresse complète


# --- .env et commande de test --------------------------------------------------------------------


def test_charger_env_lit_les_cles_sans_ecraser_l_existant(tmp_path, monkeypatch):
    fichier = tmp_path / ".env"
    fichier.write_text(
        "# un commentaire\n\nA_NOUVELLE=valeur\nB_ENTRE_GUILLEMETS='avec guillemets'\n"
        "C_EXISTANTE=du_fichier\nD_AVEC_EGAL=a=b\n",
        encoding="utf-8",
    )
    for cle in ("A_NOUVELLE", "B_ENTRE_GUILLEMETS", "D_AVEC_EGAL"):
        monkeypatch.delenv(cle, raising=False)
    monkeypatch.setenv("C_EXISTANTE", "de_l_environnement")

    alertes.charger_env(fichier)

    import os

    assert os.environ["A_NOUVELLE"] == "valeur"
    assert os.environ["B_ENTRE_GUILLEMETS"] == "avec guillemets"
    assert os.environ["C_EXISTANTE"] == "de_l_environnement"
    assert os.environ["D_AVEC_EGAL"] == "a=b"
    for cle in ("A_NOUVELLE", "B_ENTRE_GUILLEMETS", "D_AVEC_EGAL"):
        os.environ.pop(cle, None)


def test_charger_env_sans_fichier_ne_fait_rien(tmp_path):
    alertes.charger_env(tmp_path / "absent.env")


def test_commande_test_envoie_un_message(discord, monkeypatch, capsys, tmp_path):
    monkeypatch.chdir(tmp_path)  # pas de .env ici : on part de l'environnement
    monkeypatch.setenv(alertes.VARIABLE_WEBHOOK, discord.url)

    assert alertes.main(["--test"]) == 0

    assert "envoyé" in capsys.readouterr().out
    assert "test d'alerte" in discord.recus[0]["content"]


def test_commande_test_sans_webhook_dit_quoi_faire(monkeypatch, capsys, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv(alertes.VARIABLE_WEBHOOK, raising=False)

    assert alertes.main(["--test"]) == 1

    assert ".env" in capsys.readouterr().out


def test_commande_test_ne_montre_jamais_l_url(discord, monkeypatch, capsys, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(alertes.VARIABLE_WEBHOOK, discord.url)
    discord.statut = 500

    assert alertes.main(["--test"]) == 1

    sortie = capsys.readouterr()
    assert SECRET not in sortie.out + sortie.err
