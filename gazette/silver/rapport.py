from collections import Counter
from pathlib import Path

from gazette.silver import regles
from gazette.silver.passage import Passage
from gazette.silver.tables import ecrire_remplacant

TOP = 5
EXEMPLES_PAR_REGLE = 3


def _taux(nombre: int, total: int) -> str:
    return f"{nombre / total:.1%}" if total else "0.0%"


def _cellule(texte: str) -> str:
    return texte.replace("|", "\\|").replace("\n", " ")


def construire_rapport(passage: Passage, sources: list[str]) -> str:
    """Le rapport de qualité d'un passage, en Markdown : entrés, gardés, rejetés, top 5."""
    inattendues = sorted({a.source for a in passage.entres} - set(sources))
    sources = [*sources, *inattendues]
    entres = Counter(a.source for a in passage.entres)
    gardes = Counter(a.source for a in passage.gardes)
    rejetes = Counter(r.article.source for r in passage.rejets)
    par_regle_source = Counter((r.regle, r.article.source) for r in passage.rejets)
    nb_entres, nb_rejetes = len(passage.entres), len(passage.rejets)

    out = [
        f"# Rapport de qualité du {passage.jour}",
        "",
        "| Entrés | Gardés | Rejetés | Taux de rejet |",
        "| ---: | ---: | ---: | ---: |",
        f"| {nb_entres} | {len(passage.gardes)} | {nb_rejetes} | {_taux(nb_rejetes, nb_entres)} |",
        "",
    ]
    if not nb_entres:
        return "\n".join([*out, "Aucun article collecté ce jour-là.", ""])

    out += [
        "## Par source",
        "",
        "| Source | Entrés | Gardés | Rejetés | Taux de rejet |",
        "| --- | ---: | ---: | ---: | ---: |",
        *[
            f"| {s} | {entres[s]} | {gardes[s]} | {rejetes[s]} | {_taux(rejetes[s], entres[s])} |"
            for s in sources
        ],
        "",
        "## Rejets par règle et par source",
        "",
        "| Règle | " + " | ".join(sources) + " | Total |",
        "| --- |" + " ---: |" * (len(sources) + 1),
    ]
    for identifiant, _ in regles.regles():
        valeurs = [par_regle_source[(identifiant, s)] for s in sources]
        out.append(f"| {identifiant} | " + " | ".join(map(str, valeurs)) + f" | {sum(valeurs)} |")

    out += ["", f"## Les {TOP} rejets les plus fréquents", ""]
    if not passage.rejets:
        out.append("Aucun rejet.")
    else:
        out += [
            "| Règle | Source | Rejetés | Part des entrés de la source |",
            "| --- | --- | ---: | ---: |",
        ]
        plus_frequents = sorted(par_regle_source.items(), key=lambda kv: (-kv[1], kv[0]))[:TOP]
        out += [
            f"| {regle} | {source} | {n} | {_taux(n, entres[source])} |"
            for (regle, source), n in plus_frequents
        ]

    if passage.rejets:
        out += ["", "## Exemples de rejets"]
        for identifiant, _ in regles.regles():
            concernes = [r for r in passage.rejets if r.regle == identifiant]
            if not concernes:
                continue
            out += ["", f"### {identifiant} ({len(concernes)})", ""]
            for rejet in concernes[:EXEMPLES_PAR_REGLE]:
                ligne = f"- {rejet.article.source} : « {_cellule(rejet.article.titre)} »"
                if rejet.doublon_de:
                    gardee = rejet.doublon_de
                    ligne += f" (doublon de {gardee.source} : « {_cellule(gardee.titre)} »)"
                out.append(ligne)
    return "\n".join([*out, ""])


def ecrire_rapport(dossier: Path, passage: Passage, sources: list[str]) -> Path:
    """Écrit rapports/qualite-AAAA-MM-JJ.md ; relancer le même jour remplace le rapport."""
    chemin = dossier / f"qualite-{passage.jour}.md"
    ecrire_remplacant(chemin, construire_rapport(passage, sources).encode("utf-8"))
    return chemin
