import dagster as dg

# Une partition par jour, heure de Paris. end_offset=1 rend la partition du jour
# disponible dès minuit : le run de 6 h collecte donc la date du jour, pas la veille.
partitions_quotidiennes = dg.DailyPartitionsDefinition(
    start_date="2026-10-19",
    timezone="Europe/Paris",
    end_offset=1,
)
