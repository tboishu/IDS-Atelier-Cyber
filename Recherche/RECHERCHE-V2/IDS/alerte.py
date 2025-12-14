import requests
import json


def send_discord(result):

    fields = {
        "Nom du Poste": 'IDS-2025',
        "Pourcentage d'Anomalie " : 100 - result.get("anomaly_rate"),
        "Recommandation" : "Vérification de l'intégrité"
    }
    webhook_url="https://discordapp.com/api/webhooks/1442761023663767563/yl7ouChHX1X4K9hX_mnhEGzfXvbSFoHft-vqgEbPVQHpXxxh-ATSLlOCOIK7XAvW9X9K"
    title = "ANOMALIE DETECTEE"
    data = {
        "embeds": [
            {
                "title": title,
                "color": 15158332,  # rouge
                "fields": [
                    {"name": key, "value": str(val), "inline": False}
                    for key, val in fields.items()
                ]
            }
        ]
    }
    requests.post(webhook_url, json=data)


#--------------------------------------------
# à ajoute dans le code main si Anomaly
'''

send_discord(
    {
        "Source": row.get("src_ip"),
        "Destination": row.get("dst_ip"),
        "Service": row.get("service"),
        "Event": row.get("event_type"),
        "Erreur": f"{err:.6f}"
    }
)

'''