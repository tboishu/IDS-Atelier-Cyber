from test_anomaly import  test_anomaly_detection
from format import main
from alerte import send_discord
from recup_log import retrieve_and_delete_file

import time
import os

def supprimer(file_path):
    chemin_fichier = os.path.join(file_path)

    try:
        if os.path.exists(chemin_fichier):
            os.remove(chemin_fichier)
            print(f"[✓] Fichier supprimé : {chemin_fichier}")
        else:
            print("[!] Le fichier n'existe pas dans ce dossier.")
    except Exception as e:
        print(f"[ERREUR] Impossible de supprimer : {e}")


if __name__ == "__main__":
    while 2 > 0:
        retrieve_and_delete_file(
            hostname="172.16.62.132",
            username="ids-2025",
            password="IDS-2025",
            sudo_password="IDS-2025",
            remote_path="/var/log/suricata/eve.json",
            local_path="/home/matdaarson/Documents/ATELIER CYBER/RECHERCHE-V2/IDS/logs")
        
        time.sleep(0.5)
        main()
        time.sleep(0.5)
        result = test_anomaly_detection("data/logs.csv")
        if result.get("anomaly_rate") >= 50 : 
            print('SAFE')
        else : 
            print("NOT SAFE : envoi d'une alerte")
            send_discord(result)
        supprimer('data/logs.csv')
        supprimer('logs/eve.json')
        time.sleep(15)



