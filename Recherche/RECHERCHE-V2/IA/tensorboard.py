import os
import subprocess
import webbrowser
import time

def launch_tensorboard(logdir="logs", port=6006):
    """
    Lance TensorBoard sur le dossier de logs indiqué.

    Args:
        logdir (str): le dossier contenant les logs TensorFlow.
        port (int): port d'écoute pour TensorBoard.
    """
    # Création du dossier de logs si nécessaire
    os.makedirs(logdir, exist_ok=True)

    # Commande TensorBoard
    command = [
        "tensorboard",
        f"--logdir={logdir}",
        f"--port={port}",
        "--reload_interval=5"
    ]

    print(f"[INFO] Lancement de TensorBoard sur http://localhost:{port} ...")

    # Lancer TensorBoard
    process = subprocess.Popen(command)

    # Attendre un peu avant d'ouvrir le navigateur
    time.sleep(2)

    # Ouvrir automatiquement la page dans le navigateur
    webbrowser.open(f"http://localhost:{port}")

    return process


if __name__ == "__main__":
    launch_tensorboard(logdir="logs", port=6006)
