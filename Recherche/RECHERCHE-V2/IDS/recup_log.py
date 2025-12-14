import paramiko
import os
import time

def retrieve_and_delete_file(
    hostname,
    username,
    password=None,
    sudo_password=None,
    key_file=None,
    remote_path=None,
    local_path=None,
    port=22
):
    try:
        # --- Connexion SSH ---
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if key_file:
            private_key = paramiko.RSAKey.from_private_key_file(key_file)
            ssh.connect(hostname, port=port, username=username, pkey=private_key)
        else:
            ssh.connect(hostname, port=port, username=username, password=password)

        print("[+] Connecté au serveur SSH")

        # --- Ouverture du canal SFTP ---
        sftp = ssh.open_sftp()

        # --- Téléchargement ---
        file_name = os.path.basename(remote_path)
        local_file = os.path.join(local_path, file_name)

        print(f"[+] Téléchargement de {remote_path} vers {local_file} ...")
        sftp.get(remote_path, local_file)
        print("[+] Téléchargement terminé")

         # --- Vider le contenu du fichier avec sudo ---
        print(f"[+] Vidage du fichier distant via sudo : {remote_path}")

        clear_cmd = f"sudo sh -c \"> '{remote_path}'\""
        stdin, stdout, stderr = ssh.exec_command(clear_cmd, get_pty=True)

        # Envoi du mot de passe sudo
        stdin.write(sudo_password + "\n")
        stdin.flush()

        time.sleep(1)

        error = stderr.read().decode()
        if error.strip():
            print("[ERREUR sudo] :", error)
        else:
            print("[+] Contenu du fichier vidé avec succès")

        ssh.close()
        print("[✓] Opération terminée")

    except Exception as e:
        print(f"[ERREUR] {e}")


'''
# Exemple d’utilisation :
if __name__ == "__main__":
   retrieve_and_delete_file(
        hostname="172.16.62.132",
        username="ids-2025",
        password="IDS-2025",
        sudo_password="IDS-2025",
        remote_path="/var/log/suricata/eve.json",
        local_path="/home/matdaarson/Documents/ATELIER CYBER/IDS/LOGS")'''
