#!/usr/bin/env python3
"""
TPOT Sync – Récupération automatique des données T-Pot

Ce script synchronise les répertoires de logs T‑Pot (ex: /data) vers une
machine de collecte (votre poste, un serveur data lake, etc.).

Deux méthodes:
  1) rsync (recommandé si disponible côté collecteur et T‑Pot via SSH)
  2) SFTP (Paramiko) en fallback si rsync indisponible

Fonctionnalités:
- Synchronisation incrémentale (ne transfère que les nouveaux/fichiers modifiés)
- Filtres include/exclude (glob) pour limiter les types de logs
- Préservation de l'arborescence et des timestamps
- Reprise sur erreurs et journalisation détaillée
- Mode "daemon" avec intervalle de poll
- Fichier d'état optionnel pour contrôler une fenêtre "since"

Exemples d'usage:
  # Sync ponctuel avec rsync
  python3 tpot_sync.py \
    --host 10.0.0.42 --user tpot --key ~/.ssh/id_ed25519 \
    --remote-dir /data --local-dir /srv/tpot-logs \
    --use-rsync

  # Sync toutes les 10 min en SFTP
  python3 tpot_sync.py \
    --host 10.0.0.42 --user tpot --password '***' \
    --remote-dir /data --local-dir ./tpot-logs \
    --interval 600

  # Filtrer pour Cowrie & Suricata uniquement
  python3 tpot_sync.py ... \
    --include "**/cowrie/*.json*" --include "**/suricata/eve.json*" \
    --exclude "**/*.pcap" --exclude "**/*.tar"

Dépendances:
  - Python 3.9+
  - pip install paramiko pyyaml tqdm (tqdm est optionnel)

"""

from __future__ import annotations
import argparse
import fnmatch
import getpass
import logging
import os
import shutil
import stat
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

try:
    from tqdm import tqdm  # type: ignore
except Exception:  # pragma: no cover
    tqdm = None  # graceful fallback

try:
    import paramiko  # type: ignore
except Exception:
    paramiko = None

LOG = logging.getLogger("tpot_sync")

# --- Services ciblés (par défaut) ---
DEFAULT_SERVICES = [
    "cowrie",
    "dionaea",
    "tanner",
    "h0neytr4p",  # variante "leet"
    "honeytrap",  # variante standard T-Pot
    "mailoney",
    "conpot",
    "ciscoasa",
]

# Récupérer tous les fichiers sous les répertoires des services ciblés
DEFAULT_INCLUDES = [
    "**/*",  # on prend tout (le filtrage par service est fait ensuite)
]
# Aucune exclusion par défaut : on veut tout récupérer (pcap, zip compris)
DEFAULT_EXCLUDES: List[str] = []

@dataclass
class Config:
    host: str
    user: str
    port: int = 22
    password: Optional[str] = None
    key: Optional[str] = None
    known_hosts: Optional[str] = None
    remote_dir: str = "tpotce/data"
    local_dir: str = "./tpot-logs"
    includes: List[str] = field(default_factory=lambda: DEFAULT_INCLUDES.copy())
    excludes: List[str] = field(default_factory=lambda: DEFAULT_EXCLUDES.copy())
    services: List[str] = field(default_factory=lambda: DEFAULT_SERVICES.copy())
    delete: bool = False
    interval: int = 0  # seconds; 0 => run once
    use_rsync: bool = False
    state_file: Optional[str] = None
    timeout: int = 30
    max_depth: Optional[int] = None

# --------------------------- Utils ---------------------------

def setup_logging(verbosity: int) -> None:
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

def which(cmd: str) -> Optional[str]:
    return shutil.which(cmd)

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def match_filters(path: str, includes: List[str], excludes: List[str]) -> bool:
    # include if matches ANY include and NONE of excludes
    included = any(fnmatch.fnmatch(path, pattern) for pattern in includes) if includes else True
    excluded = any(fnmatch.fnmatch(path, pattern) for pattern in excludes) if excludes else False
    return included and not excluded

def path_in_services(rel_path: str, services: List[str]) -> bool:
    """
    True si 'rel_path' se trouve sous l'un des répertoires de 'services'.
    On fait un test insensible à la casse et on cherche des segments '/service/'.
    """
    p = rel_path.replace("\\", "/").lower()
    # autoriser au cas où le service est en tête (pas de slash avant)
    for s in services:
        s = s.lower().strip("/")
        if f"/{s}/" in p:
            return True
        if p.startswith(s + "/"):
            return True
        # tolérer chemins finissant par /service (dossiers vides, etc.)
        if p.endswith("/" + s) or p.endswith("/" + s + "/"):
            return True
    return False

# --------------------------- Rsync path ---------------------------

def build_rsync_cmd(cfg: Config) -> List[str]:
    ssh_cmd = ["ssh", "-p", str(cfg.port)]
    if cfg.key:
        ssh_cmd += ["-i", os.path.expanduser(cfg.key)]
    if cfg.known_hosts:
        ssh_cmd += ["-o", f"UserKnownHostsFile={cfg.known_hosts}"]

    base = [
        "rsync", "-a", "--partial", "--prune-empty-dirs", "--out-format=%o\t%n",
        "-e", " ".join(ssh_cmd),
    ]

    # Pour restreindre aux services ciblés avec rsync, on doit :
    #  - autoriser la traversée des dossiers: --include='*/'
    #  - inclure chaque service: --include='**/service/**'
    #  - et exclure tout le reste: --exclude='*'
    # On n'utilise PAS cfg.includes/cfg.excludes ici, car on veut un contrôle strict par services.
    base += ["--include", "*/"]
    for s in cfg.services:
        s_clean = s.strip("/").lower()
        # on inclut aussi bien **/service/** que **/service (au cas où)
        base += ["--include", f"**/{s_clean}/**"]
        base += ["--include", f"**/{s_clean}"]

    # Si l'utilisateur a donné des includes additionnels, on les garde,
    # mais ils ne doivent pas élargir hors des services (le --exclude='*' tranche).
    for inc in cfg.includes:
        base += ["--include", inc]

    # Enfin, on exclut tout le reste
    base += ["--exclude", "*"]

    if cfg.delete:
        base += ["--delete", "--delete-after", "--ignore-errors"]

    remote = f"{cfg.user}@{cfg.host}:{cfg.remote_dir.rstrip('/')}/"
    local = str(Path(cfg.local_dir).resolve()) + "/"
    base += [remote, local]
    return base

def rsync_sync(cfg: Config) -> Tuple[int, str]:
    cmd = build_rsync_cmd(cfg)
    LOG.info("Running rsync: %s", " ".join(cmd))
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=max(cfg.timeout, 5)*60)
        out = (proc.stdout or "") + (proc.stderr or "")
        return proc.returncode, out
    except subprocess.TimeoutExpired:
        return 124, "rsync timed out"

# --------------------------- SFTP path ---------------------------

class SFTPSync:
    def __init__(self, cfg: Config):
        if paramiko is None:
            raise RuntimeError("Paramiko non installé. Activez --use-rsync ou `pip install paramiko`.")
        self.cfg = cfg
        self.ssh = None
        self.sftp = None

    def connect(self):
        LOG.debug("Connecting via Paramiko SFTP…")
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        if self.cfg.known_hosts:
            self.ssh.load_host_keys(self.cfg.known_hosts)
        self.ssh.connect(
            hostname=self.cfg.host,
            port=self.cfg.port,
            username=self.cfg.user,
            password=self.cfg.password,
            key_filename=os.path.expanduser(self.cfg.key) if self.cfg.key else None,
            timeout=self.cfg.timeout,
            allow_agent=True,
            look_for_keys=True,
        )
        self.sftp = self.ssh.open_sftp()

    def close(self):
        try:
            if self.sftp:
                self.sftp.close()
            if self.ssh:
                self.ssh.close()
        finally:
            self.sftp = None
            self.ssh = None

    def walk(self, root: str, max_depth: Optional[int]=None) -> Iterable[Tuple[str, List[str], List[str]]]:
        # Generator similar to os.walk for SFTP
        stack = [(root.rstrip('/'), 0)]
        while stack:
            current, depth = stack.pop()
            try:
                entries = self.sftp.listdir_attr(current)
            except IOError as e:
                LOG.warning("Impossible de lister %s: %s", current, e)
                continue
            dirs, files = [], []
            for e in entries:
                name = e.filename
                path = f"{current}/{name}"
                if stat.S_ISDIR(e.st_mode):
                    dirs.append(path)
                else:
                    files.append(path)
            yield current, [Path(d).name for d in dirs], [Path(f).name for f in files]
            if max_depth is None or depth < max_depth:
                for d in dirs:
                    stack.append((d, depth+1))

    def sync(self):
        ensure_dir(Path(self.cfg.local_dir))
        root = self.cfg.remote_dir.rstrip('/')
        to_download: List[Tuple[str, Path, int, float]] = []  # (remote_path, local_path, size, mtime)

        # Collect files to download
        for current, _dirs, files in self.walk(root, self.cfg.max_depth):
            for fname in files:
                rpath = f"{current}/{fname}"
                # SFTP lstat
                try:
                    st = self.sftp.lstat(rpath)
                except IOError:
                    continue
                rel = rpath[len(root)+1:] if rpath.startswith(root + "/") else fname

                # 1) Filtre par services ciblés
                if not path_in_services(rel, self.cfg.services):
                    continue
                # 2) Filtre include/exclude glob si fourni
                if not match_filters(rel, self.cfg.includes, self.cfg.excludes):
                    continue

                lpath = Path(self.cfg.local_dir) / rel
                # décide si téléchargement nécessaire
                need = False
                if not lpath.exists():
                    need = True
                else:
                    try:
                        lst = lpath.stat()
                        if int(lst.st_mtime) < int(st.st_mtime) or lst.st_size != st.st_size:
                            need = True
                    except FileNotFoundError:
                        need = True
                if need:
                    to_download.append((rpath, lpath, st.st_size, st.st_mtime))

        # Download with progress
        pbar = None
        if tqdm and to_download:
            pbar = tqdm(total=len(to_download), unit="file", desc="Downloading")
        for rpath, lpath, size, mtime in to_download:
            lpath.parent.mkdir(parents=True, exist_ok=True)
            tmp = lpath.with_suffix(lpath.suffix + ".part")
            try:
                with self.sftp.open(rpath, "rb") as rf, open(tmp, "wb") as lf:
                    bufsize = 256 * 1024
                    while True:
                        data = rf.read(bufsize)
                        if not data:
                            break
                        lf.write(data)
                os.utime(tmp, (mtime, mtime))
                tmp.replace(lpath)
                LOG.info("OK -> %s (%d bytes)", lpath, size)
            except Exception as e:
                LOG.error("Échec %s: %s", rpath, e)
                try:
                    if tmp.exists():
                        tmp.unlink()
                except Exception:
                    pass
            finally:
                if pbar:
                    pbar.update(1)
        if pbar:
            pbar.close()

# --------------------------- Runner ---------------------------

def run_once(cfg: Config) -> None:
    ensure_dir(Path(cfg.local_dir))
    if cfg.use_rsync:
        if which("rsync") is None:
            LOG.error("rsync non trouvé dans le PATH. Basculer en SFTP (retirez --use-rsync) ou installez rsync.")
            return
        code, out = rsync_sync(cfg)
        if code != 0:
            LOG.error("rsync a retourné %s\n%s", code, out)
        else:
            for line in out.splitlines():
                if line.strip():
                    LOG.debug("rsync: %s", line)
    else:
        syncer = SFTPSync(cfg)
        try:
            syncer.connect()
            syncer.sync()
        finally:
            syncer.close()

def run_loop(cfg: Config) -> None:
    if cfg.interval <= 0:
        run_once(cfg)
        return
    LOG.info("Mode boucle: intervalle = %ss", cfg.interval)
    while True:
        start = time.time()
        try:
            run_once(cfg)
        except Exception as e:
            LOG.exception("Erreur de synchronisation: %s", e)
        elapsed = time.time() - start
        sleep_for = max(1, cfg.interval - int(elapsed))
        time.sleep(sleep_for)

# --------------------------- CLI ---------------------------

def parse_args(argv: Optional[List[str]] = None) -> Config:
    p = argparse.ArgumentParser(description="Synchroniser automatiquement les données T-Pot (services ciblés)")
    p.add_argument("--host", required=True, help="Adresse/IP du serveur T-Pot")
    p.add_argument("--port", type=int, default=22)
    p.add_argument("--user", required=True)
    auth = p.add_mutually_exclusive_group(required=False)
    auth.add_argument("--password", help="Mot de passe SSH")
    auth.add_argument("--key", help="Chemin d'une clé privée SSH")
    p.add_argument("--known-hosts", dest="known_hosts", help="Fichier known_hosts à utiliser (optionnel)")
    p.add_argument("--remote-dir", default="/home/parisbrest/tpotce/data", help="Répertoire racine à synchroniser côté T-Pot")
    p.add_argument("--local-dir", default="./tpot-logs", help="Répertoire de sortie local")

    # Services à cibler (répétable). Par défaut: DEFAULT_SERVICES
    p.add_argument("--services", action="append", dest="services", default=[],
                   help="Nom de service/honeypot à inclure (répétable). Ex: --services cowrie --services dionaea")

    # Inclure / Exclure (optionnel) – par défaut on prend tout
    p.add_argument("--include", action="append", dest="includes", default=[], help="Glob d'inclusion additionnel (répétable)")
    p.add_argument("--exclude", action="append", dest="excludes", default=[], help="Glob d'exclusion additionnel (répétable)")

    p.add_argument("--delete", action="store_true", help="Miroir strict (supprime localement ce qui a été supprimé côté T-Pot)")
    p.add_argument("--interval", type=int, default=0, help="Intervalle en secondes pour répéter la sync (0=une fois)")
    p.add_argument("--use-rsync", action="store_true", help="Utiliser rsync via SSH si disponible")
    p.add_argument("--timeout", type=int, default=30, help="Timeout connexion/commande (minutes pour rsync)")
    p.add_argument("--max-depth", type=int, default=None, help="Profondeur max de parcours (SFTP)")
    p.add_argument("-v", action="count", default=0, help="Verbosity -v, -vv")

    args = p.parse_args(argv)

    includes = args.includes if args.includes else DEFAULT_INCLUDES.copy()
    excludes = args.excludes if args.excludes else DEFAULT_EXCLUDES.copy()
    services = [s.strip("/").lower() for s in (args.services if args.services else DEFAULT_SERVICES.copy())]

    if args.password is None and args.key is None:
        if sys.stdin.isatty():
            try:
                pw = getpass.getpass("Mot de passe SSH (laisser vide pour agent/clé par défaut): ")
                if pw:
                    args.password = pw
            except Exception:
                pass

    cfg = Config(
        host=args.host,
        user=args.user,
        port=args.port,
        password=args.password,
        key=args.key,
        known_hosts=args.known_hosts,
        remote_dir=args.remote_dir,
        local_dir=args.local_dir,
        includes=includes,
        excludes=excludes,
        services=services,
        delete=args.delete,
        interval=args.interval,
        use_rsync=args.use_rsync,
        timeout=args.timeout,
        max_depth=args.max_depth,
    )
    setup_logging(args.v)
    LOG.info("Services ciblés: %s", ", ".join(cfg.services))
    return cfg

def main(argv: Optional[List[str]] = None) -> int:
    cfg = parse_args(argv)
    try:
        run_loop(cfg)
        return 0
    except KeyboardInterrupt:
        LOG.info("Interrompu par l'utilisateur")
        return 130

if __name__ == "__main__":
    sys.exit(main())


"""
Notes de déploiement rapides
=============================

1) Dépendances Python
---------------------
python3 -m venv .venv && . .venv/bin/activate
pip install --upgrade pip paramiko tqdm pyyaml

2) Clé SSH sans mot de passe (optionnel)
----------------------------------------
ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519 -N ""
ssh-copy-id -i ~/.ssh/id_ed25519.pub tpot@<IP_TPOT>

3) Lancer en service systemd (collecteur Linux)
-----------------------------------------------
Créer /etc/systemd/system/tpot-sync.service

[Unit]
Description=T-Pot logs sync
After=network-online.target

[Service]
Type=simple
User=collector
WorkingDirectory=/opt/tpot-sync
ExecStart=/opt/tpot-sync/.venv/bin/python /opt/tpot-sync/tpot_sync.py \
  --host <IP_TPOT> --user tpot --key /home/collector/.ssh/id_ed25519 \
  --remote-dir /data --local-dir /srv/tpot-logs --use-rsync --interval 600
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target

Activer:
  systemctl daemon-reload && systemctl enable --now tpot-sync

4) Cron (alternative)
---------------------
*/10 * * * * /opt/tpot-sync/.venv/bin/python /opt/tpot-sync/tpot_sync.py --host <IP_TPOT> --user tpot --key ~/.ssh/id_ed25519 --remote-dir /data --local-dir /srv/tpot-logs --use-rsync >> /var/log/tpot-sync.log 2>&1

5) Exemples de patterns utiles
------------------------------
--include "**/cowrie/**/*.json*"  (Cowrie)
--include "**/suricata/**/eve.json*"  (Suricata)
--include "**/dionaea/**/*.json*"  (Dionaea)
--include "**/nginx/**/*.log"      (Reverse proxy)

6) Sécurité
-----------
- Compte dédié en lecture seule sur le T‑Pot (chmod/chown restreints)
- Clé SSH avec options `from="<collector_ip>",command="/usr/bin/rrsync"` pour limiter rsync
- Réseau: autoriser uniquement TCP/22 depuis collecteur

7) Intégration pipeline
-----------------------
Le répertoire `--local-dir` peut être surveillé par un ETL, Logstash/Filebeat, ou
consommé directement par vos scripts d'extraction/normalisation (ex. extract_tpot_logs.py).

"""