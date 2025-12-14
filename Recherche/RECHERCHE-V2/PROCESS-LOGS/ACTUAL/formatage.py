#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
extract_tpot_logs.py  —  "max info" edition

Usage:
  python3 extract_tpot_logs.py --data-dir /data --out-file tpot_logs.parquet
  python3 extract_tpot_logs.py --data-dir /data --out-file tpot_logs.csv --format csv

- Scanne le dossier T-Pot et normalise un maximum de logs (cowrie, dionaea, tanner, h0neytr4p,
  mailoney, conpot, ciscoasa, suricata/eve.json, CSV génériques, JSON/JSONL/.log JSON).
- Ajoute de nombreux champs protocolaires: HTTP, DNS, TLS/JA3, SMTP, SCADA/Modbus,
  URL/Host/UA, hashes (md5/sha1/sha256), JA3/JA3s, SNI, etc.
- Détecte automatiquement le honeypot ("hp") depuis le chemin/nom de fichier.
- Écrit en flux par paquets pour limiter la RAM.

Notes:
- Ne fait pas de géolocalisation ni d'enrichissement externe (hors-scope offline).
- Les champs absents restent à None (colonnes présentes si rencontrées).

Author: PROJET ATELIER
"""
import argparse
import os
import json
import gzip
import io
import csv
from datetime import datetime
from pathlib import Path
from typing import Iterator, Dict, Any, Optional, List, Iterable
import pandas as pd
# --------------------------- Helpers ---------------------------

def iter_lines(path: Path) -> Iterator[str]:
    opener = gzip.open if path.suffix == ".gz" else open
    mode = "rt"
    with opener(path, mode, encoding="utf-8", errors="ignore") as f:
        for line in f:
            yield line.rstrip("\n")

def read_bytes_head(path: Path, nbytes: int = 65536) -> bytes:
    if path.suffix == ".gz":
        with gzip.open(path, "rb") as f:
            return f.read(nbytes)
    else:
        with open(path, "rb") as f:
            return f.read(nbytes)

def sniff_delimiter(sample_bytes: bytes, default: str = ",") -> str:
    try:
        sample_text = sample_bytes.decode("utf-8", errors="ignore")
        dialect = csv.Sniffer().sniff(sample_text, delimiters=[",",";","\t","|"])
        return dialect.delimiter
    except Exception:
        return default

def lower_nodot(s: str) -> str:
    return s.lower().replace(" ", "").replace("-", "").replace("_", "").replace(".", "")

def first_present(cols: List[str], aliases: List[str]) -> Optional[str]:
    norm_cols = {lower_nodot(c): c for c in cols}
    for a in aliases:
        key = lower_nodot(a)
        if key in norm_cols:
            return norm_cols[key]
    return None

def safe_int(v: Any) -> Optional[int]:
    try:
        if pd.isna(v):
            return None
        return int(str(v).split(".")[0])
    except Exception:
        return None

def safe_float(v: Any) -> Optional[float]:
    try:
        if pd.isna(v):
            return None
        return float(v)
    except Exception:
        return None

def get_nested(d: Dict[str, Any], *keys: Iterable[str], default=None):
    """
    get_nested(obj, "a","b") -> obj["a"]["b"] if exists else default.
    Accepts dotted keys like "http.hostname".
    """
    cur = d
    for k in keys:
        if cur is None:
            return default
        if isinstance(k, str) and "." in k:
            parts = k.split(".")
            for p in parts:
                if isinstance(cur, dict) and p in cur:
                    cur = cur[p]
                else:
                    return default
        else:
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                return default
    return cur if cur is not None else default

def to_iso(ts) -> Optional[str]:
    if not ts:
        return None
    # try direct
    if isinstance(ts, (int, float)):
        try:
            return datetime.utcfromtimestamp(float(ts)).isoformat() + "Z"
        except Exception:
            return None
    s = str(ts)
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            # Naive -> assume UTC
            if not dt.tzinfo:
                return dt.isoformat() + "Z"
            return dt.astimezone(tz=None).isoformat()
        except Exception:
            continue
    # Already ISO-ish?
    if "T" in s or s.endswith("Z"):
        return s
    return None

def detect_hp_from_path(path: Path) -> Optional[str]:
    p = f"{path.parent.as_posix()}/{path.name}".lower()
    for name in ("cowrie","dionaea","tanner","h0neytr4p","honeypot","mailoney","conpot","ciscoasa","asa","suricata","eve.json","elk","elastic"):
        if name in p:
            if name == "honeypot":  # generic
                continue
            if name == "eve.json" or name == "suricata":
                return "suricata"
            if name in ("asa", "ciscoasa"):
                return "ciscoasa"
            return name
    # basic filename hints
    n = path.name.lower()
    if n.startswith("cowrie"):
        return "cowrie"
    if n == "eve.json" or n.endswith("eve.json"):
        return "suricata"
    return None

# ---------------------- CSV Parsing Support ----------------------

CSV_ALIASES = {
    "timestamp": ["timestamp","time","@timestamp","date","event_time","eventtime","ts"],
    "src_ip": ["src_ip","source_ip","client_ip","ip_src","ipsrc","src","source","srcip","network.src_ip","ip.src","flow.src_ip","srcaddr"],
    "dest_ip": ["dest_ip","dst_ip","destination_ip","server_ip","ip_dst","ipdst","dst","destination","dstip","network.dst_ip","ip.dst","flow.dst_ip","dstaddr"],
    "src_port": ["src_port","sport","source_port","tcp_srcport","udp_srcport","l4_sport","flow.src_port"],
    "dest_port": ["dest_port","dport","destination_port","tcp_dstport","udp_dstport","l4_dport","flow.dst_port"],
    "protocol": ["protocol","proto","l4_proto","transport","ipprotocol","network.transport"],
    "event_type": ["event_type","event","alert","signature","category","msg","message"],
    "service": ["service","sensor","app","application","service_name","honeypot","hp"],
    "username": ["username","user","login","account"],
    "password": ["password","passwd","pass"],
    "command": ["command","cmd","input","pdata/logstash-2025.10.09.json'ayload_cmd","request","query"],
    "file": ["file","filename","path","filepath","uri","url","resource"],
    "payload": ["payload","payload_hash","sha256","md5","sha1","sample_hash"],
    "host": ["host","hostname","http.host","server_host","dst_host"],
    "method": ["method","http_method","request_method"],
    "url": ["url","uri","request_uri","http.url"],
    "ua": ["user_agent","ua","http_user_agent","agent"],
    "status": ["status","status_code","code","http_status"],
}

def yield_from_csv(path: Path, chunksize: int = 50000) -> Iterator[Dict[str,Any]]:
    head = read_bytes_head(path)
    delimiter = sniff_delimiter(head, default=",")
    try:
        it = pd.read_csv(
            path,
            chunksize=chunksize,
            sep=delimiter,
            engine="python",
            dtype=str,
            encoding="utf-8",
            on_bad_lines="skip"
        )
    except TypeError:
        it = pd.read_csv(
            path,
            chunksize=chunksize,
            sep=delimiter,
            engine="python",
            dtype=str,
            encoding="utf-8",
            error_bad_lines=False
        )
    for df in it:
        cols = list(df.columns)
        colmap = {}
        for key, aliases in CSV_ALIASES.items():
            found = first_present(cols, aliases)
            if found:
                colmap[key] = found

        for _, row in df.iterrows():
            obj = {}
            if "timestamp" in colmap:
                obj["timestamp"] = row[colmap["timestamp"]]
            if "src_ip" in colmap:
                obj["src_ip"] = row[colmap["src_ip"]]
            if "dest_ip" in colmap:
                obj["dest_ip"] = row[colmap["dest_ip"]]
            if "src_port" in colmap:
                obj["src_port"] = safe_int(row[colmap["src_port"]])
            if "dest_port" in colmap:
                obj["dest_port"] = safe_int(row[colmap["dest_port"]])
            if "protocol" in colmap:
                obj["protocol"] = row[colmap["protocol"]]
            if "event_type" in colmap:
                obj["event_type"] = row[colmap["event_type"]]
            if "service" in colmap:
                obj["service"] = row[colmap["service"]]
            if "username" in colmap:
                obj["username"] = row[colmap["username"]]
            if "password" in colmap:
                obj["password"] = row[colmap["password"]]
            if "command" in colmap:
                obj["input"] = row[colmap["command"]]
            if "file" in colmap:
                obj["file"] = row[colmap["file"]]
            if "payload" in colmap:
                obj["payload"] = row[colmap["payload"]]
            if "host" in colmap:
                obj["host"] = row[colmap["host"]]
            if "method" in colmap:
                obj["http_method"] = row[colmap["method"]]
            if "url" in colmap:
                obj["url"] = row[colmap["url"]]
            if "ua" in colmap:
                obj["user_agent"] = row[colmap["ua"]]
            if "status" in colmap:
                obj["status"] = safe_int(row[colmap["status"]])

            obj["_hp_hint"] = detect_hp_from_path(path)
            yield normalize_event(obj, str(path))

# ---------------------- Normalization Core ----------------------

BASE_FIELDS = {
    # core
    "timestamp","src_ip","dst_ip","src_port","dst_port","proto","service","event_type",
    "sensor","hp","session_id","source_file",
    # auth/commands
    "user","password","command","tty_log",
    # files/payloads
    "filename","url","host","method","status","user_agent","referer",
    "payload_hash","md5","sha1","sha256","sha512",
    # TLS/DNS/Flow
    "sni","tls_version","ja3","ja3s","dns_qname","dns_qtype","dns_rcode",
    "bytes_toserver","bytes_toclient","pkts_toserver","pkts_toclient",
    "flow_id","app_proto",
    # SMTP/Mailoney
    "smtp_helo","smtp_mail_from","smtp_rcpt_to","smtp_subject",
    # Conpot/ICS
    "ics_proto","modbus_unit_id","modbus_function","modbus_addr","modbus_len",
    # CiscoASA/syslog-ish
    "facility","severity","msgid","action",
    # misc
    "raw"
}

def _blank_row(src_path: str) -> Dict[str, Any]:
   return {k: None for k in BASE_FIELDS} | {"source_file": src_path}

def normalize_event(obj: Dict[str,Any], src_path: str) -> Dict[str,Any]:
    row = _blank_row('')
    # Detect honeypot type
    hp_hint = obj.pop("_hp_hint", None) or detect_hp_from_path(Path(src_path))
    row["hp"] = hp_hint

    # Timestamp
    row["timestamp"] = (
        obj.get("timestamp") or obj.get("@timestamp") or obj.get("time") or obj.get("event_time")
    )
    row["timestamp"] = to_iso(row["timestamp"])

    # Generic IP/ports/proto/service/event_type
    row["src_ip"] = obj.get("src_ip") or obj.get("source_ip") or obj.get("client_ip")
    row["dst_ip"] = obj.get("dest_ip") or obj.get("dst_ip") or obj.get("server_ip")
    row["src_port"] = obj.get("src_port") or obj.get("sport")
    row["dst_port"] = obj.get("dest_port") or obj.get("dport")
    row["proto"] = obj.get("protocol") or obj.get("proto") or obj.get("transport")
    row["service"] = obj.get("service") or obj.get("sensor") or obj.get("app") or obj.get("application")
    row["event_type"] = obj.get("event_type") or obj.get("event") or obj.get("category") or obj.get("signature")
    row["sensor"] = obj.get("sensor")
    row["session_id"] = obj.get("session") or obj.get("sid") or obj.get("sessionid") or obj.get("conn") or obj.get("connection")

    # Auth / commands (Cowrie & co)
    row["user"] = obj.get("username") or obj.get("user") or obj.get("login") or obj.get("account")
    row["password"] = obj.get("password") or obj.get("passwd")
    row["command"] = obj.get("input") or obj.get("command") or obj.get("cmd")
    row["tty_log"] = obj.get("ttylog") or obj.get("tty_log")

    # File / payload
    row["filename"] = obj.get("file") or obj.get("filename") or obj.get("path") or obj.get("filepath")
    row["url"] = obj.get("url") or obj.get("uri") or obj.get("request_uri")
    row["host"] = obj.get("host") or get_nested(obj, "http.host")
    row["method"] = obj.get("http_method") or obj.get("method") or get_nested(obj, "http.http_method")
    row["status"] = obj.get("status") or get_nested(obj, "http.status")
    row["user_agent"] = obj.get("user_agent") or get_nested(obj, "http.http_user_agent") or get_nested(obj, "http.user_agent")
    row["referer"] = obj.get("referer") or get_nested(obj, "http.http_refer")

    # Hashes
    row["payload_hash"] = obj.get("payload") or obj.get("payload_hash")
    row["md5"] = obj.get("md5")
    row["sha1"] = obj.get("sha1")
    row["sha256"] = obj.get("sha256")
    row["sha512"] = obj.get("sha512")

    # TLS / JA3 / DNS / Flow (Suricata-friendly)
    row["sni"] = obj.get("sni") or get_nested(obj, "tls.sni") or get_nested(obj, "tls.server_name")
    row["tls_version"] = obj.get("tls_version") or get_nested(obj, "tls.version")
    row["ja3"] = obj.get("ja3") or get_nested(obj, "tls.ja3")
    row["ja3s"] = obj.get("ja3s") or get_nested(obj, "tls.ja3s")
    row["dns_qname"] = obj.get("dns_qname") or get_nested(obj, "dns.rrname") or get_nested(obj, "dns.query.rrname")
    row["dns_qtype"] = obj.get("dns_qtype") or get_nested(obj, "dns.rrtype") or get_nested(obj, "dns.query.rrtype")
    row["dns_rcode"] = obj.get("dns_rcode") or get_nested(obj, "dns.rcode")
    row["bytes_toserver"] = obj.get("bytes_toserver") or get_nested(obj, "flow.bytes_toserver")
    row["bytes_toclient"] = obj.get("bytes_toclient") or get_nested(obj, "flow.bytes_toclient")
    row["pkts_toserver"] = obj.get("pkts_toserver") or get_nested(obj, "flow.pkts_toserver")
    row["pkts_toclient"] = obj.get("pkts_toclient") or get_nested(obj, "flow.pkts_toclient")
    row["flow_id"] = obj.get("flow_id") or get_nested(obj, "flow_id") or get_nested(obj, "flow.id")
    row["app_proto"] = obj.get("app_proto") or get_nested(obj, "app_proto")

    # SMTP / Mailoney
    row["smtp_helo"] = obj.get("helo") or obj.get("smtp_helo")
    row["smtp_mail_from"] = obj.get("mail_from") or obj.get("smtp_mail_from") or get_nested(obj, "smtp.mail_from")
    row["smtp_rcpt_to"] = obj.get("rcpt_to") or obj.get("smtp_rcpt_to") or get_nested(obj, "smtp.rcpt_to")
    row["smtp_subject"] = obj.get("subject") or get_nested(obj, "smtp.subject")

    # Conpot / ICS / Modbus (fields commonly seen)
    row["ics_proto"] = obj.get("ics_proto") or obj.get("protocol") if (hp_hint == "conpot") else row["ics_proto"]
    row["modbus_unit_id"] = obj.get("unit_id") or get_nested(obj, "modbus.unit_id")
    row["modbus_function"] = obj.get("function_code") or get_nested(obj, "modbus.function_code")
    row["modbus_addr"] = obj.get("address") or get_nested(obj, "modbus.address")
    row["modbus_len"] = obj.get("length") or get_nested(obj, "modbus.length")

    # Cisco ASA / syslog-ish extras (best-effort)
    row["facility"] = obj.get("facility")
    row["severity"] = obj.get("severity") or obj.get("level")
    row["msgid"] = obj.get("msgid") or obj.get("message_id")
    row["action"] = obj.get("action") or obj.get("acl_action")

    # Raw
    if "raw" in obj and obj["raw"]:
        row["raw"] = obj["raw"]
    else:
        try:
            row["raw"] = json.dumps(obj, ensure_ascii=False)
        except Exception:
            row["raw"] = str(obj)

    # Type-specific enrichments
    if hp_hint == "suricata":
        _apply_suricata_overrides(row, obj)
    elif hp_hint == "cowrie":
        _apply_cowrie_overrides(row, obj)
    elif hp_hint == "dionaea":
        _apply_dionaea_overrides(row, obj)
    elif hp_hint in ("tanner","h0neytr4p"):
        _apply_httpish_overrides(row, obj)
    elif hp_hint == "mailoney":
        _apply_mailoney_overrides(row, obj)
    elif hp_hint == "conpot":
        _apply_conpot_overrides(row, obj)
    elif hp_hint == "ciscoasa":
        _apply_ciscoasa_overrides(row, obj)

    # cast numeric strings where obvious
    for k in ("src_port","dst_port","status","bytes_toserver","bytes_toclient","pkts_toserver","pkts_toclient","modbus_unit_id","modbus_addr","modbus_len"):
        if row.get(k) is not None:
            if k in ("bytes_toserver","bytes_toclient"):
                row[k] = safe_float(row[k])
            else:
                row[k] = safe_int(row[k])

    return row

def _apply_suricata_overrides(row: Dict[str,Any], obj: Dict[str,Any]) -> None:
    # Pull common fields out of embedded structures
    if "alert" in obj and isinstance(obj["alert"], dict):
        row["event_type"] = obj["alert"].get("signature") or row["event_type"]
        # keep raw alert too
        try:
            alert_raw = json.dumps(obj["alert"], ensure_ascii=False)
            row["raw"] = alert_raw
        except Exception:
            pass
    if "http" in obj and isinstance(obj["http"], dict):
        h = obj["http"]
        row["method"] = row["method"] or h.get("http_method")
        row["url"] = row["url"] or h.get("url") or h.get("hostname") or h.get("uri")
        row["host"] = row["host"] or h.get("hostname")
        row["status"] = row["status"] or h.get("status")
        row["user_agent"] = row["user_agent"] or h.get("http_user_agent")
        row["referer"] = row["referer"] or h.get("http_refer")
    if "dns" in obj and isinstance(obj["dns"], dict):
        d = obj["dns"]
        row["dns_qname"] = row["dns_qname"] or d.get("rrname")
        row["dns_qtype"] = row["dns_qtype"] or d.get("rrtype")
        row["dns_rcode"] = row["dns_rcode"] or d.get("rcode")
    if "tls" in obj and isinstance(obj["tls"], dict):
        t = obj["tls"]
        row["sni"] = row["sni"] or t.get("sni") or t.get("server_name")
        row["tls_version"] = row["tls_version"] or t.get("version")
        row["ja3"] = row["ja3"] or t.get("ja3")
        row["ja3s"] = row["ja3s"] or t.get("ja3s")
    if "flow" in obj and isinstance(obj["flow"], dict):
        f = obj["flow"]
        row["bytes_toserver"] = row["bytes_toserver"] or f.get("bytes_toserver")
        row["bytes_toclient"] = row["bytes_toclient"] or f.get("bytes_toclient")
        row["pkts_toserver"] = row["pkts_toserver"] or f.get("pkts_toserver")
        row["pkts_toclient"] = row["pkts_toclient"] or f.get("pkts_toclient")
        row["app_proto"] = row["app_proto"] or f.get("app_proto")

def _apply_cowrie_overrides(row: Dict[str,Any], obj: Dict[str,Any]) -> None:
    # Cowrie often uses eventid/ssh keys and 'message'
    row["event_type"] = row["event_type"] or obj.get("eventid") or obj.get("event") or obj.get("message")
    if not row["src_ip"]:
        row["src_ip"] = obj.get("src_ip") or obj.get("src")
    if not row["dst_ip"]:
        row["dst_ip"] = obj.get("dst_ip") or obj.get("dest_ip")
    if not row["proto"]:
        row["proto"] = obj.get("protocol") or "tcp"
    # commands often in "input"; downloads have hashes
    for h in ("sha256","sha1","md5"):
        if obj.get(h):
            row[h] = obj[h]

def _apply_dionaea_overrides(row: Dict[str,Any], obj: Dict[str,Any]) -> None:
    # Dionaea uses remote/local host/port, url, hashes
    row["src_ip"] = row["src_ip"] or obj.get("remote_host")
    row["src_port"] = row["src_port"] or obj.get("remote_port")
    row["dst_ip"] = row["dst_ip"] or obj.get("local_host")
    row["dst_port"] = row["dst_port"] or obj.get("local_port")
    row["url"] = row["url"] or obj.get("url")
    for h in ("sha256","sha1","md5","sha512"):
        if obj.get(h):
            row[h] = obj[h]

def _apply_httpish_overrides(row: Dict[str,Any], obj: Dict[str,Any]) -> None:
    # Tanner / h0neytr4p (HTTP honeypots)
    if not row["method"]:
        row["method"] = obj.get("method") or get_nested(obj, "request.method")
    if not row["url"]:
        row["url"] = obj.get("url") or get_nested(obj, "request.url") or obj.get("uri")
    if not row["host"]:
        row["host"] = obj.get("host") or get_nested(obj, "request.host")
    if not row["user_agent"]:
        row["user_agent"] = obj.get("user_agent") or get_nested(obj, "headers.User-Agent") or get_nested(obj, "headers.user-agent")
    if not row["status"]:
        row["status"] = obj.get("status") or get_nested(obj, "response.status")

def _apply_mailoney_overrides(row: Dict[str,Any], obj: Dict[str,Any]) -> None:
    row["smtp_helo"] = row["smtp_helo"] or obj.get("helo")
    row["smtp_mail_from"] = row["smtp_mail_from"] or obj.get("mail_from")
    row["smtp_rcpt_to"] = row["smtp_rcpt_to"] or obj.get("rcpt_to")
    row["smtp_subject"] = row["smtp_subject"] or obj.get("subject")

def _apply_conpot_overrides(row: Dict[str,Any], obj: Dict[str,Any]) -> None:
    row["ics_proto"] = row["ics_proto"] or obj.get("protocol")
    row["modbus_unit_id"] = row["modbus_unit_id"] or obj.get("unit_id")
    row["modbus_function"] = row["modbus_function"] or obj.get("function_code")
    row["modbus_addr"] = row["modbus_addr"] or obj.get("address")
    row["modbus_len"] = row["modbus_len"] or obj.get("length")

def _apply_ciscoasa_overrides(row: Dict[str,Any], obj: Dict[str,Any]) -> None:
    # Best-effort parsing of common Cisco ASA fields if JSONified
    row["facility"] = row["facility"] or obj.get("facility")
    row["severity"] = row["severity"] or obj.get("severity")
    row["msgid"] = row["msgid"] or obj.get("message_id") or obj.get("msgid")
    if not row["src_ip"]:
        row["src_ip"] = obj.get("src") or obj.get("src_ip")
    if not row["dst_ip"]:
        row["dst_ip"] = obj.get("dst") or obj.get("dest_ip")
    row["action"] = row["action"] or obj.get("action")

# ------------------- Directory Scanner & Writers -------------------

def scan_and_parse(data_dir: Path) -> Iterator[Dict[str,Any]]:
    """
    - CSV/CSV.GZ via pandas (mapping alias -> schéma)
    - cowrie*.json(.gz)
    - suricata eve.json (ou dossiers 'suricata')
    - JSON/JSONL/.log (si JSON par ligne ou JSON complet)
    - Compatibilité Logstash/Elasticsearch : déplie le champ "_source"
    """
    for root, _, files in os.walk(data_dir):
        for fname in files:
            p = Path(root) / fname
            low = fname.lower()
            try:
                # ---------- CSV ----------
                if low.endswith(".csv") or low.endswith(".csv.gz"):
                    for obj in yield_from_csv(p):
                        yield obj

                # ---------- Cowrie ----------
                elif (low.startswith("cowrie") and low.endswith(".json")) or ("cowrie" in low and (low.endswith(".json") or low.endswith(".json.gz"))):
                    for line in iter_lines(p):
                        line = line.strip()
                        if not line:
                            continue
                        obj = _safe_json(line)
                        if not obj:
                            obj = _json_from_embedded(line)
                        if obj:
                            obj["_hp_hint"] = "cowrie"
                            yield normalize_event(obj, str(p))

                # ---------- Suricata (eve.json / dossier suricata) ----------
                elif low == "eve.json" or low.endswith("eve.json") or "suricata" in root.lower():
                    for line in iter_lines(p):
                        line = line.strip()
                        if not line:
                            continue
                        obj = _safe_json(line)
                        if obj:
                            obj["_hp_hint"] = "suricata"
                            yield normalize_event(obj, str(p))

                # ---------- JSON / JSONL / .log JSON ----------
                elif low.endswith(".json") or low.endswith(".jsonl") or low.endswith(".log") or low.endswith(".json.gz") or low.endswith(".jsonl.gz"):
                    hp = detect_hp_from_path(p)
                    any_yield = False
                    for line in iter_lines(p):
                        line = line.strip()
                        if not line:
                            continue
                        obj = _safe_json(line)
                        if obj:
                            # 🧩 PATCH — compatibilité Logstash / ElasticSearch
                            # Si un champ "_source" existe, on déplie pour accéder aux vraies données
                            if "_source" in obj and isinstance(obj["_source"], dict):
                                obj = obj["_source"]

                            if hp:
                                obj["_hp_hint"] = hp
                            yield normalize_event(obj, str(p))
                            any_yield = True

                    # Si rien n'a été lu ligne par ligne → essai JSON complet / liste
                    if not any_yield:
                        entire = _read_entire_text(p)
                        if entire:
                            try:
                                obj = json.loads(entire)
                                if isinstance(obj, list):
                                    for el in obj:
                                        if isinstance(el, dict):
                                            if "_source" in el and isinstance(el["_source"], dict):
                                                el = el["_source"]
                                            if hp:
                                                el["_hp_hint"] = hp
                                            yield normalize_event(el, str(p))
                                elif isinstance(obj, dict):
                                    if "_source" in obj and isinstance(obj["_source"], dict):
                                        obj = obj["_source"]
                                    if hp:
                                        obj["_hp_hint"] = hp
                                    yield normalize_event(obj, str(p))
                            except Exception:
                                pass

                # ---------- Autres fichiers ignorés ----------
                else:
                    continue

            except Exception as e:
                print(f"Warning: failed to parse {p}: {e}")


def _safe_json(line: str) -> Optional[Dict[str,Any]]:
    try:
        o = json.loads(line)
        return o if isinstance(o, dict) else None
    except Exception:
        return None

def _json_from_embedded(line: str) -> Optional[Dict[str,Any]]:
    try:
        idx = line.find("{")
        if idx >= 0:
            return json.loads(line[idx:])
    except Exception:
        pass
    return None

def _read_entire_text(p: Path) -> Optional[str]:
    try:
        opener = gzip.open if p.suffix == ".gz" else open
        with opener(p, "rt", encoding="utf-8", errors="ignore") as fh:
            return fh.read()
    except Exception:
        return None

def chunked_write(rows_iter: Iterator[Dict[str,Any]], out_file: Path, fmt: str = "csv", chunksize: int = 10000):
    """
    Écrit les données extraites en CSV par blocs (chunks) pour limiter la RAM.
    - Écrit le header une seule fois.
    - Continue d'ajouter les lignes suivantes au même fichier.
    """
    if fmt != "csv":
        fmt = "csv"  # force CSV par sécurité

    out_file.parent.mkdir(parents=True, exist_ok=True)

    buffer = []
    total_written = 0
    header_written = out_file.exists() and out_file.stat().st_size > 0

    for row in rows_iter:
        buffer.append(row)
        if len(buffer) >= chunksize:
            df = pd.DataFrame(buffer)
            df.to_csv(out_file, index=False, mode="a", header=not header_written, encoding="utf-8")
            header_written = True
            total_written += len(buffer)
            buffer = []

    # écrire le dernier paquet
    if buffer:
        df = pd.DataFrame(buffer)
        df.to_csv(out_file, index=False, mode="a", header=not header_written, encoding="utf-8")
        total_written += len(buffer)

    print(f"[✓] Écriture terminée : {total_written} lignes sauvegardées dans {out_file}")


def _write_df(df: pd.DataFrame, out_file: Path, fmt: str, append: bool):
    if fmt == "csv":
        header = not append
        df.to_csv(out_file, index=False, mode="a", header=header)
    else:
        raise RuntimeError("Parquet is handled directly in chunked_write()")

# ------------------------------ CLI ------------------------------

def main():
    parser = argparse.ArgumentParser(description="Extract and normalize T-Pot logs (max info)")
    parser.add_argument("--data-dir", default="data", help="T-Pot data folder (default /data)")
    parser.add_argument("--out-file", default="tpot_logs.csv", help="Output file (csv or parquet)")
    parser.add_argument("--format", choices=["csv","parquet"], default=None, help="Output format (auto from extension)")
    parser.add_argument("--chunksize", type=int, default=10000, help="Rows per write chunk")
    args = parser.parse_args()

    data_dir = Path(os.path.expanduser(args.data_dir))
    out_file = Path(args.out_file)
    fmt = args.format or ("parquet" if out_file.suffix.lower() == ".parquet" else "csv")

    if not data_dir.exists():
        print(f"Error: data dir {data_dir} not found.")
        return

    print(f"Scanning {data_dir} ... (this may take time on large installs)")
    rows = scan_and_parse(data_dir)
    chunked_write(rows, out_file, fmt, chunksize=args.chunksize)
    print("Done. Output:", out_file)


if __name__ == "__main__":
    main()
