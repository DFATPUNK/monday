#!/usr/bin/env python3
"""
monday_workspace_sync.py
========================

Réplique un workspace monday.com d'un compte SOURCE (production) vers un compte
CIBLE (recette) via l'API GraphQL : workspace, dossiers, boards, groupes,
colonnes (y compris sous-éléments), éléments, sous-éléments, valeurs,
relations (Connect boards / dépendances), updates + réponses, fichiers.

Principe clé : synchronisation « en place » (upsert), pas de suppression /
recréation. Les boards et colonnes de la recette gardent donc les mêmes IDs
d'une exécution à l'autre, ce qui permet aux workflows construits une fois en
recette de continuer à fonctionner après chaque synchro.

  * Les éléments sont rapprochés via une colonne texte « ID source (sync) ».
  * Une colonne « Hash source (sync) » évite de réécrire les éléments inchangés
    (économie du quota d'appels API quotidien).
  * Les updates déjà copiées sont reconnues grâce au marqueur [sync:u<ID>].

Hors périmètre (limites API, voir README) : workflows / automatisations,
vues autres que la vue principale, dashboards, docs, historique Emails &
Activités, auteurs et dates d'origine des éléments/updates.

Usage
-----
  export MONDAY_SRC_TOKEN=...   # token API d'un admin de leongrosse10
  export MONDAY_DST_TOKEN=...   # token API d'un admin de recette-lg
  python monday_workspace_sync.py \
      --source-workspace "🚧 Pôles Construction / Rénovation & Patrimoine" \
      [--target-workspace "Nom en recette"] [--boards "P1 - Affaires,P1 - Contacts"] \
      [--no-updates] [--no-files] [--archive-missing] [--report report.json]

STATUT : prototype à valider d'abord sur un workspace cible jetable
(--target-workspace "TEST synchro"). Non exécuté contre l'API réelle.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import logging
import mimetypes
import os
import re
import sys
import time
from collections import defaultdict

import requests

API_URL = "https://api.monday.com/v2"
FILE_URL = "https://api.monday.com/v2/file"
API_VERSION = os.getenv("MONDAY_API_VERSION", "2026-07")

SYNC_ID_TITLE = os.getenv("SYNC_ID_TITLE", "ID source (sync)")
SYNC_HASH_TITLE = os.getenv("SYNC_HASH_TITLE", "Hash source (sync)")

BATCH_SIZE = int(os.getenv("SYNC_BATCH_SIZE", "10"))
PAGE_SIZE = 200

# Colonnes créées en phase 2 (elles référencent d'autres boards / colonnes)
LATE_COLUMN_TYPES = {"board_relation", "dependency", "mirror", "lookup", "formula"}
# Colonnes jamais recréées (gérées automatiquement ou impossibles via API)
NEVER_CREATE = {"name", "subtasks", "subitems", "item_id"}
# Valeurs non écrivables via API (calculées, UI seulement, ou traitées à part)
READ_ONLY_VALUES = {
    "name", "subtasks", "subitems", "formula", "mirror", "lookup", "creation_log",
    "last_updated", "auto_number", "item_id", "progress", "button", "vote",
    "time_tracking", "doc", "direct_doc", "file", "board_relation", "dependency",
    "integration", "ai",
}
RELATION_TYPES = {"board_relation", "dependency"}
VOLATILE_KEYS = {"changed_at", "post_id", "updated_at"}

log = logging.getLogger("monday-sync")


class MondayError(Exception):
    pass


class DailyLimitReached(MondayError):
    pass


# --------------------------------------------------------------------------- #
# Client API
# --------------------------------------------------------------------------- #
class Monday:
    RETRY_CODES = {
        "COMPLEXITY_BUDGET_EXHAUSTED", "ComplexityException", "RATE_LIMIT_EXCEEDED",
        "maxConcurrencyExceeded", "IP_RATE_LIMIT_EXCEEDED", "Rate Limit Exceeded",
    }

    def __init__(self, token: str, label: str):
        self.token = token
        self.label = label
        self.calls = 0
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": token,
            "API-Version": API_VERSION,
            "Content-Type": "application/json",
        })

    def gql(self, query: str, variables: dict | None = None, retries: int = 8) -> dict:
        for attempt in range(retries):
            self.calls += 1
            try:
                r = self.session.post(API_URL, json={"query": query, "variables": variables or {}}, timeout=180)
            except requests.RequestException as exc:
                wait = min(120, 5 * 2 ** attempt)
                log.warning("[%s] erreur réseau (%s), nouvel essai dans %ss", self.label, exc, wait)
                time.sleep(wait)
                continue
            if r.status_code == 429 or r.status_code >= 500:
                wait = int(r.headers.get("Retry-After") or 0) or min(120, 5 * 2 ** attempt)
                log.warning("[%s] HTTP %s, nouvel essai dans %ss", self.label, r.status_code, wait)
                time.sleep(wait)
                continue
            try:
                data = r.json()
            except ValueError:
                raise MondayError(f"[{self.label}] réponse non JSON (HTTP {r.status_code}): {r.text[:300]}")
            errors = data.get("errors") or []
            if "error_message" in data:
                errors.append({"message": data["error_message"], "extensions": {"code": data.get("error_code")}})
            if errors:
                codes = {str((e.get("extensions") or {}).get("code")) for e in errors}
                if "DAILY_LIMIT_EXCEEDED" in codes:
                    raise DailyLimitReached(f"[{self.label}] quota d'appels quotidien atteint")
                if codes & self.RETRY_CODES:
                    wait = max([int((e.get("extensions") or {}).get("retry_in_seconds") or 0) for e in errors] + [0]) or 30
                    log.warning("[%s] limite de débit (%s), pause %ss", self.label, ",".join(codes), wait)
                    time.sleep(wait)
                    continue
                raise MondayError(f"[{self.label}] {json.dumps(errors, ensure_ascii=False)[:1500]}")
            return data.get("data") or {}
        raise MondayError(f"[{self.label}] abandon après {retries} essais")

    def upload(self, mutation: str, filename: str, content: bytes, retries: int = 5) -> dict:
        mime = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        for attempt in range(retries):
            self.calls += 1
            r = requests.post(
                FILE_URL,
                headers={"Authorization": self.token, "API-Version": API_VERSION},
                data={"query": mutation},
                files={"variables[file]": (filename, content, mime)},
                timeout=600,
            )
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(int(r.headers.get("Retry-After") or 0) or 10 * (attempt + 1))
                continue
            data = r.json()
            if data.get("errors") or data.get("error_message"):
                raise MondayError(f"[{self.label}] upload {filename}: {json.dumps(data, ensure_ascii=False)[:800]}")
            return data.get("data") or {}
        raise MondayError(f"[{self.label}] upload {filename}: abandon")

    # -- helpers ----------------------------------------------------------- #
    def paged(self, query: str, root: str, variables: dict | None = None, limit: int = 100):
        page = 1
        while True:
            v = dict(variables or {}, page=page, limit=limit)
            rows = self.gql(query, v).get(root) or []
            yield from rows
            if len(rows) < limit:
                return
            page += 1


# --------------------------------------------------------------------------- #
# Fonctions pures (testables sans API)
# --------------------------------------------------------------------------- #
def parse_json(value):
    if value in (None, "", "null"):
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return value


def strip_volatile(obj):
    if isinstance(obj, dict):
        return {k: strip_volatile(v) for k, v in obj.items() if k not in VOLATILE_KEYS}
    if isinstance(obj, list):
        return [strip_volatile(v) for v in obj]
    return obj


def dropdown_labels_from_settings(settings) -> dict:
    """Retourne {id: nom} quel que soit le format (settings typé ou settings_str)."""
    out = {}
    labels = (settings or {}).get("labels")
    if isinstance(labels, list):
        for lab in labels:
            if isinstance(lab, dict) and "id" in lab:
                out[str(lab["id"])] = lab.get("name") or lab.get("label")
    elif isinstance(labels, dict):
        out = {str(k): v for k, v in labels.items()}
    return out


def convert_value(col_type: str, cv: dict, ctx: "Ctx", src_settings: dict | None = None):
    """Convertit une valeur de colonne source en valeur écrivable dans la cible.
    Retourne None si la valeur est vide ou non transférable."""
    if col_type in READ_ONLY_VALUES:
        return None
    raw = parse_json(cv.get("value"))
    text = cv.get("text") or ""
    if raw in (None, {}, []) and not text:
        return None

    if col_type == "status":
        return {"label": text} if text else None
    if col_type == "dropdown":
        names = []
        id_to_name = dropdown_labels_from_settings(src_settings)
        ids = (raw or {}).get("ids") if isinstance(raw, dict) else None
        if ids and id_to_name:
            names = [id_to_name.get(str(i)) for i in ids if id_to_name.get(str(i))]
        if not names and text:
            names = [t.strip() for t in text.split(",") if t.strip()]
        return {"labels": names} if names else None
    if col_type == "people":
        out = []
        for p in (raw or {}).get("personsAndTeams", []):
            if p.get("kind") == "team":
                tid = ctx.team_map.get(str(p.get("id")))
                if tid:
                    out.append({"id": int(tid), "kind": "team"})
            else:
                uid = ctx.user_map.get(str(p.get("id")))
                if uid:
                    out.append({"id": int(uid), "kind": "person"})
                else:
                    ctx.warn_once(f"user:{p.get('id')}", f"Utilisateur source {p.get('id')} absent de la cible (par email) : ignoré")
        return {"personsAndTeams": out} if out else None
    if col_type == "tags":
        names = [t.strip() for t in text.split(",") if t.strip()]
        ids = [ctx.tag_id(n) for n in names]
        ids = [i for i in ids if i]
        return {"tag_ids": ids} if ids else None
    if col_type in ("text", "numbers"):
        if isinstance(raw, (str, int, float)):
            return str(raw)
        return text or None
    if col_type == "long_text":
        return {"text": (raw or {}).get("text", text) if isinstance(raw, dict) else text}
    if col_type == "checkbox":
        checked = isinstance(raw, dict) and str(raw.get("checked")).lower() == "true"
        return {"checked": "true"} if checked else None
    if col_type == "date":
        if not isinstance(raw, dict) or not raw.get("date"):
            return None
        out = {"date": raw["date"]}
        if raw.get("time"):
            out["time"] = raw["time"]
        return out
    if col_type == "timeline":
        if not isinstance(raw, dict) or not raw.get("from"):
            return None
        return {"from": raw["from"], "to": raw.get("to") or raw["from"]}
    if col_type == "location":
        if not isinstance(raw, dict):
            return None
        return {k: raw[k] for k in ("lat", "lng", "address") if raw.get(k) is not None}
    # email, phone, link, country, rating, hour, week, world_clock, color_picker…
    if isinstance(raw, dict):
        return strip_volatile(raw)
    return raw


def remap_settings(settings, board_map: dict, col_map_all: dict, own_col_map: dict):
    """Remplace dans un JSON de settings les IDs de boards / colonnes source par
    ceux de la cible (best effort : Connect boards, miroir, formule, dépendance)."""
    def walk(o):
        if isinstance(o, dict):
            return {own_col_map.get(k, col_map_all.get(k, k)) if k in own_col_map or k in col_map_all else k: walk(v)
                    for k, v in o.items()}
        if isinstance(o, list):
            return [walk(v) for v in o]
        if isinstance(o, int) and str(o) in board_map:
            return int(board_map[str(o)])
        if isinstance(o, str):
            if o in board_map:
                return board_map[o]
            if o in own_col_map:
                return own_col_map[o]
            if o in col_map_all:
                return col_map_all[o]
            if "{" in o:  # formule : {column_id}
                return re.sub(r"\{([a-z0-9_]+)\}", lambda m: "{" + own_col_map.get(m.group(1), m.group(1)) + "}", o)
        return o
    return walk(copy.deepcopy(settings or {}))


def item_hash(name: str, group_title: str, values: dict, relations: dict, files: list) -> str:
    payload = json.dumps([name, group_title, values, relations, sorted(files)], sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(payload.encode()).hexdigest()


def body_with_header(update: dict, marker: str) -> str:
    creator = (update.get("creator") or {})
    who = creator.get("name") or creator.get("email") or "inconnu"
    when = (update.get("created_at") or "")[:16].replace("T", " ")
    return (f"<p><em>Copie prod — auteur d'origine : {who} — {when} UTC</em></p>"
            f"{update.get('body') or ''}<p><small>[sync:{marker}]</small></p>")


MARKER_RE = re.compile(r"\[sync:([ur]\d+)\]")


# --------------------------------------------------------------------------- #
# Contexte de synchronisation
# --------------------------------------------------------------------------- #
class Ctx:
    def __init__(self, src: Monday, dst: Monday, args):
        self.src, self.dst, self.args = src, dst, args
        self.user_map: dict[str, str] = {}
        self.team_map: dict[str, str] = {}
        self.board_map: dict[str, str] = {}          # board source -> board cible (y compris sous-éléments)
        self.col_map: dict[str, dict[str, str]] = defaultdict(dict)  # board src -> {col src -> col cible}
        self.sync_cols: dict[str, tuple[str, str]] = {}  # board cible -> (col ID, col hash)
        self.item_map: dict[str, str] = {}
        self.dirty_items: set[str] = set()           # items source créés/modifiés pendant ce run
        self.src_boards: dict[str, dict] = {}
        self.tag_cache: dict[str, str] = {}
        self.warned: set[str] = set()
        self.same_account = False  # source et cible sur le même compte monday (mode test)
        self.report = defaultdict(list)
        self.stats = defaultdict(int)

    def warn_once(self, key, msg):
        if key not in self.warned:
            self.warned.add(key)
            log.warning(msg)
            self.report["warnings"].append(msg)

    def tag_id(self, name: str):
        if name not in self.tag_cache:
            try:
                d = self.dst.gql("mutation($n:String!){ create_or_get_tag(tag_name:$n){ id } }", {"n": name})
                self.tag_cache[name] = str(d["create_or_get_tag"]["id"])
            except MondayError as exc:
                self.warn_once(f"tag:{name}", f"Tag '{name}' non créé : {exc}")
                self.tag_cache[name] = None
        return self.tag_cache[name]


# --------------------------------------------------------------------------- #
# Lecture
# --------------------------------------------------------------------------- #
COLS_FIELDS = "columns{ id title type description settings }"
COLS_FIELDS_LEGACY = "columns{ id title type description settings_str }"


def fetch_board(api: Monday, board_id: str) -> dict:
    q = "query($b:[ID!]){ boards(ids:$b){ id name type board_kind description board_folder_id %s groups{ id title color position } } }"
    try:
        b = api.gql(q % COLS_FIELDS, {"b": [board_id]})["boards"][0]
    except MondayError:
        b = api.gql(q % COLS_FIELDS_LEGACY, {"b": [board_id]})["boards"][0]
        for c in b["columns"]:
            c["settings"] = parse_json(c.pop("settings_str", None)) or {}
    for c in b["columns"]:
        c["settings"] = parse_json(c.get("settings")) or {}
    return b


def norm(name: str) -> str:
    """Normalise un nom pour les comparaisons (emojis, espaces insécables, casse)."""
    import unicodedata
    s = unicodedata.normalize("NFKC", name or "").replace("️", "").replace(" ", " ")
    return re.sub(r"\s+", " ", s).strip().casefold()


def list_workspaces(api: Monday) -> list[dict]:
    return list(api.paged("query($page:Int,$limit:Int){ workspaces(limit:$limit, page:$page){ id name kind description } }", "workspaces"))


def get_workspace(api: Monday, ws_id: str) -> dict | None:
    rows = api.gql("query($ids:[ID!]){ workspaces(ids:$ids){ id name kind description } }", {"ids": [ws_id]}).get("workspaces") or []
    return rows[0] if rows else None


def find_workspace(api: Monday, ref: str) -> dict | None:
    """Trouve un workspace par ID (chiffres) ou par nom normalisé."""
    ref = (ref or "").strip()
    if ref.isdigit():
        return get_workspace(api, ref)
    return next((w for w in list_workspaces(api) if norm(w["name"]) == norm(ref)), None)


def list_boards(api: Monday, ws_id: str) -> list[dict]:
    q = "query($ws:[ID!],$page:Int,$limit:Int){ boards(workspace_ids:$ws, limit:$limit, page:$page, state:active){ id name type board_folder_id } }"
    return [b for b in api.paged(q, "boards", {"ws": [ws_id]}) if b.get("type") not in ("sub_items_board", "document")]


def list_folders(api: Monday, ws_id: str) -> list[dict]:
    q = "query($ws:[ID!],$page:Int,$limit:Int){ folders(workspace_ids:$ws, limit:$limit, page:$page){ id name parent{ id } } }"
    return list(api.paged(q, "folders", {"ws": [ws_id]}))


CV = "column_values{ id type text value ... on BoardRelationValue{ linked_item_ids } ... on DependencyValue{ linked_item_ids } }"
ITEM_FIELDS = f"id name group{{ id title }} {CV} subitems{{ id name {CV} }}"


def iter_items(api: Monday, board_id: str, fields: str = ITEM_FIELDS):
    first = api.gql(f"query($b:[ID!],$l:Int!){{ boards(ids:$b){{ items_page(limit:$l){{ cursor items{{ {fields} }} }} }} }}",
                    {"b": [board_id], "l": PAGE_SIZE})
    page = first["boards"][0]["items_page"]
    while True:
        yield from page["items"]
        if not page.get("cursor"):
            return
        page = api.gql(f"query($c:String!,$l:Int!){{ next_items_page(cursor:$c, limit:$l){{ cursor items{{ {fields} }} }} }}",
                       {"c": page["cursor"], "l": PAGE_SIZE})["next_items_page"]


def iter_board_updates(api: Monday, board_id: str):
    q = ("query($b:[ID!],$page:Int,$limit:Int){ boards(ids:$b){ updates(limit:$limit, page:$page){ "
         "id item_id body text_body created_at creator{ name email } assets{ id name } "
         "replies{ id body text_body created_at creator{ name email } } } } }")
    page = 1
    while True:
        rows = api.gql(q, {"b": [board_id], "page": page, "limit": 100})["boards"][0]["updates"] or []
        yield from rows
        if len(rows) < 100:
            return
        page += 1


# --------------------------------------------------------------------------- #
# Étapes de synchronisation
# --------------------------------------------------------------------------- #
def build_people_maps(ctx: Ctx):
    q = "query($page:Int,$limit:Int){ users(limit:$limit, page:$page){ id email } }"
    by_email = {u["email"].lower(): u["id"] for u in ctx.dst.paged(q, "users", limit=500) if u.get("email")}
    for u in ctx.src.paged(q, "users", limit=500):
        if u.get("email") and u["email"].lower() in by_email:
            ctx.user_map[str(u["id"])] = str(by_email[u["email"].lower()])
    tq = "query{ teams{ id name } }"
    dst_teams = {t["name"]: t["id"] for t in ctx.dst.gql(tq).get("teams") or []}
    for t in ctx.src.gql(tq).get("teams") or []:
        if t["name"] in dst_teams:
            ctx.team_map[str(t["id"])] = str(dst_teams[t["name"]])
    log.info("Utilisateurs rapprochés par email : %d — équipes par nom : %d", len(ctx.user_map), len(ctx.team_map))


def ensure_workspace(ctx: Ctx, src_ws: dict, target_name: str) -> str:
    existing = find_workspace(ctx.dst, target_name)
    if existing:
        log.info("Workspace cible existant : %s (%s)", existing["name"], existing["id"])
        return str(existing["id"])
    log.warning("Aucun workspace nommé « %s » en recette : il va être CRÉÉ. "
                "Pour utiliser un workspace existant, passez --target-workspace-id <ID>.", target_name)
    product_id = None
    try:
        products = ctx.dst.gql("query{ account{ products{ id kind } } }")["account"]["products"] or []
        product_id = next((p["id"] for p in products if p.get("kind") == "crm"), None)
    except MondayError as exc:
        ctx.warn_once("products", f"Impossible de lire les produits du compte cible : {exc}")
    m = ("mutation($n:String!,$k:WorkspaceKind!,$d:String%s){ create_workspace(name:$n, kind:$k, description:$d%s){ id } }")
    v = {"n": target_name, "k": src_ws.get("kind") or "open", "d": src_ws.get("description") or ""}
    try:
        if product_id:
            ws = ctx.dst.gql(m % (",$p:ID", ", account_product_id:$p"), dict(v, p=product_id))["create_workspace"]
        else:
            raise MondayError("pas de produit CRM détecté")
    except MondayError as exc:
        ctx.warn_once("ws-product", f"Workspace créé sans rattachement CRM ({exc})")
        ws = ctx.dst.gql(m % ("", ""), v)["create_workspace"]
    log.info("Workspace cible créé : %s (%s)", target_name, ws["id"])
    return str(ws["id"])


def ensure_folders(ctx: Ctx, src_ws: str, dst_ws: str) -> dict:
    src = list_folders(ctx.src, src_ws)
    dst = list_folders(ctx.dst, dst_ws)
    dst_index = {((f.get("parent") or {}).get("id"), f["name"]): str(f["id"]) for f in dst}
    fmap: dict[str, str] = {}
    pending = list(src)
    while pending:
        progressed = False
        for f in list(pending):
            parent = (f.get("parent") or {}).get("id")
            if parent and str(parent) not in fmap:
                continue
            dst_parent = fmap.get(str(parent)) if parent else None
            key = (dst_parent, f["name"])
            if key not in dst_index:
                v = {"n": f["name"], "w": dst_ws}
                m = "mutation($n:String!,$w:ID!%s){ create_folder(name:$n, workspace_id:$w%s){ id } }"
                res = ctx.dst.gql(m % (",$p:ID", ", parent_folder_id:$p") if dst_parent else m % ("", ""),
                                  dict(v, p=dst_parent) if dst_parent else v)
                dst_index[key] = str(res["create_folder"]["id"])
                ctx.stats["folders_created"] += 1
            fmap[str(f["id"])] = dst_index[key]
            pending.remove(f)
            progressed = True
        if not progressed:
            ctx.warn_once("folders", "Arborescence de dossiers incomplète (parents introuvables)")
            break
    return fmap


def create_column(ctx: Ctx, board_id: str, col: dict, defaults=None) -> str | None:
    base = {"b": board_id, "t": col["title"], "ty": col["type"], "de": col.get("description") or None}
    want_id = col["id"] if re.fullmatch(r"[a-z_]{1,20}", col["id"] or "") else None
    attempts = []
    if defaults:
        attempts += [(want_id, defaults), (None, defaults)]
    attempts += [(want_id, None), (None, None)]
    tried, last = set(), None
    for cid, d in attempts:
        key = (cid, json.dumps(d, sort_keys=True) if d else None)
        if key in tried:
            continue
        tried.add(key)
        sig = ["$b:ID!", "$t:String!", "$ty:ColumnType!", "$de:String"]
        args = ["board_id:$b", "title:$t", "column_type:$ty", "description:$de"]
        v = dict(base)
        if cid:
            sig.append("$id:String"); args.append("id:$id"); v["id"] = cid
        if d:
            sig.append("$d:JSON"); args.append("defaults:$d"); v["d"] = d
        try:
            res = ctx.dst.gql(f"mutation({','.join(sig)}){{ create_column({', '.join(args)}){{ id }} }}", v)
            ctx.stats["columns_created"] += 1
            if d is None and defaults:
                ctx.report["columns_without_settings"].append(f"{board_id}/{col['title']} ({col['type']})")
            return res["create_column"]["id"]
        except MondayError as exc:
            last = exc
    ctx.report["columns_failed"].append(f"{board_id}/{col['title']} ({col['type']}): {last}")
    log.warning("Colonne '%s' (%s) non créée : %s", col["title"], col["type"], last)
    return None


def ensure_sync_columns(ctx: Ctx, dst_board: dict) -> tuple[str, str]:
    bid = str(dst_board["id"])
    if bid in ctx.sync_cols:
        return ctx.sync_cols[bid]
    existing = {c["title"]: c["id"] for c in dst_board["columns"]}
    out = []
    for title in (SYNC_ID_TITLE, SYNC_HASH_TITLE):
        cid = existing.get(title) or create_column(ctx, bid, {"id": "", "title": title, "type": "text"})
        if not cid:
            raise MondayError(f"Impossible de créer la colonne technique '{title}' sur le board {bid}")
        out.append(cid)
    ctx.sync_cols[bid] = (out[0], out[1])
    return ctx.sync_cols[bid]


def match_columns(ctx: Ctx, src_board: dict, dst_board: dict, late: bool, created_board: bool):
    """Crée/rapproche les colonnes (par titre + type, dans l'ordre)."""
    sid, did = str(src_board["id"]), str(dst_board["id"])
    pool = defaultdict(list)
    for c in dst_board["columns"]:
        pool[(c["title"], c["type"])].append(c["id"])
    used = set(ctx.col_map[sid].values())
    for col in src_board["columns"]:
        if col["type"] in NEVER_CREATE or col["id"] in ctx.col_map[sid]:
            continue
        if (col["type"] in LATE_COLUMN_TYPES) != late:
            continue
        candidates = [c for c in pool.get((col["title"], col["type"]), []) if c not in used]
        if candidates:
            ctx.col_map[sid][col["id"]] = candidates[0]
            used.add(candidates[0])
            continue
        defaults = copy.deepcopy(col["settings"]) or None
        if col["settings"] and col["type"] in LATE_COLUMN_TYPES:
            col_map_all = {k: v for m in ctx.col_map.values() for k, v in m.items()}
            defaults = remap_settings(col["settings"], ctx.board_map, col_map_all, ctx.col_map[sid])
            if col["type"] in RELATION_TYPES | {"mirror", "lookup"}:
                src_ids = [str(x) for x in (col["settings"].get("boardIds") or [])]
                missing = [b for b in src_ids if b not in ctx.board_map]
                # Même compte : un board lié hors périmètre existe déjà côté cible, on garde son ID
                if missing and not ctx.same_account:
                    ctx.report["columns_failed"].append(
                        f"{src_board['name']}/{col['title']}: board(s) lié(s) {missing} hors du périmètre synchronisé")
                    continue
        new_id = create_column(ctx, did, col, defaults)
        if new_id:
            ctx.col_map[sid][col["id"]] = new_id
            used.add(new_id)
    ctx.col_map[sid]["name"] = "name"


def ensure_board(ctx: Ctx, src_board: dict, dst_ws: str, fmap: dict, dst_by_name: dict) -> tuple[dict, bool]:
    if norm(src_board["name"]) in dst_by_name:
        return fetch_board(ctx.dst, dst_by_name[norm(src_board["name"])]), False
    v = {"n": src_board["name"], "k": src_board.get("board_kind") or "public", "w": dst_ws,
         "d": src_board.get("description") or None}
    sig, args = "$n:String!,$k:BoardKind!,$w:ID,$d:String", "board_name:$n, board_kind:$k, workspace_id:$w, description:$d, empty:true"
    folder = fmap.get(str(src_board.get("board_folder_id") or ""))
    if folder:
        sig += ",$f:ID"; args += ", folder_id:$f"; v["f"] = folder
    res = ctx.dst.gql(f"mutation({sig}){{ create_board({args}){{ id }} }}", v)
    ctx.stats["boards_created"] += 1
    time.sleep(1.6)  # create_board : 40/min
    return fetch_board(ctx.dst, res["create_board"]["id"]), True


def ensure_groups(ctx: Ctx, src_board: dict, dst_board: dict, created_board: bool) -> dict:
    gmap = {}
    dst_titles = {g["title"]: g["id"] for g in dst_board["groups"]}
    for g in sorted(src_board["groups"], key=lambda g: float(g.get("position") or 0)):
        if g["title"] in dst_titles:
            gmap[g["id"]] = dst_titles[g["title"]]
            continue
        res = ctx.dst.gql("mutation($b:ID!,$n:String!){ create_group(board_id:$b, group_name:$n){ id } }",
                          {"b": dst_board["id"], "n": g["title"]})
        gmap[g["id"]] = res["create_group"]["id"]
        dst_titles[g["title"]] = gmap[g["id"]]
    if created_board:  # supprime le groupe par défaut créé par monday s'il n'existe pas en prod
        src_titles = {g["title"] for g in src_board["groups"]}
        for g in dst_board["groups"]:
            if g["title"] not in src_titles:
                try:
                    ctx.dst.gql("mutation($b:ID!,$g:String!){ delete_group(board_id:$b, group_id:$g){ id } }",
                                {"b": dst_board["id"], "g": g["id"]})
                except MondayError:
                    pass
    return gmap


def convert_item(ctx: Ctx, src_board: dict, item: dict):
    """Retourne (values, relations, file_assets) pour un élément source."""
    sid = str(src_board["id"])
    settings = {c["id"]: c["settings"] for c in src_board["columns"]}
    types = {c["id"]: c["type"] for c in src_board["columns"]}
    values, relations, files = {}, {}, []
    for cv in item.get("column_values") or []:
        ctype = cv.get("type") or types.get(cv["id"])
        dst_col = ctx.col_map[sid].get(cv["id"])
        if ctype in RELATION_TYPES:
            ids = cv.get("linked_item_ids") or []
            if ids and dst_col:
                relations[dst_col] = sorted(str(i) for i in ids)
            continue
        if ctype == "file":
            raw = parse_json(cv.get("value"))
            file_entries = raw.get("files") if isinstance(raw, dict) else []

            for f in file_entries or []:
                if not isinstance(f, dict):
                    continue

                if f.get("assetId"):
                    files.append((
                        dst_col,
                        str(f["assetId"]),
                        f.get("name") or "fichier",
                    ))
                elif f.get("fileType") == "LINK" or f.get("linkToFile"):
                    ctx.warn_once(
                        f"link:{cv['id']}",
                        f"Colonne fichier '{cv['id']}' : "
                        "liens externes (Drive/OneDrive…) non transférables",
                    )
            continue
        if not dst_col:
            continue
        try:
            val = convert_value(ctype, cv, ctx, settings.get(cv["id"]))
        except Exception as exc:  # garde-fou : une valeur exotique ne bloque pas l'élément
            ctx.warn_once(f"conv:{sid}:{cv['id']}", f"Conversion impossible pour la colonne {cv['id']} ({ctype}) : {exc}")
            val = None
        if val is not None:
            values[dst_col] = val
    return values, relations, files


def run_batched(ctx: Ctx, ops: list[tuple[str, str, dict]]):
    """ops = [(alias, champ_mutation_avec_$vars, variables_typées)] ; exécute par lots.
    Chaque op : (alias, "create_item(board_id:$b_0, ...){ id }", {"b_0": ("ID!", valeur), ...})."""
    results = {}
    for i in range(0, len(ops), BATCH_SIZE):
        chunk = ops[i:i + BATCH_SIZE]
        sig, body, vars_ = [], [], {}
        for alias, field, v in chunk:
            for name, (typ, val) in v.items():
                sig.append(f"${name}:{typ}")
                vars_[name] = val
            body.append(f"{alias}: {field}")
        q = f"mutation({','.join(sig)}){{ {' '.join(body)} }}"
        try:
            results.update(ctx.dst.gql(q, vars_))
        except DailyLimitReached:
            raise
        except MondayError:
            for alias, field, v in chunk:  # isole les opérations en échec
                try:
                    q1 = f"mutation({','.join(f'${n}:{t}' for n, (t, _) in v.items())}){{ {alias}: {field} }}"
                    results.update(ctx.dst.gql(q1, {n: val for n, (_, val) in v.items()}))
                except MondayError as exc:
                    results[alias] = {"__error__": str(exc)}
    return results


def write_values_one_by_one(ctx: Ctx, board_id: str, item_id: str, values: dict):
    for col, val in values.items():
        try:
            ctx.dst.gql("mutation($b:ID!,$i:ID!,$v:JSON!){ change_multiple_column_values(board_id:$b, item_id:$i, column_values:$v, create_labels_if_missing:true){ id } }",
                        {"b": board_id, "i": item_id, "v": {col: val}})
        except MondayError as exc:
            ctx.report["values_failed"].append(f"board {board_id} / item {item_id} / {col}: {str(exc)[:300]}")


def sync_rows(ctx: Ctx, src_board: dict, dst_board_id: str, rows: list[dict], existing: dict,
              gmap: dict | None, parent_map: dict | None = None):
    """Upsert d'une liste d'éléments (ou de sous-éléments si parent_map est fourni)."""
    id_col, hash_col = ctx.sync_cols[dst_board_id]
    creates, updates = [], []
    for it in rows:
        values, relations, files = convert_item(ctx, src_board, it)
        gtitle = (it.get("group") or {}).get("title", "")
        h = item_hash(it["name"], gtitle, values, relations, [f[2] for f in files])
        ex = existing.get(str(it["id"]))
        if ex:
            ctx.item_map[str(it["id"])] = ex["id"]
            if ex["hash"] != h or ctx.args.force:
                updates.append((it, values, h, ex))
        else:
            creates.append((it, values, h))
    # Créations
    ops = []
        # Exclude subitems whose parent was not synchronized.
    eligible_creates = []

    for it, values, h in creates:
        if parent_map is not None:
            parent_id = parent_map.get(str(it["id"]))
            if not parent_id:
                ctx.report["items_failed"].append(...)
                continue

        eligible_creates.append((it, values, h))

    # Créations
    for n, (it, values, h) in enumerate(eligible_creates):
        cv = dict(values, **{id_col: str(it["id"]), hash_col: h})

        if parent_map is None:
            field = (
                f"create_item("
                f"board_id:$b_{n}, "
                f"item_name:$n_{n}, "
                f"group_id:$g_{n}, "
                f"column_values:$v_{n}, "
                f"create_labels_if_missing:true"
                f"){{ id }}"
            )
            v = {
                f"b_{n}": ("ID!", dst_board_id),
                f"n_{n}": ("String!", it["name"]),
                f"g_{n}": (
                    "String",
                    gmap.get((it.get("group") or {}).get("id")),
                ),
                f"v_{n}": ("JSON", cv),
            }
        else:
            # Safe because eligible_creates was filtered above.
            parent_id = parent_map[str(it["id"])]
            field = (
                f"create_subitem("
                f"parent_item_id:$p_{n}, "
                f"item_name:$n_{n}, "
                f"column_values:$v_{n}, "
                f"create_labels_if_missing:true"
                f"){{ id }}"
            )
            v = {
                f"p_{n}": ("ID!", parent_id),
                f"n_{n}": ("String!", it["name"]),
                f"v_{n}": ("JSON", cv),
            }

        ops.append((f"c{n}", field, v))

    res = run_batched(ctx, ops)

    # Use the same filtered list so cN remains aligned with the result.
    for n, (it, values, h) in enumerate(eligible_creates):
        r = res.get(f"c{n}") or {}

        if "__error__" in r:
            # Retry with minimal creation, then write values individually.
            try:
                if parent_map is None:
                    r = ctx.dst.gql(
                        "mutation("
                        "$b:ID!,"
                        "$n:String!,"
                        "$g:String,"
                        "$v:JSON"
                        "){ create_item("
                        "board_id:$b, "
                        "item_name:$n, "
                        "group_id:$g, "
                        "column_values:$v"
                        "){ id } }",
                        {
                            "b": dst_board_id,
                            "n": it["name"],
                            "g": gmap.get((it.get("group") or {}).get("id")),
                            "v": {
                                id_col: str(it["id"]),
                                hash_col: h,
                            },
                        },
                    )["create_item"]
                else:
                    parent_id = parent_map[str(it["id"])]
                    r = ctx.dst.gql(
                        "mutation("
                        "$p:ID!,"
                        "$n:String!,"
                        "$v:JSON"
                        "){ create_subitem("
                        "parent_item_id:$p, "
                        "item_name:$n, "
                        "column_values:$v"
                        "){ id } }",
                        {
                            "p": parent_id,
                            "n": it["name"],
                            "v": {
                                id_col: str(it["id"]),
                                hash_col: h,
                            },
                        },
                    )["create_subitem"]

                write_values_one_by_one(ctx, dst_board_id, r["id"], values)

            except MondayError as exc:
                ctx.report["items_failed"].append(
                    f"{src_board['name']} / {it['name']} ({it['id']}): {exc}"
                )
                continue

        ctx.item_map[str(it["id"])] = str(r["id"])
        ctx.dirty_items.add(str(it["id"]))
        ctx.stats[
            "items_created" if parent_map is None else "subitems_created"
        ] += 1
    # Mises à jour
    ops = []
    for n, (it, values, h, ex) in enumerate(updates):
        cv = dict(values, **{hash_col: h, "name": it["name"]})
        field = f"change_multiple_column_values(board_id:$b_{n}, item_id:$i_{n}, column_values:$v_{n}, create_labels_if_missing:true){{ id }}"
        ops.append((f"u{n}", field, {f"b_{n}": ("ID!", dst_board_id), f"i_{n}": ("ID!", ex["id"]), f"v_{n}": ("JSON!", cv)}))
    res = run_batched(ctx, ops)
    for n, (it, values, h, ex) in enumerate(updates):
        if "__error__" in (res.get(f"u{n}") or {}):
            write_values_one_by_one(ctx, dst_board_id, ex["id"], dict(values, **{hash_col: h, "name": it["name"]}))
        ctx.dirty_items.add(str(it["id"]))
        ctx.stats["items_updated"] += 1
        if parent_map is None and gmap:
            target_group = gmap.get((it.get("group") or {}).get("id"))
            if target_group and target_group != ex.get("group"):
                try:
                    ctx.dst.gql("mutation($i:ID!,$g:String!){ move_item_to_group(item_id:$i, group_id:$g){ id } }",
                                {"i": ex["id"], "g": target_group})
                except MondayError as exc:
                    ctx.report["warnings"].append(f"Déplacement de groupe impossible ({it['name']}) : {exc}")
    return creates


def read_existing(ctx: Ctx, dst_board_id: str, subitems: bool = False) -> dict:
    """Index des éléments déjà synchronisés : id source -> {id, hash, group, files}."""
    id_col, hash_col = ctx.sync_cols[dst_board_id]
    fields = f'id group{{ id }} column_values(ids:["{id_col}","{hash_col}"]){{ id text }}'
    out = {}
    for it in iter_items(ctx.dst, dst_board_id, fields):
        vals = {cv["id"]: cv.get("text") or "" for cv in it["column_values"]}
        if vals.get(id_col):
            out[vals[id_col]] = {"id": str(it["id"]), "hash": vals.get(hash_col, ""), "group": (it.get("group") or {}).get("id")}
    return out


def subitems_board_id(board: dict) -> str | None:
    for c in board["columns"]:
        if c["type"] in ("subtasks", "subitems"):
            ids = (c.get("settings") or {}).get("boardIds") or []
            if ids:
                return str(ids[0])
    return None


def sync_board_items(ctx: Ctx, src_board: dict, dst_board: dict, gmap: dict):
    sid, did = str(src_board["id"]), str(dst_board["id"])
    existing = read_existing(ctx, did)
    items = list(iter_items(ctx.src, sid))
    ctx.src_boards[sid]["_items"] = items
    sync_rows(ctx, src_board, did, items, existing, gmap)
    log.info("  %s : %d éléments source, %d créés au total, %d mis à jour au total",
             src_board["name"], len(items), ctx.stats["items_created"], ctx.stats["items_updated"])

    # Sous-éléments
    src_sub_id = subitems_board_id(src_board)
    subs = [(s, it) for it in items for s in (it.get("subitems") or [])]
    if not src_sub_id or not subs:
        return
    src_sub = fetch_board(ctx.src, src_sub_id)
    src_sub["_items"] = [s for s, _ in subs]  # pour les phases relations / fichiers / updates
    ctx.src_boards[src_sub_id] = src_sub
    dst_sub_id = subitems_board_id(fetch_board(ctx.dst, did))
    if not dst_sub_id:
        # Only use a parent that was actually synchronized to the target.
        init_parent = next(
            (
                (subitem, parent)
                for subitem, parent in subs
                if ctx.item_map.get(str(parent["id"]))
            ),
            None,
        )

        if init_parent is None:
            ctx.warn_once(
                f"subitems-parent:{sid}",
                f"{src_board['name']} : aucun parent de sous-élément "
                "n'est mappé dans la cible ; sous-éléments ignorés",
            )
            return

        _, parent = init_parent
        ctx.dst.gql(
            "mutation($p:ID!,$n:String!){ "
            "create_subitem(parent_item_id:$p, item_name:$n){ id } }",
            {
                "p": ctx.item_map[str(parent["id"])],
                "n": "__init_sync__",
            },
        )

        dst_sub_id = subitems_board_id(fetch_board(ctx.dst, did))
        if not dst_sub_id:
            ctx.report["warnings"].append(
                f"{src_board['name']} : board de sous-éléments cible introuvable"
            )
            return
    ctx.board_map[src_sub_id] = dst_sub_id
    dst_sub = fetch_board(ctx.dst, dst_sub_id)
    match_columns(ctx, src_sub, dst_sub, late=False, created_board=False)
    ensure_sync_columns(ctx, fetch_board(ctx.dst, dst_sub_id))
    existing_sub = read_existing(ctx, dst_sub_id)
    parent_map = {
        str(subitem["id"]): ctx.item_map.get(str(parent["id"]))
        for subitem, parent in subs
    }

    rows = []
    skipped = 0

    for subitem, _ in subs:
        if parent_map.get(str(subitem["id"])):
            rows.append(subitem)
        else:
            skipped += 1

    if skipped:
        ctx.warn_once(
            f"subitems-unmapped:{sid}",
            f"{src_board['name']} : {skipped} sous-élément(s) ignoré(s) "
            "car leur élément parent n'est pas mappé dans la cible",
        )
    sync_rows(ctx, src_sub, dst_sub_id, rows, existing_sub, None, parent_map)
    # supprime le sous-élément technique d'initialisation
    for it in iter_items(ctx.dst, dst_sub_id, "id name"):
        if it["name"] == "__init_sync__":
            ctx.dst.gql("mutation($i:ID!){ delete_item(item_id:$i){ id } }", {"i": it["id"]})


def sync_relations(ctx: Ctx):
    for sid, sb in ctx.src_boards.items():
        if sid not in ctx.board_map:
            continue
        rows = list(sb.get("_items") or [])
        ops = []
        for it in rows:
            if str(it["id"]) not in ctx.dirty_items or str(it["id"]) not in ctx.item_map:
                continue
            _, relations, _ = convert_item(ctx, sb, it)
            if not relations:
                continue
            cv = {}
            for col, ids in relations.items():
                if ctx.same_account:  # élément hors périmètre : il existe dans le même compte, on garde son ID
                    mapped = [int(ctx.item_map.get(i, i)) for i in ids]
                else:
                    mapped = [int(ctx.item_map[i]) for i in ids if i in ctx.item_map]
                if len(mapped) < len(ids):
                    ctx.warn_once(f"rel:{sid}:{col}", f"{sb['name']} : certains éléments liés sont hors périmètre et ne sont pas reliés")
                cv[col] = {"item_ids": mapped}
            n = len(ops)
            ops.append((f"r{n}", f"change_multiple_column_values(board_id:$b_{n}, item_id:$i_{n}, column_values:$v_{n}){{ id }}",
                        {f"b_{n}": ("ID!", ctx.board_map[sid]), f"i_{n}": ("ID!", ctx.item_map[str(it["id"])]), f"v_{n}": ("JSON!", cv)}))
        res = run_batched(ctx, ops)
        errs = [r["__error__"] for r in res.values() if isinstance(r, dict) and "__error__" in r]
        ctx.stats["relations_written"] += len(ops) - len(errs)
        ctx.report["relations_failed"] += errs


def download(api: Monday, asset_ids: list[str]) -> dict:
    out = {}
    for i in range(0, len(asset_ids), 50):
        chunk = asset_ids[i:i + 50]
        for a in api.gql("query($ids:[ID!]!){ assets(ids:$ids){ id name public_url } }", {"ids": chunk}).get("assets") or []:
            try:
                r = requests.get(a["public_url"], timeout=600)
                r.raise_for_status()
                out[str(a["id"])] = (a["name"], r.content)
            except requests.RequestException as exc:
                log.warning("Téléchargement de %s impossible : %s", a.get("name"), exc)
    return out


def sync_files(ctx: Ctx):
    for sid, sb in ctx.src_boards.items():
        rows = list(sb.get("_items") or [])
        if sid not in ctx.board_map or not rows:
            continue
        for it in rows:
            if str(it["id"]) not in ctx.dirty_items:
                continue
            _, _, files = convert_item(ctx, sb, it)
            files = [f for f in files if f[0]]
            if not files:
                continue
            dst_item = ctx.item_map.get(str(it["id"]))
            cur = ctx.dst.gql("query($i:[ID!]){ items(ids:$i){ assets{ name } } }", {"i": [dst_item]})["items"]
            present = {a["name"] for a in (cur[0].get("assets") or [])} if cur else set()
            todo = [f for f in files if f[2] not in present]
            blobs = download(ctx.src, [f[1] for f in todo])
            for col, aid, name in todo:
                if aid not in blobs:
                    continue
                m = f'mutation($file: File!){{ add_file_to_column(item_id: {int(dst_item)}, column_id: "{col}", file: $file){{ id }} }}'
                try:
                    ctx.dst.upload(m, blobs[aid][0], blobs[aid][1])
                    ctx.stats["files_uploaded"] += 1
                except MondayError as exc:
                    ctx.report["files_failed"].append(f"{name}: {exc}")


def sync_updates(ctx: Ctx):
    for sid, dst_bid in list(ctx.board_map.items()):
        if sid not in ctx.src_boards:
            continue
        existing = {}
        for u in iter_board_updates(ctx.dst, dst_bid):
            for m in MARKER_RE.findall(u.get("text_body") or u.get("body") or ""):
                existing[m] = str(u["id"])
            for rep in u.get("replies") or []:
                for m in MARKER_RE.findall(rep.get("text_body") or rep.get("body") or ""):
                    existing[m] = str(rep["id"])
        src_updates = list(iter_board_updates(ctx.src, sid))
        for u in reversed(src_updates):  # du plus ancien au plus récent
            item_dst = ctx.item_map.get(str(u.get("item_id")))
            if not item_dst:
                continue
            mk = f"u{u['id']}"
            if mk not in existing:
                try:
                    res = ctx.dst.gql("mutation($i:ID!,$b:String!){ create_update(item_id:$i, body:$b){ id } }",
                                      {"i": item_dst, "b": body_with_header(u, mk)})
                    existing[mk] = str(res["create_update"]["id"])
                    ctx.stats["updates_created"] += 1
                except MondayError as exc:
                    ctx.report["updates_failed"].append(f"update {u['id']}: {exc}")
                    continue
                if not ctx.args.no_files and u.get("assets"):
                    blobs = download(ctx.src, [str(a["id"]) for a in u["assets"]])
                    for aid, (name, content) in blobs.items():
                        m = f"mutation($file: File!){{ add_file_to_update(update_id: {int(existing[mk])}, file: $file){{ id }} }}"
                        try:
                            ctx.dst.upload(m, name, content)
                            ctx.stats["files_uploaded"] += 1
                        except MondayError as exc:
                            ctx.report["files_failed"].append(f"{name}: {exc}")
            for rep in sorted(u.get("replies") or [], key=lambda r: r.get("created_at") or ""):
                rk = f"r{rep['id']}"
                if rk in existing:
                    continue
                try:
                    res = ctx.dst.gql("mutation($p:ID!,$b:String!){ create_update(parent_id:$p, body:$b){ id } }",
                                      {"p": existing[mk], "b": body_with_header(rep, rk)})
                    existing[rk] = str(res["create_update"]["id"])
                    ctx.stats["replies_created"] += 1
                except MondayError as exc:
                    ctx.report["updates_failed"].append(f"reply {rep['id']}: {exc}")


def archive_missing(ctx: Ctx):
    for sid, dst_bid in ctx.board_map.items():
        sb = ctx.src_boards.get(sid)
        if not sb or "_items" not in sb:
            continue
        src_ids = {str(i["id"]) for i in sb["_items"]}
        for src_id, ex in read_existing(ctx, dst_bid).items():
            if src_id not in src_ids:
                try:
                    ctx.dst.gql("mutation($i:ID!){ archive_item(item_id:$i){ id } }", {"i": ex["id"]})
                    ctx.stats["items_archived"] += 1
                except MondayError as exc:
                    ctx.report["warnings"].append(f"Archivage impossible {ex['id']}: {exc}")


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
def main(argv=None):
    p = argparse.ArgumentParser(description="Réplique un workspace monday.com (prod -> recette)")
    p.add_argument("--source-workspace", required=True, help="Nom ou ID (recommandé) du workspace source")
    p.add_argument("--target-workspace", help="Nom du workspace cible (défaut : même nom ; créé s'il n'existe pas)")
    p.add_argument("--target-workspace-id", help="ID d'un workspace EXISTANT en recette à utiliser comme cible")
    p.add_argument("--diagnose", action="store_true",
                   help="Affiche ce que le script voit (workspaces, boards) sans rien écrire en recette")
    p.add_argument("--boards", help="Liste de boards à synchroniser, séparés par des virgules (défaut : tous)")
    p.add_argument("--no-updates", action="store_true", help="Ne pas copier les updates")
    p.add_argument("--no-files", action="store_true", help="Ne pas copier les fichiers")
    p.add_argument("--archive-missing", action="store_true", help="Archiver en recette les éléments supprimés en prod")
    p.add_argument("--force", action="store_true", help="Réécrire tous les éléments même si inchangés")
    p.add_argument("--report", default="sync_report.json")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    src = Monday(os.environ["MONDAY_SRC_TOKEN"], "prod")
    dst = Monday(os.environ["MONDAY_DST_TOKEN"], "recette")
    ctx = Ctx(src, dst, args)
    started = time.time()
    status = "error"
    try:
        src_ws = find_workspace(src, args.source_workspace)
        if not src_ws:
            raise MondayError(f"Workspace source introuvable en prod : « {args.source_workspace} ». "
                              "Utilisez son ID (URL …/workspaces/<ID>) et vérifiez que l'utilisateur du token en est membre.")
        log.info("Workspace source : %s (%s)", src_ws["name"], src_ws["id"])
        wanted = {norm(b) for b in args.boards.split(",")} if args.boards else None
        all_src_boards = list_boards(src, str(src_ws["id"]))
        src_list = [b for b in all_src_boards if not wanted or norm(b["name"]) in wanted]
        log.info("Boards visibles en prod dans ce workspace : %d — retenus : %d (%s)", len(all_src_boards), len(src_list),
                 ", ".join(b["name"] for b in src_list))
        ctx.report["source"] = {"workspace": src_ws, "boards": [{"id": b["id"], "name": b["name"], "type": b.get("type")} for b in all_src_boards]}
        if not src_list:
            raise MondayError("Aucun board à synchroniser : le token prod ne voit aucun board de ce workspace "
                              "(utilisateur non membre / boards privés) ou le filtre --boards ne correspond à aucun nom.")

        if args.target_workspace_id:
            tgt = get_workspace(dst, args.target_workspace_id)
            if not tgt:
                raise MondayError(f"Workspace cible {args.target_workspace_id} introuvable en recette (ou non accessible au token).")
        else:
            tgt = find_workspace(dst, args.target_workspace or src_ws["name"])
        ctx.report["target"] = {"workspace": tgt or f"à créer : {args.target_workspace or src_ws['name']}"}

        # Garde-fou : même compte et cible = source => le script écrirait dans le workspace source
        try:
            acc_src = src.gql("query{ account{ id slug } }")["account"]
            acc_dst = dst.gql("query{ account{ id slug } }")["account"]
            ctx.same_account = str(acc_src["id"]) == str(acc_dst["id"])
        except MondayError as exc:
            ctx.warn_once("account", f"Impossible de comparer les comptes source et cible : {exc}")
        ctx.report["same_account"] = ctx.same_account
        if ctx.same_account:
            log.info("Source et cible sont sur le même compte (%s) : mode test intra-compte.", acc_src.get("slug"))
        if tgt and str(tgt["id"]) == str(src_ws["id"]):
            raise MondayError(
                "La cible est le workspace SOURCE lui-même (même compte, même nom ou même ID). Le script refuse "
                "d'écrire dedans. Indiquez le workspace cible avec --target-workspace-id <ID> (input GitHub "
                "target_workspace_id) ou --target-workspace \"<autre nom>\".")
        if args.diagnose:
            log.info("DIAGNOSTIC — cible : %s", tgt or "aucun workspace correspondant, il serait CRÉÉ")
            if tgt:
                log.info("Boards déjà présents en recette : %s", ", ".join(b["name"] for b in list_boards(dst, str(tgt["id"]))) or "aucun")
            status = "diagnose"
            return

        build_people_maps(ctx)
        dst_ws = str(tgt["id"]) if tgt else ensure_workspace(ctx, src_ws, args.target_workspace or src_ws["name"])
        log.info("Workspace cible utilisé : %s", dst_ws)
        fmap = ensure_folders(ctx, str(src_ws["id"]), dst_ws)
        dst_by_name = {norm(b["name"]): str(b["id"]) for b in list_boards(dst, dst_ws)}

        # Phase 1 : boards, groupes, colonnes simples
        pairs = []
        for b in src_list:
            sb = fetch_board(src, str(b["id"]))
            ctx.src_boards[str(sb["id"])] = sb
            db, created = ensure_board(ctx, sb, dst_ws, fmap, dst_by_name)
            ctx.board_map[str(sb["id"])] = str(db["id"])
            gmap = ensure_groups(ctx, sb, db, created)
            match_columns(ctx, sb, db, late=False, created_board=created)
            pairs.append((sb, db, gmap, created))
            log.info("Board prêt : %s -> %s", sb["name"], db["id"])
        # Phase 2 : colonnes Connect boards / miroir / formule / dépendance
        for sb, db, gmap, created in pairs:
            match_columns(ctx, sb, fetch_board(dst, db["id"]), late=True, created_board=created)
        # Phase 3 : éléments et sous-éléments
        for sb, db, gmap, created in pairs:
            ensure_sync_columns(ctx, fetch_board(dst, db["id"]))
            sync_board_items(ctx, sb, db, gmap)
        # Phase 4 : relations
        sync_relations(ctx)
        # Phase 5 : fichiers et updates
        if not args.no_files:
            sync_files(ctx)
        if not args.no_updates:
            sync_updates(ctx)
        # Phase 6 : nettoyage
        if args.archive_missing:
            archive_missing(ctx)
        status = "ok"
    except DailyLimitReached as exc:
        log.error("%s — relancez après minuit UTC, la synchro reprendra là où elle s'est arrêtée.", exc)
        status = "daily_limit"
    except Exception as exc:
        ctx.report["error"] = f"{type(exc).__name__}: {exc}"
        log.error("ÉCHEC : %s", ctx.report["error"])
        raise
    finally:
        ctx.report["status"] = status
        ctx.report["stats"] = dict(ctx.stats)
        ctx.report["api_calls"] = {"prod": src.calls, "recette": dst.calls}
        ctx.report["duration_s"] = round(time.time() - started)
        ctx.report["not_synced_by_design"] = [
            "Workflows (Workflow Builder) et automatisations : pas d'API publique de lecture/création structurée",
            "Vues de board (Kanban, Gantt, Calendrier…), dashboards, docs, formulaires",
            "Historique Emails & Activités (seules les activités personnalisées sont recréables)",
            "Auteur/date d'origine des éléments et updates (reportés en en-tête des updates)",
            "Valeurs Vote, Time tracking, formules/miroirs (recalculés), liens externes des colonnes fichiers",
        ]
        with open(args.report, "w", encoding="utf-8") as fh:
            json.dump(ctx.report, fh, ensure_ascii=False, indent=2)
        log.info("Rapport : %s — stats %s — appels API %s", args.report, dict(ctx.stats), ctx.report["api_calls"])


if __name__ == "__main__":
    main()