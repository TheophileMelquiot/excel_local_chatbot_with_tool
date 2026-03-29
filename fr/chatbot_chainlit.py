"""
Chatbot Excel — Interface Chainlit (alternative à Gradio).

Reproduit toutes les fonctionnalités de chatbot.py avec l'interface Chainlit :
- Upload de fichier Excel (.xlsx / .xls)
- Parsing LLM local (OllamaIntentParser) avec fallback regex (IntentParser)
- Recherche mono-colonne, multi-valeurs, multi-colonnes (ET)
- Affichage des résultats en tableau markdown
- Sauvegarde des résultats en fichier Excel téléchargeable (cl.Action)
- Statut LLM au démarrage
- Aperçu des premières lignes du fichier chargé

Utilisation :
    chainlit run fr/chatbot_chainlit.py --port 8000
    # ou depuis le dossier fr/ :
    chainlit run chatbot_chainlit.py --port 8000
"""

from __future__ import annotations

import os
import re
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Assurer que le dossier fr/ est dans le path pour les imports locaux
sys.path.insert(0, str(Path(__file__).parent))

import chainlit as cl

from excel_query_engine import (
    DataType,
    ExcelExporter,
    ExcelQueryEngine,
    LogicalOperator,
)
from llm_parser import OllamaIntentParser, is_llm_ready, start_llm_preload

MAX_PREVIEW_COLUMNS = 8
MAX_CELL_DISPLAY_LEN = 30


# ============================================================================
# ANALYSEUR D'INTENTIONS — basé sur des règles, entièrement hors-ligne
# (copié de chatbot.py pour rester indépendant du fichier Gradio)
# ============================================================================

class IntentParser:
    """Analyse les messages en langage naturel pour en extraire des paramètres de requête.

    Fonctionne sans modèle ML — 100% hors-ligne.
    """

    _SEARCH_VERBS = (
        r"(?:chercher|cherche|rechercher|recherche|trouver|trouve|"
        r"afficher|affiche|montrer|montre|filtrer|filtre|"
        r"lister|liste|donner|donne(?:-moi)?|obtenir|obtiens|"
        r"search|find|look\s*(?:for|up)?|query|get|show|where|filter|list|give(?:\s*me)?)"
    )

    _QUERY_RE = re.compile(
        rf"^\s*{_SEARCH_VERBS}"
        r"\s+(?P<values>.+?)"
        r"(?:\s+(?:dans|dans\s+la\s+colonne|in|from|of|under|at|on)\s+(?:column\s+|colonne\s+)?(?P<column>.+?))?"
        r"\s*$",
        re.IGNORECASE,
    )

    _VALUE_SEP = re.compile(r"\s*(?:,|\bet\b|\bou\b|\band\b|\bor\b)\s*", re.IGNORECASE)

    _MULTI_COL_CONNECTORS = re.compile(
        r"\s+(?:avec|contenant(?:\s+la\s+valeur)?|with|containing(?:\s+the\s+value)?)\s+",
        re.IGNORECASE,
    )

    _PAIR_RE = re.compile(
        r"(?:la\s+valeur\s+)?(.+?)"
        r"\s+(?:dans|dans\s+la\s+colonne|in)\s+(?:colonne\s+|column\s+)?(.+?)$",
        re.IGNORECASE,
    )

    _DANS_IN_RE = re.compile(r"\b(?:dans|in)\b", re.IGNORECASE)

    @classmethod
    def _preprocess(cls, message: str) -> str:
        msg = message.strip().rstrip(".?!")
        msg = re.sub(r"\s{2,}", " ", msg)
        return msg

    @classmethod
    def _try_parse_value(cls, raw: str) -> Any:
        raw = raw.strip().strip("\"'")
        try:
            if "." in raw:
                return float(raw)
            return int(raw)
        except ValueError:
            return raw

    @classmethod
    def parse_multi_column(cls, message: str) -> Optional[Dict[str, Any]]:
        message = cls._preprocess(message)
        return cls._parse_multi_column_inner(message)

    @classmethod
    def _parse_multi_column_inner(cls, message: str) -> Optional[Dict[str, Any]]:
        m = re.match(rf"^\s*{cls._SEARCH_VERBS}\s+(.+)", message.strip(), re.IGNORECASE)
        if not m:
            return None

        content = m.group(1).strip()
        dans_matches = list(cls._DANS_IN_RE.finditer(content))
        if len(dans_matches) < 2:
            return None

        segments = cls._MULTI_COL_CONNECTORS.split(content)

        if len(segments) == 1:
            et_segments = re.split(r"\s+(?:et|and)\s+", content, flags=re.IGNORECASE)
            if len(et_segments) >= 2:
                if all(cls._DANS_IN_RE.search(seg) for seg in et_segments):
                    segments = et_segments

        if len(segments) < 2:
            return None

        criteria: List[Dict[str, Any]] = []
        for segment in segments:
            pair_match = cls._PAIR_RE.match(segment.strip())
            if pair_match:
                value = cls._try_parse_value(pair_match.group(1))
                column = pair_match.group(2).strip().strip("\"'")
                criteria.append({"value": value, "column": column})

        if len(criteria) < 2:
            return None

        return {"multi_criteria": criteria}

    @classmethod
    def parse(cls, message: str) -> Optional[Dict[str, Any]]:
        message = cls._preprocess(message)
        multi = cls._parse_multi_column_inner(message)
        if multi is not None:
            return multi

        m = cls._QUERY_RE.match(message.strip())
        if not m:
            return None

        raw_values = m.group("values").strip().strip("\"'")
        column_hint = m.group("column")
        if column_hint:
            column_hint = column_hint.strip().strip("\"'")

        parts = cls._VALUE_SEP.split(raw_values)
        values: List[Any] = []
        for p in parts:
            p = p.strip().strip("\"'")
            if not p:
                continue
            values.append(cls._try_parse_value(p))

        if not values:
            return None

        return {
            "values": values if len(values) > 1 else values[0],
            "column_hint": column_hint,
        }


# ============================================================================
# LOGIQUE MÉTIER
# ============================================================================

WELCOME = (
    "👋 Bienvenue dans le **Chatbot de requêtes Excel** !\n\n"
    "1. **Téléversez un fichier Excel** (.xlsx / .xls) en cliquant sur l'icône de pièce jointe.\n"
    "2. Posez-moi une question de recherche, par exemple :\n"
    "   - `chercher 12345`\n"
    "   - `trouver Jean dans Nom`\n"
    "   - `chercher 100 et 200 dans Montant`\n"
    "   - `recherche 723 dans id avec dupont dans nom`\n"
    "3. Cliquez sur **💾 Sauvegarder les résultats en Excel** pour exporter les résultats.\n\n"
    "Tapez **aide** pour plus de détails ou **colonnes** pour voir les colonnes disponibles."
)

HELP_TEXT = (
    "📖 **Commandes disponibles**\n\n"
    "| Commande | Exemple |\n"
    "|---|---|\n"
    "| Rechercher une valeur | `chercher 12345` |\n"
    "| Rechercher dans une colonne | `trouver Jean dans Nom` |\n"
    "| Rechercher plusieurs valeurs | `chercher 100, 200, 300 dans Montant` |\n"
    "| Recherche multi-colonnes | `recherche 723 dans id avec dupont dans nom` |\n"
    "| Recherche multi-colonnes | `recherche 723 dans id contenant la valeur dupont dans nom` |\n"
    "| Lister les colonnes | `colonnes` |\n"
    "| Afficher l'aide | `aide` |\n\n"
    "Vous pouvez aussi utiliser des verbes comme *rechercher*, *trouver*, *afficher*, *montrer*, *filtrer*."
)


def _format_column_info(engine: ExcelQueryEngine) -> str:
    """Retourne un tableau markdown des profils de colonnes."""
    lines = ["| # | Colonne | Type | Non-vide |", "|---|---------|------|----------|"]
    for p in engine.profiles:
        lines.append(
            f"| {p.index + 1} | {p.name} | {p.detected_type.value} | {p.non_empty_count} |"
        )
    return "\n".join(lines)


def _format_rows_preview(rows: List[Dict[str, str]], limit: int = 10) -> str:
    """Retourne un aperçu en tableau markdown des lignes correspondantes."""
    if not rows:
        return ""
    headers = list(rows[0].keys())
    display_headers = headers[:MAX_PREVIEW_COLUMNS]
    truncated = len(headers) > MAX_PREVIEW_COLUMNS

    lines = [
        "| " + " | ".join(display_headers) + (" | ..." if truncated else "") + " |",
        "| " + " | ".join(["---"] * len(display_headers)) + (" | ---" if truncated else "") + " |",
    ]
    for row in rows[:limit]:
        cells = [str(row.get(h, ""))[:MAX_CELL_DISPLAY_LEN] for h in display_headers]
        lines.append("| " + " | ".join(cells) + (" | ..." if truncated else "") + " |")

    if len(rows) > limit:
        lines.append(
            f"\n*… et {len(rows) - limit} lignes supplémentaires. "
            "Utilisez le bouton **💾 Sauvegarder les résultats en Excel** pour obtenir toutes les lignes.*"
        )

    return "\n".join(lines)


def _search_all_columns(engine: ExcelQueryEngine, values: Any) -> Tuple[str, List[Dict[str, str]]]:
    """Cherche *values* dans toutes les colonnes et retourne (message, lignes)."""
    all_matches: List[Dict[str, str]] = []
    search_vals = values if isinstance(values, list) else [values]

    for profile in engine.profiles:
        if profile.detected_type == DataType.EMPTY:
            continue
        try:
            result = engine.search(
                criteria=[(search_vals, profile.name)],
                mode=LogicalOperator.OR,
                include_partial=True,
            )
            if result.matched_rows:
                existing = {tuple(r.items()) for r in all_matches}
                for row in result.matched_rows:
                    if tuple(row.items()) not in existing:
                        all_matches.append(row)
                        existing.add(tuple(row.items()))
        except Exception:
            continue

    if not all_matches:
        return f"Aucun résultat trouvé pour **{values}** dans aucune colonne.", []

    preview = _format_rows_preview(all_matches)
    msg = (
        f"🔍 **{len(all_matches)}** ligne(s) trouvée(s) correspondant à **{values}** "
        f"(recherche dans toutes les colonnes).\n\n{preview}"
    )
    return msg, all_matches


def _search_specific_column(
    engine: ExcelQueryEngine, values: Any, column_hint: str
) -> Tuple[str, List[Dict[str, str]]]:
    """Cherche *values* dans une colonne spécifique. Retourne (message, lignes)."""
    search_vals = values if isinstance(values, list) else [values]
    try:
        result = engine.search(
            criteria=[(search_vals, column_hint)],
            mode=LogicalOperator.OR,
            include_partial=True,
        )
    except ValueError as exc:
        return f"❌ {exc}", []

    if not result.matched_rows:
        return f"Aucun résultat trouvé pour **{values}** dans la colonne **{column_hint}**.", []

    preview = _format_rows_preview(result.matched_rows)
    msg = (
        f"🔍 **{result.total_matches}** ligne(s) trouvée(s) correspondant à **{values}** "
        f"dans la colonne **{column_hint}**.\n\n{preview}"
    )
    return msg, result.matched_rows


def _search_multi_criteria(
    engine: ExcelQueryEngine, criteria_list: List[Dict[str, Any]]
) -> Tuple[str, List[Dict[str, str]]]:
    """Recherche multi-colonnes (ET). Retourne (message, lignes)."""
    criteria = [(item["value"], item["column"]) for item in criteria_list]
    try:
        result = engine.search(
            criteria=criteria,
            mode=LogicalOperator.AND,
            include_partial=True,
        )
    except ValueError as exc:
        return f"❌ {exc}", []

    desc = " ET ".join(
        f"**{c['value']}** dans **{c['column']}**" for c in criteria_list
    )
    if not result.matched_rows:
        return f"Aucun résultat trouvé pour {desc}.", []

    preview = _format_rows_preview(result.matched_rows)
    msg = f"🔍 **{result.total_matches}** ligne(s) trouvée(s) pour {desc}.\n\n{preview}"
    return msg, result.matched_rows


# ============================================================================
# GESTION DES ACTIONS
# ============================================================================

@cl.action_callback("sauvegarder_excel")
async def on_action_save(action: cl.Action):
    """Exporte les derniers résultats en Excel et les envoie à l'utilisateur."""
    engine: Optional[ExcelQueryEngine] = cl.user_session.get("engine")
    last_result_rows: List[Dict[str, str]] = cl.user_session.get("last_result_rows") or []

    if not last_result_rows or engine is None:
        await cl.Message(content="⚠️ Aucun résultat à sauvegarder. Effectuez d'abord une recherche.").send()
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(tempfile.gettempdir(), f"resultats_requete_{timestamp}.xlsx")
    ExcelExporter.export_results(
        out_path,
        engine.headers,
        last_result_rows,
        query_summary=f"Export de {len(last_result_rows)} lignes le {timestamp}",
    )

    elements = [
        cl.File(
            name=f"resultats_requete_{timestamp}.xlsx",
            path=out_path,
            display="inline",
        )
    ]
    await cl.Message(
        content=f"✅ **{len(last_result_rows)}** ligne(s) exportée(s) dans le fichier Excel ci-dessous.",
        elements=elements,
    ).send()


# ============================================================================
# ÉVÉNEMENTS CHAINLIT
# ============================================================================

@cl.on_chat_start
async def on_chat_start():
    """Initialise la session et affiche le message de bienvenue."""
    # Initialiser le parser LLM (partagé pour la session)
    llm_parser = OllamaIntentParser()
    if llm_parser.is_available():
        start_llm_preload()  # non-bloquant

    # Stocker l'état dans la session utilisateur
    cl.user_session.set("engine", None)
    cl.user_session.set("last_result_rows", [])
    cl.user_session.set("llm_parser", llm_parser)

    # Statut LLM
    if not llm_parser.is_available():
        llm_status = "🔴 LLM non disponible — mode regex uniquement."
    elif is_llm_ready():
        llm_status = "🟢 LLM local prêt."
    else:
        llm_status = "🟡 LLM local en cours de chargement en arrière-plan…"

    # Message de bienvenue + statut LLM
    await cl.Message(content=WELCOME).send()
    await cl.Message(content=llm_status).send()


@cl.on_message
async def on_message(message: cl.Message):
    """Traite chaque message entrant : upload, commandes méta ou requête de recherche."""

    # ── 1. Gestion des fichiers uploadés ─────────────────────────────────────
    uploaded_files = [el for el in (message.elements or []) if hasattr(el, "path") and el.path]
    if uploaded_files:
        for uploaded_file in uploaded_files:
            filepath = uploaded_file.path
            name = getattr(uploaded_file, "name", os.path.basename(filepath))
            try:
                engine = ExcelQueryEngine(filepath)
                cl.user_session.set("engine", engine)
                cl.user_session.set("last_result_rows", [])

                status = (
                    f"✅ Fichier **{name}** chargé — "
                    f"{len(engine.rows)} lignes, {len(engine.headers)} colonnes."
                )

                # Aperçu des premières lignes
                preview_rows = engine.rows[:3]
                headers = engine.headers
                if preview_rows and headers:
                    preview_lines = [
                        "**Aperçu des données (3 premières lignes) :**\n",
                        "| " + " | ".join(headers[:MAX_PREVIEW_COLUMNS])
                        + (" | ..." if len(headers) > MAX_PREVIEW_COLUMNS else "") + " |",
                        "| " + " | ".join(["---"] * min(len(headers), MAX_PREVIEW_COLUMNS))
                        + (" | ---" if len(headers) > MAX_PREVIEW_COLUMNS else "") + " |",
                    ]
                    for row in preview_rows:
                        cells = [
                            str(row.get(h, ""))[:MAX_CELL_DISPLAY_LEN]
                            for h in headers[:MAX_PREVIEW_COLUMNS]
                        ]
                        preview_lines.append(
                            "| " + " | ".join(cells)
                            + (" | ..." if len(headers) > MAX_PREVIEW_COLUMNS else "") + " |"
                        )
                    preview_md = "\n".join(preview_lines)
                else:
                    preview_md = ""

                col_info = _format_column_info(engine)
                content = f"{status}\n\n{preview_md}\n\n**Colonnes disponibles :**\n\n{col_info}"
                await cl.Message(content=content).send()

            except Exception as exc:
                await cl.Message(content=f"❌ Erreur lors du chargement du fichier : {exc}").send()
        return  # ne pas traiter le texte du message si seul un fichier était joint

    # ── 2. Commandes méta ─────────────────────────────────────────────────────
    msg = (message.content or "").strip()
    if not msg:
        return

    if msg.lower() in ("aide", "help", "?"):
        await cl.Message(content=HELP_TEXT).send()
        return

    if msg.lower() in ("colonnes", "cols", "columns", "headers", "en-têtes"):
        engine: Optional[ExcelQueryEngine] = cl.user_session.get("engine")
        if engine is None:
            await cl.Message(content="⚠️ Aucun fichier chargé.").send()
        else:
            await cl.Message(content=_format_column_info(engine)).send()
        return

    # ── 3. Vérifier qu'un fichier est chargé ─────────────────────────────────
    engine = cl.user_session.get("engine")
    if engine is None:
        await cl.Message(
            content="⚠️ Veuillez d'abord téléverser un fichier Excel (cliquez sur l'icône 📎)."
        ).send()
        return

    # ── 4. Parsing de l'intention ─────────────────────────────────────────────
    llm_parser: OllamaIntentParser = cl.user_session.get("llm_parser")
    parsed = None
    llm_status_note = ""

    async with cl.Step(name="Analyse de la requête", type="tool") as step:
        step.input = msg

        if llm_parser.is_available() and not is_llm_ready():
            llm_status_note = "\n\n> ⏳ *LLM encore en chargement, analyse regex utilisée pour cette requête.*"

        if llm_parser.is_available():
            parsed = llm_parser.parse(msg)
            if parsed is not None:
                step.output = f"LLM → `{parsed}`"
            else:
                step.output = "LLM → aucun résultat, fallback regex"

        if parsed is None:
            parsed = IntentParser.parse(msg)
            if parsed is not None:
                step.output = f"Regex → `{parsed}`"
            else:
                step.output = "Aucun résultat (regex)"

    if parsed is None:
        await cl.Message(
            content=(
                "🤔 Je n'ai pas compris cette requête. Essayez quelque chose comme :\n"
                "- `chercher 12345`\n"
                "- `trouver Jean dans Nom`\n"
                "- `recherche 723 dans id avec dupont dans nom`\n\n"
                "Tapez **aide** pour la liste complète des commandes."
                + llm_status_note
            )
        ).send()
        return

    # ── 5. Exécution de la recherche ──────────────────────────────────────────
    async with cl.Step(name="Recherche dans le fichier Excel", type="tool") as step:
        step.input = str(parsed)

        if "multi_criteria" in parsed:
            result_msg, result_rows = _search_multi_criteria(engine, parsed["multi_criteria"])
        else:
            values = parsed["values"]
            column_hint = parsed.get("column_hint")
            if column_hint is None:
                result_msg, result_rows = _search_all_columns(engine, values)
            else:
                result_msg, result_rows = _search_specific_column(engine, values, column_hint)

        step.output = f"{len(result_rows)} ligne(s) trouvée(s)"

    # Mettre à jour les derniers résultats
    cl.user_session.set("last_result_rows", result_rows)

    # ── 6. Affichage des résultats + bouton de sauvegarde ─────────────────────
    actions = []
    if result_rows:
        actions = [
            cl.Action(
                name="sauvegarder_excel",
                value="save",
                label="💾 Sauvegarder les résultats en Excel",
            )
        ]

    await cl.Message(content=result_msg + llm_status_note, actions=actions).send()
