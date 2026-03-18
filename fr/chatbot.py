"""
Chatbot Excel — Interface de chatbot hors-ligne pour le moteur de requêtes Excel.

Utilise Gradio pour l'interface et un analyseur d'intentions basé sur des règles
afin que l'application fonctionne entièrement hors-ligne sans connexion internet
ni API LLM externe.

Constantes :
    MAX_PREVIEW_COLUMNS  – nombre max de colonnes affichées dans l'aperçu
    MAX_CELL_DISPLAY_LEN – nombre max de caractères par cellule dans l'aperçu

Utilisation :
    python chatbot.py                       # port par défaut 7860
    python chatbot.py --port 8080           # port personnalisé
    python chatbot.py --share               # créer un lien public (nécessite internet)
"""

import argparse
import os
import re
import tempfile
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import gradio as gr

from excel_query_engine import (
    ColumnProfile,
    DataType,
    ExcelExporter,
    ExcelQueryEngine,
    LogicalOperator,
)

MAX_PREVIEW_COLUMNS = 8
MAX_CELL_DISPLAY_LEN = 30


# ============================================================================
# ANALYSEUR D'INTENTIONS — basé sur des règles, entièrement hors-ligne
# ============================================================================

class IntentParser:
    """Analyse les messages en langage naturel pour en extraire des paramètres de requête.

    Conçu pour fonctionner sans modèle ML afin que le chatbot tourne 100% hors-ligne.
    Gère les motifs courants tels que :
        "chercher 12345"
        "trouver Jean dans la colonne Nom"
        "rechercher 12345 dans Numéro de Facture"
        "chercher 100 et 200 dans Montant"
        "chercher Alice ou Bob dans Nom"
        "search 12345"
        "find John in Name"
    """

    # Verbes qui signalent une intention de recherche (français et anglais)
    _SEARCH_VERBS = (
        r"(?:chercher|cherche|rechercher|recherche|trouver|trouve|"
        r"afficher|affiche|montrer|montre|filtrer|filtre|"
        r"search|find|look\s*(?:for|up)?|query|get|show|where|filter)"
    )

    # Motif : <verbe> <valeurs> [dans [la colonne] <indice_colonne>]
    _QUERY_RE = re.compile(
        rf"^\s*{_SEARCH_VERBS}"
        r"\s+(?P<values>.+?)"
        r"(?:\s+(?:dans|dans\s+la\s+colonne|in|from|of|under|at|on)\s+(?:column\s+|colonne\s+)?(?P<column>.+?))?"
        r"\s*$",
        re.IGNORECASE,
    )

    # Séparateurs entre plusieurs valeurs
    _VALUE_SEP = re.compile(r"\s*(?:,|\bet\b|\bou\b|\band\b|\bor\b)\s*", re.IGNORECASE)

    @classmethod
    def parse(cls, message: str) -> Optional[Dict[str, Any]]:
        """Retourne un dict ``{values, column_hint}`` ou *None* si le message
        ne ressemble pas à une requête."""
        m = cls._QUERY_RE.match(message.strip())
        if not m:
            return None

        raw_values = m.group("values").strip().strip("\"'")
        column_hint = m.group("column")
        if column_hint:
            column_hint = column_hint.strip().strip("\"'")

        # Séparer sur les virgules / "et" / "ou"
        parts = cls._VALUE_SEP.split(raw_values)
        values: List[Any] = []
        for p in parts:
            p = p.strip().strip("\"'")
            if not p:
                continue
            # Essayer d'interpréter comme un nombre
            try:
                if "." in p:
                    values.append(float(p))
                else:
                    values.append(int(p))
            except ValueError:
                values.append(p)

        if not values:
            return None

        return {
            "values": values if len(values) > 1 else values[0],
            "column_hint": column_hint,
        }


# ============================================================================
# APPLICATION CHATBOT
# ============================================================================

class ExcelChatbot:
    """Chatbot basé sur Gradio qui encapsule le moteur de requêtes Excel."""

    WELCOME = (
        "👋 Bienvenue dans le **Chatbot de requêtes Excel** !\n\n"
        "1. Téléversez un fichier Excel à l'aide du panneau de gauche.\n"
        "2. Posez-moi une question de recherche, par exemple :\n"
        "   - `chercher 12345`\n"
        "   - `trouver Jean dans Nom`\n"
        "   - `chercher 100 et 200 dans Montant`\n"
        "3. Cliquez sur **Sauvegarder les résultats en Excel** pour exporter.\n\n"
        "Tapez **aide** pour plus de détails ou **colonnes** pour voir les colonnes disponibles."
    )

    HELP_TEXT = (
        "📖 **Commandes disponibles**\n\n"
        "| Commande | Exemple |\n"
        "|---|---|\n"
        "| Rechercher une valeur | `chercher 12345` |\n"
        "| Rechercher dans une colonne | `trouver Jean dans Nom` |\n"
        "| Rechercher plusieurs valeurs | `chercher 100, 200, 300 dans Montant` |\n"
        "| Lister les colonnes | `colonnes` |\n"
        "| Afficher l'aide | `aide` |\n\n"
        "Vous pouvez aussi utiliser des verbes comme *rechercher*, *trouver*, *afficher*, *montrer*, *filtrer*."
    )

    def __init__(self) -> None:
        self.engine: Optional[ExcelQueryEngine] = None
        self.last_result = None
        self.last_result_rows: List[Dict[str, str]] = []

    # ------------------------------------------------------------------ utils
    def _format_column_info(self) -> str:
        """Retourne un tableau markdown des profils de colonnes."""
        if self.engine is None:
            return "Aucun fichier chargé."
        lines = ["| # | Colonne | Type | Non-vide |", "|---|---------|------|----------|"]
        for p in self.engine.profiles:
            lines.append(
                f"| {p.index + 1} | {p.name} | {p.detected_type.value} | {p.non_empty_count} |"
            )
        return "\n".join(lines)

    def _rows_to_display(self, rows: List[Dict[str, str]], limit: int = 50) -> List[List[str]]:
        """Convertit les dicts de lignes en liste de listes pour Gradio Dataframe."""
        if not rows:
            return []
        headers = list(rows[0].keys())
        data = []
        for row in rows[:limit]:
            data.append([row.get(h, "") for h in headers])
        return data

    # -------------------------------------------------------------- handlers
    def load_file(self, file) -> Tuple[str, str]:
        """Gère le téléversement de fichier — retourne (message_statut, info_colonnes_md)."""
        if file is None:
            return "⚠️ Aucun fichier sélectionné.", ""
        filepath = file.name if hasattr(file, "name") else str(file)
        try:
            self.engine = ExcelQueryEngine(filepath)
            self.last_result = None
            self.last_result_rows = []
            status = (
                f"✅ Fichier **{os.path.basename(filepath)}** chargé — "
                f"{len(self.engine.rows)} lignes, {len(self.engine.headers)} colonnes."
            )
            return status, self._format_column_info()
        except Exception as exc:
            self.engine = None
            return f"❌ Erreur lors du chargement du fichier : {exc}", ""

    def chat(self, message: str, history: list) -> str:
        """Traite un message de chat et retourne une réponse."""
        msg = message.strip()
        if not msg:
            return "Veuillez saisir un message."

        # --- commandes méta ---------------------------------------------------
        if msg.lower() in ("aide", "help", "?"):
            return self.HELP_TEXT

        if msg.lower() in ("colonnes", "cols", "columns", "headers", "en-têtes"):
            return self._format_column_info()

        # --- requêtes ---------------------------------------------------------
        if self.engine is None:
            return "⚠️ Veuillez d'abord téléverser un fichier Excel (utilisez le panneau de gauche)."

        parsed = IntentParser.parse(msg)
        if parsed is None:
            return (
                "🤔 Je n'ai pas compris cette requête. Essayez quelque chose comme :\n"
                "- `chercher 12345`\n"
                "- `trouver Jean dans Nom`\n\n"
                "Tapez **aide** pour la liste complète des commandes."
            )

        values = parsed["values"]
        column_hint = parsed["column_hint"]

        # Si pas d'indice de colonne, chercher dans toutes les colonnes
        if column_hint is None:
            return self._search_all_columns(values)

        # Chercher dans la colonne spécifiée
        return self._search_specific_column(values, column_hint)

    def _search_all_columns(self, values: Any) -> str:
        """Cherche *values* dans toutes les colonnes et retourne les résultats combinés."""
        all_matches: List[Dict[str, str]] = []
        searched_cols: List[str] = []
        search_vals = values if isinstance(values, list) else [values]

        for profile in self.engine.profiles:
            if profile.detected_type == DataType.EMPTY:
                continue
            try:
                result = self.engine.search(
                    criteria=[(search_vals, profile.name)],
                    mode=LogicalOperator.OR,
                    include_partial=True,
                )
                if result.matched_rows:
                    # Dédupliquer
                    existing = {tuple(r.items()) for r in all_matches}
                    for row in result.matched_rows:
                        if tuple(row.items()) not in existing:
                            all_matches.append(row)
                            existing.add(tuple(row.items()))
                    searched_cols.append(profile.name)
            except Exception:
                continue

        self.last_result_rows = all_matches
        if not all_matches:
            return f"Aucun résultat trouvé pour **{values}** dans aucune colonne."

        preview = self._format_rows_preview(all_matches)
        return (
            f"🔍 **{len(all_matches)}** ligne(s) trouvée(s) correspondant à **{values}** "
            f"(recherche dans toutes les colonnes).\n\n{preview}"
        )

    def _search_specific_column(self, values: Any, column_hint: str) -> str:
        """Cherche *values* dans une colonne spécifique."""
        search_vals = values if isinstance(values, list) else [values]
        try:
            result = self.engine.search(
                criteria=[(search_vals, column_hint)],
                mode=LogicalOperator.OR,
                include_partial=True,
            )
        except ValueError as exc:
            return f"❌ {exc}"

        self.last_result_rows = result.matched_rows
        if not result.matched_rows:
            return f"Aucun résultat trouvé pour **{values}** dans la colonne **{column_hint}**."

        preview = self._format_rows_preview(result.matched_rows)
        return (
            f"🔍 **{result.total_matches}** ligne(s) trouvée(s) correspondant à **{values}** "
            f"dans la colonne **{column_hint}**.\n\n{preview}"
        )

    def _format_rows_preview(self, rows: List[Dict[str, str]], limit: int = 10) -> str:
        """Retourne un aperçu en tableau markdown des lignes correspondantes."""
        if not rows:
            return ""
        headers = list(rows[0].keys())
        # Tronquer l'affichage si trop de colonnes
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
            lines.append(f"\n*… et {len(rows) - limit} lignes supplémentaires. Cliquez sur **Sauvegarder les résultats en Excel** pour obtenir toutes les lignes.*")

        return "\n".join(lines)

    def save_results(self) -> Optional[str]:
        """Exporte les derniers résultats de requête dans un fichier Excel et retourne le chemin."""
        if not self.last_result_rows:
            return None
        if self.engine is None:
            return None

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(tempfile.gettempdir(), f"resultats_requete_{timestamp}.xlsx")
        ExcelExporter.export_results(
            out_path,
            self.engine.headers,
            self.last_result_rows,
            query_summary=f"Export de {len(self.last_result_rows)} lignes le {timestamp}",
        )
        return out_path


# ============================================================================
# INTERFACE GRADIO
# ============================================================================

def build_ui() -> gr.Blocks:
    """Construit et retourne l'application Gradio Blocks."""
    bot = ExcelChatbot()

    with gr.Blocks(title="Chatbot de requêtes Excel") as app:
        gr.Markdown("# 📊 Chatbot de requêtes Excel\nPosez des questions sur vos données Excel — **100% hors-ligne**.")

        with gr.Row():
            # ---- Panneau gauche : téléversement + info colonnes ----
            with gr.Column(scale=1):
                file_input = gr.File(label="Téléverser un fichier Excel (.xlsx)", file_types=[".xlsx", ".xls"])
                load_status = gr.Markdown("")
                column_info = gr.Markdown("")

            # ---- Panneau droit : chat + sauvegarde ----
            with gr.Column(scale=2):
                chatbot_ui = gr.Chatbot(
                    value=[{"role": "assistant", "content": ExcelChatbot.WELCOME}],
                    label="Discussion",
                    height=420,
                )
                msg_input = gr.Textbox(
                    placeholder="Tapez votre requête ici… ex : 'chercher 12345 dans Facture'",
                    label="Votre message",
                    lines=1,
                )
                with gr.Row():
                    send_btn = gr.Button("Envoyer", variant="primary")
                    save_btn = gr.Button("💾 Sauvegarder les résultats en Excel")
                save_output = gr.File(label="Télécharger les résultats", visible=False)

        # ---- Callbacks -------------------------------------------------------
        def on_file_upload(file):
            status, cols = bot.load_file(file)
            # Réinitialiser le chat
            welcome = [{"role": "assistant", "content": status}]
            return status, cols, welcome

        def on_send(message, history):
            if not message.strip():
                return "", history
            history = history + [{"role": "user", "content": message}]
            reply = bot.chat(message, history)
            history = history + [{"role": "assistant", "content": reply}]
            return "", history

        def on_save():
            path = bot.save_results()
            if path is None:
                return gr.update(visible=False, value=None)
            return gr.update(visible=True, value=path)

        file_input.change(
            fn=on_file_upload,
            inputs=[file_input],
            outputs=[load_status, column_info, chatbot_ui],
        )
        msg_input.submit(fn=on_send, inputs=[msg_input, chatbot_ui], outputs=[msg_input, chatbot_ui])
        send_btn.click(fn=on_send, inputs=[msg_input, chatbot_ui], outputs=[msg_input, chatbot_ui])
        save_btn.click(fn=on_save, outputs=[save_output])

    return app


# ============================================================================
# POINT D'ENTRÉE
# ============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Chatbot de requêtes Excel")
    parser.add_argument("--port", type=int, default=7860, help="Port pour le serveur")
    parser.add_argument("--share", action="store_true", help="Créer un lien public Gradio")
    args = parser.parse_args()

    app = build_ui()
    app.launch(server_port=args.port, share=args.share)


if __name__ == "__main__":
    main()
