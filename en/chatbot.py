"""
Excel Chatbot — Offline chatbot interface for the Excel Query Engine.

Uses Gradio for the UI and a rule-based intent parser so the application
works entirely offline without any internet connection or external LLM API.

Constants:
    MAX_PREVIEW_COLUMNS  – max columns shown in a chat preview table
    MAX_CELL_DISPLAY_LEN – max characters per cell in a chat preview table

Usage:
    python chatbot.py                       # default port 7860
    python chatbot.py --port 8080           # custom port
    python chatbot.py --share               # create a public link (needs internet)
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
# INTENT PARSER — rule-based, fully offline
# ============================================================================

class IntentParser:
    """Parse natural-language chat messages into structured query parameters.

    Designed to work without any ML model so the chatbot runs 100% offline.
    Handles common patterns such as:
        "search 12345"
        "find John in column Name"
        "look for 12345 in Invoice Number"
        "search 100 and 200 in Amount"
        "search Alice or Bob in Name"
    """

    # Verbs that signal a search intent
    _SEARCH_VERBS = r"(?:search|find|look\s*(?:for|up)?|query|get|show|where|filter)"

    # Pattern: <verb> <values> [in [column] <col_hint>]
    _QUERY_RE = re.compile(
        rf"^\s*{_SEARCH_VERBS}"
        r"\s+(?P<values>.+?)"
        r"(?:\s+(?:in|from|of|under|at|on)\s+(?:column\s+)?(?P<column>.+?))?"
        r"\s*$",
        re.IGNORECASE,
    )

    # Separators between multiple values
    _VALUE_SEP = re.compile(r"\s*(?:,|\band\b|\bor\b)\s*", re.IGNORECASE)

    @classmethod
    def parse(cls, message: str) -> Optional[Dict[str, Any]]:
        """Return a dict ``{values, column_hint}`` or *None* if the message
        does not look like a query."""
        m = cls._QUERY_RE.match(message.strip())
        if not m:
            return None

        raw_values = m.group("values").strip().strip("\"'")
        column_hint = m.group("column")
        if column_hint:
            column_hint = column_hint.strip().strip("\"'")

        # Split on commas / "and" / "or"
        parts = cls._VALUE_SEP.split(raw_values)
        values: List[Any] = []
        for p in parts:
            p = p.strip().strip("\"'")
            if not p:
                continue
            # Try to interpret as a number
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
# CHATBOT APPLICATION
# ============================================================================

class ExcelChatbot:
    """Gradio-based chatbot wrapping the Excel Query Engine."""

    WELCOME = (
        "👋 Welcome to the **Excel Query Chatbot**!\n\n"
        "1. Upload an Excel file using the panel on the left.\n"
        "2. Ask me to search for values, for example:\n"
        "   - `search 12345`\n"
        "   - `find John in Name`\n"
        "   - `search 100 and 200 in Amount`\n"
        "3. Click **Save results to Excel** whenever you want to export.\n\n"
        "Type **help** for more details or **columns** to see available columns."
    )

    HELP_TEXT = (
        "📖 **Available commands**\n\n"
        "| Command | Example |\n"
        "|---|---|\n"
        "| Search for a value | `search 12345` |\n"
        "| Search in a specific column | `find John in Name` |\n"
        "| Search multiple values | `search 100, 200, 300 in Amount` |\n"
        "| List columns | `columns` |\n"
        "| Show help | `help` |\n\n"
        "You can also use verbs like *find*, *look for*, *query*, *get*, *show*, *filter*."
    )

    def __init__(self) -> None:
        self.engine: Optional[ExcelQueryEngine] = None
        self.last_result = None
        self.last_result_rows: List[Dict[str, str]] = []

    # ------------------------------------------------------------------ utils
    def _format_column_info(self) -> str:
        """Return a markdown table of column profiles."""
        if self.engine is None:
            return "No file loaded."
        lines = ["| # | Column | Type | Non-empty |", "|---|--------|------|-----------|"]
        for p in self.engine.profiles:
            lines.append(
                f"| {p.index + 1} | {p.name} | {p.detected_type.value} | {p.non_empty_count} |"
            )
        return "\n".join(lines)

    def _rows_to_display(self, rows: List[Dict[str, str]], limit: int = 50) -> List[List[str]]:
        """Convert row dicts to a list-of-lists for Gradio Dataframe."""
        if not rows:
            return []
        headers = list(rows[0].keys())
        data = []
        for row in rows[:limit]:
            data.append([row.get(h, "") for h in headers])
        return data

    # -------------------------------------------------------------- handlers
    def load_file(self, file) -> Tuple[str, str]:
        """Handle file upload — returns (status_message, column_info_md)."""
        if file is None:
            return "⚠️ No file selected.", ""
        filepath = file.name if hasattr(file, "name") else str(file)
        try:
            self.engine = ExcelQueryEngine(filepath)
            self.last_result = None
            self.last_result_rows = []
            status = (
                f"✅ Loaded **{os.path.basename(filepath)}** — "
                f"{len(self.engine.rows)} rows, {len(self.engine.headers)} columns."
            )
            return status, self._format_column_info()
        except Exception as exc:
            self.engine = None
            return f"❌ Error loading file: {exc}", ""

    def chat(self, message: str, history: list) -> str:
        """Process a chat message and return a response string."""
        msg = message.strip()
        if not msg:
            return "Please type a message."

        # --- meta commands ---------------------------------------------------
        if msg.lower() in ("help", "?"):
            return self.HELP_TEXT

        if msg.lower() in ("columns", "cols", "headers"):
            return self._format_column_info()

        # --- queries ----------------------------------------------------------
        if self.engine is None:
            return "⚠️ Please upload an Excel file first (use the panel on the left)."

        parsed = IntentParser.parse(msg)
        if parsed is None:
            return (
                "🤔 I didn't understand that query. Try something like:\n"
                "- `search 12345`\n"
                "- `find John in Name`\n\n"
                "Type **help** for the full list of commands."
            )

        values = parsed["values"]
        column_hint = parsed["column_hint"]

        # If no column hint, search across all columns
        if column_hint is None:
            return self._search_all_columns(values)

        # Search in the specified column
        return self._search_specific_column(values, column_hint)

    def _search_all_columns(self, values: Any) -> str:
        """Search every column for *values* and return the combined results."""
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
                    # Deduplicate
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
            return f"No results found for **{values}** in any column."

        preview = self._format_rows_preview(all_matches)
        return (
            f"🔍 Found **{len(all_matches)}** row(s) matching **{values}** "
            f"(searched all columns).\n\n{preview}"
        )

    def _search_specific_column(self, values: Any, column_hint: str) -> str:
        """Search a specific column for *values*."""
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
            return f"No results found for **{values}** in column **{column_hint}**."

        preview = self._format_rows_preview(result.matched_rows)
        return (
            f"🔍 Found **{result.total_matches}** row(s) matching **{values}** "
            f"in column **{column_hint}**.\n\n{preview}"
        )

    def _format_rows_preview(self, rows: List[Dict[str, str]], limit: int = 10) -> str:
        """Return a markdown table preview of matched rows."""
        if not rows:
            return ""
        headers = list(rows[0].keys())
        # Truncate column display if too many columns
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
            lines.append(f"\n*… and {len(rows) - limit} more rows. Click **Save results to Excel** to get all rows.*")

        return "\n".join(lines)

    def save_results(self) -> Optional[str]:
        """Export the last query results to an Excel file and return the path."""
        if not self.last_result_rows:
            return None
        if self.engine is None:
            return None

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = os.path.join(tempfile.gettempdir(), f"query_results_{timestamp}.xlsx")
        ExcelExporter.export_results(
            out_path,
            self.engine.headers,
            self.last_result_rows,
            query_summary=f"Exported {len(self.last_result_rows)} rows on {timestamp}",
        )
        return out_path


# ============================================================================
# GRADIO UI
# ============================================================================

def build_ui() -> gr.Blocks:
    """Construct and return the Gradio Blocks application."""
    bot = ExcelChatbot()

    with gr.Blocks(title="Excel Query Chatbot") as app:
        gr.Markdown("# 📊 Excel Query Chatbot\nAsk questions about your Excel data — **100% offline**.")

        with gr.Row():
            # ---- Left panel: file upload + column info ----
            with gr.Column(scale=1):
                file_input = gr.File(label="Upload Excel file (.xlsx)", file_types=[".xlsx", ".xls"])
                load_status = gr.Markdown("")
                column_info = gr.Markdown("")

            # ---- Right panel: chat + save ----
            with gr.Column(scale=2):
                chatbot_ui = gr.Chatbot(
                    value=[{"role": "assistant", "content": ExcelChatbot.WELCOME}],
                    label="Chat",
                    height=420,
                )
                msg_input = gr.Textbox(
                    placeholder="Type your query here… e.g. 'search 12345 in Invoice'",
                    label="Your message",
                    lines=1,
                )
                with gr.Row():
                    send_btn = gr.Button("Send", variant="primary")
                    save_btn = gr.Button("💾 Save results to Excel")
                save_output = gr.File(label="Download results", visible=False)

        # ---- Callbacks -------------------------------------------------------
        def on_file_upload(file):
            status, cols = bot.load_file(file)
            # Reset chat
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
# ENTRY POINT
# ============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Excel Query Chatbot")
    parser.add_argument("--port", type=int, default=7860, help="Port to serve the app on")
    parser.add_argument("--share", action="store_true", help="Create a public Gradio link")
    args = parser.parse_args()

    app = build_ui()
    app.launch(server_port=args.port, share=args.share)


if __name__ == "__main__":
    main()
