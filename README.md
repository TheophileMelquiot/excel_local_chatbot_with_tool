# AI_chatbot_local_with_tool_to_manipulate_excel

A simple, **100% offline** chatbot that lets non-technical users search Excel files through a natural-language chat interface — no internet connection required.

![Chatbot UI](https://github.com/user-attachments/assets/04fd115e-5b97-4679-9744-b489f46dc7e4)

## Quick start

```bash
# Install dependencies
pip install -r requirements.txt

# Launch the chatbot
python chatbot.py
```

Then open **http://localhost:7860** in your browser.

## How to use

1. **Upload** an Excel file (`.xlsx`) using the left panel.
2. **Type a query** in the chat box, for example:
   - `search 12345` — searches all columns
   - `find John in Name` — searches only the *Name* column
   - `search 100, 200, 300 in Amount` — multiple values at once
3. **Save results** — click the **💾 Save results to Excel** button to download matching rows.

Type `help` in the chat for the full list of commands, or `columns` to see the available columns.

## Project structure

| File | Description |
|---|---|
| `chatbot.py` | Gradio-based chatbot UI + rule-based intent parser |
| `excel_query_engine.py` | Core query engine — type inference, fuzzy column matching, multi-criteria search |
| `requirements.txt` | Python dependencies |
| `tests.py` | Unit & integration tests |

## Running tests

```bash
python -m unittest tests -v
```

## Command-line options

```bash
python chatbot.py --port 8080      # custom port (default: 7860)
python chatbot.py --share           # create a public Gradio link (needs internet)
```

## Timeline

1. ✅ Excel query engine with smart type inference and fuzzy column matching
2. ✅ Offline chatbot interface (Gradio) with save-to-Excel button
3. 🔲 Deploy the application
4. 🔲 Add new tools gradually (merge, transform, pivot, …)
