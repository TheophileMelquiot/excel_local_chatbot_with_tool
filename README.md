# AI_chatbot_local_with_tool_to_manipulate_excel

A simple, **100% offline** chatbot that lets non-technical users search Excel files through a natural-language chat interface — no internet connection required.

Available in **English** and **French** (Français).

![Chatbot UI](https://github.com/user-attachments/assets/04fd115e-5b97-4679-9744-b489f46dc7e4)

## Quick start

### English version

```bash
pip install -r requirements.txt
python en/chatbot.py
```

### Version française

```bash
pip install -r requirements.txt
python fr/chatbot.py
```

Then open **http://localhost:7860** in your browser.

## How to use / Comment utiliser

### English

1. **Upload** an Excel file (`.xlsx`) using the left panel.
2. **Type a query** in the chat box, for example:
   - `search 12345` — searches all columns
   - `find John in Name` — searches only the *Name* column
   - `search 100, 200, 300 in Amount` — multiple values at once
3. **Save results** — click the **💾 Save results to Excel** button to download matching rows.

Type `help` in the chat for the full list of commands, or `columns` to see the available columns.

### Français

1. **Téléversez** un fichier Excel (`.xlsx`) à l'aide du panneau de gauche.
2. **Tapez une requête** dans la zone de chat, par exemple :
   - `chercher 12345` — recherche dans toutes les colonnes
   - `trouver Jean dans Nom` — recherche uniquement dans la colonne *Nom*
   - `chercher 100, 200, 300 dans Montant` — plusieurs valeurs à la fois
3. **Sauvegarder** — cliquez sur **💾 Sauvegarder les résultats en Excel** pour télécharger les lignes correspondantes.

Tapez `aide` dans le chat pour la liste complète des commandes, ou `colonnes` pour voir les colonnes disponibles.

## Project structure

| Path | Description |
|---|---|
| `en/` | **English version** of the chatbot |
| `en/chatbot.py` | Gradio-based chatbot UI + rule-based intent parser (English) |
| `en/excel_query_engine.py` | Core query engine — type inference, fuzzy column matching, multi-criteria search |
| `fr/` | **Version française** du chatbot |
| `fr/chatbot.py` | Interface chatbot Gradio + analyseur d'intentions (Français) |
| `fr/excel_query_engine.py` | Moteur de requêtes — inférence de type, correspondance floue, recherche multi-critères |
| `requirements.txt` | Python dependencies / Dépendances Python |
| `tests.py` | Unit & integration tests |

## Running tests

```bash
python -m unittest tests -v
```

## Command-line options

```bash
# English
python en/chatbot.py --port 8080      # custom port (default: 7860)
python en/chatbot.py --share           # create a public Gradio link (needs internet)

# Français
python fr/chatbot.py --port 8080      # port personnalisé (défaut : 7860)
python fr/chatbot.py --share           # créer un lien public Gradio (nécessite internet)
```

## Timeline

1. ✅ Excel query engine with smart type inference and fuzzy column matching
2. ✅ Offline chatbot interface (Gradio) with save-to-Excel button
3. ✅ English and French versions in separate folders
4. 🔲 Deploy the application
5. 🔲 Add new tools gradually (merge, transform, pivot, …)
