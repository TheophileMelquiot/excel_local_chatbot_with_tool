"""
LLM Intent Parser — llama-cpp-python (100% hors-ligne, sans Ollama, sans admin)

Prérequis :
    pip install llama-cpp-python   (via .whl local si internet bloqué)

Modèle requis (fichier .gguf à déposer dans models/) :
    Mistral-7B-Instruct-v0.3-Q4_K_M.gguf   (4.1 Go, recommandé)
    Mistral-7B-Instruct-v0.3-Q2_K.gguf     (2.7 Go, si RAM < 6 Go)
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

# Chemin vers le dossier contenant les fichiers .gguf
MODELS_DIR = Path(__file__).parent / "models"

# Nom du fichier modèle à utiliser (modifiable selon ce que tu as téléchargé)
MODEL_FILENAME = "Mistral-7B-Instruct-v0.3-Q4_K_M.gguf"

# Nombre de threads CPU à utiliser (met le nb de cœurs physiques de ton PC)
N_THREADS = os.cpu_count() or 4

# Nombre max de tokens générés (200 suffisent largement pour un JSON court)
MAX_TOKENS = 200

# ── Prompt système ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """Tu es un assistant spécialisé dans l'analyse de requêtes de recherche sur des fichiers Excel.
Ton seul rôle est d'extraire les paramètres de recherche depuis un message en langage naturel.

RÈGLES STRICTES :
- Retourne UNIQUEMENT un objet JSON valide, sans aucun texte avant ou après.
- N'invente pas de valeurs : utilise exactement ce que l'utilisateur a écrit.
- Si le message n'est pas une requête de recherche, retourne exactement : null

FORMATS DE RÉPONSE :

1. Recherche dans UNE colonne (une ou plusieurs valeurs) :
{"values": ["valeur1", "valeur2"], "column_hint": "NomColonne"}

Exemples :
"chercher 12345 dans Facture"
→ {"values": ["12345"], "column_hint": "Facture"}

"trouver Jean ou Marie dans Nom"
→ {"values": ["Jean", "Marie"], "column_hint": "Nom"}

2. Recherche dans PLUSIEURS colonnes (logique ET) :
{"multi_criteria": [{"value": "val1", "column": "Col1"}, {"value": "val2", "column": "Col2"}]}

Exemples :
"recherche 723 dans id avec dupont dans nom"
→ {"multi_criteria": [{"value": "723", "column": "id"}, {"value": "dupont", "column": "nom"}]}

3. Recherche sans colonne précisée :
{"values": ["valeur"], "column_hint": null}

Exemple :
"chercher DUPONT"
→ {"values": ["DUPONT"], "column_hint": null}

4. Pas une requête de recherche → retourne exactement : null

Exemples : "aide", "colonnes", "bonjour"
→ null"""


# ── Chargement du modèle (singleton) ──────────────────────────────────────────

_llm_instance = None   # instance unique, chargée une seule fois


def _get_llm():
    """Charge le modèle GGUF en mémoire (une seule fois, puis réutilisé)."""
    global _llm_instance

    if _llm_instance is not None:
        return _llm_instance

    try:
        from llama_cpp import Llama  # noqa: import inside function pour éviter l'erreur si absent
    except ImportError:
        logger.error(
            "llama-cpp-python n'est pas installé.\n"
            "Installe-le avec : pip install llama-cpp-python\n"
            "Ou depuis le .whl local : pip install --no-index --find-links=./packages/ llama-cpp-python"
        )
        return None

    model_path = MODELS_DIR / MODEL_FILENAME

    if not model_path.exists():
        logger.error(
            "Modèle introuvable : %s\n"
            "Télécharge-le depuis HuggingFace et dépose-le dans le dossier models/",
            model_path,
        )
        return None

    logger.info("⏳ Chargement du modèle %s (première utilisation, ~5-15s)...", MODEL_FILENAME)
    print(f"⏳ Chargement du modèle LLM en mémoire... (une seule fois au démarrage)")

    try:
        _llm_instance = Llama(
            model_path  = str(model_path),
            n_ctx       = 1024,      # fenêtre de contexte (1024 suffit pour nos prompts courts)
            n_threads   = N_THREADS, # threads CPU
            n_gpu_layers= 0,         # 0 = CPU uniquement (pas besoin de GPU)
            verbose     = False,     # pas de logs internes de llama.cpp
        )
        print(f"✅ Modèle chargé ({MODEL_FILENAME})")
        return _llm_instance

    except Exception as exc:
        logger.error("Erreur lors du chargement du modèle : %s", exc)
        return None


# ── Parser principal ───────────────────────────────────────────────────────────

class OllamaIntentParser:
    """
    Analyse les messages via un LLM local (llama-cpp-python).
    Interface identique à IntentParser.parse() — substitution transparente.

    Le nom 'OllamaIntentParser' est conservé pour ne pas modifier chatbot.py.
    """

    def __init__(self, model: str = MODEL_FILENAME, **kwargs):
        # 'model' ignoré ici (on utilise MODEL_FILENAME), gardé pour compatibilité
        self._available: Optional[bool] = None

    # ── disponibilité ─────────────────────────────────────────────────────────

    def is_available(self) -> bool:
        """Vérifie que llama-cpp-python est installé ET que le fichier modèle existe."""
        if self._available is not None:
            return self._available

        # Vérifier llama_cpp
        try:
            import llama_cpp  # noqa
        except ImportError:
            logger.warning("llama-cpp-python non installé → mode regex uniquement")
            self._available = False
            return False

        # Vérifier le fichier modèle
        model_path = MODELS_DIR / MODEL_FILENAME
        if not model_path.exists():
            logger.warning("Fichier modèle introuvable : %s", model_path)
            self._available = False
            return False

        self._available = True
        return True

    # ── parsing ───────────────────────────────────────────────────────────────

    def parse(self, message: str) -> Optional[dict]:
        """
        Envoie le message au LLM local et retourne un dict structuré ou None.

        Retourne :
            {"values": [...], "column_hint": str | None}
            {"multi_criteria": [{"value": ..., "column": ...}, ...]}
            None  → pas une requête ou LLM indisponible
        """
        if not self.is_available():
            return None

        llm = _get_llm()
        if llm is None:
            return None

        # Format de prompt Mistral Instruct : <s>[INST] ... [/INST]
        prompt = f"<s>[INST] {SYSTEM_PROMPT}\n\nMessage utilisateur : {message} [/INST]"

        try:
            output = llm(
                prompt,
                max_tokens  = MAX_TOKENS,
                temperature = 0.0,   # zéro créativité → réponses stables
                stop        = ["\n\n", "</s>"],  # arrêter après le JSON
                echo        = False,
            )
        except Exception as exc:
            logger.warning("Erreur lors de l'inférence LLM : %s", exc)
            return None

        raw = output["choices"][0]["text"].strip()

        # Parfois le modèle encadre le JSON avec ```json … ```
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()

        # Cas "null" → pas une requête
        if raw.lower() in ("null", "none", ""):
            return None

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("JSON invalide reçu du LLM : %r", raw)
            return None

        if parsed is None:
            return None

        return self._validate(parsed)

    # ── validation ────────────────────────────────────────────────────────────

    @staticmethod
    def _validate(data: Any) -> Optional[dict]:
        """Vérifie que le JSON retourné respecte le format attendu."""
        if not isinstance(data, dict):
            return None

        # Format multi-colonnes
        if "multi_criteria" in data:
            criteria = data["multi_criteria"]
            if (
                isinstance(criteria, list)
                and len(criteria) >= 2
                and all(
                    isinstance(c, dict) and "value" in c and "column" in c
                    for c in criteria
                )
            ):
                return data
            return None

        # Format colonne unique
        if "values" in data:
            values = data["values"]
            if not isinstance(values, list) or len(values) == 0:
                return None
            # Normaliser les types (int/float si possible)
            normalized = []
            for v in values:
                s = str(v).strip()
                try:
                    normalized.append(int(s))
                except ValueError:
                    try:
                        normalized.append(float(s))
                    except ValueError:
                        normalized.append(s)
            data["values"] = normalized
            return data

        return None