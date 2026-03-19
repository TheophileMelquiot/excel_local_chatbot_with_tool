"""
LLM Intent Parser — llama-cpp-python (100% hors-ligne, sans Ollama, sans admin)

Prérequis :
    pip install llama-cpp-python   (via .whl local si internet bloqué)

Modèle requis (fichier .gguf à déposer dans models/) :
    Phi-3.5-mini-instruct-Q4_K_M.gguf   (2.2 Go, recommandé)
    Phi-3.5-mini-instruct-Q2_K.gguf     (1.4 Go, si RAM < 4 Go)
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
import os
import threading
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

# Chemin vers le dossier contenant les fichiers .gguf
MODELS_DIR = Path(__file__).parent / "models"

# Nom du fichier modèle à utiliser (modifiable selon ce que tu as téléchargé)
MODEL_FILENAME = "Phi-3.5-mini-instruct-Q4_K_M.gguf"

# Nombre de threads CPU à utiliser (cœurs physiques, éviter l'hyperthreading)
N_THREADS = max((os.cpu_count() or 4) // 2, 1)

# Nombre max de tokens générés (200 suffisent largement pour un JSON court)
MAX_TOKENS = 200

# Timeout en secondes pour l'inférence LLM — fallback regex si dépassé
INFERENCE_TIMEOUT = 20

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
"chercher 12345 dans Facture" → {"values": ["12345"], "column_hint": "Facture"}
"trouver Jean ou Marie dans Nom" → {"values": ["Jean", "Marie"], "column_hint": "Nom"}

2. Recherche dans PLUSIEURS colonnes (logique ET) :
{"multi_criteria": [{"value": "val1", "column": "Col1"}, {"value": "val2", "column": "Col2"}]}

Exemples :
"recherche 723 dans id avec dupont dans nom"
→ {"multi_criteria": [{"value": "723", "column": "id"}, {"value": "dupont", "column": "nom"}]}

3. Recherche avec comparaison numérique (pour usage futur) :
{"comparisons": [{"operator": ">", "value": 500, "column": "Montant"}]}

Opérateurs supportés : >, <, >=, <=, =, !=
Exemples :
"lignes où le montant dépasse 500" → {"comparisons": [{"operator": ">", "value": 500, "column": "Montant"}]}
"montant entre 100 et 500" → {"comparisons": [{"operator": ">=", "value": 100, "column": "Montant"}, {"operator": "<=", "value": 500, "column": "Montant"}]}

4. Recherche sans colonne précisée :
{"values": ["valeur"], "column_hint": null}

Exemple :
"chercher DUPONT" → {"values": ["DUPONT"], "column_hint": null}

5. Pas une requête de recherche → retourne exactement : null
Exemples : "aide", "colonnes", "bonjour" → null"""


# ── Chargement du modèle (background thread) ──────────────────────────────────

_llm_instance = None
_llm_loading_event = threading.Event()   # set() quand le modèle est prêt (ou a échoué)
_llm_load_error = False                  # True si le chargement a échoué
_llm_loading_thread = None


def _load_llm_background():
    """Charge le modèle dans un thread séparé (appelé au démarrage de l'app)."""
    global _llm_instance, _llm_load_error
    try:
        from llama_cpp import Llama
        model_path = MODELS_DIR / MODEL_FILENAME
        if not model_path.exists():
            logger.error("Modèle introuvable : %s", model_path)
            _llm_load_error = True
            _llm_loading_event.set()
            return
        logger.info("⏳ Chargement du modèle %s en arrière-plan...", MODEL_FILENAME)
        print(f"⏳ Chargement du LLM en arrière-plan ({MODEL_FILENAME})...")
        _llm_instance = Llama(
            model_path=str(model_path),
            n_ctx=1024,
            n_threads=N_THREADS,
            n_gpu_layers=0,
            verbose=False,
        )
        print(f"✅ Modèle LLM chargé ({MODEL_FILENAME})")
    except ImportError:
        logger.error("llama-cpp-python non installé")
        _llm_load_error = True
    except Exception as exc:
        logger.error("Erreur chargement LLM : %s", exc)
        _llm_load_error = True
    finally:
        _llm_loading_event.set()  # signaler dans tous les cas (succès ou échec)


def start_llm_preload():
    """
    Lance le chargement du modèle en arrière-plan.
    À appeler une seule fois au démarrage de l'application.
    Idempotent : plusieurs appels n'ont aucun effet.
    """
    global _llm_loading_thread
    if _llm_loading_thread is not None:
        return  # déjà lancé
    try:
        from llama_cpp import Llama  # noqa: F401 — vérifier que la lib est présente
        model_path = MODELS_DIR / MODEL_FILENAME
        if not model_path.exists():
            logger.warning("Modèle introuvable au démarrage, préchargement ignoré : %s", model_path)
            return
    except ImportError:
        logger.warning("llama-cpp-python absent, préchargement ignoré")
        return

    _llm_loading_thread = threading.Thread(target=_load_llm_background, daemon=True)
    _llm_loading_thread.start()


def is_llm_ready() -> bool:
    """Retourne True si le modèle est chargé et prêt à l'emploi."""
    return _llm_loading_event.is_set() and _llm_instance is not None


def _get_llm():
    """
    Retourne l'instance LLM si elle est prête, sinon None.
    Ne bloque PAS — l'appelant doit gérer le fallback.
    """
    if _llm_loading_event.is_set():
        return _llm_instance  # peut être None si chargement échoué
    return None  # pas encore prêt → fallback regex


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
        """
        Retourne True si llama-cpp-python est installé ET le fichier modèle existe.
        Ne vérifie PAS si le modèle est déjà chargé en mémoire (voir is_llm_ready()).
        """
        if self._available is not None:
            return self._available
        try:
            import llama_cpp  # noqa
        except ImportError:
            self._available = False
            return False
        model_path = MODELS_DIR / MODEL_FILENAME
        if not model_path.exists():
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
            {"comparisons": [{"operator": ..., "value": ..., "column": ...}, ...]}
            None  → pas une requête ou LLM indisponible
        """
        if not self.is_available():
            return None

        llm = _get_llm()
        if llm is None:
            return None  # modèle pas encore prêt ou échec → fallback regex immédiat

        # Format de prompt Phi-3.5-mini instruct : <|user|>...<|end|><|assistant|>
        prompt = f"<|user|>\n{SYSTEM_PROMPT}\n\nMessage utilisateur : {message}<|end|>\n<|assistant|>"

        def _infer():
            return llm(
                prompt,
                max_tokens=MAX_TOKENS,
                temperature=0.0,
                stop=["<|end|>", "<|user|>", "\n\n"],
                echo=False,
            )

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(_infer)
                output = future.result(timeout=INFERENCE_TIMEOUT)
        except concurrent.futures.TimeoutError:
            logger.warning("Timeout LLM (%ds) → fallback regex", INFERENCE_TIMEOUT)
            return None
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

        # Format comparaisons numériques (pour usage futur)
        if "comparisons" in data:
            comparisons = data["comparisons"]
            valid_ops = {">", "<", ">=", "<=", "=", "!="}
            if (
                isinstance(comparisons, list)
                and len(comparisons) >= 1
                and all(
                    isinstance(c, dict)
                    and "operator" in c
                    and "value" in c
                    and "column" in c
                    and c["operator"] in valid_ops
                    for c in comparisons
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