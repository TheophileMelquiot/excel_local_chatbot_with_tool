"""
Tests for the Excel Query Chatbot and Intent Parser.
"""

import os
import tempfile
import unittest

from openpyxl import Workbook

from chatbot import ExcelChatbot, IntentParser
from excel_query_engine import (
    ColumnMatcher,
    ColumnProfiler,
    DataNormalizer,
    DataType,
    ExcelQueryEngine,
    LogicalOperator,
    SearchEngine,
)


# ============================================================================
# Helpers
# ============================================================================

def _create_test_excel(filepath: str) -> str:
    """Create a small Excel file for testing and return its path."""
    wb = Workbook()
    ws = wb.active
    ws.title = "TestData"

    ws.append(["Name", "Age", "City", "Invoice"])
    ws.append(["Alice", 30, "Paris", 1001])
    ws.append(["Bob", 25, "London", 1002])
    ws.append(["Charlie", 35, "Paris", 1003])
    ws.append(["Alice", 28, "Berlin", 1004])
    ws.append(["David", 40, "London", 1005])

    wb.save(filepath)
    return filepath


# ============================================================================
# Intent Parser tests
# ============================================================================

class TestIntentParser(unittest.TestCase):
    """Unit tests for IntentParser."""

    def test_simple_search(self):
        result = IntentParser.parse("search Alice")
        self.assertIsNotNone(result)
        self.assertEqual(result["values"], "Alice")
        self.assertIsNone(result["column_hint"])

    def test_search_with_column(self):
        result = IntentParser.parse("find Alice in Name")
        self.assertIsNotNone(result)
        self.assertEqual(result["values"], "Alice")
        self.assertEqual(result["column_hint"], "Name")

    def test_search_number(self):
        result = IntentParser.parse("search 12345")
        self.assertIsNotNone(result)
        self.assertEqual(result["values"], 12345)

    def test_search_multiple_values(self):
        result = IntentParser.parse("search Alice, Bob in Name")
        self.assertIsNotNone(result)
        self.assertEqual(result["values"], ["Alice", "Bob"])
        self.assertEqual(result["column_hint"], "Name")

    def test_search_with_and(self):
        result = IntentParser.parse("find 100 and 200 in Amount")
        self.assertIsNotNone(result)
        self.assertEqual(result["values"], [100, 200])
        self.assertEqual(result["column_hint"], "Amount")

    def test_search_with_or(self):
        result = IntentParser.parse("look for Alice or Bob in Name")
        self.assertIsNotNone(result)
        self.assertEqual(result["values"], ["Alice", "Bob"])
        self.assertEqual(result["column_hint"], "Name")

    def test_various_verbs(self):
        for verb in ["search", "find", "look for", "query", "get", "show", "filter"]:
            result = IntentParser.parse(f"{verb} test")
            self.assertIsNotNone(result, f"Verb '{verb}' was not recognized")

    def test_non_query_message(self):
        self.assertIsNone(IntentParser.parse("hello"))
        self.assertIsNone(IntentParser.parse("what is the weather?"))
        self.assertIsNone(IntentParser.parse(""))

    def test_column_keyword_in(self):
        result = IntentParser.parse("search 1001 in Invoice")
        self.assertIsNotNone(result)
        self.assertEqual(result["values"], 1001)
        self.assertEqual(result["column_hint"], "Invoice")

    def test_float_value(self):
        result = IntentParser.parse("search 3.14")
        self.assertIsNotNone(result)
        self.assertEqual(result["values"], 3.14)


# ============================================================================
# ExcelQueryEngine tests (with fallback header detection)
# ============================================================================

class TestExcelQueryEngine(unittest.TestCase):
    """Integration tests for ExcelQueryEngine with fallback header detection."""

    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.mkdtemp()
        cls.test_file = os.path.join(cls.tmpdir, "test_data.xlsx")
        _create_test_excel(cls.test_file)
        cls.engine = ExcelQueryEngine(cls.test_file)

    def test_headers_loaded(self):
        self.assertEqual(self.engine.headers, ["Name", "Age", "City", "Invoice"])

    def test_row_count(self):
        self.assertEqual(len(self.engine.rows), 5)

    def test_search_exact(self):
        result = self.engine.search(
            criteria=[("Alice", "Name")],
            mode=LogicalOperator.OR,
            include_partial=False,
        )
        self.assertEqual(result.total_matches, 2)

    def test_search_number(self):
        result = self.engine.search(
            criteria=[(1001, "Invoice")],
            mode=LogicalOperator.OR,
            include_partial=False,
        )
        self.assertEqual(result.total_matches, 1)

    def test_search_partial(self):
        result = self.engine.search(
            criteria=[("Ali", "Name")],
            mode=LogicalOperator.OR,
            include_partial=True,
        )
        # "Ali" is a partial match for "Alice"
        self.assertGreaterEqual(result.total_matches, 2)

    def test_search_no_match(self):
        result = self.engine.search(
            criteria=[("ZZZ_NoMatch", "Name")],
            mode=LogicalOperator.OR,
            include_partial=False,
        )
        self.assertEqual(result.total_matches, 0)

    def test_export(self):
        out_path = os.path.join(self.tmpdir, "export_test.xlsx")
        self.engine.search(
            criteria=[("Alice", "Name")],
            mode=LogicalOperator.OR,
            include_partial=False,
            export_filepath=out_path,
        )
        self.assertTrue(os.path.exists(out_path))


# ============================================================================
# Chatbot tests
# ============================================================================

class TestExcelChatbot(unittest.TestCase):
    """Integration tests for ExcelChatbot."""

    @classmethod
    def setUpClass(cls):
        cls.tmpdir = tempfile.mkdtemp()
        cls.test_file = os.path.join(cls.tmpdir, "test_data.xlsx")
        _create_test_excel(cls.test_file)

    def _make_bot(self):
        bot = ExcelChatbot()

        class FakeFile:
            def __init__(self, path):
                self.name = path

        bot.load_file(FakeFile(self.test_file))
        return bot

    def test_help_command(self):
        bot = self._make_bot()
        reply = bot.chat("help", [])
        self.assertIn("Available commands", reply)

    def test_columns_command(self):
        bot = self._make_bot()
        reply = bot.chat("columns", [])
        self.assertIn("Name", reply)
        self.assertIn("Age", reply)

    def test_search_query(self):
        bot = self._make_bot()
        reply = bot.chat("search Alice in Name", [])
        self.assertIn("2", reply)  # 2 rows match
        self.assertEqual(len(bot.last_result_rows), 2)

    def test_search_all_columns(self):
        bot = self._make_bot()
        reply = bot.chat("search Paris", [])
        self.assertIn("row", reply.lower())
        self.assertGreaterEqual(len(bot.last_result_rows), 2)

    def test_unknown_message(self):
        bot = self._make_bot()
        reply = bot.chat("hello world", [])
        self.assertIn("didn't understand", reply)

    def test_no_file_loaded(self):
        bot = ExcelChatbot()
        reply = bot.chat("search Alice", [])
        self.assertIn("upload", reply.lower())

    def test_save_results(self):
        bot = self._make_bot()
        bot.chat("search Alice in Name", [])
        path = bot.save_results()
        self.assertIsNotNone(path)
        self.assertTrue(os.path.exists(path))

    def test_save_no_results(self):
        bot = self._make_bot()
        path = bot.save_results()
        self.assertIsNone(path)


# ============================================================================
# Data components unit tests
# ============================================================================

class TestDataNormalizer(unittest.TestCase):
    def test_normalize_cell(self):
        self.assertEqual(DataNormalizer.normalize_cell(None), "")
        self.assertEqual(DataNormalizer.normalize_cell("  hello  "), "hello")
        self.assertEqual(DataNormalizer.normalize_cell(42), "42")

    def test_try_parse_number(self):
        self.assertEqual(DataNormalizer.try_parse_number("42"), 42.0)
        self.assertEqual(DataNormalizer.try_parse_number("3.14"), 3.14)
        self.assertIsNone(DataNormalizer.try_parse_number("abc"))

    def test_infer_type(self):
        self.assertEqual(DataNormalizer.infer_type("42"), DataType.NUMERIC)
        self.assertEqual(DataNormalizer.infer_type("hello"), DataType.TEXT)
        self.assertEqual(DataNormalizer.infer_type(""), DataType.EMPTY)


class TestColumnMatcher(unittest.TestCase):
    def test_exact_match(self):
        name, idx, conf = ColumnMatcher.find_column("Name", ["Name", "Age", "City"])
        self.assertEqual(name, "Name")
        self.assertEqual(idx, 0)
        self.assertEqual(conf, 1.0)

    def test_case_insensitive(self):
        name, idx, conf = ColumnMatcher.find_column("name", ["Name", "Age", "City"])
        self.assertEqual(name, "Name")
        self.assertEqual(conf, 1.0)

    def test_no_match(self):
        name, idx, conf = ColumnMatcher.find_column("zzzzz", ["Name", "Age", "City"])
        self.assertIsNone(name)
        self.assertEqual(conf, 0.0)

    def test_fuzzy_match(self):
        name, idx, conf = ColumnMatcher.find_column("Nme", ["Name", "Age", "City"])
        self.assertEqual(name, "Name")
        self.assertGreater(conf, 0.5)


class TestSearchEngine(unittest.TestCase):
    def test_infer_numeric(self):
        self.assertEqual(SearchEngine.infer_search_term_type(42), DataType.NUMERIC)
        self.assertEqual(SearchEngine.infer_search_term_type("42"), DataType.NUMERIC)

    def test_infer_text(self):
        self.assertEqual(SearchEngine.infer_search_term_type("hello"), DataType.TEXT)

    def test_exact_match(self):
        self.assertTrue(SearchEngine.exact_match("42", "42", DataType.NUMERIC))
        self.assertFalse(SearchEngine.exact_match("42", "43", DataType.NUMERIC))

    def test_partial_match(self):
        self.assertTrue(SearchEngine.partial_match("Ali", "Alice", DataType.TEXT))
        self.assertFalse(SearchEngine.partial_match("Alice", "Alice", DataType.TEXT))


if __name__ == "__main__":
    unittest.main()
