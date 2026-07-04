from __future__ import annotations

import tempfile
import unittest
from datetime import date
from pathlib import Path

from tweet_extractor.apify_client import (
    ApifyError,
    iter_date_windows,
    load_apify_token,
    parse_date,
    split_window,
)


class ApifyClientHelpersTest(unittest.TestCase):
    def test_missing_token_returns_empty_string(self) -> None:
        self.assertEqual(load_apify_token(None, None), "")

    def test_loads_token_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            token_file = Path(tmp) / "token"
            token_file.write_text("abc\n", encoding="utf-8")
            self.assertEqual(load_apify_token(None, token_file), "abc")

    def test_iter_date_windows(self) -> None:
        windows = iter_date_windows(date(2024, 1, 1), date(2024, 1, 5), 2)
        self.assertEqual(
            windows,
            [
                (date(2024, 1, 1), date(2024, 1, 3)),
                (date(2024, 1, 3), date(2024, 1, 5)),
            ],
        )

    def test_split_window(self) -> None:
        self.assertEqual(
            split_window(date(2024, 1, 1), date(2024, 1, 5)),
            ((date(2024, 1, 1), date(2024, 1, 3)), (date(2024, 1, 3), date(2024, 1, 5))),
        )
        self.assertIsNone(split_window(date(2024, 1, 1), date(2024, 1, 2)))

    def test_parse_date(self) -> None:
        self.assertEqual(parse_date("2024-01-01"), date(2024, 1, 1))


if __name__ == "__main__":
    unittest.main()

