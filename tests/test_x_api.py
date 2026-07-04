from __future__ import annotations

import unittest
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError

from tweet_extractor.x_api import XApiClient, XApiError


class XApiClientTest(unittest.TestCase):
    def test_missing_token_fails_fast(self) -> None:
        with self.assertRaises(XApiError):
            XApiClient("")

    def test_access_denied_error_mentions_endpoint_access(self) -> None:
        client = XApiClient("token", sleep_on_rate_limit=False)
        error = HTTPError(
            "https://api.x.com/2/tweets/search/all",
            403,
            "Forbidden",
            {},
            None,
        )
        error.read = MagicMock(return_value=b'{"title":"Forbidden"}')

        with patch("tweet_extractor.x_api.urlopen", side_effect=error):
            with self.assertRaisesRegex(XApiError, "access"):
                client.full_archive_page("ChrisWillx")


if __name__ == "__main__":
    unittest.main()

