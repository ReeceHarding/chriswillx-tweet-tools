from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .models import EXPANSIONS, MEDIA_FIELDS, POLL_FIELDS, TWEET_FIELDS, USER_FIELDS


API_BASE = "https://api.x.com/2"


class XApiError(RuntimeError):
    pass


@dataclass(frozen=True)
class Page:
    data: list[dict[str, Any]]
    meta: dict[str, Any]
    includes: dict[str, Any]
    errors: list[dict[str, Any]]


class XApiClient:
    def __init__(self, bearer_token: str, *, base_url: str = API_BASE, sleep_on_rate_limit: bool = True) -> None:
        if not bearer_token:
            raise XApiError("Missing X bearer token. Set X_BEARER_TOKEN or pass --bearer-token.")
        self.bearer_token = bearer_token
        self.base_url = base_url.rstrip("/")
        self.sleep_on_rate_limit = sleep_on_rate_limit

    def user_by_username(self, username: str) -> dict[str, Any]:
        response = self._get(
            f"/users/by/username/{username}",
            {"user.fields": USER_FIELDS},
        )
        if "data" not in response:
            raise XApiError(f"Could not resolve username {username!r}: {response}")
        return response["data"]

    def full_archive_page(
        self,
        username: str,
        *,
        pagination_token: str | None = None,
        max_results: int = 500,
        exclude_replies: bool = False,
        exclude_retweets: bool = False,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> Page:
        query_parts = [f"from:{username}"]
        if exclude_replies:
            query_parts.append("-is:reply")
        if exclude_retweets:
            query_parts.append("-is:retweet")
        params: dict[str, Any] = {
            "query": " ".join(query_parts),
            "max_results": max_results,
            "tweet.fields": TWEET_FIELDS,
            "expansions": EXPANSIONS,
            "media.fields": MEDIA_FIELDS,
            "poll.fields": POLL_FIELDS,
        }
        if pagination_token:
            params["next_token"] = pagination_token
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        response = self._get("/tweets/search/all", params)
        return Page(
            data=response.get("data", []),
            meta=response.get("meta", {}),
            includes=response.get("includes", {}),
            errors=response.get("errors", []),
        )

    def user_timeline_page(
        self,
        user_id: str,
        *,
        pagination_token: str | None = None,
        max_results: int = 100,
        exclude_replies: bool = False,
        exclude_retweets: bool = False,
    ) -> Page:
        exclude = []
        if exclude_replies:
            exclude.append("replies")
        if exclude_retweets:
            exclude.append("retweets")
        params: dict[str, Any] = {
            "max_results": max_results,
            "tweet.fields": TWEET_FIELDS,
            "expansions": EXPANSIONS,
            "media.fields": MEDIA_FIELDS,
            "poll.fields": POLL_FIELDS,
        }
        if exclude:
            params["exclude"] = ",".join(exclude)
        if pagination_token:
            params["pagination_token"] = pagination_token
        response = self._get(f"/users/{user_id}/tweets", params)
        return Page(
            data=response.get("data", []),
            meta=response.get("meta", {}),
            includes=response.get("includes", {}),
            errors=response.get("errors", []),
        )

    def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}{path}?{urlencode(params)}"
        request = Request(url, headers={"Authorization": f"Bearer {self.bearer_token}"})
        while True:
            try:
                with urlopen(request, timeout=60) as response:
                    body = response.read().decode("utf-8")
                    return json.loads(body)
            except HTTPError as error:
                body_text = error.read().decode("utf-8", errors="replace")
                if error.code == 429 and self.sleep_on_rate_limit:
                    reset = error.headers.get("x-rate-limit-reset")
                    wait_seconds = self._rate_limit_wait(reset)
                    print(f"Rate limited. Sleeping {wait_seconds}s before retrying.", file=sys.stderr)
                    time.sleep(wait_seconds)
                    continue
                detail = self._format_error(error.code, body_text)
                raise XApiError(detail) from error

    @staticmethod
    def _rate_limit_wait(reset_header: str | None) -> int:
        if not reset_header:
            return 900
        try:
            return max(1, int(reset_header) - int(time.time()) + 5)
        except ValueError:
            return 900

    @staticmethod
    def _format_error(status: int, body_text: str) -> str:
        try:
            body = json.loads(body_text)
        except json.JSONDecodeError:
            body = body_text
        if status in {401, 403}:
            return (
                f"X API access denied ({status}). For a complete export, the bearer token must have "
                f"access to the requested endpoint. Response: {body}"
            )
        return f"X API request failed ({status}): {body}"

