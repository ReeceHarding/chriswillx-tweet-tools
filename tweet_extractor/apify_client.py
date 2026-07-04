from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


APIFY_BASE = "https://api.apify.com/v2"
DEFAULT_ACTOR_ID = "fastcrawler/tweet-x-twitter-scraper-0-2-1k-pay-per-result-v2"


class ApifyError(RuntimeError):
    pass


@dataclass(frozen=True)
class ApifyRunResult:
    run_id: str
    dataset_id: str | None
    status: str
    item_count: int
    items: list[dict[str, Any]]


class ApifyClient:
    def __init__(self, token: str, *, base_url: str = APIFY_BASE) -> None:
        if not token:
            raise ApifyError("Missing Apify token. Set APIFY_TOKEN or pass --apify-token-file.")
        self.token = token
        self.base_url = base_url.rstrip("/")

    def run_actor_and_get_items(
        self,
        actor_id: str,
        actor_input: dict[str, Any],
        *,
        timeout_seconds: int = 900,
        poll_seconds: int = 5,
        retries: int = 2,
    ) -> ApifyRunResult:
        failures = []
        for attempt in range(retries + 1):
            try:
                return self._run_actor_once(
                    actor_id,
                    actor_input,
                    timeout_seconds=timeout_seconds,
                    poll_seconds=poll_seconds,
                )
            except ApifyError as error:
                failures.append(str(error))
                if attempt >= retries:
                    raise ApifyError("Apify actor failed after retries: " + " | ".join(failures)) from error
                time.sleep(min(30, 5 * (attempt + 1)))
        raise ApifyError("Apify actor failed unexpectedly.")

    def _run_actor_once(
        self,
        actor_id: str,
        actor_input: dict[str, Any],
        *,
        timeout_seconds: int,
        poll_seconds: int,
    ) -> ApifyRunResult:
        run = self._request_json(
            "POST",
            f"/acts/{actor_id.replace('/', '~')}/runs",
            {"token": self.token, "waitForFinish": 0},
            actor_input,
        )["data"]
        run_id = str(run["id"])
        deadline = time.monotonic() + timeout_seconds

        while run.get("status") in {"READY", "RUNNING"}:
            if time.monotonic() > deadline:
                raise ApifyError(f"Timed out waiting for Apify run {run_id}.")
            time.sleep(poll_seconds)
            run = self.get_run(run_id)

        status = str(run.get("status", "UNKNOWN"))
        if status != "SUCCEEDED":
            raise ApifyError(f"Apify run {run_id} ended with status {status}.")

        dataset_id = run.get("defaultDatasetId")
        items = self.get_dataset_items(str(dataset_id)) if dataset_id else []
        return ApifyRunResult(
            run_id=run_id,
            dataset_id=str(dataset_id) if dataset_id else None,
            status=status,
            item_count=len(items),
            items=items,
        )

    def get_run(self, run_id: str) -> dict[str, Any]:
        return self._request_json("GET", f"/actor-runs/{run_id}", {"token": self.token})["data"]

    def get_dataset_items(self, dataset_id: str) -> list[dict[str, Any]]:
        response = self._request_json(
            "GET",
            f"/datasets/{dataset_id}/items",
            {"token": self.token, "clean": "true", "format": "json"},
        )
        if not isinstance(response, list):
            raise ApifyError(f"Unexpected dataset response for {dataset_id}: {response!r}")
        return response

    def _request_json(
        self,
        method: str,
        path: str,
        query: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> Any:
        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{urlencode(query)}"
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = Request(url, data=data, headers=headers, method=method)
        try:
            with urlopen(request, timeout=60) as response:
                payload = response.read().decode("utf-8")
                return json.loads(payload) if payload else {}
        except HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            raise ApifyError(f"Apify API request failed ({error.code}): {detail}") from error


def load_apify_token(token: str | None, token_file: Path | None) -> str:
    if token:
        return token.strip()
    if token_file and token_file.exists():
        return token_file.read_text(encoding="utf-8").strip()
    return ""


def iter_date_windows(start: date, end: date, days: int) -> list[tuple[date, date]]:
    windows = []
    cursor = start
    while cursor < end:
        next_cursor = min(cursor + timedelta(days=days), end)
        windows.append((cursor, next_cursor))
        cursor = next_cursor
    return windows


def split_window(start: date, end: date) -> tuple[tuple[date, date], tuple[date, date]] | None:
    if (end - start).days <= 1:
        return None
    midpoint = start + timedelta(days=(end - start).days // 2)
    return (start, midpoint), (midpoint, end)


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()
