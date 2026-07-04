from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


TWEET_FIELDS = ",".join(
    [
        "id",
        "text",
        "author_id",
        "created_at",
        "conversation_id",
        "in_reply_to_user_id",
        "referenced_tweets",
        "attachments",
        "entities",
        "public_metrics",
        "possibly_sensitive",
        "lang",
        "source",
        "context_annotations",
        "edit_history_tweet_ids",
        "note_tweet",
        "article",
    ]
)

USER_FIELDS = "id,name,username,created_at,description,verified,verified_type,public_metrics"

EXPANSIONS = ",".join(
    [
        "referenced_tweets.id",
        "referenced_tweets.id.author_id",
        "attachments.media_keys",
        "attachments.poll_ids",
        "author_id",
        "in_reply_to_user_id",
        "entities.mentions.username",
    ]
)

MEDIA_FIELDS = "media_key,type,url,preview_image_url,alt_text,public_metrics,duration_ms,height,width"
POLL_FIELDS = "id,options,duration_minutes,end_datetime,voting_status"


@dataclass(frozen=True)
class TweetRecord:
    id: str
    created_at: str
    text: str
    author_id: str
    username: str
    conversation_id: str | None
    in_reply_to_user_id: str | None
    referenced_tweets: list[dict[str, Any]]
    public_metrics: dict[str, Any]
    lang: str | None
    possibly_sensitive: bool | None
    raw: dict[str, Any]

    @classmethod
    def from_api(cls, tweet: dict[str, Any], username: str) -> "TweetRecord":
        return cls(
            id=str(tweet["id"]),
            created_at=str(tweet.get("created_at", "")),
            text=str(tweet.get("text", "")),
            author_id=str(tweet.get("author_id", "")),
            username=username,
            conversation_id=tweet.get("conversation_id"),
            in_reply_to_user_id=tweet.get("in_reply_to_user_id"),
            referenced_tweets=tweet.get("referenced_tweets", []),
            public_metrics=tweet.get("public_metrics", {}),
            lang=tweet.get("lang"),
            possibly_sensitive=tweet.get("possibly_sensitive"),
            raw=tweet,
        )

    @classmethod
    def from_twscrape(cls, tweet: Any) -> "TweetRecord":
        raw = _model_to_dict(tweet)
        user = getattr(tweet, "user", None)
        username = str(getattr(user, "username", raw.get("user", {}).get("username", "")))
        tweet_id = str(getattr(tweet, "id_str", None) or getattr(tweet, "id"))
        date = getattr(tweet, "date", "")
        if isinstance(date, datetime):
            created_at = date.isoformat()
        else:
            created_at = str(date)
        referenced_tweets = []
        retweeted_tweet = getattr(tweet, "retweetedTweet", None)
        quoted_tweet = getattr(tweet, "quotedTweet", None)
        if retweeted_tweet is not None:
            referenced_tweets.append({"type": "retweeted", "id": str(getattr(retweeted_tweet, "id", ""))})
        if quoted_tweet is not None:
            referenced_tweets.append({"type": "quoted", "id": str(getattr(quoted_tweet, "id", ""))})
        in_reply_to = getattr(tweet, "inReplyToTweetIdStr", None) or getattr(tweet, "inReplyToTweetId", None)
        if in_reply_to is not None:
            referenced_tweets.append({"type": "replied_to", "id": str(in_reply_to)})

        return cls(
            id=tweet_id,
            created_at=created_at,
            text=str(getattr(tweet, "rawContent", "")),
            author_id=str(getattr(user, "id_str", None) or getattr(user, "id", "")),
            username=username,
            conversation_id=str(
                getattr(tweet, "conversationIdStr", None) or getattr(tweet, "conversationId", "") or ""
            )
            or None,
            in_reply_to_user_id=str(getattr(getattr(tweet, "inReplyToUser", None), "id", "") or "") or None,
            referenced_tweets=referenced_tweets,
            public_metrics={
                "retweet_count": getattr(tweet, "retweetCount", None),
                "reply_count": getattr(tweet, "replyCount", None),
                "like_count": getattr(tweet, "likeCount", None),
                "quote_count": getattr(tweet, "quoteCount", None),
                "bookmark_count": getattr(tweet, "bookmarkedCount", None),
                "impression_count": getattr(tweet, "viewCount", None),
            },
            lang=getattr(tweet, "lang", None),
            possibly_sensitive=getattr(tweet, "possibly_sensitive", None),
            raw=raw,
        )

    @classmethod
    def from_apify(cls, item: dict[str, Any], username: str) -> "TweetRecord":
        author = item.get("author") or item.get("user") or {}
        tweet_id = (
            item.get("id")
            or item.get("id_str")
            or item.get("tweetId")
            or item.get("tweet_id")
            or item.get("rest_id")
            or item.get("url", "").rstrip("/").split("/")[-1]
        )
        created_at = item.get("createdAt") or item.get("created_at") or item.get("date") or ""
        text = item.get("text") or item.get("full_text") or item.get("rawContent") or item.get("content") or ""
        conversation_id = item.get("conversationId") or item.get("conversation_id_str") or item.get("conversation_id")
        referenced_tweets = []
        if item.get("isRetweet") or item.get("retweeted_status"):
            referenced_tweets.append({"type": "retweeted", "id": str(item.get("retweetId") or "")})
        quote_id = item.get("quoteId") or item.get("quoted_tweet_id") or item.get("quotedTweetId")
        if quote_id:
            referenced_tweets.append({"type": "quoted", "id": str(quote_id)})
        reply_id = item.get("inReplyToTweetId") or item.get("in_reply_to_status_id_str")
        if reply_id:
            referenced_tweets.append({"type": "replied_to", "id": str(reply_id)})

        return cls(
            id=str(tweet_id),
            created_at=str(created_at),
            text=str(text),
            author_id=str(author.get("id") or author.get("rest_id") or item.get("authorId") or ""),
            username=str(author.get("userName") or author.get("screen_name") or author.get("username") or username),
            conversation_id=str(conversation_id) if conversation_id else None,
            in_reply_to_user_id=str(item.get("inReplyToUserId") or item.get("in_reply_to_user_id_str") or "") or None,
            referenced_tweets=referenced_tweets,
            public_metrics={
                "retweet_count": item.get("retweetCount") or item.get("retweet_count"),
                "reply_count": item.get("replyCount") or item.get("reply_count"),
                "like_count": item.get("likeCount") or item.get("favorite_count") or item.get("like_count"),
                "quote_count": item.get("quoteCount") or item.get("quote_count"),
                "bookmark_count": item.get("bookmarkCount") or item.get("bookmark_count"),
                "impression_count": item.get("viewCount") or item.get("views") or item.get("impression_count"),
            },
            lang=item.get("lang"),
            possibly_sensitive=item.get("possibly_sensitive") or item.get("possiblySensitive"),
            raw=item,
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "created_at": self.created_at,
            "text": self.text,
            "author_id": self.author_id,
            "username": self.username,
            "conversation_id": self.conversation_id,
            "in_reply_to_user_id": self.in_reply_to_user_id,
            "referenced_tweets": self.referenced_tweets,
            "public_metrics": self.public_metrics,
            "lang": self.lang,
            "possibly_sensitive": self.possibly_sensitive,
            "raw": self.raw,
        }

    def to_csv_row(self) -> dict[str, str]:
        metrics = self.public_metrics or {}
        references = ";".join(
            f"{item.get('type', '')}:{item.get('id', '')}" for item in self.referenced_tweets
        )
        return {
            "id": self.id,
            "created_at": self.created_at,
            "text": self.text,
            "author_id": self.author_id,
            "username": self.username,
            "conversation_id": self.conversation_id or "",
            "in_reply_to_user_id": self.in_reply_to_user_id or "",
            "referenced_tweets": references,
            "retweet_count": str(metrics.get("retweet_count", "")),
            "reply_count": str(metrics.get("reply_count", "")),
            "like_count": str(metrics.get("like_count", "")),
            "quote_count": str(metrics.get("quote_count", "")),
            "bookmark_count": str(metrics.get("bookmark_count", "")),
            "impression_count": str(metrics.get("impression_count", "")),
            "lang": self.lang or "",
            "possibly_sensitive": "" if self.possibly_sensitive is None else str(self.possibly_sensitive),
        }


def _model_to_dict(value: Any) -> dict[str, Any]:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if hasattr(value, "dict"):
        return value.dict()
    if hasattr(value, "__dict__"):
        return dict(value.__dict__)
    return {"value": str(value)}
