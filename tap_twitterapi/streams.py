from __future__ import annotations

import math
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import requests
from singer_sdk import typing as th
from singer_sdk.streams import Stream
from singer_sdk.streams.core import Stream as CoreStream
from singer_sdk.streams import RESTStream

from .auth import build_auth


# -------------------------------
# Shared tweet schema definition
# -------------------------------

def _tweet_schema_properties(include_ctx: bool = True, include_parent: bool = False) -> th.PropertiesList:
    """Return a PropertiesList matching twitterapi.io tweet object (superset) + context."""
    props = [
        th.Property("type", th.StringType),
        th.Property("id", th.StringType),
        th.Property("url", th.StringType),
        th.Property("text", th.StringType),
        th.Property("source", th.StringType),
        th.Property("retweetCount", th.IntegerType),
        th.Property("replyCount", th.IntegerType),
        th.Property("likeCount", th.IntegerType),
        th.Property("quoteCount", th.IntegerType),
        th.Property("viewCount", th.IntegerType),
        th.Property("createdAt", th.StringType),  # ISO8601 string from API
        th.Property("lang", th.StringType),
        th.Property("bookmarkCount", th.IntegerType),
        th.Property("isReply", th.BooleanType),
        th.Property("inReplyToId", th.StringType),
        th.Property("conversationId", th.StringType),
        th.Property("displayTextRange", th.ArrayType(th.IntegerType)),
        th.Property("inReplyToUserId", th.StringType),
        th.Property("inReplyToUsername", th.StringType),

        th.Property(
            "author",
            th.ObjectType(
                th.Property("type", th.StringType),
                th.Property("userName", th.StringType),
                th.Property("url", th.StringType),
                th.Property("id", th.StringType),
                th.Property("name", th.StringType),
                th.Property("isBlueVerified", th.BooleanType),
                th.Property("verifiedType", th.StringType),
                th.Property("profilePicture", th.StringType),
                th.Property("coverPicture", th.StringType),
                th.Property("description", th.StringType),
                th.Property("location", th.StringType),
                th.Property("followers", th.IntegerType),
                th.Property("following", th.IntegerType),
                th.Property("canDm", th.BooleanType),
                th.Property("createdAt", th.StringType),
                th.Property("favouritesCount", th.IntegerType),
                th.Property("hasCustomTimelines", th.BooleanType),
                th.Property("isTranslator", th.BooleanType),
                th.Property("mediaCount", th.IntegerType),
                th.Property("statusesCount", th.IntegerType),
                th.Property("withheldInCountries", th.ArrayType(th.StringType)),
                th.Property("affiliatesHighlightedLabel", th.ObjectType()),
                th.Property("possiblySensitive", th.BooleanType),
                th.Property("pinnedTweetIds", th.ArrayType(th.StringType)),
                th.Property("isAutomated", th.BooleanType),
                th.Property("automatedBy", th.StringType),
                th.Property("unavailable", th.BooleanType),
                th.Property("message", th.StringType),
                th.Property("unavailableReason", th.StringType),
                th.Property(
                    "profile_bio",
                    th.ObjectType(
                        th.Property("description", th.StringType),
                        th.Property(
                            "entities",
                            th.ObjectType(
                                th.Property(
                                    "description",
                                    th.ObjectType(
                                        th.Property(
                                            "urls",
                                            th.ArrayType(
                                            th.ObjectType(
                                                th.Property("display_url", th.StringType),
                                                th.Property("expanded_url", th.StringType),
                                                th.Property("indices", th.ArrayType(th.IntegerType)),
                                                th.Property("url", th.StringType),
                                            )
                                            )
                                        )
                                    ),
                                ),
                                th.Property(
                                    "url",
                                    th.ObjectType(
                                        th.Property(
                                            "urls",
                                            th.ArrayType(
                                                th.ObjectType(
                                                    th.Property("display_url", th.StringType),
                                                    th.Property("expanded_url", th.StringType),
                                                    th.Property("indices", th.ArrayType(th.IntegerType)),
                                                    th.Property("url", th.StringType),
                                                )
                                            )
                                        )
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        ),

        th.Property(
            "entities",
            th.ObjectType(
                th.Property(
                    "hashtags",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("indices", th.ArrayType(th.IntegerType)),
                            th.Property("text", th.StringType),
                        )
                    ),
                ),
                th.Property(
                    "urls",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("display_url", th.StringType),
                            th.Property("expanded_url", th.StringType),
                            th.Property("indices", th.ArrayType(th.IntegerType)),
                            th.Property("url", th.StringType),
                        )
                    ),
                ),
                th.Property(
                    "user_mentions",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("id_str", th.StringType),
                            th.Property("name", th.StringType),
                            th.Property("screen_name", th.StringType),
                        )
                    ),
                ),
            ),
        ),

        th.Property("quoted_tweet", th.ObjectType()),
        th.Property("retweeted_tweet", th.ObjectType()),
        th.Property("isLimitedReply", th.BooleanType),
    ]

    if include_ctx:
        props.extend([
            th.Property("_ctx_username", th.StringType),
            th.Property("_ctx_hashtag", th.StringType),
        ])

    if include_parent:
        props.append(th.Property("parentTweetId", th.StringType))

    return th.PropertiesList(*props)


class TwitterAPIStream(RESTStream):
    """Base stream with helpers and sane defaults."""
    url_base = "https://api.twitterapi.io"

    primary_keys = ["id"]
    replication_key = "createdAt"

    # Use header auth
    @property
    def authenticator(self):
        return build_auth(self)

    # Generic backoff for 429/5xx
    @property
    def backoff_max_tries(self) -> int:
        return 5

    def _now_unix(self) -> int:
        return int(time.time())

    def _iso_to_unix(self, iso_s: str) -> Optional[int]:
        try:
            dt = datetime.fromisoformat(iso_s.replace("Z", "+00:00"))
            return int(dt.timestamp())
        except Exception:
            return None

    def _start_unix(self) -> Optional[int]:
        # Prefer bookmark; fallback to config since_time if provided.
        v = self.get_starting_replication_key_value(context=None)
        if v:
            # v is a string 'createdAt'; try to parse as ISO first, else unix
            unix_v = self._iso_to_unix(v)
            if unix_v is None:
                try:
                    unix_v = int(v)
                except Exception:
                    unix_v = None
            return unix_v
        cfg = self.config.get("since_time")
        if cfg:
            return self._iso_to_unix(cfg) or None
        return None

    def _until_unix(self) -> Optional[int]:
        cfg = self.config.get("until_time")
        if cfg:
            return self._iso_to_unix(cfg) or None
        return None

    def _limit_cfg(self, key: str, default: int) -> int:
        v = self.config.get(key)
        return int(v) if isinstance(v, int) and v > 0 else default

    # Build advanced search query strings from date constraints
    def _build_date_query_part(self, since_unix: Optional[int], until_unix: Optional[int]) -> str:
        """Build the date portion of an advanced search query."""
        parts = []
        if since_unix:
            # Convert unix to datetime for Twitter search format
            dt_since = datetime.fromtimestamp(since_unix, tz=timezone.utc)
            # Twitter format: since:YYYY-MM-DD_HH:MM:SS_UTC
            parts.append(f"since:{dt_since.strftime('%Y-%m-%d_%H:%M:%S')}_UTC")
        if until_unix:
            dt_until = datetime.fromtimestamp(until_unix, tz=timezone.utc)
            parts.append(f"until:{dt_until.strftime('%Y-%m-%d_%H:%M:%S')}_UTC")
        return " ".join(parts)

    # Small helper using the stream's session & auth
    def _http_get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.url_base}{path}"
        headers = self.authenticator.auth_headers or {}
        resp = self.requests_session.get(url, params=params, headers=headers, timeout=60)
        if resp.status_code >= 400:
            # Let the SDK raise on bad responses consistently
            self.logger.error("HTTP %s %s params=%s body=%s", resp.status_code, url, params, resp.text[:500])
            resp.raise_for_status()
        return resp.json()

    # Inject context columns for analytics consistently
    def post_process(self, row: Dict[str, Any], context: Optional[dict] = None) -> Dict[str, Any]:
        context = context or {}
        # context-aware columns (never per-table sharding!)
        row.setdefault("_ctx_username", context.get("_ctx_username"))
        row.setdefault("_ctx_hashtag", context.get("_ctx_hashtag"))
        if "parentTweetId" in context and "parentTweetId" not in row:
            row["parentTweetId"] = context["parentTweetId"]
        return row


# -------------------------------
# Parent streams
# -------------------------------

class HashtagTweetsStream(TwitterAPIStream):
    """Tweets by hashtag via Advanced Search (no per-hashtag tables)."""
    name = "hashtag_tweets"

    # Schema
    schema = _tweet_schema_properties(include_ctx=True).to_dict()
    schema["additionalProperties"] = True

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        hashtags: List[str] = self.config.get("hashtags") or []
        if not hashtags:
            return []

        max_pages = self._limit_cfg("max_pages_per_partition", 1)
        max_records = self._limit_cfg("max_records_per_partition", 1000)

        since_unix = self._start_unix()
        until_unix = self._until_unix() or self._now_unix()
        if since_unix and until_unix and until_unix < since_unix:
            until_unix = since_unix

        emitted = 0

        for tag in hashtags:
            cursor = ""
            pages = 0
            while True:
                if pages >= max_pages or emitted >= max_records:
                    break
                
                # Build advanced search query for hashtag
                date_part = self._build_date_query_part(since_unix, until_unix)
                query = f"#{tag}"
                if date_part:
                    query = f"{query} {date_part}"
                
                params = {
                    "query": query,
                    "queryType": "Latest",
                    "cursor": cursor
                }

                data = self._http_get("/twitter/tweet/advanced_search", params)
                tweets = data.get("tweets") or []
                for t in tweets:
                    t["_ctx_hashtag"] = tag
                    t["_ctx_username"] = None
                    yield t
                    emitted += 1
                    if emitted >= max_records:
                        break

                pages += 1
                if not data.get("has_next_page"):
                    break
                cursor = data.get("next_cursor") or ""
                if not cursor:
                    break


class MentionsStream(TwitterAPIStream):
    """Mentions of a user (by username)."""
    name = "mentions"

    schema = _tweet_schema_properties(include_ctx=True).to_dict()
    schema["additionalProperties"] = True

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        usernames: List[str] = self.config.get("usernames") or []
        if not usernames:
            return []

        max_pages = self._limit_cfg("max_pages_per_partition", 1)
        max_records = self._limit_cfg("max_records_per_partition", 1000)

        since_unix = self._start_unix()
        until_unix = self._until_unix() or self._now_unix()
        if since_unix and until_unix and until_unix < since_unix:
            until_unix = since_unix

        emitted = 0

        for uname in usernames:
            cursor = ""
            pages = 0
            while True:
                if pages >= max_pages or emitted >= max_records:
                    break
                params = {"userName": uname, "cursor": cursor}  # Fixed: userName not username
                if since_unix:
                    params["sinceTime"] = since_unix
                if until_unix:
                    params["untilTime"] = until_unix

                data = self._http_get("/twitter/user/mentions", params)
                tweets = data.get("tweets") or []
                for t in tweets:
                    t["_ctx_username"] = uname
                    t["_ctx_hashtag"] = None
                    yield t
                    emitted += 1
                    if emitted >= max_records:
                        break

                pages += 1
                if not data.get("has_next_page"):
                    break
                cursor = data.get("next_cursor") or ""
                if not cursor:
                    break


class UserTweetsStream(TwitterAPIStream):
    """Tweets by username: union of Advanced Search + Last Tweets (de-duped)."""
    name = "user_tweets"

    schema = _tweet_schema_properties(include_ctx=True).to_dict()
    schema["additionalProperties"] = True

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        usernames: List[str] = self.config.get("usernames") or []
        if not usernames:
            return []

        max_pages = self._limit_cfg("max_pages_per_partition", 1)
        max_records = self._limit_cfg("max_records_per_partition", 1000)
        max_parent = self._limit_cfg("max_parent_tweets", 1000)

        since_unix = self._start_unix()
        until_unix = self._until_unix() or self._now_unix()
        if since_unix and until_unix and until_unix < since_unix:
            until_unix = since_unix

        include_replies_last = bool(self.config.get("last_tweets_include_replies", False))

        for uname in usernames:
            seen_ids: set[str] = set()
            emitted_for_user = 0

            # 1) Advanced search (from:username)
            cursor = ""
            pages = 0
            while emitted_for_user < max_parent and pages < max_pages:
                # Build advanced search query for user tweets
                date_part = self._build_date_query_part(since_unix, until_unix)
                query = f"from:{uname}"
                if date_part:
                    query = f"{query} {date_part}"
                
                params = {
                    "query": query,
                    "queryType": "Latest",
                    "cursor": cursor
                }

                data = self._http_get("/twitter/tweet/advanced_search", params)
                tweets = data.get("tweets") or []
                for t in tweets:
                    tid = str(t.get("id"))
                    if not tid or tid in seen_ids:
                        continue
                    t["_ctx_username"] = uname
                    t["_ctx_hashtag"] = None
                    yield t
                    seen_ids.add(tid)
                    emitted_for_user += 1
                    if emitted_for_user >= max_parent:
                        break
                pages += 1
                if emitted_for_user >= max_parent or not data.get("has_next_page"):
                    break
                cursor = data.get("next_cursor") or ""
                if not cursor:
                    break

            # 2) Last tweets (de-dupe)
            if emitted_for_user < max_parent:
                cursor = ""
                pages = 0
                while emitted_for_user < max_parent and pages < max_pages:
                    params = {"userName": uname, "cursor": cursor, "includeReplies": include_replies_last}
                    data = self._http_get("/twitter/user/last_tweets", params)
                    tweets = data.get("tweets") or []
                    for t in tweets:
                        tid = str(t.get("id"))
                        if not tid or tid in seen_ids:
                            continue
                        t["_ctx_username"] = uname
                        t["_ctx_hashtag"] = None
                        yield t
                        seen_ids.add(tid)
                        emitted_for_user += 1
                        if emitted_for_user >= max_parent:
                            break
                    pages += 1
                    if emitted_for_user >= max_parent or not data.get("has_next_page"):
                        break
                    cursor = data.get("next_cursor") or ""
                    if not cursor:
                        break

    # Provide child context so SDK can traverse to replies/quotes when selected
    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {
            "tweetId": record.get("id"),
            "_ctx_username": record.get("_ctx_username"),
            "_ctx_hashtag": record.get("_ctx_hashtag"),
            "parentTweetId": record.get("id"),
        }


# -------------------------------
# Child streams
# -------------------------------

class TweetRepliesStream(TwitterAPIStream):
    """Replies for parent tweet."""
    name = "tweet_replies"
    parent_stream_type = UserTweetsStream
    state_partitioning_keys = ["parentTweetId"]

    # Child schema includes parent join column
    schema = _tweet_schema_properties(include_ctx=True, include_parent=True).to_dict()
    schema["additionalProperties"] = True

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        if not context or not context.get("tweetId"):
            return []
        tweet_id = str(context["tweetId"])

        max_children = self._limit_cfg("children_max_per_parent", 1000)
        max_pages = self._limit_cfg("max_pages_per_partition", 1)

        since_unix = self._start_unix()
        until_unix = self._until_unix() or self._now_unix()
        if since_unix and until_unix and until_unix < since_unix:
            until_unix = since_unix

        emitted = 0
        cursor = ""
        pages = 0
        while pages < max_pages and emitted < max_children:
            params = {"tweetId": tweet_id, "cursor": cursor}
            if since_unix:
                params["sinceTime"] = since_unix
            if until_unix:
                params["untilTime"] = until_unix

            data = self._http_get("/twitter/tweet/replies", params)
            replies = data.get("replies") or []
            for r in replies:
                r["parentTweetId"] = tweet_id
                # preserve upstream context for analytics
                r["_ctx_username"] = context.get("_ctx_username")
                r["_ctx_hashtag"] = context.get("_ctx_hashtag")
                yield r
                emitted += 1
                if emitted >= max_children:
                    break
            pages += 1
            if not data.get("has_next_page"):
                break
            cursor = data.get("next_cursor") or ""
            if not cursor:
                break


class TweetQuotesStream(TwitterAPIStream):
    """Quotes for parent tweet."""
    name = "tweet_quotes"
    parent_stream_type = UserTweetsStream
    state_partitioning_keys = ["parentTweetId"]

    schema = _tweet_schema_properties(include_ctx=True, include_parent=True).to_dict()
    schema["additionalProperties"] = True

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        if not context or not context.get("tweetId"):
            return []
        tweet_id = str(context["tweetId"])

        max_children = self._limit_cfg("children_max_per_parent", 1000)
        max_pages = self._limit_cfg("max_pages_per_partition", 1)
        include_replies = bool(self.config.get("quotes_include_replies", True))

        since_unix = self._start_unix()
        until_unix = self._until_unix() or self._now_unix()
        if since_unix and until_unix and until_unix < since_unix:
            until_unix = since_unix

        emitted = 0
        cursor = ""
        pages = 0
        while pages < max_pages and emitted < max_children:
            params = {"tweetId": tweet_id, "cursor": cursor, "includeReplies": include_replies}
            if since_unix:
                params["sinceTime"] = since_unix
            if until_unix:
                params["untilTime"] = until_unix

            data = self._http_get("/twitter/tweet/quotes", params)
            quotes = data.get("tweets") or []
            for q in quotes:
                q["parentTweetId"] = tweet_id
                q["_ctx_username"] = context.get("_ctx_username")
                q["_ctx_hashtag"] = context.get("_ctx_hashtag")
                yield q
                emitted += 1
                if emitted >= max_children:
                    break
            pages += 1
            if not data.get("has_next_page"):
                break
            cursor = data.get("next_cursor") or ""
            if not cursor:
                break
        