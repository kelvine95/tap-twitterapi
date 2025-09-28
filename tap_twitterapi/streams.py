from __future__ import annotations

import csv
import math
import time
import calendar
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

from singer_sdk import typing as th
from singer_sdk.streams import RESTStream

from .auth import build_auth


# ============================================================
# Shared tweet schema
# ============================================================

def _tweet_schema_properties(include_ctx: bool = True, include_parent: bool = False) -> th.PropertiesList:
    """Return a PropertiesList matching twitterapi.io tweet object (superset) + context + snapshot."""
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

    # Snapshot timestamp for metric versioning
    props.append(th.Property("_snapshot_ts", th.StringType))

    return th.PropertiesList(*props)


# ============================================================
# Base stream
# ============================================================

class TwitterAPIStream(RESTStream):
    """Base stream with helpers, credit control, stateful refresh queue, and sane defaults."""
    url_base = "https://api.twitterapi.io"

    primary_keys = ["id"]
    replication_key = "createdAt"

    # Auth
    @property
    def authenticator(self):
        return build_auth(self)

    # Backoff defaults
    @property
    def backoff_max_tries(self) -> int:
        return 5

    # ------------- Helpers -------------
    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _now_unix(self) -> int:
        return int(self._now().timestamp())

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
            unix_v = self._iso_to_unix(v) or (int(v) if str(v).isdigit() else None)
            return unix_v
        cfg = self.config.get("since_time")
        if cfg:
            return self._iso_to_unix(cfg) or None
        return None

    def _until_unix(self) -> Optional[int]:
        cfg = self.config.get("until_time")
        if cfg:
            return self._iso_to_unix(cfg) or None
        # else, allow min_age gating:
        h = self.config.get("min_tweet_age_hours")
        if isinstance(h, int) and h > 0:
            return int((self._now() - timedelta(hours=h)).timestamp())
        return None

    def _build_date_query_part(self, since_unix: Optional[int], until_unix: Optional[int]) -> str:
        """Build the date portion of an advanced search query."""
        parts = []
        if since_unix:
            dt_since = datetime.fromtimestamp(since_unix, tz=timezone.utc)
            parts.append(f"since:{dt_since.strftime('%Y-%m-%d_%H:%M:%S')}_UTC")
        if until_unix:
            dt_until = datetime.fromtimestamp(until_unix, tz=timezone.utc)
            parts.append(f"until:{dt_until.strftime('%Y-%m-%d_%H:%M:%S')}_UTC")
        return " ".join(parts)

    def _http_get(self, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.url_base}{path}"
        headers = self.authenticator.auth_headers or {}
        resp = self.requests_session.get(url, params=params, headers=headers, timeout=60)
        if resp.status_code >= 400:
            self.logger.error("HTTP %s %s params=%s body=%s", resp.status_code, url, params, resp.text[:500])
            resp.raise_for_status()
        return resp.json()

    # ------------- Snapshot + refresh scheduling -------------

    def _schedule_one_time_refresh(self, tweet_row: dict) -> None:
        """Schedule a one-time refresh of this tweet exactly once at +refresh_delay_hours."""
        if not tweet_row:
            return
        tid = str(tweet_row.get("id") or "").strip()
        created_iso = tweet_row.get("createdAt")
        if not tid or not created_iso:
            return

        created_unix = self._iso_to_unix(created_iso)
        if not created_unix:
            return

        delay_h = int(self.config.get("refresh_delay_hours", 48))
        due_unix = created_unix + delay_h * 3600

        state = dict(self.tap.state or {})
        rstate = state.get("tweet_refresh") or {"queue": {}, "done": {}}

        # If already done, never requeue
        if tid in rstate.get("done", {}):
            return
        # If already queued, don't duplicate
        if tid in rstate.get("queue", {}):
            return

        rstate["queue"][tid] = {"due": due_unix, "attempts": 0, "created": created_unix}
        # prune old done entries (>30d)
        cutoff = self._now_unix() - 30 * 86400
        done = rstate.get("done", {})
        for k, v in list(done.items()):
            if isinstance(v, dict):
                ts = v.get("ts") or 0
            else:
                ts = int(v) if str(v).isdigit() else 0
            if ts and ts < cutoff:
                done.pop(k, None)
        rstate["done"] = done

        state["tweet_refresh"] = rstate
        self.tap.state = state  # trigger state emission

    def post_process(self, row: Dict[str, Any], context: Optional[dict] = None) -> Dict[str, Any]:
        context = context or {}
        # context-aware columns
        row.setdefault("_ctx_username", context.get("_ctx_username"))
        row.setdefault("_ctx_hashtag", context.get("_ctx_hashtag"))
        if "parentTweetId" in context and "parentTweetId" not in row:
            row["parentTweetId"] = context["parentTweetId"]
        # stamp when we fetched this
        row["_snapshot_ts"] = self._now().isoformat().replace("+00:00", "Z")

        # Schedule a single refresh at +48h (or configured) for original tweets and children alike.
        # (If you want parents only, add: if not row.get("isReply"): ...)
        try:
            self._schedule_one_time_refresh(row)
        except Exception as e:
            self.logger.debug("schedule_one_time_refresh failed: %s", e)

        return row

    # ------------- Budget/credits helpers -------------

    def _consume_and_check(self, count: int, category: str) -> int:
        """Consume up to 'count' tweets from the global budget for category.
        Returns how many are actually allowed."""
        if count <= 0:
            return 0
        return self.tap.runtime.consume_tweets(count, category)


# ============================================================
# Parent streams (fair-share budget across all parents)
# ============================================================

class HashtagTweetsStream(TwitterAPIStream):
    """Tweets by hashtag via Advanced Search, fair-sharing budget across all parent partitions."""
    name = "hashtag_tweets"

    schema = _tweet_schema_properties(include_ctx=True).to_dict()
    schema["additionalProperties"] = True

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        tags: List[str] = self.config.get("hashtags") or []
        if not tags:
            return []

        # register these partitions with the global runtime for fair-sharing
        self.tap.runtime.register_parent_partitions(len(tags))

        since_unix = self._start_unix()
        until_unix = self._until_unix() or self._now_unix()

        for tag in tags:
            # allowance for this partition
            part_allowance = self.tap.runtime.allowance_for_next_parent_partition()
            if part_allowance <= 0:
                break

            emitted_for_tag = 0
            cursor = ""
            pages = 0

            while part_allowance > 0:
                date_part = self._build_date_query_part(since_unix, until_unix)
                query = f"#{tag}" + (f" {date_part}" if date_part else "")
                params = {"query": query, "queryType": "Latest", "cursor": cursor}

                data = self._http_get("/twitter/tweet/advanced_search", params)
                tweets = data.get("tweets") or []

                if not tweets:
                    break

                # cap to remaining budget for this partition
                to_emit = min(len(tweets), part_allowance)
                # also cap by global budget consumption
                allowed = self._consume_and_check(to_emit, category="parent")
                if allowed <= 0:
                    # no budget left globally
                    return

                for t in tweets[:allowed]:
                    t["_ctx_hashtag"] = tag
                    t["_ctx_username"] = None
                    yield t
                    emitted_for_tag += 1
                    part_allowance -= 1
                    if part_allowance <= 0:
                        break

                if part_allowance <= 0 or not data.get("has_next_page"):
                    break

                cursor = data.get("next_cursor") or ""
                if not cursor:
                    break

                pages += 1


class MentionsStream(TwitterAPIStream):
    """Mentions of a user (by username), fair-shared with other parents."""
    name = "mentions"

    schema = _tweet_schema_properties(include_ctx=True).to_dict()
    schema["additionalProperties"] = True

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        usernames: List[str] = self.config.get("usernames") or []
        if not usernames:
            return []

        self.tap.runtime.register_parent_partitions(len(usernames))

        since_unix = self._start_unix()
        until_unix = self._until_unix() or self._now_unix()

        for uname in usernames:
            part_allowance = self.tap.runtime.allowance_for_next_parent_partition()
            if part_allowance <= 0:
                break

            emitted_for_user = 0
            cursor = ""

            while part_allowance > 0:
                params = {"userName": uname, "cursor": cursor}
                if since_unix:
                    params["sinceTime"] = since_unix
                if until_unix:
                    params["untilTime"] = until_unix

                data = self._http_get("/twitter/user/mentions", params)
                tweets = data.get("tweets") or []
                if not tweets:
                    break

                to_emit = min(len(tweets), part_allowance)
                allowed = self._consume_and_check(to_emit, category="parent")
                if allowed <= 0:
                    return

                for t in tweets[:allowed]:
                    t["_ctx_username"] = uname
                    t["_ctx_hashtag"] = None
                    yield t
                    emitted_for_user += 1
                    part_allowance -= 1
                    if part_allowance <= 0:
                        break

                if part_allowance <= 0 or not data.get("has_next_page"):
                    break
                cursor = data.get("next_cursor") or ""
                if not cursor:
                    break


class UserTweetsStream(TwitterAPIStream):
    """Tweets by username: Advanced Search + Last Tweets (de-duped), fair-shared with other parents."""
    name = "user_tweets"

    schema = _tweet_schema_properties(include_ctx=True).to_dict()
    schema["additionalProperties"] = True

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        usernames: List[str] = self.config.get("usernames") or []
        if not usernames:
            return []

        include_replies_last = bool(self.config.get("last_tweets_include_replies", False))
        self.tap.runtime.register_parent_partitions(len(usernames))

        since_unix = self._start_unix()
        until_unix = self._until_unix() or self._now_unix()

        for uname in usernames:
            part_allowance = self.tap.runtime.allowance_for_next_parent_partition()
            if part_allowance <= 0:
                break

            seen_ids: set[str] = set()

            # 1) Advanced search (from:username)
            cursor = ""
            while part_allowance > 0:
                date_part = self._build_date_query_part(since_unix, until_unix)
                query = f"from:{uname}" + (f" {date_part}" if date_part else "")
                params = {"query": query, "queryType": "Latest", "cursor": cursor}

                data = self._http_get("/twitter/tweet/advanced_search", params)
                tweets = data.get("tweets") or []
                if not tweets:
                    break

                # de-dupe + allow
                new_tweets = [t for t in tweets if str(t.get("id") or "") not in seen_ids]
                to_emit = min(len(new_tweets), part_allowance)
                allowed = self._consume_and_check(to_emit, category="parent")
                if allowed <= 0:
                    return

                for t in new_tweets[:allowed]:
                    tid = str(t.get("id"))
                    seen_ids.add(tid)
                    t["_ctx_username"] = uname
                    t["_ctx_hashtag"] = None
                    yield t
                    part_allowance -= 1
                    if part_allowance <= 0:
                        break

                if part_allowance <= 0 or not data.get("has_next_page"):
                    break

                cursor = data.get("next_cursor") or ""
                if not cursor:
                    break

            # 2) Last tweets as a top-up (only if we still have allowance)
            if part_allowance > 0:
                cursor = ""
                while part_allowance > 0:
                    params = {"userName": uname, "cursor": cursor, "includeReplies": include_replies_last}
                    data = self._http_get("/twitter/user/last_tweets", params)
                    tweets = data.get("tweets") or []
                    if not tweets:
                        break

                    new_tweets = [t for t in tweets if str(t.get("id") or "") not in seen_ids]
                    to_emit = min(len(new_tweets), part_allowance)
                    allowed = self._consume_and_check(to_emit, category="parent")
                    if allowed <= 0:
                        return

                    for t in new_tweets[:allowed]:
                        tid = str(t.get("id"))
                        seen_ids.add(tid)
                        t["_ctx_username"] = uname
                        t["_ctx_hashtag"] = None
                        yield t
                        part_allowance -= 1
                        if part_allowance <= 0:
                            break

                    if part_allowance <= 0 or not data.get("has_next_page"):
                        break
                    cursor = data.get("next_cursor") or ""
                    if not cursor:
                        break

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        return {
            "tweetId": record.get("id"),
            "_ctx_username": record.get("_ctx_username"),
            "_ctx_hashtag": record.get("_ctx_hashtag"),
            "parentTweetId": record.get("id"),
        }


# ============================================================
# Child streams (budgeted)
# ============================================================

class TweetRepliesStream(TwitterAPIStream):
    """Replies for parent tweet (budgeted as 'child')."""
    name = "tweet_replies"
    parent_stream_type = UserTweetsStream
    state_partitioning_keys = ["parentTweetId"]

    schema = _tweet_schema_properties(include_ctx=True, include_parent=True).to_dict()
    schema["additionalProperties"] = True

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        if not context or not context.get("tweetId"):
            return []
        tweet_id = str(context["tweetId"])

        since_unix = self._start_unix()
        until_unix = self._until_unix() or self._now_unix()

        cursor = ""
        while True:
            params = {"tweetId": tweet_id, "cursor": cursor}
            if since_unix:
                params["sinceTime"] = since_unix
            if until_unix:
                params["untilTime"] = until_unix

            data = self._http_get("/twitter/tweet/replies", params)
            replies = data.get("replies") or []
            if not replies:
                break

            allowed = self._consume_and_check(len(replies), category="child")
            if allowed <= 0:
                return

            for r in replies[:allowed]:
                r["parentTweetId"] = tweet_id
                r["_ctx_username"] = context.get("_ctx_username")
                r["_ctx_hashtag"] = context.get("_ctx_hashtag")
                yield r

            if allowed < len(replies) or not data.get("has_next_page"):
                break
            cursor = data.get("next_cursor") or ""
            if not cursor:
                break


class TweetQuotesStream(TwitterAPIStream):
    """Quotes for parent tweet (budgeted as 'child')."""
    name = "tweet_quotes"
    parent_stream_type = UserTweetsStream
    state_partitioning_keys = ["parentTweetId"]

    schema = _tweet_schema_properties(include_ctx=True, include_parent=True).to_dict()
    schema["additionalProperties"] = True

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        if not context or not context.get("tweetId"):
            return []
        tweet_id = str(context["tweetId"])

        include_replies = bool(self.config.get("quotes_include_replies", True))
        since_unix = self._start_unix()
        until_unix = self._until_unix() or self._now_unix()

        cursor = ""
        while True:
            params = {"tweetId": tweet_id, "cursor": cursor, "includeReplies": include_replies}
            if since_unix:
                params["sinceTime"] = since_unix
            if until_unix:
                params["untilTime"] = until_unix

            data = self._http_get("/twitter/tweet/quotes", params)
            quotes = data.get("tweets") or []
            if not quotes:
                break

            allowed = self._consume_and_check(len(quotes), category="child")
            if allowed <= 0:
                return

            for q in quotes[:allowed]:
                q["parentTweetId"] = tweet_id
                q["_ctx_username"] = context.get("_ctx_username")
                q["_ctx_hashtag"] = context.get("_ctx_hashtag")
                yield q

            if allowed < len(quotes) or not data.get("has_next_page"):
                break
            cursor = data.get("next_cursor") or ""
            if not cursor:
                break


# ============================================================
# Refresh stream (one-time at +48h)
# ============================================================

class TweetRefreshStream(TwitterAPIStream):
    """Rehydrate tweets exactly once, when due (createdAt + refresh_delay_hours)."""
    name = "tweet_refresh"

    schema = _tweet_schema_properties(include_ctx=False).to_dict()
    schema["additionalProperties"] = True

    def _load_refresh_state(self) -> Dict[str, Any]:
        state = dict(self.tap.state or {})
        return state.get("tweet_refresh") or {"queue": {}, "done": {}}

    def _save_refresh_state(self, rstate: Dict[str, Any]) -> None:
        state = dict(self.tap.state or {})
        state["tweet_refresh"] = rstate
        self.tap.state = state

    def _pick_due_ids(self, rstate: Dict[str, Any], max_ids: int) -> List[str]:
        now_u = self._now_unix()
        due = []
        for tid, meta in rstate.get("queue", {}).items():
            due_ts = int(meta.get("due") or 0)
            if due_ts and due_ts <= now_u:
                due.append(tid)
            if len(due) >= max_ids:
                break
        return due

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        batch_size = int(self.config.get("lookup_batch_size", 50))

        rstate = self._load_refresh_state()
        if not rstate.get("queue"):
            return []

        # while budget allows and there are due ids, fetch in batches
        while True:
            # respect budget for "refresh"
            max_batch_allowed = self._consume_and_check(batch_size, category="refresh")
            if max_batch_allowed <= 0:
                break

            ids = self._pick_due_ids(rstate, max_batch_allowed)
            if not ids:
                break

            # call lookup
            params = {"tweet_ids": ",".join(ids)}
            data = self._http_get("/twitter/tweets", params)
            tweets = data.get("tweets") or []

            # Emit
            for t in tweets:
                yield t

            # Mark done (only for those returned) and prune from queue
            now_ts = self._now_unix()
            done = rstate.get("done", {})
            for t in tweets:
                tid = str(t.get("id"))
                rstate["queue"].pop(tid, None)
                done[tid] = {"ts": now_ts}
            rstate["done"] = done

            self._save_refresh_state(rstate)

            # If fewer than requested returned, remove remaining ids after 3 attempts to avoid infinite loop
            returned_ids = {str(t.get("id")) for t in tweets}
            missing = [i for i in ids if i not in returned_ids]
            if missing:
                for mid in missing:
                    meta = rstate["queue"].get(mid) or {}
                    meta["attempts"] = int(meta.get("attempts", 0)) + 1
                    # backoff 24h for next try
                    meta["due"] = self._now_unix() + 24 * 3600
                    if meta["attempts"] >= 3:
                        # give up gracefully
                        rstate["queue"].pop(mid, None)
                        rstate["done"][mid] = {"ts": now_ts, "status": "gave_up"}
                    else:
                        rstate["queue"][mid] = meta
                self._save_refresh_state(rstate)

            if len(tweets) < max_batch_allowed:
                # might still have more due; continue
                continue
