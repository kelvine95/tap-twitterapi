"""Stream type classes for tap-twitter-api."""

import datetime
from datetime import timezone
from typing import Any, Dict, Optional, Iterable, List
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from singer_sdk.helpers.jsonpath import extract_jsonpath
from tap_twitterapi.auth import TwitterAuthenticator
import backoff
import requests

class TwitterStream(RESTStream):
    """Twitter stream base class."""

    url_base = "https://api.twitterapi.io"
    records_jsonpath = "$.tweets[*]"
    next_page_token_jsonpath = "$.next_cursor"

    @property
    def authenticator(self) -> TwitterAuthenticator:
        """Return the authenticator for Twitter API."""
        return TwitterAuthenticator(self, self.config["api_key"])

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if self.authenticator:
            headers.update(self.authenticator.auth_headers)
        return headers

    @staticmethod
    def _coerce_datetime_iso(dt_str: str) -> str:
        """Convert Twitter-style or ISO-ish strings to RFC3339 UTC (…Z).
        Returns the original value on failure.
        """
        if not isinstance(dt_str, str) or not dt_str.strip():
            return dt_str
        s = dt_str.strip()

        # Fast path: already ISO — normalize to Z
        if "T" in s:
            try:
                # allow both 'Z' and '+00:00'
                iso = datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
                return iso.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            except Exception:
                pass

        # Twitter-style: 'Mon Sep 15 00:12:16 +0000 2025'
        try:
            parsed = datetime.datetime.strptime(s, "%a %b %d %H:%M:%S %z %Y")
            return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        except Exception:
            return dt_str  # leave as-is if we can't parse
    
    def post_process(self, row: dict, context: dict | None) -> dict:
        """Normalize fields after parsing but before emission."""
        rec = dict(row)

        # Top-level createdAt
        if "createdAt" in rec and isinstance(rec["createdAt"], str):
            rec["createdAt"] = self._coerce_datetime_iso(rec["createdAt"])

        # Nested author.createdAt if present
        author = rec.get("author")
        if isinstance(author, dict) and isinstance(author.get("createdAt"), str):
            author["createdAt"] = self._coerce_datetime_iso(author["createdAt"])

        return rec

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return the URL parameters for the request."""
        params = {}
        if next_page_token:
            params["cursor"] = next_page_token
        else:
            params["cursor"] = ""  # First page uses empty string
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records."""
        json_response = response.json()
        yield from extract_jsonpath(self.records_jsonpath, json_response)

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Extract next page token from response."""
        json_response = response.json()
        if json_response.get("has_next_page"):
            return json_response.get("next_cursor")
        return None
    
    def request_decorator(self, func):
        """Apply backoff decorator to requests."""
        return backoff.on_exception(
            backoff.expo,
            (requests.exceptions.RequestException,),
            max_tries=5,
            factor=2,
        )(func)
    
    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Override to apply record limits."""
        max_records_per_entity = self.config.get("max_records_per_entity", 100)
        records_fetched = 0
        
        for record in super().get_records(context):
            if records_fetched >= max_records_per_entity:
                break
            records_fetched += 1
            yield record


class UserInfoStream(TwitterStream):
    """Stream for user info."""

    name = "users"
    path = "/twitter/user/info"
    records_jsonpath = "$.data"
    primary_keys = ["id"]
    replication_key = None
    
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("userName", th.StringType),
        th.Property("name", th.StringType),
        th.Property("url", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("followers", th.IntegerType),
        th.Property("following", th.IntegerType),
        th.Property("isBlueVerified", th.BooleanType),
        th.Property("profilePicture", th.StringType),
        th.Property("description", th.StringType),
        th.Property("location", th.StringType),
        # Add metadata for tracking
        th.Property("_tap_username", th.StringType),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Get records for all usernames."""
        for username in self.config.get("usernames", []):
            user_context = {"username": username}
            for record in super().get_records(user_context):
                record["_tap_username"] = username
                yield record

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return the URL parameters for the request."""
        params = {}
        if context and "username" in context:
            params["userName"] = context["username"]
        return params

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response for user info endpoint."""
        json_response = response.json()
        if "data" in json_response:
            yield json_response["data"]


class UserTweetsStream(TwitterStream):
    """Stream for user tweets using advanced search."""

    name = "user_tweets"
    path = "/twitter/tweet/advanced_search"
    primary_keys = ["id"]
    replication_key = "createdAt"
    state_partitioning_keys = ["_tap_username"]
    
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("url", th.StringType),
        th.Property("text", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("lang", th.StringType),
        th.Property("likeCount", th.IntegerType),
        th.Property("retweetCount", th.IntegerType),
        th.Property("replyCount", th.IntegerType),
        th.Property("quoteCount", th.IntegerType),
        th.Property("viewCount", th.IntegerType),
        th.Property("bookmarkCount", th.IntegerType),
        th.Property("isReply", th.BooleanType),
        th.Property("inReplyToId", th.StringType),
        th.Property("conversationId", th.StringType),
        th.Property(
            "author",
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("userName", th.StringType),
                th.Property("name", th.StringType),
            ),
        ),
        th.Property(
            "entities",
            th.ObjectType(
                th.Property(
                    "hashtags",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("text", th.StringType),
                        )
                    ),
                ),
                th.Property(
                    "urls",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("url", th.StringType),
                            th.Property("expanded_url", th.StringType),
                        )
                    ),
                ),
            ),
        ),
        # Add metadata for tracking
        th.Property("_tap_username", th.StringType),
        th.Property("_tap_stream_type", th.StringType),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Get records for all usernames."""
        for username in self.config.get("usernames", []):
            user_context = {"username": username}
            max_records = self.config.get("max_records_per_entity", 100)
            count = 0
            for record in super().get_records(user_context):
                if count >= max_records:
                    break
                record["_tap_username"] = username
                record["_tap_stream_type"] = "user_tweet"
                count += 1
                yield record

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return the URL parameters for the request."""
        params = super().get_url_params(context, next_page_token)
        
        if context and "username" in context:
            start_date = self.get_starting_timestamp(context)
            if start_date:
                since_str = start_date.strftime("%Y-%m-%d_%H:%M:%S_UTC")
                params["query"] = f"from:{context['username']} since:{since_str}"
            else:
                # Default to 30 days ago if no start date
                since_date = datetime.datetime.now() - datetime.timedelta(days=30)
                since_str = since_date.strftime("%Y-%m-%d_%H:%M:%S_UTC")
                params["query"] = f"from:{context['username']} since:{since_str}"
            params["queryType"] = "Latest"
        
        return params


class MentionsStream(TwitterStream):
    """Stream for user mentions."""

    name = "mentions"
    path = "/twitter/user/mentions"
    primary_keys = ["id"]
    replication_key = "createdAt"
    state_partitioning_keys = ["_tap_username"]
    
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("url", th.StringType),
        th.Property("text", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("lang", th.StringType),
        th.Property("likeCount", th.IntegerType),
        th.Property("retweetCount", th.IntegerType),
        th.Property("replyCount", th.IntegerType),
        th.Property("quoteCount", th.IntegerType),
        th.Property("viewCount", th.IntegerType),
        th.Property("bookmarkCount", th.IntegerType),
        th.Property("isReply", th.BooleanType),
        th.Property("inReplyToId", th.StringType),
        th.Property("conversationId", th.StringType),
        th.Property(
            "author",
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("userName", th.StringType),
                th.Property("name", th.StringType),
            ),
        ),
        th.Property(
            "entities",
            th.ObjectType(
                th.Property(
                    "hashtags",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("text", th.StringType),
                        )
                    ),
                ),
            ),
        ),
        # Add metadata for tracking
        th.Property("_tap_username", th.StringType),
        th.Property("_tap_stream_type", th.StringType),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Get records for all usernames."""
        for username in self.config.get("usernames", []):
            user_context = {"username": username}
            max_records = self.config.get("max_records_per_entity", 100)
            count = 0
            for record in super().get_records(user_context):
                if count >= max_records:
                    break
                record["_tap_username"] = username
                record["_tap_stream_type"] = "mention"
                count += 1
                yield record

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return the URL parameters for the request."""
        params = super().get_url_params(context, next_page_token)
        
        if context and "username" in context:
            params["userName"] = context["username"]
            
            start_date = self.get_starting_timestamp(context)
            if start_date:
                params["sinceTime"] = int(start_date.timestamp())
        
        return params


class HashtagTweetsStream(TwitterStream):
    """Stream for hashtag tweets."""

    name = "hashtag_tweets"
    path = "/twitter/tweet/advanced_search"
    primary_keys = ["id"]
    replication_key = "createdAt"
    state_partitioning_keys = ["_tap_hashtag"]
    
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("url", th.StringType),
        th.Property("text", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("lang", th.StringType),
        th.Property("likeCount", th.IntegerType),
        th.Property("retweetCount", th.IntegerType),
        th.Property("replyCount", th.IntegerType),
        th.Property("quoteCount", th.IntegerType),
        th.Property("viewCount", th.IntegerType),
        th.Property("bookmarkCount", th.IntegerType),
        th.Property("isReply", th.BooleanType),
        th.Property("inReplyToId", th.StringType),
        th.Property("conversationId", th.StringType),
        th.Property(
            "author",
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("userName", th.StringType),
                th.Property("name", th.StringType),
            ),
        ),
        th.Property(
            "entities",
            th.ObjectType(
                th.Property(
                    "hashtags",
                    th.ArrayType(
                        th.ObjectType(
                            th.Property("text", th.StringType),
                        )
                    ),
                ),
            ),
        ),
        # Add metadata for tracking
        th.Property("_tap_hashtag", th.StringType),
        th.Property("_tap_stream_type", th.StringType),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Get records for all hashtags."""
        for hashtag in self.config.get("hashtags", []):
            hashtag_context = {"hashtag": hashtag}
            max_records = self.config.get("max_records_per_entity", 100)
            count = 0
            for record in super().get_records(hashtag_context):
                if count >= max_records:
                    break
                record["_tap_hashtag"] = hashtag
                record["_tap_stream_type"] = "hashtag_tweet"
                count += 1
                yield record

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return the URL parameters for the request."""
        params = super().get_url_params(context, next_page_token)
        
        if context and "hashtag" in context:
            start_date = self.get_starting_timestamp(context)
            if start_date:
                since_str = start_date.strftime("%Y-%m-%d_%H:%M:%S_UTC")
                params["query"] = f"#{context['hashtag']} since:{since_str}"
            else:
                # Default to 30 days ago if no start date
                since_date = datetime.datetime.now() - datetime.timedelta(days=30)
                since_str = since_date.strftime("%Y-%m-%d_%H:%M:%S_UTC")
                params["query"] = f"#{context['hashtag']} since:{since_str}"
            params["queryType"] = "Latest"
        
        return params


class TweetRepliesStream(TwitterStream):
    """Stream for tweet replies."""

    name = "tweet_replies"
    path = "/twitter/tweet/replies"
    records_jsonpath = "$.replies[*]"
    primary_keys = ["id"]
    replication_key = "createdAt"
    parent_stream_type = UserTweetsStream  # Will pull tweet IDs from parent streams
    
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("url", th.StringType),
        th.Property("text", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("lang", th.StringType),
        th.Property("likeCount", th.IntegerType),
        th.Property("retweetCount", th.IntegerType),
        th.Property("replyCount", th.IntegerType),
        th.Property("quoteCount", th.IntegerType),
        th.Property("viewCount", th.IntegerType),
        th.Property("bookmarkCount", th.IntegerType),
        th.Property("isReply", th.BooleanType),
        th.Property("inReplyToId", th.StringType),
        th.Property("conversationId", th.StringType),
        th.Property(
            "author",
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("userName", th.StringType),
                th.Property("name", th.StringType),
            ),
        ),
        # Add metadata for tracking
        th.Property("_tap_parent_tweet_id", th.StringType),
        th.Property("_tap_stream_type", th.StringType),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Get replies for tweets from parent streams."""
        # Get tweet IDs from parent streams (stored in state)
        parent_tweet_ids = self._get_parent_tweet_ids()
        
        max_replies_per_tweet = self.config.get("max_replies_per_tweet", 20)
        max_parent_tweets = self.config.get("max_parent_tweets_per_run", 10)
        
        for tweet_id in parent_tweet_ids[:max_parent_tweets]:
            tweet_context = {"tweet_id": tweet_id}
            count = 0
            for record in super().get_records(tweet_context):
                if count >= max_replies_per_tweet:
                    break
                record["_tap_parent_tweet_id"] = tweet_id
                record["_tap_stream_type"] = "reply"
                count += 1
                yield record

    def _get_parent_tweet_ids(self) -> List[str]:
        """Get tweet IDs from the state of parent streams."""
        # This would be populated from the state of parent streams
        # For now, return empty list - in production, this would read from state
        return []

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return the URL parameters for the request."""
        params = super().get_url_params(context, next_page_token)
        
        if context and "tweet_id" in context:
            params["tweetId"] = context["tweet_id"]
            
            start_date = self.get_starting_timestamp(context)
            if start_date:
                params["sinceTime"] = int(start_date.timestamp())
        
        return params


class TweetQuotesStream(TwitterStream):
    """Stream for tweet quotes."""

    name = "tweet_quotes"
    path = "/twitter/tweet/quotes"
    primary_keys = ["id"]
    replication_key = "createdAt"
    parent_stream_type = UserTweetsStream  # Will pull tweet IDs from parent streams
    
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("url", th.StringType),
        th.Property("text", th.StringType),
        th.Property("createdAt", th.DateTimeType),
        th.Property("lang", th.StringType),
        th.Property("likeCount", th.IntegerType),
        th.Property("retweetCount", th.IntegerType),
        th.Property("replyCount", th.IntegerType),
        th.Property("quoteCount", th.IntegerType),
        th.Property("viewCount", th.IntegerType),
        th.Property("bookmarkCount", th.IntegerType),
        th.Property("isReply", th.BooleanType),
        th.Property("inReplyToId", th.StringType),
        th.Property("conversationId", th.StringType),
        th.Property(
            "author",
            th.ObjectType(
                th.Property("id", th.StringType),
                th.Property("userName", th.StringType),
                th.Property("name", th.StringType),
            ),
        ),
        # Add metadata for tracking
        th.Property("_tap_parent_tweet_id", th.StringType),
        th.Property("_tap_stream_type", th.StringType),
    ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Get quotes for tweets from parent streams."""
        # Get tweet IDs from parent streams (stored in state)
        parent_tweet_ids = self._get_parent_tweet_ids()
        
        max_quotes_per_tweet = self.config.get("max_quotes_per_tweet", 20)
        max_parent_tweets = self.config.get("max_parent_tweets_per_run", 10)
        
        for tweet_id in parent_tweet_ids[:max_parent_tweets]:
            tweet_context = {"tweet_id": tweet_id}
            count = 0
            for record in super().get_records(tweet_context):
                if count >= max_quotes_per_tweet:
                    break
                record["_tap_parent_tweet_id"] = tweet_id
                record["_tap_stream_type"] = "quote"
                count += 1
                yield record

    def _get_parent_tweet_ids(self) -> List[str]:
        """Get tweet IDs from the state of parent streams."""
        # This would be populated from the state of parent streams
        # For now, return empty list - in production, this would read from state
        return []

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return the URL parameters for the request."""
        params = super().get_url_params(context, next_page_token)
        
        if context and "tweet_id" in context:
            params["tweetId"] = context["tweet_id"]
            params["includeReplies"] = True
            
            start_date = self.get_starting_timestamp(context)
            if start_date:
                params["sinceTime"] = int(start_date.timestamp())
        
        return params
    