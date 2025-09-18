"""Twitter tap class."""

from typing import List
from singer_sdk import Tap, Stream
from singer_sdk import typing as th
from tap_twitter_api.streams import (
    UserInfoStream,
    UserTweetsStream,
    MentionsStream,
    HashtagTweetsStream,
    TweetRepliesStream,
    TweetQuotesStream,
)


class TapTwitter(Tap):
    """Twitter tap class."""

    name = "tap-twitter-api"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,
            description="The API key to authenticate against the Twitter API",
        ),
        th.Property(
            "usernames",
            th.ArrayType(th.StringType),
            required=True,
            description="List of usernames to track",
        ),
        th.Property(
            "hashtags",
            th.ArrayType(th.StringType),
            default=[],
            description="List of hashtags to track",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            default="2025-01-01T00:00:00Z",
            description="The earliest record date to sync",
        ),
        # Limits to prevent API credit burn
        th.Property(
            "max_records_per_entity",
            th.IntegerType,
            default=100,
            description="Maximum number of records to return per username/hashtag per stream",
        ),
        th.Property(
            "max_replies_per_tweet",
            th.IntegerType,
            default=20,
            description="Maximum number of replies to fetch per tweet",
        ),
        th.Property(
            "max_quotes_per_tweet",
            th.IntegerType,
            default=20,
            description="Maximum number of quotes to fetch per tweet",
        ),
        th.Property(
            "max_parent_tweets_per_run",
            th.IntegerType,
            default=10,
            description="Maximum number of parent tweets to process for replies/quotes per run",
        ),
        th.Property(
            "enable_child_streams",
            th.BooleanType,
            default=True,
            description="Enable fetching replies and quotes for tweets",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        streams = [
            UserInfoStream(tap=self),
            UserTweetsStream(tap=self),
            MentionsStream(tap=self),
            HashtagTweetsStream(tap=self),
        ]
        
        # Only add child streams if enabled
        if self.config.get("enable_child_streams", True):
            streams.extend([
                TweetRepliesStream(tap=self),
                TweetQuotesStream(tap=self),
            ])
        
        return streams


if __name__ == "__main__":
    TapTwitter.cli()
    