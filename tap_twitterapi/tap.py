from __future__ import annotations
from typing import List, Type
from singer_sdk import Tap, Stream
from singer_sdk import typing as th
from .streams import (
    HashtagTweetsStream,
    MentionsStream,
    UserTweetsStream,
    TweetRepliesStream,
    TweetQuotesStream,
)


class TapTwitter(Tap):
    """Singer Tap for twitterapi.io."""
    
    name = "tap-twitterapi"
    
    # Config schema exposed via --about / --discover
    config_jsonschema = th.PropertiesList(
        th.Property("api_key", th.StringType, required=True),
        th.Property("usernames", th.ArrayType(th.StringType), description="List of screen names to track."),
        th.Property("hashtags", th.ArrayType(th.StringType), description="List of hashtags to track, no #."),
        th.Property("since_time", th.StringType, description="ISO8601 lower bound for createdAt (optional)."),
        th.Property("until_time", th.StringType, description="ISO8601 upper bound (optional)."),
        # Local/test cost controls
        th.Property("max_pages_per_partition", th.IntegerType, default=1),
        th.Property("max_parent_tweets", th.IntegerType, default=1000),
        th.Property("max_records_per_partition", th.IntegerType, default=1000),
        th.Property("children_max_per_parent", th.IntegerType, default=1000),
        # Endpoint toggles
        th.Property("last_tweets_include_replies", th.BooleanType, default=False),
        th.Property("quotes_include_replies", th.BooleanType, default=True),
        th.Property("enable_children", th.BooleanType, default=True),
    ).to_dict()
    
    def discover_streams(self) -> List[Stream]:
        streams: List[Stream] = [
            HashtagTweetsStream(self),
            MentionsStream(self),
            UserTweetsStream(self),
            TweetRepliesStream(self),
            TweetQuotesStream(self),
        ]
        # If children disabled, deselect them at discovery time (still catalog-visible).
        if not self.config.get("enable_children", True):
            for s in streams:
                if s.name in {"tweet_replies", "tweet_quotes"}:
                    s.selected = False
        return streams


if __name__ == "__main__":
    TapTwitter.cli()
    