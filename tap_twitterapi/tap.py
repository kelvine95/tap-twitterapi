from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Type

from singer_sdk import Tap, Stream
from singer_sdk import typing as th

from .streams import (
    HashtagTweetsStream,
    MentionsStream,
    UserTweetsStream,
    TweetRepliesStream,
    TweetQuotesStream,
    TweetRefreshStream,
)


# ============================================================
# Runtime credit/budget manager
# ============================================================

@dataclass
class _Buckets:
    parents: int
    children: int
    refresh: int


class CreditManager:
    """
    Converts monthly credit quota * budget_pct into a per-run tweet budget,
    splits into buckets (parents/children/refresh), then allocates fairly
    across parent partitions (usernames + hashtags + mentions).
    """
    def __init__(self, tap: "TapTwitter"):
        cfg = tap.config

        self.credits_per_tweet = int(cfg.get("credits_per_tweet", 15))
        monthly_quota = int(cfg.get("monthly_credit_quota", 10_500_000))
        budget_pct = float(cfg.get("monthly_credit_budget_pct", 0.65))
        runs_per_day = float(cfg.get("runs_per_day", 1.0))

        # Compute per-run *tweet* budget (not credits)
        # days in current month
        now = datetime.now(timezone.utc)
        days_in_month = calendar.monthrange(now.year, now.month)[1]
        monthly_budget_credits = int(monthly_quota * budget_pct)
        daily_budget_credits = monthly_budget_credits // days_in_month
        per_run_credits = int(daily_budget_credits / max(runs_per_day, 1.0))
        self.per_run_tweet_budget = max(per_run_credits // max(self.credits_per_tweet, 1), 0)

        # Split into buckets
        parents_ratio = float(cfg.get("parent_budget_ratio", 0.75))   # default 75%
        children_ratio = float(cfg.get("child_budget_ratio", 0.15))   # default 15%
        refresh_ratio = float(cfg.get("refresh_budget_ratio", 0.10))  # default 10%

        # Normalize if needed
        total_ratio = parents_ratio + children_ratio + refresh_ratio
        if total_ratio <= 0:
            parents_ratio, children_ratio, refresh_ratio = 1.0, 0.0, 0.0
            total_ratio = 1.0

        parents = int(self.per_run_tweet_budget * parents_ratio / total_ratio)
        children = int(self.per_run_tweet_budget * children_ratio / total_ratio)
        refresh = int(self.per_run_tweet_budget * refresh_ratio / total_ratio)

        self.buckets = _Buckets(parents=parents, children=children, refresh=refresh)

        # Parent partitions (dynamic)
        self.total_parent_partitions = 0
        self.remaining_parent_partitions = 0

    # ---- parent partitions registration ----
    def register_parent_partitions(self, count: int) -> None:
        if count <= 0:
            return
        self.total_parent_partitions += count
        self.remaining_parent_partitions += count

    def allowance_for_next_parent_partition(self) -> int:
        """
        Fair-share remaining parent tweets across remaining parent partitions.
        """
        if self.remaining_parent_partitions <= 0:
            return 0
        remaining = self.buckets.parents
        if remaining <= 0:
            return 0
        allowance = max(remaining // self.remaining_parent_partitions, 0)
        # Ensure at least a trickle if we still have some budget
        allowance = max(allowance, 0)
        # Reserve doesn't decrement here; consumption happens in consume_tweets
        # Decrement remaining partitions now that this partition starts
        self.remaining_parent_partitions -= 1
        return allowance

    # ---- consumption ----
    def consume_tweets(self, count: int, category: str) -> int:
        """
        Consume up to 'count' tweets from the appropriate bucket.
        Returns how many we actually allow.
        """
        if count <= 0:
            return 0
        if category == "parent":
            bucket_val = self.buckets.parents
            allowed = min(count, bucket_val)
            self.buckets = _Buckets(self.buckets.parents - allowed, self.buckets.children, self.buckets.refresh)
            return allowed
        elif category == "child":
            bucket_val = self.buckets.children
            allowed = min(count, bucket_val)
            self.buckets = _Buckets(self.buckets.parents, self.buckets.children - allowed, self.buckets.refresh)
            return allowed
        elif category == "refresh":
            bucket_val = self.buckets.refresh
            allowed = min(count, bucket_val)
            self.buckets = _Buckets(self.buckets.parents, self.buckets.children, self.buckets.refresh - allowed)
            return allowed
        else:
            # unknown category: deny
            return 0


# ============================================================
# Tap
# ============================================================

class TapTwitter(Tap):
    """Singer Tap for twitterapi.io."""
    name = "tap-twitterapi"

    # Config schema
    config_jsonschema = th.PropertiesList(
        th.Property("api_key", th.StringType, required=True),

        # Pull targets
        th.Property("usernames", th.ArrayType(th.StringType), description="Screen names to track."),
        th.Property("hashtags", th.ArrayType(th.StringType), description="Hashtags (no #)."),

        # Time bounds
        th.Property("since_time", th.StringType, description="ISO8601 lower bound for createdAt (optional)."),
        th.Property("until_time", th.StringType, description="ISO8601 upper bound (optional)."),
        th.Property("min_tweet_age_hours", th.IntegerType, default=0,
                    description="If >0, cap 'until' to now - this many hours."),

        # Endpoint toggles
        th.Property("last_tweets_include_replies", th.BooleanType, default=False),
        th.Property("quotes_include_replies", th.BooleanType, default=True),
        th.Property("enable_children", th.BooleanType, default=True),

        # One-time refresh control
        th.Property("refresh_delay_hours", th.IntegerType, default=48,
                    description="Refresh a tweet exactly once this many hours after createdAt."),
        th.Property("lookup_batch_size", th.IntegerType, default=50,
                    description="Batch size for tweet refresh lookups."),

        # Budget/credit model
        th.Property("credits_per_tweet", th.IntegerType, default=15,
                    description="Provider credits charged per returned tweet."),
        th.Property("monthly_credit_quota", th.IntegerType, default=10_500_000,
                    description="Total credits available per month."),
        th.Property("monthly_credit_budget_pct", th.NumberType, default=0.65,
                    description="Fraction of monthly credits to spend (0.0 - 1.0)."),
        th.Property("runs_per_day", th.NumberType, default=1.0,
                    description="How many tap runs per day."),
        th.Property("parent_budget_ratio", th.NumberType, default=0.75,
                    description="Share of per-run tweet budget for parents."),
        th.Property("child_budget_ratio", th.NumberType, default=0.15,
                    description="Share of per-run tweet budget for children."),
        th.Property("refresh_budget_ratio", th.NumberType, default=0.10,
                    description="Share of per-run tweet budget for refreshes."),
    ).to_dict()

    # Expose a runtime manager shared by all streams
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.runtime = CreditManager(self)

    def discover_streams(self) -> List[Stream]:
        streams: List[Stream] = [
            HashtagTweetsStream(self),
            MentionsStream(self),
            UserTweetsStream(self),
            TweetRepliesStream(self),
            TweetQuotesStream(self),
            TweetRefreshStream(self),   # new
        ]

        if not self.config.get("enable_children", True):
            for s in streams:
                if s.name in {"tweet_replies", "tweet_quotes"}:
                    s.selected = False
        return streams


if __name__ == "__main__":
    TapTwitter.cli()
