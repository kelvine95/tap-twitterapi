from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List

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
    Turn monthly credits * pct into a per-run tweet budget, split into buckets,
    and fair-share parents across all parent partitions (usernames + hashtags + mentions).
    """
    def __init__(self, tap: "TapTwitter"):
        cfg = tap.config

        self.credits_per_tweet = int(cfg.get("credits_per_tweet", 15))
        monthly_quota = int(cfg.get("monthly_credit_quota", 10_500_000))
        budget_pct = float(cfg.get("monthly_credit_budget_pct", 0.65))
        runs_per_day = float(cfg.get("runs_per_day", 1.0))

        now = datetime.now(timezone.utc)
        days_in_month = calendar.monthrange(now.year, now.month)[1]

        monthly_budget_credits = int(monthly_quota * budget_pct)
        daily_budget_credits = monthly_budget_credits // max(days_in_month, 1)
        per_run_credits = int(daily_budget_credits / max(runs_per_day, 1.0))
        self.per_run_tweet_budget = max(per_run_credits // max(self.credits_per_tweet, 1), 0)

        parents_ratio = float(cfg.get("parent_budget_ratio", 0.75))
        children_ratio = float(cfg.get("child_budget_ratio", 0.15))
        refresh_ratio = float(cfg.get("refresh_budget_ratio", 0.10))
        total_ratio = parents_ratio + children_ratio + refresh_ratio or 1.0

        parents = int(self.per_run_tweet_budget * parents_ratio / total_ratio)
        children = int(self.per_run_tweet_budget * children_ratio / total_ratio)
        refresh = int(self.per_run_tweet_budget * refresh_ratio / total_ratio)
        self.buckets = _Buckets(parents=parents, children=children, refresh=refresh)

        # parent partition accounting
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
        Evenly allocate remaining parent budget across remaining partitions.
        If budget < partitions, give 1 to as many partitions as possible.
        """
        if self.remaining_parent_partitions <= 0:
            return 0
        remaining_budget = self.buckets.parents
        if remaining_budget <= 0:
            self.remaining_parent_partitions -= 1
            return 0

        if remaining_budget < self.remaining_parent_partitions:
            allowance = 1  # trickle so everyone gets a shot
        else:
            allowance = remaining_budget // self.remaining_parent_partitions

        # mark that this partition has started
        self.remaining_parent_partitions -= 1
        return max(allowance, 1)

    # ---- consumption ----
    def consume_tweets(self, count: int, category: str) -> int:
        if count <= 0:
            return 0
        if category == "parent":
            allowed = min(count, self.buckets.parents)
            self.buckets = _Buckets(self.buckets.parents - allowed, self.buckets.children, self.buckets.refresh)
            return allowed
        if category == "child":
            allowed = min(count, self.buckets.children)
            self.buckets = _Buckets(self.buckets.parents, self.buckets.children - allowed, self.buckets.refresh)
            return allowed
        if category == "refresh":
            allowed = min(count, self.buckets.refresh)
            self.buckets = _Buckets(self.buckets.parents, self.buckets.children, self.buckets.refresh - allowed)
            return allowed
        return 0


# ============================================================
# Tap
# ============================================================

class TapTwitter(Tap):
    """Singer Tap for twitterapi.io."""
    name = "tap-twitterapi"

    config_jsonschema = th.PropertiesList(
        th.Property("api_key", th.StringType, required=True),

        # Pull targets
        th.Property("usernames", th.ArrayType(th.StringType), description="Screen names to track."),
        th.Property("hashtags", th.ArrayType(th.StringType), description="Hashtags to track (no #)."),

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
                    description="Refresh exactly once this many hours after createdAt."),
        th.Property("lookup_batch_size", th.IntegerType, default=50,
                    description="Batch size for refresh lookup calls."),

        # Budget/credit model
        th.Property("credits_per_tweet", th.IntegerType, default=15,
                    description="Provider credits per returned tweet."),
        th.Property("monthly_credit_quota", th.IntegerType, default=10_500_000,
                    description="Total credits per month."),
        th.Property("monthly_credit_budget_pct", th.NumberType, default=0.65,
                    description="Fraction of monthly credits to spend (0-1)."),
        th.Property("runs_per_day", th.NumberType, default=1.0,
                    description="How many tap runs per day."),
        th.Property("parent_budget_ratio", th.NumberType, default=0.75,
                    description="Share of per-run tweet budget for parent streams."),
        th.Property("child_budget_ratio", th.NumberType, default=0.15,
                    description="Share for children (replies/quotes)."),
        th.Property("refresh_budget_ratio", th.NumberType, default=0.10,
                    description="Share for one-time refreshes."),
    ).to_dict()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # shared runtime/budget manager
        self.runtime = CreditManager(self)

    def discover_streams(self) -> List[Stream]:
        streams: List[Stream] = [
            HashtagTweetsStream(self),
            MentionsStream(self),
            UserTweetsStream(self),
            TweetRepliesStream(self),
            TweetQuotesStream(self),
            TweetRefreshStream(self),
        ]
        if not self.config.get("enable_children", True):
            for s in streams:
                if s.name in {"tweet_replies", "tweet_quotes"}:
                    s.selected = False
        return streams


if __name__ == "__main__":
    TapTwitter.cli()
