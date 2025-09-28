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
# Runtime credit/budget manager with hard splits
# ============================================================

@dataclass
class _Buckets:
    # parent buckets
    parent_user: int
    parent_hashtag: int
    parent_mention: int
    # child buckets
    child_replies: int
    child_quotes: int
    # refresh
    refresh: int


class CreditManager:
    """
    Compute a per-run tweet budget from monthly credits (NOT per-day),
    split 50% parents / 50% children, then:
      - parents: 60% usernames, 40% hashtags (mentions optional)
      - children: 50% replies, 50% quotes
    Enforce fair-share across username partitions and hashtag partitions separately.
    """
    def __init__(self, tap: "TapTwitter"):
        cfg = tap.config

        # Costs
        self.credits_per_tweet = int(cfg.get("credits_per_tweet", 15))
        monthly_quota = int(cfg.get("monthly_credit_quota", 10_500_000))
        budget_pct = float(cfg.get("monthly_credit_budget_pct", 0.65))  # 65% for the MONTH
        runs_per_day = float(cfg.get("runs_per_day", 1.0))

        # calendar split -> per-run credits -> per-run tweet budget
        now = datetime.now(timezone.utc)
        days_in_month = max(calendar.monthrange(now.year, now.month)[1], 1)
        monthly_budget_credits = int(monthly_quota * budget_pct)
        daily_budget_credits = monthly_budget_credits // days_in_month
        per_run_credits = int(daily_budget_credits / max(runs_per_day, 1.0))
        self.per_run_tweet_budget = max(per_run_credits // max(self.credits_per_tweet, 1), 0)

        # Top-level split: parents vs children vs refresh
        parent_ratio = float(cfg.get("parent_ratio", 0.50))
        child_ratio = float(cfg.get("child_ratio", 0.50))
        refresh_ratio = float(cfg.get("refresh_ratio", 0.00))  # refresh draws from parent/child? keep separate 0 by default
        total_top = parent_ratio + child_ratio + refresh_ratio or 1.0

        parent_total = int(self.per_run_tweet_budget * parent_ratio / total_top)
        child_total = int(self.per_run_tweet_budget * child_ratio / total_top)
        refresh_total = int(self.per_run_tweet_budget * refresh_ratio / total_top)

        # Parent split: usernames vs hashtags (mentions optional)
        user_weight = float(cfg.get("parent_usernames_weight", 0.60))
        hashtag_weight = float(cfg.get("parent_hashtags_weight", 0.40))
        mention_weight = float(cfg.get("parent_mentions_weight", 0.00))
        total_parent_w = max(user_weight + hashtag_weight + mention_weight, 1e-9)

        parent_user = int(parent_total * user_weight / total_parent_w)
        parent_hashtag = int(parent_total * hashtag_weight / total_parent_w)
        parent_mention = int(parent_total * mention_weight / total_parent_w)

        # Children split: replies vs quotes
        replies_weight = float(cfg.get("child_replies_weight", 0.50))
        quotes_weight = float(cfg.get("child_quotes_weight", 0.50))
        total_child_w = max(replies_weight + quotes_weight, 1e-9)

        child_replies = int(child_total * replies_weight / total_child_w)
        child_quotes = int(child_total * quotes_weight / total_child_w)

        self.buckets = _Buckets(
            parent_user=parent_user,
            parent_hashtag=parent_hashtag,
            parent_mention=parent_mention,
            child_replies=child_replies,
            child_quotes=child_quotes,
            refresh=refresh_total,
        )

        # partition accounting (separate for user/hashtag/mention)
        self.user_partitions_total = 0
        self.user_partitions_left = 0

        self.hashtag_partitions_total = 0
        self.hashtag_partitions_left = 0

        self.mention_partitions_total = 0
        self.mention_partitions_left = 0

    # ---- registration ----
    def register_username_partitions(self, count: int) -> None:
        if count > 0:
            self.user_partitions_total += count
            self.user_partitions_left += count

    def register_hashtag_partitions(self, count: int) -> None:
        if count > 0:
            self.hashtag_partitions_total += count
            self.hashtag_partitions_left += count

    def register_mention_partitions(self, count: int) -> None:
        if count > 0:
            self.mention_partitions_total += count
            self.mention_partitions_left += count

    # ---- per-partition allowances (even split inside each group) ----
    def _allowance_even(self, remaining_budget: int, remaining_parts: int) -> int:
        if remaining_parts <= 0 or remaining_budget <= 0:
            return 0
        if remaining_budget < remaining_parts:
            return 1  # trickle: guarantee everyone gets at least 1 if anything remains
        return remaining_budget // remaining_parts

    def allowance_for_next_username_partition(self) -> int:
        alw = self._allowance_even(self.buckets.parent_user, self.user_partitions_left)
        self.user_partitions_left = max(self.user_partitions_left - 1, 0)
        return alw

    def allowance_for_next_hashtag_partition(self) -> int:
        alw = self._allowance_even(self.buckets.parent_hashtag, self.hashtag_partitions_left)
        self.hashtag_partitions_left = max(self.hashtag_partitions_left - 1, 0)
        return alw

    def allowance_for_next_mention_partition(self) -> int:
        alw = self._allowance_even(self.buckets.parent_mention, self.mention_partitions_left)
        self.mention_partitions_left = max(self.mention_partitions_left - 1, 0)
        return alw

    # ---- consumption ----
    def consume_tweets(self, count: int, category: str) -> int:
        if count <= 0:
            return 0
        # parent buckets
        if category == "parent_user":
            allowed = min(count, self.buckets.parent_user)
            self.buckets = _Buckets( self.buckets.parent_user - allowed, self.buckets.parent_hashtag,
                                     self.buckets.parent_mention, self.buckets.child_replies,
                                     self.buckets.child_quotes, self.buckets.refresh )
            return allowed
        if category == "parent_hashtag":
            allowed = min(count, self.buckets.parent_hashtag)
            self.buckets = _Buckets( self.buckets.parent_user, self.buckets.parent_hashtag - allowed,
                                     self.buckets.parent_mention, self.buckets.child_replies,
                                     self.buckets.child_quotes, self.buckets.refresh )
            return allowed
        if category == "parent_mention":
            allowed = min(count, self.buckets.parent_mention)
            self.buckets = _Buckets( self.buckets.parent_user, self.buckets.parent_hashtag,
                                     self.buckets.parent_mention - allowed, self.buckets.child_replies,
                                     self.buckets.child_quotes, self.buckets.refresh )
            return allowed

        # child buckets
        if category == "child_replies":
            allowed = min(count, self.buckets.child_replies)
            self.buckets = _Buckets( self.buckets.parent_user, self.buckets.parent_hashtag,
                                     self.buckets.parent_mention, self.buckets.child_replies - allowed,
                                     self.buckets.child_quotes, self.buckets.refresh )
            return allowed
        if category == "child_quotes":
            allowed = min(count, self.buckets.child_quotes)
            self.buckets = _Buckets( self.buckets.parent_user, self.buckets.parent_hashtag,
                                     self.buckets.parent_mention, self.buckets.child_replies,
                                     self.buckets.child_quotes - allowed, self.buckets.refresh )
            return allowed

        # refresh
        if category == "refresh":
            allowed = min(count, self.buckets.refresh)
            self.buckets = _Buckets( self.buckets.parent_user, self.buckets.parent_hashtag,
                                     self.buckets.parent_mention, self.buckets.child_replies,
                                     self.buckets.child_quotes, self.buckets.refresh - allowed )
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
        th.Property("children_max_per_parent", th.IntegerType, default=50),

        # One-time refresh control
        th.Property("refresh_delay_hours", th.IntegerType, default=48,
                    description="Refresh exactly once this many hours after createdAt."),
        th.Property("lookup_batch_size", th.IntegerType, default=50,
                    description="Batch size for refresh lookup calls."),

        # Credit/budget model (MONTHLY -> per-run)
        th.Property("credits_per_tweet", th.IntegerType, default=15,
                    description="Provider credits per returned tweet."),
        th.Property("monthly_credit_quota", th.IntegerType, default=10_500_000,
                    description="Total credits per month."),
        th.Property("monthly_credit_budget_pct", th.NumberType, default=0.65,
                    description="Fraction of monthly credits to spend over the month (0-1)."),
        th.Property("runs_per_day", th.NumberType, default=1.0,
                    description="How many tap runs per day."),

        # Top-level split
        th.Property("parent_ratio", th.NumberType, default=0.50, description="Budget share for parents."),
        th.Property("child_ratio", th.NumberType, default=0.50, description="Budget share for children."),
        th.Property("refresh_ratio", th.NumberType, default=0.00, description="Optional share for refresh lookups (usually 0; refresh is cheap)."),

        # Parent split
        th.Property("parent_usernames_weight", th.NumberType, default=0.60, description="Share of parent budget for usernames."),
        th.Property("parent_hashtags_weight", th.NumberType, default=0.40, description="Share for hashtags."),
        th.Property("parent_mentions_weight", th.NumberType, default=0.00, description="Share for mentions (default 0)."),

        # Children split
        th.Property("child_replies_weight", th.NumberType, default=0.50, description="Share of child budget for replies."),
        th.Property("child_quotes_weight", th.NumberType, default=0.50, description="Share for quotes."),
    ).to_dict()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # shared runtime/budget manager
        self.runtime = CreditManager(self)

    def discover_streams(self) -> List[Stream]:
        streams: List[Stream] = [
            HashtagTweetsStream(self),
            MentionsStream(self),     # optional; default budget is 0
            UserTweetsStream(self),
            TweetRepliesStream(self),
            TweetQuotesStream(self),
            TweetRefreshStream(self), # one-time +48h rehydrate
        ]
        if not self.config.get("enable_children", True):
            for s in streams:
                if s.name in {"tweet_replies", "tweet_quotes"}:
                    s.selected = False
        return streams


if __name__ == "__main__":
    TapTwitter.cli()
