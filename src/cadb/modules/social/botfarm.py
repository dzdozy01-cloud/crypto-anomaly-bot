"""Bot-farm / coordinated-inauthentic-behaviour detection.

The signature of a paid shill campaign is not any single post — it is the
*statistical uniformity* of the crowd producing them. We score five orthogonal
signals and combine them, which is far harder to evade than any one heuristic:

1. **Account-age variance** — farms register accounts in batches, so the
   coefficient of variation of account age collapses toward zero.
2. **Text near-duplication** — shingled Jaccard similarity over posts.
3. **Posting-cadence regularity** — humans are bursty; schedulers are periodic
   (low variance of inter-arrival times).
4. **Follower-profile uniformity** — near-identical low follower counts.
5. **Mention velocity** — the spike itself, which gates the whole score.
"""

from __future__ import annotations

import math
import re
import statistics
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from ...core.stats import clamp

__all__ = ["BotFarmDetector", "BotFarmVerdict", "SocialPost"]

_WORD_RE = re.compile(r"[a-z0-9$#]+")


@dataclass
class SocialPost:
    """One normalised social post from any platform."""

    platform: str                 # x | telegram | reddit | ...
    post_id: str
    author_id: str
    text: str
    timestamp: int                # epoch ms
    tickers: set[str] = field(default_factory=set)
    author_age_days: float | None = None
    author_followers: int | None = None
    author_post_count: int | None = None
    is_reply: bool = False
    sentiment: float = 0.0

    @property
    def fingerprint(self) -> frozenset[str]:
        """Word-level shingle set used for near-duplicate detection."""
        words = _WORD_RE.findall(self.text.lower())
        if len(words) < 3:
            return frozenset(words)
        return frozenset(" ".join(words[i: i + 3]) for i in range(len(words) - 2))


@dataclass
class BotFarmVerdict:
    is_bot_farm: bool
    score: float                       # 0..1 confidence
    age_variance_cv: float | None      # coefficient of variation of account ages
    duplicate_ratio: float
    cadence_regularity: float
    follower_uniformity: float
    unique_authors: int
    posts_considered: int
    reasons: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "is_bot_farm": self.is_bot_farm,
            "score": round(self.score, 4),
            "age_cv": round(self.age_variance_cv, 4) if self.age_variance_cv is not None else None,
            "duplicate_ratio": round(self.duplicate_ratio, 4),
            "cadence_regularity": round(self.cadence_regularity, 4),
            "follower_uniformity": round(self.follower_uniformity, 4),
            "unique_authors": self.unique_authors,
            "posts": self.posts_considered,
            "reasons": self.reasons,
        }


class BotFarmDetector:
    """Sliding-window coordinated-behaviour detector, one instance per ticker."""

    def __init__(
        self,
        window_s: int = 600,
        min_posts: int = 12,
        max_age_days: float = 30.0,
        age_cv_threshold: float = 0.35,
        duplicate_threshold: float = 0.4,
        maxlen: int = 2000,
    ) -> None:
        self.window_ms = window_s * 1000
        self.min_posts = min_posts
        self.max_age_days = max_age_days
        self.age_cv_threshold = age_cv_threshold
        self.duplicate_threshold = duplicate_threshold
        self.posts: deque[SocialPost] = deque(maxlen=maxlen)

    def add(self, post: SocialPost) -> None:
        self.posts.append(post)
        self._evict(post.timestamp)

    def _evict(self, now_ms: int) -> None:
        cutoff = now_ms - self.window_ms
        while self.posts and self.posts[0].timestamp < cutoff:
            self.posts.popleft()

    # ---- individual signals --------------------------------------------
    @staticmethod
    def _age_cv(ages: list[float]) -> float | None:
        """Coefficient of variation of account ages; low CV == batch-registered."""
        if len(ages) < 4:
            return None
        mean = statistics.fmean(ages)
        if mean <= 0:
            return 0.0
        return statistics.pstdev(ages) / mean

    def _find_cohort(self, posts: list[SocialPost]) -> tuple[set[str], float | None]:
        """Isolate the largest cluster of similarly-aged young accounts.

        Critical detail: a farm never operates in a vacuum — its posts are mixed
        into organic chatter, so the *global* age CV stays high and naive
        detection misses it entirely. We instead find the densest cohort of
        young accounts registered around the same time and evaluate that
        subgroup, which is exactly what a manual investigator looks for.

        Returns ``(cohort_author_ids, cohort_age_cv)``.
        """
        by_author: dict[str, float] = {}
        for p in posts:
            if p.author_age_days is not None and p.author_age_days <= self.max_age_days:
                by_author.setdefault(p.author_id, p.author_age_days)
        if len(by_author) < 4:
            return set(), None

        items = sorted(by_author.items(), key=lambda kv: kv[1])
        ages = [a for _, a in items]
        # Sliding window over sorted ages: the tightest span holding the most
        # accounts. Bandwidth scales with age so 3-day-old and 25-day-old
        # cohorts are both detectable.
        best: tuple[int, int, int] = (0, 0, 0)  # (count, start, end)
        for i in range(len(ages)):
            bandwidth = max(3.0, ages[i] * 0.35)
            j = i
            while j + 1 < len(ages) and ages[j + 1] - ages[i] <= bandwidth:
                j += 1
            if (j - i + 1) > best[0]:
                best = (j - i + 1, i, j)

        count, start, end = best
        if count < 4:
            return set(), None
        cohort_ids = {aid for aid, _ in items[start: end + 1]}
        cohort_ages = ages[start: end + 1]
        return cohort_ids, self._age_cv(cohort_ages)

    @staticmethod
    def _duplicate_ratio(posts: list[SocialPost], sample_cap: int = 120) -> float:
        """Fraction of post pairs that are near-duplicates (Jaccard >= 0.6)."""
        sample = posts[-sample_cap:]
        prints = [p.fingerprint for p in sample if p.fingerprint]
        n = len(prints)
        if n < 3:
            return 0.0
        dupes = 0
        pairs = 0
        for i in range(n):
            for j in range(i + 1, n):
                a, b = prints[i], prints[j]
                union = len(a | b)
                if union == 0:
                    continue
                pairs += 1
                if len(a & b) / union >= 0.6:
                    dupes += 1
        return dupes / pairs if pairs else 0.0

    @staticmethod
    def _cadence_regularity(posts: list[SocialPost]) -> float:
        """1.0 == metronomic posting (scheduler); 0.0 == bursty (human)."""
        if len(posts) < 6:
            return 0.0
        ts = sorted(p.timestamp for p in posts)
        gaps = [b - a for a, b in zip(ts, ts[1:]) if b > a]
        if len(gaps) < 4:
            return 0.0
        mean = statistics.fmean(gaps)
        if mean <= 0:
            return 0.0
        cv = statistics.pstdev(gaps) / mean
        # Poisson (human) arrivals have CV ~= 1; schedulers approach 0.
        return clamp(1.0 - cv, 0.0, 1.0)

    @staticmethod
    def _follower_uniformity(posts: list[SocialPost]) -> float:
        counts = [p.author_followers for p in posts if p.author_followers is not None]
        if len(counts) < 5:
            return 0.0
        mean = statistics.fmean(counts)
        if mean <= 0:
            return 1.0
        cv = statistics.pstdev(counts) / mean
        low_follower_share = sum(1 for c in counts if c < 200) / len(counts)
        return clamp((1.0 - clamp(cv, 0.0, 1.0)) * 0.5 + low_follower_share * 0.5, 0.0, 1.0)

    # ---- composite -------------------------------------------------------
    def evaluate(self, mention_z: float | None = None) -> BotFarmVerdict:
        posts = list(self.posts)
        n = len(posts)
        if n < self.min_posts:
            return BotFarmVerdict(False, 0.0, None, 0.0, 0.0, 0.0, 0, n,
                                  ["insufficient posts"])

        authors = {p.author_id for p in posts}

        # Evaluate the suspicious cohort, not the whole (mostly organic) window.
        cohort_ids, cohort_cv = self._find_cohort(posts)
        cohort_posts = [p for p in posts if p.author_id in cohort_ids] if cohort_ids else []
        cohort_share = len(cohort_posts) / n if n else 0.0
        # Only trust the subgroup analysis once the cohort is materially present.
        analysis_posts = cohort_posts if len(cohort_posts) >= max(6, self.min_posts // 2) else posts
        on_cohort = analysis_posts is cohort_posts

        dup = self._duplicate_ratio(analysis_posts)
        cadence = self._cadence_regularity(analysis_posts)
        followers = self._follower_uniformity(analysis_posts)
        global_ages = [p.author_age_days for p in posts if p.author_age_days is not None]
        age_cv = cohort_cv if on_cohort and cohort_cv is not None else self._age_cv(global_ages)

        reasons: list[str] = []
        score = 0.0

        # 1. account-age variance within the cohort (the spec's primary signal)
        if age_cv is not None and age_cv < self.age_cv_threshold:
            tightness = 1 - age_cv / self.age_cv_threshold
            presence = clamp(cohort_share * 2.5, 0.0, 1.0) if on_cohort else 0.6
            score += 0.35 * tightness * presence
            reasons.append(
                f"{len(cohort_ids) if on_cohort else len(authors)} accounts with uniform age "
                f"(CV={age_cv:.2f}, <{self.max_age_days:.0f}d) driving {cohort_share:.0%} of posts"
            )

        # 2. text duplication — graded, because organic near-duplication is ~0%.
        # Campaigns rotate a handful of templates, so 15-25% pairwise similarity
        # is already damning; requiring a hard 40% cutoff misses rotated copy.
        dup_floor = 0.08
        if dup > dup_floor:
            ramp = clamp((dup - dup_floor) / max(self.duplicate_threshold - dup_floor, 1e-6), 0, 1)
            score += 0.25 * ramp
            reasons.append(f"{dup:.0%} near-duplicate posts" + (" in cohort" if on_cohort else ""))

        # 3. mechanical cadence
        if cadence > 0.55:
            score += 0.15 * cadence
            reasons.append(f"mechanical posting cadence ({cadence:.2f})")

        # 4. follower uniformity
        if followers > 0.6:
            score += 0.10 * followers
            reasons.append(f"uniform low-follower authors ({followers:.2f})")

        # 5. author concentration — few accounts generating many posts
        posts_per_author = (
            len(cohort_posts) / max(len(cohort_ids), 1) if on_cohort else n / max(len(authors), 1)
        )
        if posts_per_author > 3.0:
            score += clamp(0.10 * math.log1p(posts_per_author - 3.0), 0.0, 0.10)
            reasons.append(f"{posts_per_author:.1f} posts/author")

        # 6. velocity gate — coordination only matters if it is moving the needle
        if mention_z is not None and mention_z > 3.0:
            score += clamp(0.15 * (mention_z - 3.0) / 5.0, 0.0, 0.15)
            reasons.append(f"mention spike z={mention_z:.1f}")

        score = clamp(score, 0.0, 1.0)
        return BotFarmVerdict(
            is_bot_farm=score >= 0.5,
            score=score,
            age_variance_cv=age_cv,
            duplicate_ratio=dup,
            cadence_regularity=cadence,
            follower_uniformity=followers,
            unique_authors=len(authors),
            posts_considered=n,
            reasons=reasons or ["no coordination signals"],
        )
