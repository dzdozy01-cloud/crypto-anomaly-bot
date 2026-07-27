"""Module 4 — ML Manipulation Classifier."""

from .classifier import ManipulationClassifier, RuleEngine, ScoreBreakdown
from .features import FEATURE_NAMES, AssetFeatures, FeatureStore, FeatureVector
from .scorer import MLScorer
from .training import generate_labelled_set, generate_training_data

__all__ = [
    "FEATURE_NAMES",
    "AssetFeatures",
    "FeatureStore",
    "FeatureVector",
    "MLScorer",
    "ManipulationClassifier",
    "RuleEngine",
    "ScoreBreakdown",
    "generate_labelled_set",
    "generate_training_data",
]
