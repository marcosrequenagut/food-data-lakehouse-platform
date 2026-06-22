import sys
import os
import pandas as pd
from transformations import clean_text, parse_nutrient_levels

# Add the ariflow/dags folder to the path so we can import the DAG functions
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../airflow/dags"))


# TESTS
def test_clean_text_lowercase():
    """Text should be converted to lowercase"""
    assert clean_text("NESTLE") == "nestle"


def test_clean_text_accents():
    """Accents should be removed"""
    assert clean_text("Nestlé") == "nestle"


def test_clean_text_combined():
    "Lower case and accents combined"
    assert clean_text("NESTLÉ") == "nestle"


def test_clean_text_none():
    """None input should return None"""
    assert pd.isna(clean_text(None))


def test_clean_text_string():
    """Extra whitespaces should be handled"""
    assert clean_text(" nestle ") == "nestle"


def test_parse_nutrient_levels_all_keys():
    """All 4 keys present"""
    result = parse_nutrient_levels(
        "{'fat': 'moderate', 'salt': 'low', 'saturated-fat': 'high', 'sugars': 'low'}"
    )
    assert result == ("moderate", "low", "high", "low")


def test_parse_nutrient_levels_missing_keys():
    """Missing keys should return 'unknown'"""
    result = parse_nutrient_levels("{'fat': 'moderate', 'salt': 'low'}")
    assert result == ("moderate", "low", "unknown", "unknown")


def test_parse_nutrient_levels_empty():
    """Empty string should return None tuple"""
    result = parse_nutrient_levels("")
    assert result == (None, None, None, None)
