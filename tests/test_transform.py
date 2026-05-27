import sys
import os
import pandas as pd

# Add the ariflow/dags folder to the path so we can import the DAG functions
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../airflow/dags"))

from transformations import clean_text

# ==================
# TESTS — clean_text
# ==================

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