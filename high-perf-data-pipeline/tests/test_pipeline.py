import pytest
import polars as pl
from pathlib import Path
from pipeline import normalize_pricing, generate_fingerprints, PipelineConfig

# --- FIXTURES ---

@pytest.fixture
def config():
    """Returns a standard config for testing."""
    return PipelineConfig(
        input_path=Path("dummy_in.csv"),
        output_path=Path("dummy_out.csv"),
        usd_to_eur_rate=1.10
    )

# --- TESTS ---

def test_normalize_pricing_logic(config):
    """
    Test 1: Check USD conversion.
    Test 2: Check Scaling fix (197200 -> 1972.00).
    Test 3: Check standard cleaning (garbage chars).
    """
    data = pl.DataFrame({
        "raw_price": ["$110.00", "197200", "€ 50,00", "invalid"]
    }).lazy()

    # Execution
    result = normalize_pricing(data, config).collect()

    # Assertions
    # 1. $110 / 1.10 = 100.0
    assert result["price_final"][0] == 100.0
    # 2. 197200 > 10000 threshold -> divide by 100 -> 1972.0
    assert result["price_final"][1] == 1972.0
    # 3. "€ 50,00" -> 50.0
    assert result["price_final"][2] == 50.0
    # 4. "invalid" -> null
    assert result["price_final"][3] is None

def test_fingerprint_generation():
    """
    Test 1: Synonym replacement.
    Test 2: Token sorting (A B == B A).
    Test 3: Marketing noise removal.
    """
    synonyms = {"ps5": "playstation 5"}
    
    data = pl.DataFrame({
        "raw_name": [
            "Sony PS5 Edition Limitée",  # Should become: playstation 5 sony
            "Promo Sony PlayStation 5",  # Should become: playstation 5 sony
        ]
    }).lazy()

    # Execution
    result = generate_fingerprints(data, synonyms).collect()

    fp1 = result["fingerprint"][0]
    fp2 = result["fingerprint"][1]

    # Assertions
    assert "edition" not in fp1  # Marketing word removed
    assert "promo" not in fp2    # Marketing word removed
    assert fp1 == fp2            # Both should result in same fingerprint (sorted tokens)
    assert fp1 == "5 playstation sony" # Alphabetical order

def test_config_immutability(config):
    """Ensure config preserves critical financial thresholds."""
    assert config.usd_to_eur_rate == 1.10
    assert config.min_price_eur == 5.0
