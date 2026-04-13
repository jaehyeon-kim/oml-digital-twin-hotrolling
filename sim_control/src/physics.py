import numpy as np
from src.config import (
    MATERIAL_CONSTANTS,
    WEAR_PENALTY_RATE,
    SENSOR_NOISE_STD_DEV,
)
from src.schemas import Features


def calculate_baseline_force(material_type: str, features: Features):
    """
    Calculates the idealized theoretical roll force using a static metallurgical model.

    This function represents the legacy or physics-only prediction. It assumes
    perfect machine conditions and bases its calculation on the volume of steel
    deformation and material resistance.

    Formula:
    Force_base = (Hardness * Width * Draft) / Temperature

    Args:
        material_type (str): The steel grade identifier (e.g., structural, high_alloy).
        features (Features): The Pydantic model containing raw telemetry for the pass.

    Returns:
        float: The theoretical roll force in kN, rounded to 2 decimal places.
    """
    draft = features.entry_thickness_mm * features.reduction_pct
    temp = max(features.temperature_c, 1.0)
    hardness_c = MATERIAL_CONSTANTS.get(material_type, 18000.0)

    baseline_force = (hardness_c * features.width_mm * draft) / temp
    return round(baseline_force, 2)


def calculate_actual_force(baseline_force, features, wear_percent, rng):
    """
    Simulates ground truth physical sensor readings using High-Volatility Concept Drift.

    This version removes artificial 'floors' from the interactions, allowing the
    Baseline Error to range anywhere from ~2% on early passes to ~75% on final passes.

    Logic:
    1. Thermal Factor: Colder steel rapidly increases the penalty.
    2. Thickness Factor: Thinner steel rapidly increases the penalty.
    3. Interaction Multiplier: Multiplying these creates a highly non-linear curve
       that ensures extreme pass-to-pass volatility.
    """
    wear_level = max(0, min(wear_percent, 100)) / 100.0

    # 1. Base Wear Penalty
    base_drift = (wear_level**1.5) * WEAR_PENALTY_RATE

    # 2. Thermal Factor (Ideal is ~1250C. Colder steel drives this > 1.0)
    thermal_factor = max(0.1, (1250.0 - features.temperature_c) / 200.0)

    # 3. Thickness Factor (Ideal is thick > 200mm. Thin steel drives this > 1.0)
    thickness_factor = 100.0 / max(features.entry_thickness_mm, 10.0)

    # 4. Reduction Factor (Heavy reductions scale up the penalty)
    reduction_factor = 1.0 + features.reduction_pct

    # Multiply interactions together.
    # Pass 1 (Hot/Thick) -> Multiplier is ~0.15 (Tiny penalty)
    # Pass 5 (Cold/Thin) -> Multiplier is ~5.00 (Massive penalty)
    interaction_multiplier = thermal_factor * thickness_factor * reduction_factor

    total_penalty = base_drift * interaction_multiplier

    # Generate stochastic sensor noise
    sensor_noise = rng.normal(loc=0.0, scale=SENSOR_NOISE_STD_DEV)

    # Combine into final Ground Truth
    actual_force = baseline_force * (1.0 + total_penalty) + sensor_noise

    return round(max(actual_force, 0.0), 2)
