"""
Mathematical Physics Engine for the Hot Rolling Mill.

This module splits the physical calculation into two distinct paradigms:
1. The 'Baseline' (High Bias, Low Variance): A static, theoretical metallurgical equation.
2. The 'Ground Truth' (Low Bias, High Variance): A dynamic, non-linear reality influenced
   by hidden machine degradation (Concept Drift) and sensor noise.
"""

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

    This function acts as the Legacy Physics Model in our Champion/Challenger architecture.
    It assumes pristine machine conditions and bases its calculation entirely on the volume
    of steel deformation and inherent material resistance.

    The mathematical approximation used is:
    Force_base = (Hardness * Width * Draft) / Temperature

    Args:
        material_type (str): The steel grade identifier (e.g., structural, high_alloy).
        features (Features): The Pydantic model containing raw telemetry for the pass.

    Returns:
        float: The theoretical roll force in kN, rounded to 2 decimal places.
    """
    # Draft is the absolute thickness reduction in millimeters
    draft = features.entry_thickness_mm * features.reduction_pct
    temp = max(features.temperature_c, 1.0)
    hardness_c = MATERIAL_CONSTANTS.get(material_type, 18000.0)

    baseline_force = (hardness_c * features.width_mm * draft) / temp
    return round(baseline_force, 2)


def calculate_actual_force(
    baseline_force: float,
    features: Features,
    wear_percent: float,
    rng: np.random.Generator,
):
    """
    Simulates the true Ground Truth physical sensor readings using High-Volatility Concept Drift.

    This function represents the harsh reality of the factory floor. It takes the baseline
    theoretical force and applies a severe, non-linear penalty based on the hidden wear level
    of the machine and the specific geometric/thermal state of the steel.

    The mathematical formulation is:
    Force_actual = Force_base * (1 + Total_Penalty) + Gaussian_Noise(0, StdDev)

    Logic:
    1. Thermal Factor: Colder steel rapidly multiplies the penalty (harder to crush).
    2. Thickness Factor: Thinner steel rapidly multiplies the penalty.
    3. Interaction Multiplier: Multiplying these together ensures extreme pass-to-pass volatility.
       Pass 1 (Hot/Thick) might only see a 2 percent error, while Pass 5 (Cold/Thin) sees a 30 percent error.

    Args:
        baseline_force (float): The theoretical target force.
        features (Features): The geometric/thermal state of the slab.
        wear_percent (float): The hidden degradation state of the mill (0.0 to 100.0).
        rng (np.random.Generator): Random number generator for stochastic noise.

    Returns:
        float: The simulated true physical force in kN.
    """
    wear_level = max(0, min(wear_percent, 100)) / 100.0

    # 1. Base Wear Penalty: Non-linear exponent ensures wear compounds aggressively
    base_drift = (wear_level**1.5) * WEAR_PENALTY_RATE

    # 2. Thermal Factor: Ideal rolling is ~1250°C. Colder steel drives this multiplier > 1.0
    thermal_factor = max(0.1, (1250.0 - features.temperature_c) / 200.0)

    # 3. Thickness Factor: Ideal rolling is thick (>200mm). Thin finishing passes drive this > 1.0
    thickness_factor = 100.0 / max(features.entry_thickness_mm, 10.0)

    # 4. Reduction Factor: Heavier physical reductions scale up the friction penalty
    reduction_factor = 1.0 + features.reduction_pct

    # Multiply interactions together to create a highly volatile drift curve
    interaction_multiplier = thermal_factor * thickness_factor * reduction_factor

    total_penalty = base_drift * interaction_multiplier

    # Generate stochastic sensor noise ($\mathcal{N}$)
    sensor_noise = rng.normal(loc=0.0, scale=SENSOR_NOISE_STD_DEV)

    # Combine into the final Ground Truth reading
    actual_force = baseline_force * (1.0 + total_penalty) + sensor_noise

    return round(max(actual_force, 0.0), 2)
