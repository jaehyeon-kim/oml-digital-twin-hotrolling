import numpy as np
from src.config import (
    MATERIAL_CONSTANTS,
    WEAR_PENALTY_MULTIPLIER,
    SENSOR_NOISE_STD_DEV,
)
from src.schemas import Features


def calculate_baseline_force(material_type: str, features: Features) -> float:
    """
    Calculates the theoretical roll force using a simplified, static metallurgical model.

    This represents the legacy factory calculation. It assumes an idealized environment
    where force scales linearly with the volume of steel being crushed and inversely
    with temperature, ignoring complex dynamics like roll diameter, speed, and strain rate.

    Formula:
        F_base = [ C * w * (h_in - h_out) ] / T

    Args:
        material_type (str): The specific steel grade being rolled (e.g., 'structural').
        features (Features): The Pydantic model containing the raw machine sensor telemetry
                             for the current rolling pass.

    Returns:
        float: The estimated baseline roll force in kilonewtons (kN), rounded to 2 decimal places.
    """
    # Calculate absolute draft in mm (h_in - h_out)
    draft = features.entry_thickness_mm * features.reduction_pct

    # Temperature acts as the denominator. Hotter steel = softer steel = less force.
    # We use max() as a fail-safe against division by zero, though Pydantic bounds prevent it.
    temp = max(features.temperature_c, 1.0)

    # Dynamically fetch the correct metallurgical constant
    hardness_c = MATERIAL_CONSTANTS[material_type]

    # Calculate theoretical force
    baseline_force = (hardness_c * features.width_mm * draft) / temp

    return round(baseline_force, 2)


def calculate_actual_force(
    baseline_force: float, wear_percent: int, rng: np.random.Generator
) -> float:
    """
    Calculates the simulated "ground truth" physical sensor reading.

    This function corrupts the theoretical baseline by injecting both gradual
    concept drift (roller wear) and sudden random variances (sensor noise).
    This is the target variable that the Online Machine Learning models will
    attempt to predict.

    Formula:
        F_actual = F_base + wear_penalty + sensor_noise

    Args:
        baseline_force (float): The idealized theoretical force (output of calculate_baseline_force).
        wear_percent (int): The current capacity of the wear state container (0 to 100).
        rng (np.random.Generator): A seeded NumPy random generator for reproducible noise.

    Returns:
        float: The simulated true physical force in kilonewtons (kN), bounded to ensure
               it cannot drop below 0.0, rounded to 2 decimal places.
    """
    # Apply the Concept Drift (Wear Penalty)
    # As the physical rollers degrade, the machine must exert more brute force
    # to achieve the exact same thickness reduction.
    wear_level = max(0, min(wear_percent, 100)) / 100.0
    wear_penalty = wear_level * WEAR_PENALTY_MULTIPLIER

    # Add realistic Gaussian noise
    # Represents physical mechanical vibrations, hydraulic fluid inconsistencies,
    # and electrical sensor fuzziness.
    sensor_noise = rng.normal(loc=0.0, scale=SENSOR_NOISE_STD_DEV)

    # Calculate final ground truth
    actual_force = baseline_force + wear_penalty + sensor_noise

    # Ensure physical reality (hydraulic force cannot be a negative number)
    return round(max(actual_force, 0.0), 2)
