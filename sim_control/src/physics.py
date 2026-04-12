from src.config import (
    MATERIAL_CONSTANTS,
    WEAR_PENALTY_RATE,
    SENSOR_NOISE_STD_DEV,
)
from src.schemas import Features
from numpy.random import Generator


def calculate_baseline_force(material_type: str, features: Features):
    """
    Calculates the idealized theoretical roll force using a static metallurgical model.

    This function represents the legacy or physics-only prediction. It assumes
    perfect machine conditions and bases its calculation on the volume of steel
    deformation and material resistance. The formula used is a simplified version
    of the Sims or Ekelund equations:

    Formula:
    Force_base = (Hardness * Width * Draft) / Temperature

    Where:
    - Hardness is the material-specific constant (C).
    - Width is the slab width in mm.
    - Draft is the thickness reduction in mm (entry thickness * reduction percent).
    - Temperature is the steel temperature in Celsius.

    Args:
        material_type (str): The steel grade identifier used to look up
            metallurgical constants (e.g., 'structural', 'high_alloy').
        features (Features): A pydantic model containing real-time machine
            telemetry for the current pass.

    Returns:
        float: The theoretical roll force in kilonewtons (kN), rounded to 2
            decimal places.
    """
    # Calculate absolute draft (h_in - h_out equivalent)
    draft = features.entry_thickness_mm * features.reduction_pct

    # Ensure temperature is valid for division
    temp = max(features.temperature_c, 1.0)

    # Fetch hardness constant from config
    hardness_c = MATERIAL_CONSTANTS.get(material_type, 18000.0)

    # Core metallurgical calculation
    baseline_force = (hardness_c * features.width_mm * draft) / temp

    return round(baseline_force, 2)


def calculate_actual_force(
    baseline_force: float, features: Features, wear_percent: int, rng: Generator
):
    """
    Simulates ground truth physical sensor readings using Feature-Dependent Concept Drift.

    Unlike a simple constant bias, this function models Interaction Effects where
    the impact of roller wear is amplified or dampened by the slab's physical state.
    This prevents the baseline error from remaining a consistent percentage and
    challenges OML models to learn conditional relationships.

    Logic:
    1. Base Drift: A non-linear coefficient based on normalized wear level.
    2. Thermal Sensitivity: Worn rollers exert more disproportionate force on
       colder, harder steel (interaction with temperature).
    3. Mechanical Sensitivity: High-reduction passes amplify the efficiency loss
       of the roller surface (interaction with reduction_pct).
    4. Stochastic Noise: Gaussian variance representing mechanical/electrical noise.

    Formula:
    Force_actual = Force_base * (1 + (Base_drift * Thermal_sens * Reduction_sens)) + Noise

    Args:
        baseline_force (float): The idealized output from calculate_baseline_force.
        features (Features): The current telemetry used to calculate interaction effects.
        wear_percent (int): The current state of the wear container (0 to 100).
        rng (np.random.Generator): Seeded NumPy generator for reproducible noise.

    Returns:
        float: The simulated true physical force in kN, rounded to 2 decimal places.
    """
    # 1. Normalize wear level (0.0 to 1.0)
    wear_level = max(0, min(wear_percent, 100)) / 100.0

    # 2. Base Wear Penalty (Global Coefficient)
    # Power of 1.5 models the accelerated degradation of roll surfaces
    base_drift = (wear_level**1.5) * WEAR_PENALTY_RATE

    # 3. Feature Interactions (The Reality Modifier)
    # Worn rollers struggle more with colder steel (below 1100C)
    thermal_sensitivity = max(1.0, (1100 - features.temperature_c) / 200)

    # Worn rollers lose efficiency more rapidly during heavy squeezing
    reduction_sensitivity = max(1.0, features.reduction_pct * 4)

    # Combine drift factors
    total_penalty = base_drift * thermal_sensitivity * reduction_sensitivity

    # 4. Generate stochastic sensor noise
    sensor_noise = rng.normal(loc=0.0, scale=SENSOR_NOISE_STD_DEV)

    # Combine into final Ground Truth
    actual_force = baseline_force * (1 + total_penalty) + sensor_noise

    return round(max(actual_force, 0.0), 2)
