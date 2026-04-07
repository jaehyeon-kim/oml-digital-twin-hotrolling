### Data Dictionary: Hot Rolling Digital Twin

#### Identifiers (Metadata)

These are used for routing, joining streams (Event A and B), and dashboard tracking.

- **`slab_id`** (String): Unique identifier for the steel block.
- **`pass_number`** (Integer): The current rolling pass (e.g., 1 through 7).
- **`steel_grade`** (String): Material classification (e.g., "structural", "microalloyed", "high_alloy").
- **`routing_key`** (String): Composite key used for Flink state partitioning to ensure ML models remain strictly isolated by product line.

#### Raw Features (From Machine Sensors & Plant DB)

These are the exact 13 base parameters derived from Table 2 of the [reference paper](https://www.sciencedirect.com/science/article/pii/S2949917823000445).

- **`reheating_time_min`** (Float): Time the slab spent in the furnace.
- **`roll_diameter_mm`** (Float): Physical diameter of the work rolls.
- **`roll_crown_mm`** (Float): The slight barrel-shape curve of the rolls to compensate for bending.
- **`entry_thickness_mm`** (Float): Thickness of the slab _before_ this pass.
- **`width_mm`** (Float): Width of the steel slab.
- **`length_mm`** (Float): Length of the steel slab.
- **`temperature_c`** (Float): Surface temperature of the steel at entry.
- **`speed_m_s`** (Float): Rolling speed of the mill.
- **`wait_time_sec`** (Float): Inter-pass time since the previous roll.
- **`reduction_pct`** (Float): The percentage of thickness being crushed in this pass.
- **`strain`** (Float): The total plastic deformation of the steel.
- **`strain_rate`** (Float): The speed at which the deformation occurs.
- **`flow_stress_mpa`** (Float): The internal resistance of the metal to being deformed.

#### Engineered Features (Calculated on the fly in Flink)

Engineered features to instantly grasp non-linear physics.

- **`absolute_draft_mm`** (Float): `entry_thickness_mm * reduction_pct`.
  - The actual millimeters of steel being crushed. 10% of 200mm is vastly different physics than 10% of 20mm.
- **`temp_draft_interaction`** (Float): `absolute_draft_mm / temperature_c`.
  - Captures a crucial physical reality: crushing thick steel when it has cooled down requires exponentially massive force.
- **`volume_mm3`** (Float): `entry_thickness_mm * width_mm * length_mm`.
  - Gives the model an understanding of the total thermal mass.

#### Line-Specific Simulation Features (Concept Drift Drivers)

These are the hidden physical state variables driving the concept drift. They are **strictly excluded** from the `PredictionRequestEvent` sent to the ML models so the algorithm cannot "cheat." They exist purely in the Python simulation to corrupt the Ground Truth physics. _(Note: They are published separately to the `sim-telemetry` Kafka topic so the UI Dashboard can plot the true hidden drift against the ML error rate)._

- **`wear_state_structural`** (Float): Kept at `0.0` to demonstrate stable baseline ML accuracy (No Drift).
- **`wear_state_microalloyed`** (Float): Automatically increments based on simulation clock time (`sim_ts`) to demonstrate continuous Flink model adaptation (Gradual Drift).
- **`wear_state_high_alloy`** (Float): Subject to sudden spikes triggered via the `sim-config` Kafka topic to demonstrate violent mechanical shocks (Abrupt Drift).

#### Target Variables (Predictions and Actuals)

- **`baseline_roll_force_kn`** (Float): The legacy mathematical estimate (sent in Event A).
- **`actual_roll_force_kn`** (Float): The simulated physical reality, influenced by baseline physics, the hidden line-specific wear state, and Gaussian noise (sent in Event B).
