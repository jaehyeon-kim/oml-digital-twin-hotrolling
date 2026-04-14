"""
Pydantic Schemas for Kafka Event Serialization.

This module defines the strict data contracts between the Python Simulation Engine
and the Kotlin Flink Online Machine Learning pipeline. Strong typing here guarantees
that Flink's Jackson Deserializer will parse the JSON payloads without failing.
"""

from pydantic import BaseModel, Field


# ==========================================
# Event A: Prediction Request Components
# ==========================================
class PredictionIdentifiers(BaseModel):
    """
    Metadata used by Flink for tracking, routing, and joining streams.
    These fields do not enter the ML model as features.
    """

    slab_id: str = Field(
        ..., description="Unique identifier for the physical steel block."
    )
    pass_number: int = Field(
        ge=1, le=10, description="The specific rolling pass (e.g., 1 through 7)."
    )
    steel_grade: str = Field(
        ..., description="Material classification (e.g., 'structural')."
    )
    routing_key: str = Field(
        ..., description="Composite key for Flink KeyedState partitioning."
    )


class Features(BaseModel):
    """
    Contains the 13 raw process parameters matching the real-world plant database.
    These are the independent variables ($X$) used by the AMRules and SGD models.
    """

    reheating_time_min: float = Field(
        ge=135.0, le=1188.0, description="Time the slab spent in the furnace (minutes)."
    )
    roll_diameter_mm: float = Field(
        ge=1042.0, le=1121.0, description="Physical diameter of the work rolls (mm)."
    )
    roll_crown_mm: float = Field(
        ge=0.0,
        le=217.0,
        description="Barrel-shape curve of the rolls compensating for bending (mm).",
    )
    entry_thickness_mm: float = Field(
        ge=15.0,
        le=250.0,
        description="Thickness of the slab before this specific pass (mm).",
    )
    width_mm: float = Field(
        ge=1613.0, le=4182.0, description="Width of the steel slab (mm)."
    )
    length_mm: float = Field(
        ge=1794.0, le=38139.0, description="Length of the steel slab (mm)."
    )
    temperature_c: float = Field(
        ge=889.0,
        le=1233.0,
        description="Surface temperature of the steel at entry (°C).",
    )
    speed_m_s: float = Field(
        ge=1.5, le=5.6, description="Rolling speed of the mill (m/s)."
    )
    wait_time_sec: float = Field(
        ge=6.0,
        le=225.0,
        description="Inter-pass time since the previous roll (seconds).",
    )
    reduction_pct: float = Field(
        ge=0.01,
        le=0.40,
        description="Percentage of thickness being crushed in this pass.",
    )
    strain: float = Field(
        ge=0.02, le=0.56, description="Total plastic deformation of the steel."
    )
    strain_rate: float = Field(
        ge=0.5, le=16.3, description="Speed at which the deformation occurs."
    )
    flow_stress_mpa: float = Field(
        ge=5.0,
        le=179.0,
        description="Internal resistance of the metal to deformation (MPa).",
    )


class BaselinePrediction(BaseModel):
    """
    The legacy theoretical estimate calculated by static metallurgical formulas.
    The OML model targets the *residual* (Actual - Baseline) rather than predicting
    the absolute force from scratch.
    """

    baseline_roll_force_kn: float = Field(
        ge=0.0, description="Theoretical force required (kilonewtons)."
    )


class PredictionRequestEvent(BaseModel):
    """
    Event A: Sent into Kafka the moment the slab approaches the roller.
    This acts as the trigger for Flink to calculate the AI's "Shadow Prediction".
    """

    event_type: str = Field(
        default="prediction_request",
        description="Fixed string identifying the schema type.",
    )
    timestamp: str = Field(
        ..., description="ISO 8601 timestamp of when the slab entered the pass."
    )
    identifiers: PredictionIdentifiers
    features: Features
    baseline_prediction: BaselinePrediction

    def to_json(self) -> str:
        return self.model_dump_json()


# ==========================================
# Event B: Ground Truth Components
# ==========================================
class GroundTruthIdentifiers(BaseModel):
    """
    Minimal identifiers needed by Flink's EventMatchProcessFunction to join
    this ground truth back to the original PredictionRequestEvent (Event A).
    """

    slab_id: str = Field(..., description="Unique identifier for the steel block.")
    pass_number: int = Field(
        ge=1, le=10, description="The specific pass number being measured."
    )


class GroundTruthMetrics(BaseModel):
    """
    The realistic, simulated physical readings taken after the steel is crushed.
    This acts as the dependent target variable ($y$) for the ML pipelines.
    """

    actual_roll_force_kn: float = Field(
        ge=0.0, description="The true physical force exerted (kilonewtons)."
    )
    wear_level: float = Field(
        ge=0.0,
        le=100.0,
        description="The wear level of the mill roller (hidden state).",
    )


class GroundTruthEvent(BaseModel):
    """
    Event B: Sent into Kafka after the physical pass completes.
    Represents the actual factory measurement used by Flink to evaluate and train models.
    """

    event_type: str = Field(
        default="ground_truth", description="Fixed string identifying the schema type."
    )
    timestamp: str = Field(
        ..., description="ISO 8601 timestamp of when the pass completed."
    )
    identifiers: GroundTruthIdentifiers
    ground_truth: GroundTruthMetrics

    def to_json(self) -> str:
        return self.model_dump_json()
