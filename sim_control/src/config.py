"""
Configuration and Physical Constants for the Hot Rolling Digital Twin.

This module centralizes the environmental configuration for infrastructure (Kafka, ClickHouse)
and defines the foundational physical parameters that govern the simulated steel mill.
"""

import os

# ==========================================
# Infrastructure: Kafka Broker & Topics
# ==========================================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

# Topic for Event A: What the physics engine predicts before the roll
TOPIC_PREDICTION_REQUESTS = "mill-predictions"
# Topic for Event B: What actually happened after the roll (contains hidden drift)
TOPIC_GROUND_TRUTH = "mill-groundtruth"
# Topic for real-time hidden state visualization on the dashboard
TOPIC_TELEMETRY = "sim-telemetry"
# Topic for dynamic-des internal lifecycle events
TOPIC_LIFECYCLE = "sim-lifecycle"
# Topic for receiving Control Plane commands (e.g., injecting drift)
TOPIC_CONTROL_INGRESS = "sim-controls"

# ==========================================
# Infrastructure: ClickHouse (Dashboard Sink)
# ==========================================
CH_HOST = os.getenv("CH_HOST", "localhost")
CH_PORT = int(os.getenv("CH_PORT", 8123))
CH_USER = os.getenv("CH_USER", "default")
CH_DB = os.getenv("CH_DB", "dev")

# ==========================================
# Physics & Metallurgical Constants
# ==========================================
# Represents the base resistance to deformation for different steel grades.
# Higher values require significantly more force to compress.
MATERIAL_CONSTANTS = {
    "structural": 18000.0,
    "microalloyed": 28000.0,
    "high_alloy": 45000.0,
}

# ---------------------------------------------------------
# DRIFT SCALING (The Severity of Machine Degradation)
# ---------------------------------------------------------
# 0.40 means that at 100% mechanical wear, the machine requires 40% more
# physical force to compress the steel than the baseline theoretical prediction.
# A higher value creates a wider divergence between the Baseline and the OML model,
# demonstrating the financial/physical value of adaptive machine learning.
WEAR_PENALTY_RATE = 0.40

# ---------------------------------------------------------
# STOCHASTIC SENSOR NOISE
# ---------------------------------------------------------
# Real industrial sensors are never perfectly precise. We inject a Gaussian noise
# profile with this standard deviation into the final Ground Truth reading.
# This is CRITICAL for Machine Learning: without noise, the ML models would overfit
# to a deterministic mathematical equation. Noise forces the model to learn the trend.
SENSOR_NOISE_STD_DEV = 500.0
