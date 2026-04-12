import os

# ==========================================
# Kafka Broker & Topic Configuration
# ==========================================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

TOPIC_PREDICTION_REQUESTS = "mill-predictions"
TOPIC_GROUND_TRUTH = "mill-groundtruth"
TOPIC_TELEMETRY = "sim-telemetry"
TOPIC_LIFECYCLE = "sim-lifecycle"
TOPIC_CONTROL_INGRESS = "sim-controls"

# ==========================================
# ClickHouse Configuration (For Dashboard)
# ==========================================
CH_HOST = os.getenv("CH_HOST", "localhost")
CH_PORT = int(os.getenv("CH_PORT", 8123))
CH_USER = os.getenv("CH_USER", "default")
CH_DB = os.getenv("CH_DB", "dev")

# ==========================================
# Physics & Metallurgical Constants
# ==========================================
MATERIAL_CONSTANTS = {
    "structural": 18000.0,
    "microalloyed": 28000.0,
    "high_alloy": 45000.0,
}

# ---------------------------------------------------------
# DRIFT SCALING:
# 0.15 means at 100% wear, the machine requires 15% more force
# than the baseline prediction. This scale ensures the APE
# lines on your dashboard diverge clearly.
# ---------------------------------------------------------
WEAR_PENALTY_RATE = 0.15

# Standard deviation for sensor noise (kN)
SENSOR_NOISE_STD_DEV = 500.0
