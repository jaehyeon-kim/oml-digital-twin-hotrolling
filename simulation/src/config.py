import os

# ==========================================
# Kafka Broker & Topic Configuration
# ==========================================
# Using environment variables with sensible defaults for local Factor House testing
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

# Egress Topics (Data sent out by the simulator)
TOPIC_PREDICTION_REQUESTS = "mill-predictions"
TOPIC_GROUND_TRUTH = "mill-groundtruth"
TOPIC_TELEMETRY = "sim-telemetry"
TOPIC_LIFECYCLE = "sim-lifecycle"

# Ingress Topic (Commands received from external system)
TOPIC_CONTROL_INGRESS = "sim-controls"


# ==========================================
# Physics & Metallurgical Constants
# ==========================================
# Material-specific hardness constants (The "C" in the formula)
# Structural is soft, Micro-alloyed is tough, High-Alloy is extremely resistant
MATERIAL_CONSTANTS = {
    "structural": 18000.0,
    "microalloyed": 28000.0,
    "high_alloy": 45000.0,
}

# The maximum penalty applied when wear_level reaches 1.0 (100% wear).
# E.g., if wear_level is 1.0, it adds 5000 kN to the required roll force.
WEAR_PENALTY_MULTIPLIER = 5000.0

# The standard deviation of the Gaussian noise applied to the actual force (in kN).
# Represents physical sensor vibrations and mechanical imperfections.
SENSOR_NOISE_STD_DEV = 500.0
