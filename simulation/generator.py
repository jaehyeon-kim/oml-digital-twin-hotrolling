import argparse
import logging
import time
import uuid
import numpy as np

from dynamic_des import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    KafkaEgress,
    KafkaIngress,
    Sampler,
    SimParameter,
    KafkaAdminConnector,
)

from src.config import (
    KAFKA_BROKER,
    TOPIC_CONTROL_INGRESS,
    TOPIC_GROUND_TRUTH,
    TOPIC_LIFECYCLE,
    TOPIC_PREDICTION_REQUESTS,
    TOPIC_TELEMETRY,
)

# Import from our local sibling files
from routing import custom_topic_router
from sim_logic import arrival_process, drift_engine, roll_slab

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)
logger = logging.getLogger("generator.main")


# ==========================================
# Parameter Definition
# ==========================================
mill_params = SimParameter(
    sim_id="HotRolling",
    arrival={
        "structural": DistributionConfig(dist="exponential", rate=0.2),
        "microalloyed": DistributionConfig(dist="exponential", rate=0.15),
        "high_alloy": DistributionConfig(dist="exponential", rate=0.1),
    },
    service={
        "pass_roughing": DistributionConfig(dist="normal", mean=2.0, std=0.2),
        "pass_intermediate": DistributionConfig(dist="normal", mean=4.5, std=0.5),
        "pass_finishing": DistributionConfig(dist="normal", mean=8.0, std=1.2),
    },
    resources={
        "mill_structural": CapacityConfig(current_cap=4, max_cap=10),
        "mill_microalloyed": CapacityConfig(current_cap=4, max_cap=10),
        "mill_high_alloy": CapacityConfig(current_cap=4, max_cap=10),
    },
    containers={
        "wear_structural": CapacityConfig(current_cap=0.001, max_cap=100.0),
        "wear_microalloyed": CapacityConfig(current_cap=0.001, max_cap=100.0),
        "wear_high_alloy": CapacityConfig(current_cap=0.001, max_cap=100.0),
    },
    variables={
        "velocity_structural": 0.0,
        "velocity_microalloyed": 0.0,
        "velocity_high_alloy": 0.0,
    },
)


# ==========================================
# Infrastructure Loops
# ==========================================
def telemetry_monitor(env: DynamicRealtimeEnvironment, product_lines: list[str]):
    """Streams the current system health and true hidden drift levels to the UI."""
    while True:
        for prod in product_lines:
            wear_path = f"HotRolling.containers.wear_{prod}.current_cap"
            current_wear = env.registry.get(wear_path).value
            env.publish_telemetry(wear_path, current_wear)
        yield env.timeout(2.0)


# ==========================================
# Main Execution
# ==========================================
def run(seed: int, factor: float):
    TOPICS_CONFIG = [
        {"name": TOPIC_CONTROL_INGRESS, "partitions": 1},
        {"name": TOPIC_TELEMETRY, "partitions": 1},
        {"name": TOPIC_LIFECYCLE, "partitions": 1},
        {"name": TOPIC_PREDICTION_REQUESTS, "partitions": 3},
        {"name": TOPIC_GROUND_TRUTH, "partitions": 3},
    ]

    logger.info(
        f"Connecting to Kafka Admin at {KAFKA_BROKER} to ensure topics exist..."
    )
    admin_connector = KafkaAdminConnector(bootstrap_servers=KAFKA_BROKER)
    admin_connector.create_topics(topics_config=TOPICS_CONFIG)
    time.sleep(2)

    env = DynamicRealtimeEnvironment(factor=factor)
    env.registry.register_sim_parameter(mill_params)

    ingress = KafkaIngress(bootstrap_servers=KAFKA_BROKER, topic=TOPIC_CONTROL_INGRESS)
    egress = KafkaEgress(
        bootstrap_servers=KAFKA_BROKER, topic_router=custom_topic_router
    )

    env.setup_ingress([ingress])
    env.setup_egress([egress])

    logger.info(f"Initializing Sampler with random seed: {seed}")
    sampler = Sampler(rng=np.random.default_rng(seed))
    product_lines = ["structural", "microalloyed", "high_alloy"]

    env.process(drift_engine(env, product_lines))
    env.process(telemetry_monitor(env, product_lines))

    for prod in product_lines:
        mill_res = DynamicResource(env, "HotRolling", f"mill_{prod}")
        primer_hash = uuid.uuid4().hex[:8].upper()

        env.process(roll_slab(env, primer_hash, prod, sampler, mill_res))
        env.process(arrival_process(env, prod, sampler, mill_res))

    logger.info(f"Kafka Hot Rolling Digital Twin Started! (Factor: {factor}x)")
    logger.info(" - Waiting for control signals on topic: sim-controls")

    try:
        env.run()
    except KeyboardInterrupt:
        logger.info("Simulation halted by user.")
    finally:
        env.teardown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Start the Hot Rolling Digital Twin Simulation."
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for the numpy sampler (default: 42)",
    )
    parser.add_argument(
        "--factor",
        type=float,
        default=1.0,
        help="Simulation real-time speed factor (default: 1.0)",
    )

    args = parser.parse_args()
    run(seed=args.seed, factor=args.factor)
