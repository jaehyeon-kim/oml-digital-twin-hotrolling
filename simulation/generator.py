import logging
import os
import time
import numpy as np

from dynamic_des import (
    CapacityConfig,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    KafkaAdminConnector,
    KafkaEgress,
    KafkaIngress,
    Sampler,
    SimParameter,
)
from dynamic_des.resources.container import DynamicContainer

from simulation.src.physics import calculate_baseline_force, calculate_actual_force
from simulation.src.schemas import (
    Features,
    Identifiers,
    BaselinePrediction,
    PredictionRequestEvent,
    GroundTruthIdentifiers,
    GroundTruthMetrics,
    GroundTruthEvent,
)

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(name)s: %(message)s"
)
logger = logging.getLogger("mill_generator")

# ==========================================
# 1. Parameter Definition
# ==========================================
mill_params = SimParameter(
    sim_id="HotRolling",
    arrival={
        "line_structural": DistributionConfig(dist="exponential", rate=0.06),
        "line_microalloyed": DistributionConfig(dist="exponential", rate=0.03),
        "line_high_alloy": DistributionConfig(dist="exponential", rate=0.01),
    },
    service={
        "pass_roughing": DistributionConfig(dist="normal", mean=2.0, std=0.2),
        "pass_intermediate": DistributionConfig(dist="normal", mean=4.5, std=0.5),
        "pass_finishing": DistributionConfig(dist="normal", mean=8.0, std=1.2),
    },
    resources={
        "mill_structural": CapacityConfig(current_cap=10, max_cap=10),
        "mill_microalloyed": CapacityConfig(current_cap=10, max_cap=10),
        "mill_high_alloy": CapacityConfig(current_cap=10, max_cap=10),
    },
    containers={
        "wear_state_structural": CapacityConfig(current_cap=0, max_cap=100),
        "wear_state_microalloyed": CapacityConfig(current_cap=0, max_cap=100),
        "wear_state_high_alloy": CapacityConfig(current_cap=0, max_cap=100),
    },
)


def generate_valid_features(rng: np.random.Generator, wear_float: float) -> Features:
    """Generates synthetic sensor data adhering strictly to the Pydantic boundaries."""
    return Features(
        reheating_time_min=round(rng.uniform(135.0, 1188.0), 2),
        roll_diameter_mm=round(rng.uniform(1042.0, 1121.0), 2),
        roll_crown_mm=round(rng.uniform(0.0, 217.0), 2),
        entry_thickness_mm=round(rng.uniform(15.0, 250.0), 2),
        width_mm=round(rng.uniform(1613.0, 4182.0), 2),
        length_mm=round(rng.uniform(1794.0, 38139.0), 2),
        temperature_c=round(rng.uniform(889.0, 1233.0), 2),
        speed_m_s=round(rng.uniform(1.5, 5.6), 2),
        wait_time_sec=round(rng.uniform(6.0, 225.0), 2),
        reduction_pct=round(rng.uniform(0.01, 0.40), 3),
        strain=round(rng.uniform(0.02, 0.56), 3),
        strain_rate=round(rng.uniform(0.5, 16.3), 3),
        flow_stress_mpa=round(rng.uniform(5.0, 179.0), 2),
        wear_level=wear_float,
    )


# ==========================================
# 2. Event Loops
# ==========================================
def arrival_process(
    env: DynamicRealtimeEnvironment,
    product_type: str,
    sampler: Sampler,
    mill_res: DynamicResource,
    wear_container: DynamicContainer,
):
    """Spawns new steel slabs into the appropriate production line."""
    arrival_cfg = env.registry.get_config(f"HotRolling.arrival.line_{product_type}")
    task_id = 0

    while True:
        yield env.timeout(sampler.sample(arrival_cfg))
        env.process(
            roll_slab(env, task_id, product_type, sampler, mill_res, wear_container)
        )
        task_id += 1


def roll_slab(
    env: DynamicRealtimeEnvironment,
    task_id: int,
    product_type: str,
    sampler: Sampler,
    mill_res: DynamicResource,
    wear_container: DynamicContainer,
):
    """Routes the steel through 11-17 passes, calculating physics and emitting Kafka events."""
    slab_id = f"SLAB-{product_type.upper()}-{task_id}"
    total_passes = int(
        sampler.rng.integers(11, 18)
    )  # Reversing mills use 11 to 17 passes

    for pass_num in range(1, total_passes + 1):
        task_key = f"{slab_id}-PASS-{pass_num}"

        # Determine the phase (which controls the processing time)
        if pass_num <= 5:
            pass_path = "HotRolling.service.pass_roughing"
        elif pass_num <= 10:
            pass_path = "HotRolling.service.pass_intermediate"
        else:
            pass_path = "HotRolling.service.pass_finishing"

        env.publish_event(task_key, {"path_id": pass_path, "status": "queued"})

        with mill_res.request() as req:
            yield req
            env.publish_event(task_key, {"path_id": pass_path, "status": "started"})

            # --- DYNAMIC CONCEPT DRIFT ---
            if product_type == "microalloyed":
                # Gradual Drift: 1% increase per 60 seconds of simulation time
                calculated_wear = min(100, int(env.now / 60.0))
                env.registry.update(
                    "HotRolling.containers.wear_state_microalloyed.current_cap",
                    calculated_wear,
                )

            current_wear_int = int(wear_container.capacity)
            wear_float = max(0, min(current_wear_int, 100)) / 100.0

            # --- EVENT A: PREDICTION REQUEST ---
            features = generate_valid_features(sampler.rng, wear_float)
            baseline = calculate_baseline_force(product_type, features)

            pred_event = PredictionRequestEvent(
                timestamp=env._get_iso_timestamp(env.start_datetime, env.now),
                identifiers=Identifiers(
                    slab_id=slab_id,
                    pass_number=pass_num,
                    steel_grade=product_type,
                    routing_key=f"{product_type}_P{pass_num}",
                ),
                features=features,
                baseline_prediction=BaselinePrediction(baseline_roll_force_kn=baseline),
            )
            env.publish_event(f"EVENT-A-{task_key}", pred_event.model_dump())

            # --- YIELD THE PHYSICAL ROLLING TIME ---
            pass_cfg = env.registry.get_config(pass_path)
            yield env.timeout(sampler.sample(pass_cfg))

            # --- EVENT B: GROUND TRUTH ACTUALS ---
            actual_force = calculate_actual_force(
                baseline, current_wear_int, sampler.rng
            )

            gt_event = GroundTruthEvent(
                timestamp=env._get_iso_timestamp(env.start_datetime, env.now),
                identifiers=GroundTruthIdentifiers(
                    slab_id=slab_id, pass_number=pass_num
                ),
                ground_truth=GroundTruthMetrics(actual_roll_force_kn=actual_force),
            )
            env.publish_event(f"EVENT-B-{task_key}", gt_event.model_dump())

            env.publish_event(task_key, {"path_id": pass_path, "status": "finished"})


def telemetry_monitor(env: DynamicRealtimeEnvironment, wear_containers: list):
    """Streams the current wear states to the dashboard."""
    while True:
        for c in wear_containers:
            env.publish_telemetry(f"HotRolling.wear_levels.{c.obj_id}", c.capacity)
        yield env.timeout(5.0)


# ==========================================
# 3. Main Execution Bootstrapper
# ==========================================
def run():
    BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    # 1. Topic Creation
    admin = KafkaAdminConnector(bootstrap_servers=BOOTSTRAP_SERVERS, max_tasks=100)
    admin.create_topics(
        [
            {"name": "sim-config", "partitions": 1},
            {"name": "sim-telemetry", "partitions": 1},
            {"name": "sim-events", "partitions": 3},
        ]
    )
    time.sleep(2)

    # 2. Setup Environment
    env = DynamicRealtimeEnvironment(factor=1.0)
    env.registry.register_sim_parameter(mill_params)

    ingress = KafkaIngress(topic="sim-config", bootstrap_servers=BOOTSTRAP_SERVERS)
    egress = KafkaEgress(
        telemetry_topic="sim-telemetry",
        event_topic="sim-events",
        bootstrap_servers=BOOTSTRAP_SERVERS,
    )

    env.setup_ingress([ingress])
    env.setup_egress([egress])

    sampler = Sampler(rng=np.random.default_rng(42))

    # 3. Initialize Physical Lines
    products = ["structural", "microalloyed", "high_alloy"]
    wear_objects = []

    for p in products:
        mill_res = DynamicResource(env, "HotRolling", f"mill_{p}")
        wear_cont = DynamicContainer(env, "HotRolling", f"wear_state_{p}")
        wear_objects.append(wear_cont)

        env.process(arrival_process(env, p, sampler, mill_res, wear_cont))

    env.process(telemetry_monitor(env, wear_objects))

    logger.info("Hot Rolling Digital Twin Started!")
    logger.info("- Structural Line: Stable accuracy (No drift)")
    logger.info("- Microalloyed Line: Gradual drift (Tied to sim_ts)")
    logger.info("- High-Alloy Line: Ready for abrupt drift via sim-config topic")

    try:
        env.run()
    except KeyboardInterrupt:
        logger.info("Simulation halted.")
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
