import logging
import numpy as np

from dynamic_des import (
    CapacityConfig,
    ConsoleEgress,
    DistributionConfig,
    DynamicRealtimeEnvironment,
    DynamicResource,
    LocalIngress,
    Sampler,
    SimParameter,
)
from dynamic_des.resources.container import DynamicContainer

from src.physics import calculate_baseline_force, calculate_actual_force
from src.slab import SlabState
from src.schemas import (
    Identifiers,
    BaselinePrediction,
    PredictionRequestEvent,
    GroundTruthIdentifiers,
    GroundTruthMetrics,
    GroundTruthEvent,
)

# Setup basic console logging
logging.basicConfig(
    level=logging.INFO, format="%(levelname)s [%(asctime)s] %(message)s"
)
logger = logging.getLogger("generator_local")

# ==========================================
# 1. Parameter Definition (Single Product)
# ==========================================
mill_params = SimParameter(
    sim_id="HotRolling",
    arrival={
        "line_structural": DistributionConfig(dist="exponential", rate=0.5),
    },
    service={
        "pass_roughing": DistributionConfig(dist="normal", mean=2.0, std=0.2),
        "pass_intermediate": DistributionConfig(dist="normal", mean=4.5, std=0.5),
        "pass_finishing": DistributionConfig(dist="normal", mean=8.0, std=1.2),
    },
    resources={
        "mill_structural": CapacityConfig(current_cap=1, max_cap=1),
    },
    containers={
        "wear_state_structural": CapacityConfig(current_cap=0.001, max_cap=100.0),
    },
)


# ==========================================
# 2. Event Loops
# ==========================================
def arrival_process(
    env: DynamicRealtimeEnvironment,
    sampler: Sampler,
    mill_res: DynamicResource,
    wear_container: DynamicContainer,
):
    """Generates arriving slabs based on the standard exponential distribution."""
    arrival_cfg = env.registry.get_config("HotRolling.arrival.line_structural")
    task_id = 0

    while True:
        yield env.timeout(sampler.sample(arrival_cfg))
        env.process(roll_slab(env, task_id, sampler, mill_res, wear_container))
        task_id += 1


def roll_slab(
    env: DynamicRealtimeEnvironment,
    task_id: int,
    sampler: Sampler,
    mill_res: DynamicResource,
    wear_container: DynamicContainer,
):
    """Simulates a single slab passing through the mill, calculating physics per pass."""
    slab_id = f"SLAB-STRUCT-{task_id}"

    # Bound to 3-5 passes for quick demo visualizations
    total_passes = int(sampler.rng.integers(3, 6))

    # Initialize the stateful physics tracker
    slab = SlabState(sampler.rng, total_passes)

    for pass_num in range(1, total_passes + 1):
        task_key = f"{slab_id}-P{pass_num}"

        # Routing logic for processing times
        if pass_num <= 2:
            pass_path = "HotRolling.service.pass_roughing"
        elif pass_num <= 4:
            pass_path = "HotRolling.service.pass_intermediate"
        else:
            pass_path = "HotRolling.service.pass_finishing"

        env.publish_event(task_key, {"path_id": pass_path, "status": "queued"})

        with mill_res.request() as req:
            yield req
            env.publish_event(task_key, {"path_id": pass_path, "status": "started"})

            # ----------------------------------------------------
            # PHYSICS & EVENT A: PREDICTION REQUEST
            # ----------------------------------------------------
            # Advance the slab physical state
            features = slab.next_pass()
            baseline = calculate_baseline_force("structural", features)

            pred_event = PredictionRequestEvent(
                timestamp=env._get_iso_timestamp(env.start_datetime, env.now),
                identifiers=Identifiers(
                    slab_id=slab_id,
                    pass_number=pass_num,
                    steel_grade="structural",
                    routing_key="structural_mill",
                ),
                features=features,
                baseline_prediction=BaselinePrediction(baseline_roll_force_kn=baseline),
            )
            env.publish_event(f"PRED-{task_key}", pred_event.model_dump(mode="json"))

            # Yield physical processing time
            pass_cfg = env.registry.get_config(pass_path)
            yield env.timeout(sampler.sample(pass_cfg))

            # ----------------------------------------------------
            # PHYSICS & EVENT B: GROUND TRUTH ACTUALS
            # ----------------------------------------------------
            # Fetch scheduled concept drift from registry
            current_wear_int = int(wear_container.capacity)
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
            env.publish_event(f"GT-{task_key}", gt_event.model_dump(mode="json"))

            env.publish_event(task_key, {"path_id": pass_path, "status": "finished"})


def telemetry_monitor(
    env: DynamicRealtimeEnvironment,
    wear_container: DynamicContainer,
    mill_res: DynamicResource,
):
    """Streams the current system health and drift levels to the console."""
    while True:
        env.publish_telemetry(
            "HotRolling.containers.wear_state_structural.current_cap",
            wear_container.capacity,
        )
        env.publish_telemetry("HotRolling.mill.structural.in_use", mill_res.in_use)
        yield env.timeout(2.0)


# ==========================================
# 3. Main Execution Bootstrapper
# ==========================================
def run():
    # Setup Environment
    env = DynamicRealtimeEnvironment(factor=1.0)
    env.registry.register_sim_parameter(mill_params)

    # Ingress: Abrupt Drift Schedule
    # Jumps to 40% wear at 10 seconds, and 85% critical wear at 20 seconds.
    schedule = [
        (10.0, "HotRolling.containers.wear_state_structural.current_cap", 40),
        (20.0, "HotRolling.containers.wear_state_structural.current_cap", 85),
    ]
    ingress = LocalIngress(schedule)

    # Egress: Console output for local debugging
    egress = ConsoleEgress()

    env.setup_ingress([ingress])
    env.setup_egress([egress])

    # Initialize Physics and Resources
    sampler = Sampler(rng=np.random.default_rng(42))
    mill_res = DynamicResource(env, "HotRolling", "mill_structural")
    wear_container = DynamicContainer(env, "HotRolling", "wear_state_structural")

    # Start Processes
    env.process(arrival_process(env, sampler, mill_res, wear_container))
    # env.process(telemetry_monitor(env, wear_container, mill_res))

    logger.info("Local Hot Rolling Digital Twin Started!")
    logger.info(" - Simulating Structural Steel Line.")
    logger.info(" - Watch for Concept Drift to trigger at t=10.0s and t=20.0s.")

    try:
        # Run exactly 30 seconds
        env.run(until=30)
    except KeyboardInterrupt:
        logger.info("Simulation halted by user.")
    finally:
        env.teardown()


if __name__ == "__main__":
    run()
