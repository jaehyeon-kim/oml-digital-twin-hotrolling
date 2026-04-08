import logging
import uuid
from dynamic_des import DynamicRealtimeEnvironment, DynamicResource, Sampler

from src.physics import calculate_actual_force, calculate_baseline_force
from src.schemas import (
    BaselinePrediction,
    GroundTruthEvent,
    GroundTruthIdentifiers,
    GroundTruthMetrics,
    Identifiers,
    PredictionRequestEvent,
)
from src.slab import SlabState

logger = logging.getLogger("generator.sim_logic")


# ==========================================
# Core Simulation Processes
# ==========================================
def drift_engine(env: DynamicRealtimeEnvironment, product_lines: list[str]):
    while True:
        for prod in product_lines:
            velocity = env.registry.get(f"HotRolling.variables.velocity_{prod}").value
            if velocity != 0.0:
                wear_path = f"HotRolling.containers.wear_{prod}.current_cap"
                current_wear = env.registry.get(wear_path).value
                new_wear = max(0.001, min(100.0, current_wear + velocity))
                env.registry.update(wear_path, new_wear)
        yield env.timeout(1.0)


def arrival_process(
    env: DynamicRealtimeEnvironment,
    product_type: str,
    sampler: Sampler,
    mill_res: DynamicResource,
):
    arrival_cfg = env.registry.get_config(f"HotRolling.arrival.{product_type}")
    while True:
        yield env.timeout(sampler.sample(arrival_cfg))
        # Generate inline hash
        new_hash = uuid.uuid4().hex[:8].upper()
        env.process(roll_slab(env, new_hash, product_type, sampler, mill_res))


def roll_slab(
    env: DynamicRealtimeEnvironment,
    task_key: str,
    product_type: str,
    sampler: Sampler,
    mill_res: DynamicResource,
):
    slab_id = f"SLAB-{product_type.upper()}-{task_key}"
    total_passes = int(sampler.rng.integers(3, 6))
    slab = SlabState(sampler.rng, total_passes)

    for pass_num in range(1, total_passes + 1):
        task_key_pass = f"{slab_id}-P{pass_num}"

        if pass_num <= 2:
            pass_path = "HotRolling.service.pass_roughing"
        elif pass_num <= 4:
            pass_path = "HotRolling.service.pass_intermediate"
        else:
            pass_path = "HotRolling.service.pass_finishing"

        # 1. Queued
        env.publish_event(task_key_pass, {"path_id": pass_path, "status": "queued"})
        logger.info(
            f"key: {task_key_pass}, path: {pass_path.split('.')[-1]}, status: queued, timestamp: {env.now:.3f}"
        )

        with mill_res.request() as req:
            yield req

            # 2. Started
            env.publish_event(
                task_key_pass, {"path_id": pass_path, "status": "started"}
            )
            logger.info(
                f"key: {task_key_pass}, path: {pass_path.split('.')[-1]}, status: started, timestamp: {env.now:.3f}"
            )

            features = slab.next_pass()
            baseline = calculate_baseline_force(product_type, features)

            pred_event = PredictionRequestEvent(
                timestamp=env._get_iso_timestamp(env.start_datetime, env.now),
                identifiers=Identifiers(
                    slab_id=slab_id,
                    pass_number=pass_num,
                    steel_grade=product_type,
                    routing_key=f"{product_type}_mill",
                ),
                features=features,
                baseline_prediction=BaselinePrediction(baseline_roll_force_kn=baseline),
            )
            env.publish_event(
                f"PRED-{task_key_pass}", pred_event.model_dump(mode="json")
            )

            pass_cfg = env.registry.get_config(pass_path)
            yield env.timeout(sampler.sample(pass_cfg))

            raw_wear = env.registry.get(
                f"HotRolling.containers.wear_{product_type}.current_cap"
            ).value

            # Clean Start Logic: If it's at the floor (0.001), treat as 0.
            # Otherwise, keep the precision.
            reference_wear = 0.0 if raw_wear <= 0.001 else raw_wear

            # Add +/- 2% variation to simulate parallel line differences
            # If reference_wear is 0.0, local_wear stays 0.0 (Pure baseline)
            local_wear = reference_wear * sampler.rng.uniform(0.98, 1.02)

            actual_force = calculate_actual_force(baseline, local_wear, sampler.rng)

            gt_event = GroundTruthEvent(
                timestamp=env._get_iso_timestamp(env.start_datetime, env.now),
                identifiers=GroundTruthIdentifiers(
                    slab_id=slab_id, pass_number=pass_num
                ),
                ground_truth=GroundTruthMetrics(actual_roll_force_kn=actual_force),
            )
            env.publish_event(f"GT-{task_key_pass}", gt_event.model_dump(mode="json"))

            # 3. Finished
            env.publish_event(
                task_key_pass, {"path_id": pass_path, "status": "finished"}
            )
            logger.info(
                f"key: {task_key_pass}, path: {pass_path.split('.')[-1]}, status: finished, timestamp: {env.now:.3f}"
            )
