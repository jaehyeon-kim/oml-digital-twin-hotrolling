"""
Core Simulation Logic for the Hot Rolling Digital Twin.

This module defines the Discrete Event Simulation processes using the SimPy backbone.
It controls the lifecycle of steel slabs entering the mill, the physical execution of
rolling passes, and the injection of hidden concept drift (wear and tear).
"""

import logging
import uuid
from dynamic_des import DynamicRealtimeEnvironment, DynamicResource, Sampler

from src.physics import calculate_actual_force, calculate_baseline_force
from src.schemas import (
    BaselinePrediction,
    GroundTruthEvent,
    GroundTruthIdentifiers,
    GroundTruthMetrics,
    PredictionIdentifiers,
    PredictionRequestEvent,
)
from src.slab import SlabState

logger = logging.getLogger("generator.sim_logic")


# ==========================================
# Core Simulation Processes
# ==========================================
def drift_engine(env: DynamicRealtimeEnvironment, product_lines: list[str]):
    """
    Background process that continuously modulates the hidden wear state of the mill.

    This engine supports two modes of concept drift:
    1. Abrupt (Instant): Teleports the wear to a specific value immediately.
    2. Gradual (Auto): Increments the wear by a specific step value at a set frequency,
       bouncing between 0 percent and 100 percent to simulate a continuously degrading
       and repairing machine.

    Args:
        env (DynamicRealtimeEnvironment): The real-time simulation environment.
        product_lines (list[str]): List of active steel grades to apply drift to.
    """
    while True:
        for prod in product_lines:
            velocity_var = env.registry.get(
                f"HotRolling.variables.velocity_{prod}"
            ).value

            # Check if the velocity is using the new dict schema for gradual drift
            if isinstance(velocity_var, dict):
                drift_type = velocity_var.get("type", "abrupt")

                if drift_type == "gradual":
                    step_value = float(velocity_var.get("value", 0.0))
                    freq = int(velocity_var.get("freq", 1))

                    # Only apply drift on the specified frequency interval
                    if (
                        step_value != 0.0
                        and int(env.now) > 0
                        and int(env.now) % freq == 0
                    ):
                        wear_path = f"HotRolling.containers.wear_{prod}.current_cap"
                        current_wear = env.registry.get(wear_path).value

                        new_wear = current_wear + step_value

                        # Bounce Logic: Reverse direction if hitting boundaries to keep the simulation running infinitely
                        if new_wear >= 100.0:
                            new_wear = 100.0
                            velocity_var["value"] = -abs(
                                step_value
                            )  # Force downward trajectory
                            env.registry.update(
                                f"HotRolling.variables.velocity_{prod}", velocity_var
                            )
                        elif new_wear <= 0.001:
                            new_wear = 0.001
                            velocity_var["value"] = abs(
                                step_value
                            )  # Force upward trajectory
                            env.registry.update(
                                f"HotRolling.variables.velocity_{prod}", velocity_var
                            )

                        env.registry.update(wear_path, new_wear)

            # Fallback for standard float-based abrupt velocity (legacy support)
            elif isinstance(velocity_var, (int, float)):
                velocity = float(velocity_var)
                if velocity != 0.0:
                    wear_path = f"HotRolling.containers.wear_{prod}.current_cap"
                    current_wear = env.registry.get(wear_path).value
                    new_wear = max(0.001, min(100.0, current_wear + velocity))
                    env.registry.update(wear_path, new_wear)

        # Tick every 1 simulation second
        yield env.timeout(1.0)


def arrival_process(
    env: DynamicRealtimeEnvironment,
    product_type: str,
    variable_passes: bool,
    max_passes: int,
    sampler: Sampler,
    mill_res: DynamicResource,
):
    """
    Simulates the stochastic arrival of new steel slabs from the reheating furnace.

    Args:
        env (DynamicRealtimeEnvironment): The simulation environment.
        product_type (str): The specific steel grade being generated.
        variable_passes (bool): Whether the slab requires a random number of passes.
        max_passes (int): The maximum (or fixed) number of passes per slab.
        sampler (Sampler): Configured random number generator.
        mill_res (DynamicResource): The logical resource queue to block on.
    """
    arrival_cfg = env.registry.get_config(f"HotRolling.arrival.{product_type}")
    while True:
        # Wait for the next arrival based on the configured exponential distribution
        yield env.timeout(sampler.sample(arrival_cfg))

        # Generate inline hash for tracking the slab
        new_hash = uuid.uuid4().hex[:8].upper()

        # Spawn a new concurrent rolling process for the arrived slab
        env.process(
            roll_slab(
                env,
                new_hash,
                product_type,
                variable_passes,
                max_passes,
                sampler,
                mill_res,
            )
        )


def roll_slab(
    env: DynamicRealtimeEnvironment,
    task_key: str,
    product_type: str,
    variable_passes: bool,
    max_passes: int,
    sampler: Sampler,
    mill_res: DynamicResource,
):
    """
    Executes the physical lifecycle of a single steel slab passing through the mill.

    This function acts as the primary event emitter for the Digital Twin, guaranteeing
    that PredictionRequest (Event A) is always emitted before GroundTruth (Event B)
    to prevent temporal leakage in the Online Machine Learning pipeline.

    Args:
        env (DynamicRealtimeEnvironment): The simulation environment.
        task_key (str): Unique hash for this specific slab.
        product_type (str): The steel grade.
        variable_passes (bool): If True, randomly determine pass count.
        max_passes (int): Upper bound for passes.
        sampler (Sampler): RNG for physical variations.
        mill_res (DynamicResource): The shared mill resource to lock during processing.
    """
    slab_id = f"SLAB-{product_type.upper()}-{task_key}"
    if variable_passes:
        total_passes = int(sampler.rng.integers(3, max_passes + 1))
    else:
        total_passes = max_passes

    slab = SlabState(sampler.rng, total_passes)

    for pass_num in range(1, total_passes + 1):
        task_key_pass = f"{slab_id}-P{pass_num}"

        # Determine phase of rolling to fetch correct service time distribution
        if pass_num <= 2:
            pass_path = "HotRolling.service.pass_roughing"
        elif pass_num <= 4:
            pass_path = "HotRolling.service.pass_intermediate"
        else:
            pass_path = "HotRolling.service.pass_finishing"

        # Queued
        env.publish_event(task_key_pass, {"path_id": pass_path, "status": "queued"})
        logger.info(
            f"key: {task_key_pass}, path: {pass_path.split('.')[-1]}, status: queued, timestamp: {env.now:.3f}"
        )

        # Block until the physical mill is available
        with mill_res.request() as req:
            yield req

            # Started
            env.publish_event(
                task_key_pass, {"path_id": pass_path, "status": "started"}
            )
            logger.info(
                f"key: {task_key_pass}, path: {pass_path.split('.')[-1]}, status: started, timestamp: {env.now:.3f}"
            )

            # 1. Calculate physical state before the pass occurs
            features = slab.next_pass()
            baseline = calculate_baseline_force(product_type, features)

            # 2. Emit EVENT A: The Prediction Request (What we THINK will happen)
            pred_event = PredictionRequestEvent(
                timestamp=env._get_iso_timestamp(env.start_datetime, env.now),
                identifiers=PredictionIdentifiers(
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

            # 3. Simulate the physical time it takes to crush the steel
            pass_cfg = env.registry.get_config(pass_path)
            yield env.timeout(sampler.sample(pass_cfg))

            # 4. Fetch the hidden wear state (Concept Drift) to calculate the true physical reality
            raw_wear = env.registry.get(
                f"HotRolling.containers.wear_{product_type}.current_cap"
            ).value

            # Clean Start Logic: If it is at the floor (0.001), treat as absolute 0.0
            reference_wear = 0.0 if raw_wear <= 0.001 else raw_wear

            # Add +/- 2 percent variation to simulate parallel line differences and sensor noise
            local_wear = min(100.0, reference_wear * sampler.rng.uniform(0.98, 1.02))

            actual_force = calculate_actual_force(
                baseline, features, local_wear, sampler.rng
            )

            # 5. Emit EVENT B: The Ground Truth (What ACTUALLY happened)
            gt_event = GroundTruthEvent(
                timestamp=env._get_iso_timestamp(env.start_datetime, env.now),
                identifiers=GroundTruthIdentifiers(
                    slab_id=slab_id, pass_number=pass_num
                ),
                ground_truth=GroundTruthMetrics(
                    actual_roll_force_kn=actual_force, wear_level=local_wear
                ),
            )
            env.publish_event(f"GT-{task_key_pass}", gt_event.model_dump(mode="json"))

            # Finished
            env.publish_event(
                task_key_pass, {"path_id": pass_path, "status": "finished"}
            )
            logger.info(
                f"key: {task_key_pass}, path: {pass_path.split('.')[-1]}, status: finished, timestamp: {env.now:.3f}"
            )
