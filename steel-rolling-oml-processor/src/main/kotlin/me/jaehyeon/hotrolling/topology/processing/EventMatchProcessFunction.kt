package me.jaehyeon.hotrolling.topology.processing

import me.jaehyeon.hotrolling.domain.model.GroundTruthEvent
import me.jaehyeon.hotrolling.domain.model.MatchedEvent
import me.jaehyeon.hotrolling.domain.model.PredictionRequestEvent
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Stateful stream joiner designed to handle network latency and race conditions.
 *
 * In industrial environments, Event A (Prediction) and Event B (Ground Truth) might
 * arrive out of order due to network partitions or Kafka partition skew.
 * This function utilizes Flink ValueStates to buffer whichever event arrives first
 * and waits for its partner, securely joining them by Slab ID and Pass Number.
 */
class EventMatchProcessFunction : KeyedCoProcessFunction<String, PredictionRequestEvent, GroundTruthEvent, MatchedEvent>() {
    @Transient
    private lateinit var logger: Logger

    // Buffers for storing unpaired events
    private lateinit var predictionBuffer: ValueState<PredictionRequestEvent>
    private lateinit var gtBuffer: ValueState<GroundTruthEvent>

    // Time-To-Live: If an event sits in the buffer for 5 minutes without a match,
    // it is considered an orphan (e.g., sensor failure) and purged to prevent memory leaks.
    private val stateTimeoutMs: Long = 5 * 60 * 1000L

    override fun open(parameters: Configuration) {
        logger = LoggerFactory.getLogger(EventMatchProcessFunction::class.java)

        predictionBuffer =
            runtimeContext.getState(
                ValueStateDescriptor("pred-buffer", TypeInformation.of(PredictionRequestEvent::class.java)),
            )
        gtBuffer =
            runtimeContext.getState(
                ValueStateDescriptor("gt-buffer", TypeInformation.of(GroundTruthEvent::class.java)),
            )
    }

    /**
     * Processes incoming Prediction Requests (Event A).
     */
    override fun processElement1(
        event: PredictionRequestEvent,
        ctx: Context,
        out: Collector<MatchedEvent>,
    ) {
        val earlyGt = gtBuffer.value()

        // Recovery Path: Ground Truth arrived first and is already waiting
        if (earlyGt != null) {
            logger.info(">>> [MATCH] Recovered early GT for: ${event.identifiers.slabId}-P${event.identifiers.passNumber}")
            out.collect(MatchedEvent(event, earlyGt))
            gtBuffer.clear() // Prevent memory leaks
        } else {
            // Buffer the prediction and set a Time-To-Live timer
            predictionBuffer.update(event)
            val timerTime = ctx.timerService().currentProcessingTime() + stateTimeoutMs
            ctx.timerService().registerProcessingTimeTimer(timerTime)
        }
    }

    /**
     * Processes incoming Ground Truths (Event B).
     */
    override fun processElement2(
        gt: GroundTruthEvent,
        ctx: Context,
        out: Collector<MatchedEvent>,
    ) {
        val request = predictionBuffer.value()

        // Happy Path: Prediction arrived first as expected and is waiting
        if (request != null) {
            logger.info(">>> [MATCH] Joining PRED and GT for: ${gt.identifiers.slabId}-P${gt.identifiers.passNumber}")
            out.collect(MatchedEvent(request, gt))
            predictionBuffer.clear() // Prevent memory leaks
        } else {
            // Race Condition: Ground Truth arrived before the Prediction
            logger.warn("!!! [RACE CONDITION] GT arrived early for: ${gt.identifiers.slabId}-P${gt.identifiers.passNumber}")
            gtBuffer.update(gt)
            val timerTime = ctx.timerService().currentProcessingTime() + stateTimeoutMs
            ctx.timerService().registerProcessingTimeTimer(timerTime)
        }
    }

    /**
     * Triggered when the Time-To-Live timer expires. Cleans up orphaned state.
     */
    override fun onTimer(
        timestamp: Long,
        ctx: OnTimerContext,
        out: Collector<MatchedEvent>,
    ) {
        val orphanedPred = predictionBuffer.value()
        val orphanedGt = gtBuffer.value()

        if (orphanedPred != null) {
            logger.error(
                "!!! [ORPHAN ALERT] Prediction for ${orphanedPred.identifiers.slabId}-P${orphanedPred.identifiers.passNumber} timed out. Clearing memory.",
            )
            predictionBuffer.clear()
        }

        if (orphanedGt != null) {
            logger.error(
                "!!! [ORPHAN ALERT] Ground Truth for ${orphanedGt.identifiers.slabId}-P${orphanedGt.identifiers.passNumber} timed out. Clearing memory.",
            )
            gtBuffer.clear()
        }
    }
}
