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

class EventMatchProcessFunction : KeyedCoProcessFunction<String, PredictionRequestEvent, GroundTruthEvent, MatchedEvent>() {
    @Transient
    private lateinit var logger: Logger

    private lateinit var predictionBuffer: ValueState<PredictionRequestEvent>
    private lateinit var gtBuffer: ValueState<GroundTruthEvent>

    private val stateTimeoutMs: Long = 5 * 60 * 1000L // 5 Minute Timeout

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

    override fun processElement1(
        event: PredictionRequestEvent,
        ctx: Context,
        out: Collector<MatchedEvent>,
    ) {
        val earlyGt = gtBuffer.value()

        if (earlyGt != null) {
            logger.info(">>> [MATCH] Recovered early GT for: ${event.identifiers.slabId}-P${event.identifiers.passNumber}")
            out.collect(MatchedEvent(event, earlyGt))
            gtBuffer.clear()
        } else {
            predictionBuffer.update(event)
            val timerTime = ctx.timerService().currentProcessingTime() + stateTimeoutMs
            ctx.timerService().registerProcessingTimeTimer(timerTime)
        }
    }

    override fun processElement2(
        gt: GroundTruthEvent,
        ctx: Context,
        out: Collector<MatchedEvent>,
    ) {
        val request = predictionBuffer.value()

        if (request != null) {
            logger.info(">>> [MATCH] Joining PRED and GT for: ${gt.identifiers.slabId}-P${gt.identifiers.passNumber}")
            out.collect(MatchedEvent(request, gt))
            predictionBuffer.clear()
        } else {
            logger.warn("!!! [RACE CONDITION] GT arrived early for: ${gt.identifiers.slabId}-P${gt.identifiers.passNumber}")
            gtBuffer.update(gt)
            val timerTime = ctx.timerService().currentProcessingTime() + stateTimeoutMs
            ctx.timerService().registerProcessingTimeTimer(timerTime)
        }
    }

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
