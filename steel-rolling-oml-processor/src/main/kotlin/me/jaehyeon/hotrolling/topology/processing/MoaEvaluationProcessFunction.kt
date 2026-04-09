package me.jaehyeon.hotrolling.topology.processing

import me.jaehyeon.hotrolling.domain.model.GroundTruthEvent
import me.jaehyeon.hotrolling.domain.model.MoaEvaluationResult
import me.jaehyeon.hotrolling.domain.model.PredictionRequestEvent
import me.jaehyeon.hotrolling.domain.model.SgdState
import me.jaehyeon.hotrolling.domain.model.TargetMeanState
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector
import kotlin.math.abs

class MoaEvaluationProcessFunction(
    private val ewmaLambda: Double,
    private val sgdLearningRate: Double,
    private val sgdDecay: Double,
) : KeyedCoProcessFunction<String, PredictionRequestEvent, GroundTruthEvent, MoaEvaluationResult>() {
    // Buffer State: Holds Event A until Event B arrives
    private lateinit var predictionBuffer: ValueState<PredictionRequestEvent>

    // Strategy A (POJO State): TargetMean & SGD
    private lateinit var targetMeanState: ValueState<TargetMeanState>
    private lateinit var sgdState: ValueState<SgdState>

    // Strategy B (Byte State): AMRules
    private lateinit var amRulesByteState: ValueState<ByteArray>

    @Transient
    private var amRulesLiveCache: MutableMap<String, Any> = mutableMapOf()

    override fun open(parameters: Configuration) {
        predictionBuffer =
            runtimeContext.getState(
                ValueStateDescriptor("prediction-buffer", Types.POJO(PredictionRequestEvent::class.java)),
            )
        targetMeanState =
            runtimeContext.getState(
                ValueStateDescriptor("target-mean-state", Types.POJO(TargetMeanState::class.java)),
            )
        sgdState =
            runtimeContext.getState(
                ValueStateDescriptor("sgd-state", Types.POJO(SgdState::class.java)),
            )
        amRulesByteState =
            runtimeContext.getState(
                ValueStateDescriptor("amrules-byte-state", ByteArray::class.java),
            )
        amRulesLiveCache = mutableMapOf()
    }

    override fun processElement1(
        event: PredictionRequestEvent,
        ctx: Context,
        out: Collector<MoaEvaluationResult>,
    ) {
        predictionBuffer.update(event)
    }

    override fun processElement2(
        gt: GroundTruthEvent,
        ctx: Context,
        out: Collector<MoaEvaluationResult>,
    ) {
        val request = predictionBuffer.value() ?: return

        val actualForce = gt.groundTruth.actualRollForceKn
        val baselineForce = request.baselinePrediction.baselineRollForceKn

        // --- TARGET MEAN (EWMA) ---
        val meanState = targetMeanState.value() ?: TargetMeanState()
        val targetMeanForce = if (meanState.initialized) meanState.currentMean else baselineForce

        meanState.currentMean = (ewmaLambda * actualForce) + ((1 - ewmaLambda) * targetMeanForce)
        meanState.initialized = true
        targetMeanState.update(meanState)

        // --- STOCHASTIC GRADIENT DESCENT (SGD) ---
        val sgd = sgdState.value() ?: SgdState()
        val features = request.features.values.toDoubleArray()

        if (!sgd.initialized) {
            sgd.weights = DoubleArray(features.size) { 0.0 }
            sgd.initialized = true
        }

        var sgdForce = sgd.bias
        for (i in features.indices) {
            sgdForce += sgd.weights[i] * features[i]
        }

        val sgdError = actualForce - sgdForce
        sgd.bias += sgdLearningRate * sgdError
        for (i in features.indices) {
            val weightDecay = (1.0 - sgdDecay) * sgd.weights[i]
            sgd.weights[i] = weightDecay + (sgdLearningRate * sgdError * features[i])
        }
        sgdState.update(sgd)

        // --- AMRULES (Mock) ---
        val amRulesForce = baselineForce

        // --- APE CALCULATIONS ---
        val baselineApe = calculateApe(baselineForce, actualForce)
        val targetMeanApe = calculateApe(targetMeanForce, actualForce)
        val sgdApe = calculateApe(sgdForce, actualForce)
        val amRulesApe = calculateApe(amRulesForce, actualForce)

        // --- EMIT TO CLICKHOUSE ---
        out.collect(
            MoaEvaluationResult(
                evaluationTimestamp = gt.timestamp.toEpochMilli(),
                steelGrade = request.identifiers.steelGrade,
                slabId = request.identifiers.slabId,
                passNumber = request.identifiers.passNumber,
                baselineRollForceKn = baselineForce,
                targetMeanRollForceKn = targetMeanForce,
                sgdRollForceKn = sgdForce,
                amRulesRollForceKn = amRulesForce,
                actualRollForceKn = actualForce,
                baselineApe = baselineApe,
                targetMeanApe = targetMeanApe,
                sgdApe = sgdApe,
                amRulesApe = amRulesApe,
                wearLevel = gt.groundTruth.wearLevel,
            ),
        )
        predictionBuffer.clear()
    }

    private fun calculateApe(
        predicted: Double,
        actual: Double,
    ): Double {
        if (actual == 0.0) return 0.0
        return abs((actual - predicted) / actual) * 100.0
    }

    private fun String.toEpochMilli(): Long =
        java.time.Instant
            .parse(this)
            .toEpochMilli()
}
