package me.jaehyeon.hotrolling.topology.processing

import me.jaehyeon.hotrolling.domain.model.*
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.math.abs

class MoaEvaluationProcessFunction(
    private val ewmaLambda: Double,
    private val sgdLearningRate: Double,
    private val sgdDecay: Double,
) : KeyedProcessFunction<String, MatchedEvent, MoaEvaluationResult>() {
    @Transient
    private lateinit var logger: Logger

    // ONLY ML STATES NOW
    private lateinit var targetMeanState: ValueState<TargetMeanState>
    private lateinit var sgdState: ValueState<SgdState>

    // Helper Component
    private lateinit var scaler: WelfordScaler

    override fun open(parameters: Configuration) {
        logger = LoggerFactory.getLogger(MoaEvaluationProcessFunction::class.java)

        targetMeanState =
            runtimeContext.getState(
                ValueStateDescriptor("target-mean-state", TypeInformation.of(TargetMeanState::class.java)),
            )
        sgdState =
            runtimeContext.getState(
                ValueStateDescriptor("sgd-state", TypeInformation.of(SgdState::class.java)),
            )

        // Initialize the feature scaling component
        scaler = WelfordScaler(16)
        scaler.open(runtimeContext)
    }

    override fun processElement(
        event: MatchedEvent,
        ctx: Context,
        out: Collector<MoaEvaluationResult>,
    ) {
        val request = event.prediction
        val gt = event.groundTruth

        val actualForce = gt.groundTruth.actualRollForceKn
        val baselineForce = request.baselinePrediction.baselineRollForceKn

        // ---------------------------------------------------------
        // TARGET MEAN (HYBRID EWMA)
        // Track the moving average of the ERROR (bias), not the absolute force!
        // ---------------------------------------------------------
        val meanState = targetMeanState.value() ?: TargetMeanState()
        val currentEwmaError = if (meanState.initialized) meanState.currentMean else 0.0

        // 1. Predict: Physical Baseline + The historical average error (bias)
        val targetMeanForce = baselineForce + currentEwmaError

        // 2. Calculate the true physical error for this specific pass
        val actualError = actualForce - baselineForce

        // 3. Update the EWMA with the new error
        meanState.currentMean = (ewmaLambda * actualError) + ((1 - ewmaLambda) * currentEwmaError)
        meanState.initialized = true
        targetMeanState.update(meanState)

        // ---------------------------------------------------------
        // ML STRATEGY 2: FEATURE NORMALIZATION
        // Bounds features to [0.0, 1.0] to prevent exploding gradients.
        // ---------------------------------------------------------
        val features = scaler.scale(request.features)

        // ---------------------------------------------------------
        // STOCHASTIC GRADIENT DESCENT (SGD)
        // ---------------------------------------------------------
        val sgd = sgdState.value() ?: SgdState()

        if (!sgd.initialized) {
            sgd.weights = DoubleArray(features.size) { 0.0 }
            sgd.bias = 0.0
            sgd.initialized = true
        }

        // ---------------------------------------------------------
        // ML STRATEGY 3: HYBRID RESIDUAL LEARNING (Physics-Informed ML)
        // Instead of forcing the ML model to learn the massive 4.7M kN force
        // from scratch (which causes extreme APE fluctuations and cold-starts),
        // we use the theoretical physics model as the baseline.
        // The SGD model is strictly trained to predict the RESIDUAL ERROR
        // (the concept drift / roller wear) on top of the baseline.
        // ---------------------------------------------------------
        var sgdForce = baselineForce + sgd.bias

        for (i in features.indices) {
            sgdForce += sgd.weights[i] * features[i]
        }

        // The error is the difference between actual and our hybrid prediction
        val sgdError = actualForce - sgdForce

        // Train the model ONLY on the residual error
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

        logger.info(
            "<<< [EVALUATED] ${request.identifiers.steelGrade} | ${request.identifiers.slabId}-P${request.identifiers.passNumber} | SGD APE: ${String.format(
                "%.2f",
                sgdApe,
            )}%",
        )

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
    }

    private fun calculateApe(
        predicted: Double,
        actual: Double,
    ): Double {
        if (actual == 0.0) return 0.0
        return abs((actual - predicted) / actual) * 100.0
    }

    private fun String.toEpochMilli(): Long =
        java.time.LocalDateTime
            .parse(this)
            .atZone(java.time.ZoneId.systemDefault())
            .toInstant()
            .toEpochMilli()
}
