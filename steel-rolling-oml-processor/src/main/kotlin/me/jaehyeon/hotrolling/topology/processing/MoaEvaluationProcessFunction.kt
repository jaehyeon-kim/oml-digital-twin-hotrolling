@file:Suppress("ktlint:standard:no-wildcard-imports")

package me.jaehyeon.hotrolling.topology.processing

import me.jaehyeon.hotrolling.domain.model.*
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import kotlin.math.abs

/**
 * The core Online Machine Learning engine and Shadow Mode Router.
 *
 * This function processes strictly matched Prediction and Ground Truth pairs.
 * It is partitioned (Keyed) by steel grade to ensure that machine learning models do not suffer from
 * catastrophic forgetting when switching between fundamentally different physics.
 *
 * Args:
 * ewmaLambda: Learning rate for the Target Mean tracking.
 * sgdLearningRate: Step size for the Stochastic Gradient Descent optimizer.
 * sgdDecay: L2 regularization penalty applied to SGD weights to prevent exploding gradients.
 * overpressTolerance: Maximum allowed deviation for over-pressing.
 * underpressTolerance: Maximum allowed deviation for under-pressing.
 * smoothingFactor: Alpha value for the Shadow Mode Exponentially Weighted Moving Average trust score.
 */
class MoaEvaluationProcessFunction(
    private val ewmaLambda: Double,
    private val sgdLearningRate: Double,
    private val sgdDecay: Double,
    private val overpressTolerance: Double,
    private val underpressTolerance: Double,
    private val smoothingFactor: Double,
) : KeyedProcessFunction<String, MatchedEvent, MoaEvaluationResult>() {
    @Transient
    private lateinit var logger: Logger

    // Flink Managed State (Fault-Tolerant)
    private lateinit var targetMeanState: ValueState<TargetMeanState>
    private lateinit var sgdState: ValueState<SgdState>

    // Serialized AMRules Model to bypass Kryo serialization limitations with complex Java objects
    private lateinit var amRulesByteState: ValueState<ByteArray>

    // EWMA Memory states used to calculate the Shadow Mode Trust Score
    private lateinit var amRulesApeEwmaState: ValueState<Double>
    private lateinit var baselineApeEwmaState: ValueState<Double>

    // Transient In-Memory State (Performance)
    // Caches deserialized models to avoid heavy byte parsing on every single event.
    @Transient
    private lateinit var activeAmRulesModels: MutableMap<String, AmRulesModel>
    private lateinit var scaler: WelfordScaler

    override fun open(parameters: Configuration) {
        logger = LoggerFactory.getLogger(MoaEvaluationProcessFunction::class.java)

        // Initialize state descriptors. TypeInformation ensures Flink knows exactly how to serialize these.
        targetMeanState =
            runtimeContext.getState(
                ValueStateDescriptor("target-mean-state", TypeInformation.of(TargetMeanState::class.java)),
            )
        sgdState =
            runtimeContext.getState(
                ValueStateDescriptor("sgd-state", TypeInformation.of(SgdState::class.java)),
            )
        amRulesByteState =
            runtimeContext.getState(
                ValueStateDescriptor("amrules-byte-state", TypeInformation.of(ByteArray::class.java)),
            )
        amRulesApeEwmaState =
            runtimeContext.getState(
                ValueStateDescriptor("amrules-ewma-state", Types.DOUBLE),
            )
        baselineApeEwmaState =
            runtimeContext.getState(
                ValueStateDescriptor("baseline-ewma-state", Types.DOUBLE),
            )

        activeAmRulesModels = mutableMapOf()
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
        val routingKey = ctx.currentKey

        val actualForce = gt.groundTruth.actualRollForceKn
        val baselineForce = request.baselinePrediction.baselineRollForceKn

        // 1. TARGET MEAN (HYBRID EWMA)
        // Calculates a simple moving average of the baseline error and uses it as a bias offset.
        val meanState = targetMeanState.value() ?: TargetMeanState()
        val currentEwmaError = if (meanState.initialized) meanState.currentMean else 0.0
        val targetMeanForce = baselineForce + currentEwmaError

        val actualError = actualForce - baselineForce
        meanState.currentMean = (ewmaLambda * actualError) + ((1 - ewmaLambda) * currentEwmaError)
        meanState.initialized = true
        targetMeanState.update(meanState)

        // 2. FEATURE NORMALIZATION (WELFORD SCALER)
        // Neural networks and SGD require standard scaled inputs (Z-Scores).
        // Welford algorithm calculates this on the fly without needing full dataset passes.
        val features = scaler.scale(request.features)

        // 3. STOCHASTIC GRADIENT DESCENT
        // A manual, lightweight linear regressor tracking streaming weights and biases.
        val sgd = sgdState.value() ?: SgdState()
        if (!sgd.initialized) {
            sgd.weights = DoubleArray(features.size) { 0.0 }
            sgd.bias = 0.0
            sgd.initialized = true
        }
        var sgdForce = baselineForce + sgd.bias
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

        // 4. AMRULES EVALUATION (PREQUENTIAL LEARNING)
        var amRulesModel = activeAmRulesModels[routingKey]

        // Lazy initialization and deserialization from Flink state
        if (amRulesModel == null) {
            val serializedBytes = amRulesByteState.value()
            amRulesModel =
                if (serializedBytes != null) {
                    deserializeModel(serializedBytes)
                } else {
                    AmRulesModel(features.size)
                }
            activeAmRulesModels[routingKey] = amRulesModel
        }

        // The Test-then-Train paradigm. We evaluate the model prediction BEFORE updating it.
        val trueResidual = actualForce - baselineForce
        val amRulesResidual = amRulesModel.predictAndTrain(features, trueResidual)
        val rawAmRulesForce = baselineForce + amRulesResidual

        // 5. SHADOW MODE ROUTER (SAFETY GUARDRAIL AND TRUST SCORING)
        // Fetch the historical performance (trust) of both systems
        val recentAmRulesApe = amRulesApeEwmaState.value() ?: 0.0
        val recentBaselineApe = baselineApeEwmaState.value() ?: 0.0

        // Check A: Asymmetric Two-Sided Safety Limits
        val maxSafeOverpress = baselineForce * overpressTolerance
        val maxSafeUnderpress = baselineForce * underpressTolerance

        // The model is flagged as unsafe if it tries to crush too hard (breaks machine)
        // OR if it wildly under-crushes (causes massive factory bottlenecks).
        val isPhysicallyUnsafe = amRulesResidual > maxSafeOverpress || amRulesResidual < -maxSafeUnderpress

        // Check B: Trust Deficit (Is the AI currently performing worse than pure physics)
        val isUntrusted = recentAmRulesApe > (recentBaselineApe + 1.0)

        // The Routing Decision
        val isFallbackTriggered = isPhysicallyUnsafe || isUntrusted

        val finalAmRulesForce =
            if (isFallbackTriggered) {
                logger.warn(
                    @Suppress("ktlint:standard:max-line-length")
                    ">>> [SHADOW MODE] AMRules suppressed on $routingKey. Unsafe: $isPhysicallyUnsafe | Untrusted: $isUntrusted (Recent APE: ${String.format(
                        "%.2f",
                        recentAmRulesApe,
                    )}% vs Base: ${String.format("%.2f", recentBaselineApe)}%)",
                )
                baselineForce // Route to safe physics
            } else {
                rawAmRulesForce // Route to AI
            }

        // Flush trained model back to durable state
        amRulesByteState.update(serializeModel(amRulesModel))

        // APE (Absolute Percentage Error) CALCULATIONS
        val baselineApe = calculateApe(baselineForce, actualForce)
        val targetMeanApe = calculateApe(targetMeanForce, actualForce)
        val sgdApe = calculateApe(sgdForce, actualForce)

        // Shadow APE represents what the AI wanted to do (Unfiltered)
        // Safe APE represents what actually happened on the factory floor (Filtered)
        val amRulesShadowApe = calculateApe(rawAmRulesForce, actualForce)
        val amRulesSafeApe = calculateApe(finalAmRulesForce, actualForce)

        // UPDATE EWMA TRUST SCORES
        // Updates the memory of the system so the router can make informed decisions on the next slab.
        amRulesApeEwmaState.update((smoothingFactor * amRulesShadowApe) + ((1.0 - smoothingFactor) * recentAmRulesApe))
        baselineApeEwmaState.update((smoothingFactor * baselineApe) + ((1.0 - smoothingFactor) * recentBaselineApe))

        logger.info(
            @Suppress("ktlint:standard:max-line-length")
            "<<< [EVALUATED] ${request.identifiers.steelGrade} | ${request.identifiers.slabId}-P${request.identifiers.passNumber} | Safe APE: ${String.format(
                "%.2f",
                amRulesSafeApe,
            )}% | Shadow APE: ${String.format("%.2f", amRulesShadowApe)}%",
        )

        // EMIT RESULTS TO CLICKHOUSE SINK
        out.collect(
            MoaEvaluationResult(
                evaluationTimestamp = gt.timestamp.toEpochMilli(),
                steelGrade = request.identifiers.steelGrade,
                slabId = request.identifiers.slabId,
                passNumber = request.identifiers.passNumber,
                baselineRollForceKn = baselineForce,
                targetMeanRollForceKn = targetMeanForce,
                sgdRollForceKn = sgdForce,
                amRulesRollForceKn = finalAmRulesForce,
                actualRollForceKn = actualForce,
                baselineApe = baselineApe,
                targetMeanApe = targetMeanApe,
                sgdApe = sgdApe,
                amRulesApe = amRulesSafeApe,
                amRulesShadowApe = amRulesShadowApe,
                wearLevel = gt.groundTruth.wearLevel,
                isAmRulesFallback = if (isFallbackTriggered) 1 else 0,
            ),
        )
    }

    // JAVA NATIVE SERIALIZATION HELPERS
    // MOA AMRulesRegressor is deeply complex and breaks Flink native Kryo serializers.
    // We manually convert it to standard byte arrays for safe checkpointing.

    private fun serializeModel(model: AmRulesModel): ByteArray {
        ByteArrayOutputStream().use { baos ->
            ObjectOutputStream(baos).use { oos ->
                oos.writeObject(model)
                return baos.toByteArray()
            }
        }
    }

    private fun deserializeModel(bytes: ByteArray): AmRulesModel {
        ByteArrayInputStream(bytes).use { bais ->
            ObjectInputStream(bais).use { ois ->
                return ois.readObject() as AmRulesModel
            }
        }
    }

    /**
     * Calculates the Absolute Percentage Error.
     */
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
