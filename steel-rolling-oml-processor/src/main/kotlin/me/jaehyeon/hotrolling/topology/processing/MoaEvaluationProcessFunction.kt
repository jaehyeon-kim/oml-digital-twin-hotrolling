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

class MoaEvaluationProcessFunction(
    private val ewmaLambda: Double,
    private val sgdLearningRate: Double,
    private val sgdDecay: Double,
    private val fallbackTolerance: Double,
    private val smoothingFactor: Double,
) : KeyedProcessFunction<String, MatchedEvent, MoaEvaluationResult>() {
    @Transient
    private lateinit var logger: Logger

    // ML States (Simple Models use POJO state)
    private lateinit var targetMeanState: ValueState<TargetMeanState>
    private lateinit var sgdState: ValueState<SgdState>

    // STRATEGY B: MOA Model uses Raw Byte State to avoid Kryo
    private lateinit var amRulesByteState: ValueState<ByteArray>

    // STRATEGY B: Transient In-Memory Cache for Lightning-Fast Execution
    @Transient
    private lateinit var activeAmRulesModels: MutableMap<String, AmRulesModel>

    // Helper Component
    private lateinit var scaler: WelfordScaler

    // Shadow Mode EWMA States
    private lateinit var amRulesApeEwmaState: ValueState<Double>
    private lateinit var baselineApeEwmaState: ValueState<Double>

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

        // ---------------------------------------------------------
        // TARGET MEAN (HYBRID EWMA)
        // ---------------------------------------------------------
        val meanState = targetMeanState.value() ?: TargetMeanState()
        val currentEwmaError = if (meanState.initialized) meanState.currentMean else 0.0
        val targetMeanForce = baselineForce + currentEwmaError
        val actualError = actualForce - baselineForce
        meanState.currentMean = (ewmaLambda * actualError) + ((1 - ewmaLambda) * currentEwmaError)
        meanState.initialized = true
        targetMeanState.update(meanState)

        // ---------------------------------------------------------
        // ML STRATEGY 2: FEATURE NORMALIZATION
        // ---------------------------------------------------------
        val features = scaler.scale(request.features)

        // ---------------------------------------------------------
        // ML STRATEGY 3: STOCHASTIC GRADIENT DESCENT (SGD)
        // ---------------------------------------------------------
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

        // ---------------------------------------------------------
        // ML STRATEGY 4: AMRULES (Strategy B: Byte Serialization)
        // ---------------------------------------------------------
        var amRulesModel = activeAmRulesModels[routingKey]

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

        val trueResidual = actualForce - baselineForce
        val amRulesResidual = amRulesModel.predictAndTrain(features, trueResidual)
        val rawAmRulesForce = baselineForce + amRulesResidual

        // ---------------------------------------------------------
        // ML STRATEGY 5: SHADOW MODE ROUTER (Safety + Trust)
        // ---------------------------------------------------------
        val recentAmRulesApe = amRulesApeEwmaState.value() ?: 0.0
        val recentBaselineApe = baselineApeEwmaState.value() ?: 0.0

        // 1. Physical Safety Check (Catastrophic Error Prevention)
        val maxSafeCorrection = baselineForce * fallbackTolerance
        val isPhysicallyUnsafe = abs(amRulesResidual) > maxSafeCorrection

        // 2. Trust Check (Is the AI currently worse than pure physics? 1.0% buffer)
        val isUntrusted = recentAmRulesApe > (recentBaselineApe + 1.0)

        // 3. The Ultimate Routing Decision
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
                baselineForce
            } else {
                rawAmRulesForce
            }

        // Serialize and flush back to Flink ValueState
        amRulesByteState.update(serializeModel(amRulesModel))

        // --- APE CALCULATIONS ---
        val baselineApe = calculateApe(baselineForce, actualForce)
        val targetMeanApe = calculateApe(targetMeanForce, actualForce)
        val sgdApe = calculateApe(sgdForce, actualForce)

        // Calculate both the Shadow APE (what it wanted to do) and Safe APE (what it actually did)
        val amRulesShadowApe = calculateApe(rawAmRulesForce, actualForce)
        val amRulesSafeApe = calculateApe(finalAmRulesForce, actualForce)

        // --- UPDATE TRUST SCORES (EWMA) ---
        amRulesApeEwmaState.update((smoothingFactor * amRulesShadowApe) + ((1.0 - smoothingFactor) * recentAmRulesApe))
        baselineApeEwmaState.update((smoothingFactor * baselineApe) + ((1.0 - smoothingFactor) * recentBaselineApe))

        logger.info(
            @Suppress("ktlint:standard:max-line-length")
            "<<< [EVALUATED] ${request.identifiers.steelGrade} | ${request.identifiers.slabId}-P${request.identifiers.passNumber} | Safe APE: ${String.format(
                "%.2f",
                amRulesSafeApe,
            )}% | Shadow APE: ${String.format("%.2f", amRulesShadowApe)}%",
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
                amRulesRollForceKn = finalAmRulesForce, // Using the guardrailed output
                actualRollForceKn = actualForce,
                baselineApe = baselineApe,
                targetMeanApe = targetMeanApe,
                sgdApe = sgdApe,
                amRulesApe = amRulesSafeApe, // Guardrailed APE
                amRulesShadowApe = amRulesShadowApe, // Raw Brain Error
                wearLevel = gt.groundTruth.wearLevel,
                isAmRulesFallback = if (isFallbackTriggered) 1 else 0,
            ),
        )
    }

    // =========================================================
    // NATIVE JAVA SERIALIZATION HELPERS
    // =========================================================
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
