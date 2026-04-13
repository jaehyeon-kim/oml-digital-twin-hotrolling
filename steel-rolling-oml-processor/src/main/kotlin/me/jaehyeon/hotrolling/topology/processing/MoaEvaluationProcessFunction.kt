@file:Suppress("ktlint:standard:no-wildcard-imports")

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
            if (serializedBytes != null) {
                amRulesModel = deserializeModel(serializedBytes)
            } else {
                amRulesModel = AmRulesModel(features.size)
            }
            activeAmRulesModels[routingKey] = amRulesModel
        }

        val trueResidual = actualForce - baselineForce
        val amRulesResidual = amRulesModel.predictAndTrain(features, trueResidual)
        val rawAmRulesForce = baselineForce + amRulesResidual

        // ---------------------------------------------------------
        // ML STRATEGY 5: FALLBACK GUARDRAIL (Champion/Challenger)
        // ---------------------------------------------------------
        val maxSafeCorrection = baselineForce * fallbackTolerance

        // Capture the boolean state
        val isFallbackTriggered = abs(amRulesResidual) > maxSafeCorrection
        val finalAmRulesForce =
            if (isFallbackTriggered) {
                logger.warn(
                    ">>> [FALLBACK TRIGGERED] AMRules hallucination detected on $routingKey. Attempted to correct by ${String.format(
                        "%.2f",
                        (abs(amRulesResidual) / baselineForce) * 100,
                    )}%. Falling back to Physical Baseline.",
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
        val amRulesApe = calculateApe(finalAmRulesForce, actualForce)

        logger.info(
            @Suppress("ktlint:standard:max-line-length")
            "<<< [EVALUATED] ${request.identifiers.steelGrade} | ${request.identifiers.slabId}-P${request.identifiers.passNumber} | SGD: ${String.format(
                "%.2f",
                sgdApe,
            )}% | AMRules: ${String.format("%.2f", amRulesApe)}%",
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
                amRulesApe = amRulesApe, // Guardrailed APE
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
