package me.jaehyeon.hotrolling.domain.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import java.io.Serializable

// ==========================================
// Prediction Request Event (Event A)
// ==========================================

/**
 * Represents the data available just before the steel slab enters the rollers.
 * Jackson annotations are used to map the Python snake_case JSON into Kotlin camelCase.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class PredictionRequestEvent(
    val timestamp: String,
    val identifiers: PredictionIdentifiers,
    val features: Map<String, Double>,
    @JsonProperty("baseline_prediction")
    val baselinePrediction: BaselinePrediction,
) : Serializable

@JsonIgnoreProperties(ignoreUnknown = true)
data class PredictionIdentifiers(
    @JsonProperty("slab_id") val slabId: String,
    @JsonProperty("pass_number") val passNumber: Int,
    @JsonProperty("steel_grade") val steelGrade: String,
    @JsonProperty("routing_key") val routingKey: String,
) : Serializable

@JsonIgnoreProperties(ignoreUnknown = true)
data class BaselinePrediction(
    @JsonProperty("baseline_roll_force_kn") val baselineRollForceKn: Double,
) : Serializable

// ==========================================
// Ground Truth Event (Event B)
// ==========================================

/**
 * Represents the physical reality measured after the steel has been rolled.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
data class GroundTruthEvent(
    val timestamp: String,
    val identifiers: GroundTruthIdentifiers,
    @JsonProperty("ground_truth")
    val groundTruth: GroundTruthMetrics,
) : Serializable

@JsonIgnoreProperties(ignoreUnknown = true)
data class GroundTruthIdentifiers(
    @JsonProperty("slab_id") val slabId: String,
    @JsonProperty("pass_number") val passNumber: Int,
) : Serializable

@JsonIgnoreProperties(ignoreUnknown = true)
data class GroundTruthMetrics(
    @JsonProperty("actual_roll_force_kn") val actualRollForceKn: Double,
    @JsonProperty("wear_level") val wearLevel: Double,
) : Serializable

// ==========================================
// Output Sink: MOA Evaluation Result
// ==========================================

/**
 * The final output payload containing all calculations, predictions, and errors.
 * This class is ultimately mapped into a Tab-Separated sequence for ClickHouse ingestion.
 */
data class MoaEvaluationResult(
    val evaluationTimestamp: Long,
    val steelGrade: String,
    val slabId: String,
    val passNumber: Int,
    // Raw Physical Values
    val baselineRollForceKn: Double,
    val targetMeanRollForceKn: Double,
    val sgdRollForceKn: Double,
    val amRulesRollForceKn: Double,
    val actualRollForceKn: Double,
    // Machine Learning Evaluation Metrics
    val baselineApe: Double,
    val targetMeanApe: Double,
    val sgdApe: Double,
    val amRulesApe: Double,
    val amRulesShadowApe: Double,
    // Extra details and context
    val wearLevel: Double,
    val isAmRulesFallback: Int,
) : Serializable

// ==========================================
// Internal Flink Pipeline Models
// ==========================================

/**
 * A combined state object representing a successful join between a prediction
 * request and its corresponding ground truth reality.
 */
data class MatchedEvent(
    val prediction: PredictionRequestEvent,
    val groundTruth: GroundTruthEvent,
) : Serializable
