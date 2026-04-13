package me.jaehyeon.hotrolling.domain.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import java.io.Serializable

// ==========================================
// Prediction Request Event
// ==========================================
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
// Ground Truth Event
// ==========================================
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
// Output Sink: MOA Evaluation Result (To ClickHouse)
// ==========================================
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
    // ML Evaluation Metrics
    val baselineApe: Double,
    val targetMeanApe: Double,
    val sgdApe: Double,
    val amRulesApe: Double,
    // Extra details
    val wearLevel: Double,
    val isAmRulesFallback: Int,
) : Serializable

// ==========================================
// Internal Flink Pipeline Models
// ==========================================
data class MatchedEvent(
    val prediction: PredictionRequestEvent,
    val groundTruth: GroundTruthEvent,
) : Serializable
