package me.jaehyeon.hotrolling.topology.processing

import me.jaehyeon.hotrolling.domain.model.WelfordState
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.TypeInformation
import java.io.Serializable
import kotlin.math.sqrt

class WelfordScaler(
    private val numFeatures: Int,
) : Serializable {
    @Transient
    private lateinit var welfordState: ValueState<WelfordState>

    fun open(runtimeContext: RuntimeContext) {
        val descriptor =
            ValueStateDescriptor(
                "welford-state",
                TypeInformation.of(WelfordState::class.java),
            )
        welfordState = runtimeContext.getState(descriptor)
    }

    fun scale(raw: Map<String, Double>): DoubleArray {
        // 1. Safely extract raw features
        val thickness = raw["entry_thickness_mm"] ?: 1.0
        val reduction = raw["reduction_pct"] ?: 0.1
        val tempC = raw["temperature_c"] ?: 1000.0
        val width = raw["width_mm"] ?: 1000.0
        val length = raw["length_mm"] ?: 1000.0

        val absoluteDraft = thickness * reduction
        val tempDraftInteraction = absoluteDraft / tempC
        val volume = thickness * width * length

        val rawFeatures =
            doubleArrayOf(
                raw["reheating_time_min"] ?: 0.0,
                raw["roll_diameter_mm"] ?: 0.0,
                raw["roll_crown_mm"] ?: 0.0,
                thickness,
                width,
                length,
                tempC,
                raw["speed_m_s"] ?: 0.0,
                raw["wait_time_sec"] ?: 0.0,
                reduction,
                raw["strain"] ?: 0.0,
                raw["strain_rate"] ?: 0.0,
                raw["flow_stress_mpa"] ?: 0.0,
                absoluteDraft,
                tempDraftInteraction,
                volume,
            )

        // 2. Fetch or Initialize State
        var state = welfordState.value()
        if (state == null || state.mean.isEmpty()) {
            state =
                WelfordState().apply {
                    count = 0
                    mean = DoubleArray(numFeatures) { 0.0 }
                    m2 = DoubleArray(numFeatures) { 0.0 }
                }
        }

        // 3. Welford's Math & Z-Score Scaling
        state.count += 1
        val scaledFeatures = DoubleArray(numFeatures)

        for (i in 0 until numFeatures) {
            val x = rawFeatures[i]

            // Calculate moving delta to update Mean and M2 safely
            val delta = x - state.mean[i]
            state.mean[i] += delta / state.count
            val delta2 = x - state.mean[i]
            state.m2[i] += delta * delta2

            // Calculate true Z-Score standardizing the feature
            if (state.count < 2) {
                // Not enough history for standard deviation, default to 0.0 center
                scaledFeatures[i] = 0.0
            } else {
                val variance = state.m2[i] / (state.count - 1)
                val stdDev = sqrt(variance)

                // Prevent division by zero if a feature never changes
                if (stdDev > 1e-8) {
                    scaledFeatures[i] = (x - state.mean[i]) / stdDev
                } else {
                    scaledFeatures[i] = 0.0
                }
            }
        }

        // 4. Save state back to Flink
        welfordState.update(state)

        return scaledFeatures
    }
}
