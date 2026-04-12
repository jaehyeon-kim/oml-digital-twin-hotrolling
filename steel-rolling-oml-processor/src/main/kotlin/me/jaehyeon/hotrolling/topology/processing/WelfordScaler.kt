package me.jaehyeon.hotrolling.topology.processing

import org.apache.flink.api.common.functions.RuntimeContext
import java.io.Serializable

class WelfordScaler(
    private val numFeatures: Int,
) : Serializable {
    fun open(runtimeContext: RuntimeContext) {
        // TODO: Initialize Flink ValueState<WelfordStats> here to maintain
        // running count, mean, and M2 for online Z-score normalization.
    }

    fun scale(raw: Map<String, Double>): DoubleArray {
        val thickness = raw["entry_thickness_mm"] ?: 1.0
        val reduction = raw["reduction_pct"] ?: 0.1
        val tempC = raw["temperature_c"] ?: 1000.0
        val width = raw["width_mm"] ?: 1000.0
        val length = raw["length_mm"] ?: 1000.0

        // Calculate Engineered Features
        val absoluteDraft = thickness * reduction
        val tempDraftInteraction = absoluteDraft / tempC
        val volume = thickness * width * length

        // TODO: Replace static MaxAbs scaling with true Welford's Z-score calculation
        return doubleArrayOf(
            (raw["reheating_time_min"] ?: 0.0) / 1200.0,
            (raw["roll_diameter_mm"] ?: 0.0) / 1200.0,
            (raw["roll_crown_mm"] ?: 0.0) / 250.0,
            thickness / 300.0,
            width / 4500.0,
            length / 40000.0,
            tempC / 1300.0,
            (raw["speed_m_s"] ?: 0.0) / 6.0,
            (raw["wait_time_sec"] ?: 0.0) / 250.0,
            reduction / 0.5,
            (raw["strain"] ?: 0.0) / 1.0,
            (raw["strain_rate"] ?: 0.0) / 20.0,
            (raw["flow_stress_mpa"] ?: 0.0) / 200.0,
            // Scaled Engineered Features
            absoluteDraft / 150.0,
            tempDraftInteraction / 0.2,
            volume / 50000000000.0, // 50 Billion mm3
        )
    }
}
