package me.jaehyeon.hotrolling.domain.mapper

import me.jaehyeon.hotrolling.domain.model.MoaEvaluationResult
import org.apache.flink.connector.clickhouse.convertor.POJOConvertor
import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.sql.Timestamp

class ClickHouseMapper : POJOConvertor<MoaEvaluationResult>() {
    private val tab = '\t'.code
    private val newLine = '\n'.code
    private val nullMarker = "\\N".toByteArray(StandardCharsets.UTF_8)

    override fun instrument(
        out: OutputStream,
        result: MoaEvaluationResult,
    ) {
        writeField(out, Timestamp(result.evaluationTimestamp).toString())
        out.write(tab)
        writeField(out, result.steelGrade)
        out.write(tab)
        writeField(out, result.slabId)
        out.write(tab)
        writeField(out, result.passNumber)
        out.write(tab)
        // Raw Physical Values
        writeField(out, result.baselineRollForceKn)
        out.write(tab)
        writeField(out, result.targetMeanRollForceKn)
        out.write(tab)
        writeField(out, result.sgdRollForceKn)
        out.write(tab)
        writeField(out, result.amRulesRollForceKn)
        out.write(tab)
        writeField(out, result.actualRollForceKn)
        out.write(tab)
        // ML Evaluation Metrics
        writeField(out, result.baselineApe)
        out.write(tab)
        writeField(out, result.targetMeanApe)
        out.write(tab)
        writeField(out, result.sgdApe)
        out.write(tab)
        writeField(out, result.amRulesApe)
        out.write(tab)
        // Telemetry
        writeField(out, result.wearLevel)
        out.write(newLine)
    }

    private fun writeField(
        out: OutputStream,
        value: Any?,
    ) {
        if (value == null) {
            out.write(nullMarker)
        } else {
            val str =
                value
                    .toString()
                    .replace("\\", "\\\\")
                    .replace("\t", "\\t")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r")
            out.write(str.toByteArray(StandardCharsets.UTF_8))
        }
    }
}
