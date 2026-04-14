package me.jaehyeon.hotrolling.domain.mapper

import me.jaehyeon.hotrolling.domain.model.MoaEvaluationResult
import org.apache.flink.connector.clickhouse.convertor.POJOConvertor
import java.io.OutputStream
import java.nio.charset.StandardCharsets
import java.sql.Timestamp

/**
 * Custom serialization mapper for writing Flink evaluation results to ClickHouse.
 *
 * Flink's ClickHouse connector supports writing raw byte streams directly to the database
 * for massive throughput. This class converts our MoaEvaluationResult data class into a
 * Tab-Separated Values format that ClickHouse can ingest natively.
 */
class ClickHouseMapper : POJOConvertor<MoaEvaluationResult>() {
    private val tab = '\t'.code
    private val newLine = '\n'.code

    // ClickHouse uses \N to explicitly represent a NULL value in TabSeparated format
    private val nullMarker = "\\N".toByteArray(StandardCharsets.UTF_8)

    /**
     * Translates a single evaluation result into a sequence of bytes written to the output stream.
     * Order is strictly critical here. The order of writeField calls must exactly match
     * the column order defined in the ClickHouse CREATE TABLE statement.
     */
    override fun instrument(
        out: OutputStream,
        result: MoaEvaluationResult,
    ) {
        // Convert the Epoch milliseconds back into a standard SQL Timestamp string
        writeField(out, Timestamp(result.evaluationTimestamp).toString())
        out.write(tab)

        // Identifiers
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

        // Machine Learning Evaluation Metrics
        writeField(out, result.baselineApe)
        out.write(tab)
        writeField(out, result.targetMeanApe)
        out.write(tab)
        writeField(out, result.sgdApe)
        out.write(tab)
        writeField(out, result.amRulesApe)
        out.write(tab)
        writeField(out, result.amRulesShadowApe)
        out.write(tab)

        // Hidden System States and Flags
        writeField(out, result.wearLevel)
        out.write(tab)
        writeField(out, result.isAmRulesFallback)

        // Terminate the row
        out.write(newLine)
    }

    /**
     * Helper function to safely encode variables into the byte stream.
     *
     * It handles null values by injecting the ClickHouse null marker. For strings,
     * it sanitizes tabs and newlines to prevent formatting corruption during ingestion.
     */
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
