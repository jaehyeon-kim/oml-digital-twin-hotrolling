package me.jaehyeon.hotrolling.infrastructure.clickhouse

import com.clickhouse.data.ClickHouseFormat
import me.jaehyeon.hotrolling.config.AppConfig
import me.jaehyeon.hotrolling.domain.mapper.ClickHouseMapper
import me.jaehyeon.hotrolling.domain.model.MoaEvaluationResult
import org.apache.flink.connector.base.sink.writer.ElementConverter
import org.apache.flink.connector.clickhouse.convertor.ClickHouseConvertor
import org.apache.flink.connector.clickhouse.data.ClickHousePayload
import org.apache.flink.connector.clickhouse.sink.ClickHouseAsyncSink
import org.apache.flink.connector.clickhouse.sink.ClickHouseClientConfig

/**
 * Factory object for creating the Flink ClickHouse sink.
 *
 * ClickHouse is an incredibly fast columnar database, but it performs poorly if
 * records are inserted one by one. This factory configures an asynchronous, batching
 * sink that groups evaluation results together in memory before flushing them to the
 * database in large, efficient chunks.
 */
object ClickHouseSinkFactory {
    /**
     * Creates and configures the ClickHouse Async Sink.
     *
     * Args:
     * config: The global application configuration containing database credentials and endpoints.
     *
     * Returns:
     * A fully configured ClickHouseAsyncSink ready to be attached to a Flink DataStream.
     */
    fun createSink(config: AppConfig): ClickHouseAsyncSink<MoaEvaluationResult> {
        val clientConfig =
            ClickHouseClientConfig(
                config.chEndpoint,
                config.chUser,
                "", // Password is deliberately left blank for local development
                config.chDatabase,
                config.chTable,
            )

        // Binds our custom Tab-Separated mapper to the Flink ClickHouse connector
        val elementConverter: ElementConverter<MoaEvaluationResult, ClickHousePayload> =
            ClickHouseConvertor(MoaEvaluationResult::class.java, ClickHouseMapper())

        val sink =
            ClickHouseAsyncSink<MoaEvaluationResult>(
                elementConverter,
                1000, // maxBatchSize: Number of records to accumulate before triggering a flush
                100, // maxInFlightRequests: Maximum number of concurrent HTTP writes to ClickHouse
                2000, // maxBufferedRequests: Maximum number of batches held in memory while waiting for IO
                10 * 1024 * 1024, // maxBatchSizeInBytes: 10 MB payload limit per flush
                1000, // maxTimeInBufferMS: Time-based flush trigger (flush every 1 second regardless of batch size)
                10 * 1024 * 1024, // maxRecordSizeInBytes: Sanity limit to drop absurdly large single records
                clientConfig,
            )

        // Forces the sink to use the TabSeparated format, matching our ClickHouseMapper logic
        sink.setClickHouseFormat(ClickHouseFormat.TabSeparated)
        return sink
    }
}
