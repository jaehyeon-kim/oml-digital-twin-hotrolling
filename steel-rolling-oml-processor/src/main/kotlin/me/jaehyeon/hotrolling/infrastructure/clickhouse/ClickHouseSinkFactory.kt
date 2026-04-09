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

object ClickHouseSinkFactory {
    fun createSink(config: AppConfig): ClickHouseAsyncSink<MoaEvaluationResult> {
        val clientConfig =
            ClickHouseClientConfig(
                config.chEndpoint,
                config.chUser,
                "", // Password
                config.chDatabase,
                config.chTable,
            )

        val elementConverter: ElementConverter<MoaEvaluationResult, ClickHousePayload> =
            ClickHouseConvertor(MoaEvaluationResult::class.java, ClickHouseMapper())

        val sink =
            ClickHouseAsyncSink<MoaEvaluationResult>(
                elementConverter,
                1000, // maxBatchSize
                100, // maxInFlightRequests
                2000, // maxBufferedRequests
                10 * 1024 * 1024, // maxBatchSizeInBytes (10 MB)
                1000, // maxTimeInBufferMS (1 second)
                10 * 1024 * 1024, // maxRecordSizeInBytes
                clientConfig,
            )

        sink.setClickHouseFormat(ClickHouseFormat.TabSeparated)
        return sink
    }
}
