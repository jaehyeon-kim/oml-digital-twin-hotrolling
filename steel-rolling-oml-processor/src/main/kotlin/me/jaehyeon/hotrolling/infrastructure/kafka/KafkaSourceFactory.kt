package me.jaehyeon.hotrolling.infrastructure.kafka

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import me.jaehyeon.hotrolling.config.AppConfig
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import java.time.Duration

class KafkaSourceFactory(
    private val env: StreamExecutionEnvironment,
    private val config: AppConfig,
) {
    private val logger = LoggerFactory.getLogger(KafkaSourceFactory::class.java)

    /**
     * Generic factory method to create a JSON-based Flink Kafka stream.
     */
    fun <T> createStream(
        topic: String,
        targetType: Class<T>,
        startingOffsets: OffsetsInitializer = OffsetsInitializer.earliest(),
    ): DataStream<T> {
        logger.info("Initializing Pure JSON Kafka Source for topic: '{}'", topic)

        val source =
            KafkaSource
                .builder<T>()
                .setBootstrapServers(config.bootstrapAddress)
                .setTopics(topic)
                .setGroupId(config.kafkaGroupId)
                .setStartingOffsets(startingOffsets)
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setDeserializer(JsonDeserializationSchema(targetType))
                .build()

        // Watermark Strategy: Allow up to 5 seconds of out-of-orderness for network delays
        val wmStrategy =
            WatermarkStrategy
                .forBoundedOutOfOrderness<T>(Duration.ofSeconds(5))
                .withIdleness(Duration.ofSeconds(60))

        return env
            .fromSource(source, wmStrategy, "Kafka-Source-$topic")
            .name("Read-$topic")
    }

    /**
     * Custom Deserializer that maps pure JSON Bytes into Kotlin Data Classes using Jackson.
     */
    private class JsonDeserializationSchema<T>(
        private val targetType: Class<T>,
    ) : KafkaRecordDeserializationSchema<T> {
        // Marked transient so Flink ignores it during cluster serialization
        @Transient
        private var mapper: com.fasterxml.jackson.databind.ObjectMapper? = null

        // Flink's native lifecycle method. Called exactly once per worker node.
        override fun open(context: org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext) {
            mapper =
                jacksonObjectMapper().apply {
                    registerModule(JavaTimeModule())
                    configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                }
        }

        override fun deserialize(
            record: ConsumerRecord<ByteArray, ByteArray>,
            collector: Collector<T>,
        ) {
            if (record.value() == null) return

            try {
                // Use the safely initialized mapper
                val obj = mapper!!.readValue(record.value(), targetType)
                collector.collect(obj)
            } catch (e: Exception) {
                // Highly resilient: Log and drop malformed JSON to prevent the whole pipeline from crashing
                val rawString = String(record.value())
                LoggerFactory
                    .getLogger(JsonDeserializationSchema::class.java)
                    .error("Failed to deserialize JSON. Dropping message: $rawString", e)
            }
        }

        override fun getProducedType(): TypeInformation<T> = TypeInformation.of(targetType)
    }
}
