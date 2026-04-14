package me.jaehyeon.hotrolling.infrastructure.kafka

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import me.jaehyeon.hotrolling.config.AppConfig
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.LoggerFactory

/**
 * Factory class responsible for instantiating Flink Kafka Source streams.
 *
 * It connects to the designated Kafka topics and configures the deserialization
 * schemas required to convert raw byte arrays into Kotlin POJOs.
 */
class KafkaSourceFactory(
    private val env: StreamExecutionEnvironment,
    private val config: AppConfig,
) {
    private val logger = LoggerFactory.getLogger(KafkaSourceFactory::class.java)

    /**
     * Creates a new DataStream bound to a specific Kafka topic.
     *
     * Args:
     * topic: The Kafka topic name to subscribe to.
     * typeClass: The Kotlin data class to deserialize the JSON payload into.
     *
     * Returns:
     * A Flink DataStreamSource. Watermarks are disabled because the downstream
     * stream joiner relies on processing time timers, not strictly monotonic event time.
     */
    fun <T> createStream(
        topic: String,
        typeClass: Class<T>,
    ): org.apache.flink.streaming.api.datastream.DataStreamSource<T> {
        logger.info("Initializing Pure JSON Kafka Source for topic: $topic")

        val source =
            KafkaSource
                .builder<T>()
                .setBootstrapServers(config.bootstrapAddress)
                .setTopics(topic)
                .setGroupId(config.kafkaGroupId)
                // OffsetsInitializer earliest ensures we consume the topic from the beginning
                // upon boot, which is ideal for simulation and testing environments.
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(JsonDeserializationSchema(typeClass))
                .build()

        return env.fromSource(source, WatermarkStrategy.noWatermarks(), topic)
    }

    /**
     * Custom generic JSON Deserializer utilizing Jackson.
     *
     * Serialization schemas in Flink are shipped across the network from the JobManager
     * to the TaskManager worker nodes. Therefore, heavily stateful and non-serializable
     * objects like ObjectMapper must be instantiated lazily on the worker node, NOT created
     * on the JobManager prior to submission.
     */
    class JsonDeserializationSchema<T>(
        private val typeClass: Class<T>,
    ) : DeserializationSchema<T> {
        @Transient
        private var log = LoggerFactory.getLogger(JsonDeserializationSchema::class.java)

        // Marked as Transient so Flink does not attempt to serialize it over the network.
        @Transient
        private var mapper: ObjectMapper? = null

        // Lazy Initialization: Create the mapper only when a worker physically receives data.
        private fun getMapper(): ObjectMapper {
            if (mapper == null) {
                mapper =
                    jacksonObjectMapper()
                        .registerModule(JavaTimeModule())
                        // Automatically maps Python snake case JSON keys to Kotlin camel case variables.
                        .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            }
            return mapper!!
        }

        // Re-initialize logger if it was dropped during TaskManager network serialization.
        private fun getLogger() = log ?: LoggerFactory.getLogger(JsonDeserializationSchema::class.java).also { log = it }

        override fun deserialize(message: ByteArray): T? {
            try {
                val currentMapper = getMapper()
                val root = currentMapper.readTree(message)

                // The dynamic-des engine occasionally wraps payloads in a value root node.
                // This logic safely strips the wrapper if it exists before conversion.
                val dataNode = if (root.has("value")) root.get("value") else root

                return currentMapper.treeToValue(dataNode, typeClass)
            } catch (e: Exception) {
                getLogger().error("Failed to deserialize JSON. Raw message: ${String(message)}", e)
                // Returning null drops corrupted records rather than crashing the pipeline.
                return null
            }
        }

        override fun isEndOfStream(nextElement: T): Boolean = false

        override fun getProducedType(): TypeInformation<T> = TypeInformation.of(typeClass)
    }
}
