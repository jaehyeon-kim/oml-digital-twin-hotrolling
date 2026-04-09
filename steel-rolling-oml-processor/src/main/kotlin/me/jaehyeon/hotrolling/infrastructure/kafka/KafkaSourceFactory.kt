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

class KafkaSourceFactory(
    private val env: StreamExecutionEnvironment,
    private val config: AppConfig,
) {
    private val logger = LoggerFactory.getLogger(KafkaSourceFactory::class.java)

    // Remove the ObjectMapper from the factory level, we will create it inside the Schema

    fun <T> createStream(
        topic: String,
        typeClass: Class<T>,
    ): org.apache.flink.streaming.api.datastream.DataStreamSource<T> {
        logger.info("Initializing Pure JSON Kafka Source for topic: '$topic'")

        val source =
            KafkaSource
                .builder<T>()
                .setBootstrapServers(config.bootstrapAddress)
                .setTopics(topic)
                .setGroupId(config.kafkaGroupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                // Pass ONLY the class type, not the mapper
                .setValueOnlyDeserializer(JsonDeserializationSchema(typeClass))
                .build()

        return env.fromSource(source, WatermarkStrategy.noWatermarks(), topic)
    }

    class JsonDeserializationSchema<T>(
        private val typeClass: Class<T>,
    ) : DeserializationSchema<T> {
        @Transient
        private var log = LoggerFactory.getLogger(JsonDeserializationSchema::class.java)

        // 1. Mark as @Transient so Flink doesn't try to serialize it
        @Transient
        private var mapper: ObjectMapper? = null

        // 2. Lazy Initialization: Create the mapper only when a worker needs it
        private fun getMapper(): ObjectMapper {
            if (mapper == null) {
                mapper =
                    jacksonObjectMapper()
                        .registerModule(JavaTimeModule())
                        .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            }
            return mapper!!
        }

        // Re-initialize logger if it was dropped during serialization
        private fun getLogger() = log ?: LoggerFactory.getLogger(JsonDeserializationSchema::class.java).also { log = it }

        override fun deserialize(message: ByteArray): T? {
            try {
                // Use the lazy getter
                val currentMapper = getMapper()

                val root = currentMapper.readTree(message)
                val dataNode = if (root.has("value")) root.get("value") else root

                return currentMapper.treeToValue(dataNode, typeClass)
            } catch (e: Exception) {
                getLogger().error("Failed to deserialize JSON. Raw message: ${String(message)}", e)
                return null
            }
        }

        override fun isEndOfStream(nextElement: T): Boolean = false

        override fun getProducedType(): TypeInformation<T> = TypeInformation.of(typeClass)
    }
}
