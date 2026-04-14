package me.jaehyeon.hotrolling.infrastructure.kafka

import me.jaehyeon.hotrolling.config.AppConfig
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.slf4j.LoggerFactory
import java.util.Properties

/**
 * Utility object for managing Kafka infrastructure.
 *
 * Ensures that required Kafka topics are provisioned before the Flink job attempts
 * to consume from them. If Flink attempts to consume a non-existent topic, the job
 * will enter a failure state.
 */
object KafkaUtils {
    private val logger = LoggerFactory.getLogger(KafkaUtils::class.java)

    /**
     * Connects to the Kafka AdminClient, queries existing topics, and creates any
     * missing topics required by the pipeline.
     *
     * Args:
     * config: The global application configuration containing the broker address and topic names.
     * partitions: The number of partitions to create for new topics. Multiple partitions
     * allow Flink to read data concurrently across multiple workers.
     * replication: The replication factor for high availability. Defaults to 1 for local development.
     */
    fun ensureTopicsExist(
        config: AppConfig,
        partitions: Int = 3,
        replication: Short = 1,
    ) {
        val props =
            Properties().apply {
                put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapAddress)
            }

        try {
            AdminClient.create(props).use { client ->
                val existingTopics = client.listTopics().names().get()
                val requiredTopics = listOf(config.predictionRequestsTopic, config.groundTruthTopic)

                val topicsToCreate =
                    requiredTopics
                        .filter { !existingTopics.contains(it) }
                        .map { NewTopic(it, partitions, replication) }

                if (topicsToCreate.isNotEmpty()) {
                    logger.info("Creating missing Kafka topics: ${topicsToCreate.map { it.name() }}")
                    client.createTopics(topicsToCreate).all().get()
                    logger.info("Successfully created topics.")
                } else {
                    logger.info("All required Kafka topics already exist.")
                }
            }
        } catch (e: Exception) {
            logger.error("Failed to ensure Kafka topics exist: ${e.message}", e)
            throw RuntimeException(e)
        }
    }
}
