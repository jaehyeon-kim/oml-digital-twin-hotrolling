package me.jaehyeon.hotrolling.infrastructure.kafka

import me.jaehyeon.hotrolling.config.AppConfig
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.slf4j.LoggerFactory
import java.util.Properties

object KafkaUtils {
    private val logger = LoggerFactory.getLogger(KafkaUtils::class.java)

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
