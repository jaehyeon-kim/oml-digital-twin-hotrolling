package me.jaehyeon.hotrolling

import me.jaehyeon.hotrolling.config.AppConfig
import me.jaehyeon.hotrolling.infrastructure.clickhouse.ClickHouseUtils
import me.jaehyeon.hotrolling.infrastructure.kafka.KafkaUtils
import me.jaehyeon.hotrolling.topology.HotRollingJob
import org.apache.flink.configuration.ExternalizedCheckpointRetention
import org.apache.flink.core.execution.CheckpointingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.LoggerFactory
import kotlin.system.exitProcess

/**
 * Main application entry point for the Hot Rolling Digital Twin.
 *
 * Orchestrates the booting of the Flink application, validates external infrastructure
 * such as Kafka topics and ClickHouse tables, applies global checkpointing configurations,
 * and submits the directed acyclic graph topology to the cluster.
 */
fun main() {
    val logger = LoggerFactory.getLogger("Main")
    val config = AppConfig()

    logger.info("Starting ${config.jobName} Application...")

    try {
        // Infrastructure Bootstrapping
        // Flink jobs will crash in a continuous loop if required topics or tables do not exist.
        // We proactively create them here if they are missing before the job starts.
        KafkaUtils.ensureTopicsExist(config)
        ClickHouseUtils.ensureTableExists(config)

        // Environment Setup
        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        configureEnvironment(env, config)

        // Topology Construction
        HotRollingJob.build(env, config)

        logger.info("Executing Flink Job: ${config.jobName}")
        env.execute(config.jobName)
    } catch (e: Exception) {
        logger.error("Critical error in Flink job execution", e)
        exitProcess(1)
    }
}

/**
 * Applies fault-tolerance and high-availability settings to the Flink environment.
 *
 * Checkpointing guarantees exactly-once processing semantics by regularly saving
 * the state of our machine learning models and Welford Scalers to distributed storage.
 * This ensures that if a worker node crashes, the system recovers the exact state
 * without duplicating or dropping data.
 *
 * Args:
 * env: The execution environment to configure.
 * config: The application configuration containing checkpoint intervals and timeouts.
 */
fun configureEnvironment(
    env: StreamExecutionEnvironment,
    config: AppConfig,
) {
    env.enableCheckpointing(config.checkpointInterval, CheckpointingMode.EXACTLY_ONCE)

    env.checkpointConfig.apply {
        checkpointTimeout = config.checkPointTimeout

        // Crucial for MLOps: If we intentionally cancel the job to update the Flink image,
        // we retain the state files so the new job can pick up the machine learning weights
        // right where it left off.
        externalizedCheckpointRetention = ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION

        minPauseBetweenCheckpoints = config.minPauseBetweenCheckpoints
        maxConcurrentCheckpoints = config.maxConcurrentCheckpoints
        tolerableCheckpointFailureNumber = config.tolerableCheckpointFailureNumber
    }
}
