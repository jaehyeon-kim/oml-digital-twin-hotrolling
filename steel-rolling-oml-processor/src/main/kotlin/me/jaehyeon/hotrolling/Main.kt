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

fun main() {
    val logger = LoggerFactory.getLogger("Main")
    val config = AppConfig() // Using your clean default constructor

    logger.info("Starting ${config.jobName} Application...")

    try {
        // Infrastructure Checks
        KafkaUtils.ensureTopicsExist(config)
        ClickHouseUtils.ensureTableExists(config)

        // Setup Environment
        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        configureEnvironment(env, config)

        // Build Topology
        HotRollingJob.build(env, config)

        logger.info("Executing Flink Job: ${config.jobName}")
        env.execute(config.jobName)
    } catch (e: Exception) {
        logger.error("Critical error in Flink job execution", e)
        exitProcess(1)
    }
}

fun configureEnvironment(
    env: StreamExecutionEnvironment,
    config: AppConfig,
) {
    // Enable EXACTLY_ONCE semantics for highly accurate ML metrics
    env.enableCheckpointing(config.checkpointInterval, CheckpointingMode.EXACTLY_ONCE)
    env.checkpointConfig.apply {
        checkpointTimeout = config.checkPointTimeout
        // Crucial: Keep the state files if the job is manually canceled
        externalizedCheckpointRetention = ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION
        minPauseBetweenCheckpoints = config.minPauseBetweenCheckpoints
        maxConcurrentCheckpoints = config.maxConcurrentCheckpoints
        tolerableCheckpointFailureNumber = config.tolerableCheckpointFailureNumber
    }
}
