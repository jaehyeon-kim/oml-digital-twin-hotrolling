package me.jaehyeon.hotrolling.config

import java.io.Serializable

/**
 * Global configuration for the Online Machine Learning Flink pipeline.
 *
 * This data class holds all hyperparameters for the machine learning models,
 * the structural configurations for Kafka and Flink, and the connection details
 * for the ClickHouse sink. Default values are provided for local development.
 */
data class AppConfig(
    // Online Machine Learning Hyperparameters
    /**
     * Learning rate for the Target Mean tracking. A smaller value creates a smoother,
     * slower-moving average of the baseline error.
     */
    @Suppress("ktlint:standard:no-consecutive-comments")
    val ewmaLambda: Double = 0.02,
    /**
     * Step size for the Stochastic Gradient Descent optimizer. Controls how aggressively
     * the linear weights update in response to prediction errors.
     */
    val sgdLearningRate: Double = 0.001,
    /**
     * L2 regularization penalty applied to SGD weights to prevent exploding gradients
     * and over-reliance on single features.
     */
    val sgdDecay: Double = 0.00001,
    /**
     * Maximum allowed deviation for over-pressing (Strict safety limit to prevent machine damage).
     */
    val overpressTolerance: Double = 0.25,
    /**
     * Maximum allowed deviation for under-pressing (Looser limit to prevent process bottlenecks/rework).
     */
    val underpressTolerance: Double = 0.20,
    /**
     * Alpha value for the Shadow Mode Exponentially Weighted Moving Average (EWMA) trust score.
     * A higher value makes the router forget historical errors faster and react quicker
     * to recent performance.
     */
    val smoothingFactor: Double = 0.2,
    // Kafka Configuration
    /**
     * The address of the Kafka bootstrap server.
     */
    @Suppress("ktlint:standard:no-consecutive-comments")
    val bootstrapAddress: String = System.getenv("BOOTSTRAP") ?: "kafka-1:19092",
    /**
     * Topic containing the physical features available before the steel is rolled.
     */
    val predictionRequestsTopic: String = "mill-predictions",
    /**
     * Topic containing the actual measured force and hidden wear state after the roll.
     */
    val groundTruthTopic: String = "mill-groundtruth",
    /**
     * Consumer group ID for Flink to track partition offsets.
     */
    val kafkaGroupId: String = "hot-rolling-oml-group",
    // Flink Execution Configuration
    /**
     * The name of the job as it appears in the Flink Web UI.
     */
    @Suppress("ktlint:standard:no-consecutive-comments")
    val jobName: String = "HotRollingOMLProcessor",
    /**
     * The number of parallel worker tasks to spawn for processing streams.
     */
    val parallelism: Int = 3,
    /**
     * Frequency in milliseconds at which Flink snapshots the model states to durable storage.
     */
    val checkpointInterval: Long = 10_000,
    /**
     * Maximum time in milliseconds a checkpoint is allowed to take before failing.
     */
    val checkPointTimeout: Long = 60_000,
    /**
     * Minimum time in milliseconds to wait between a completed checkpoint and the next trigger.
     */
    val minPauseBetweenCheckpoints: Long = 500,
    /**
     * Maximum number of checkpoints that can run concurrently.
     */
    val maxConcurrentCheckpoints: Int = 1,
    /**
     * Number of consecutive checkpoint failures allowed before failing the entire job.
     */
    val tolerableCheckpointFailureNumber: Int = 3,
    // ClickHouse Configuration
    /**
     * HTTP endpoint for the ClickHouse database server.
     */
    @Suppress("ktlint:standard:no-consecutive-comments")
    val chEndpoint: String = System.getenv("CH_ENDPOINT") ?: "http://ch-server:8123",
    /**
     * Target database name for evaluation metrics.
     */
    val chDatabase: String = "dev",
    /**
     * Target table name for evaluation metrics.
     */
    val chTable: String = "oml_evaluation_metrics",
    /**
     * Username for authenticating with the ClickHouse database.
     */
    val chUser: String = "default",
) : Serializable
