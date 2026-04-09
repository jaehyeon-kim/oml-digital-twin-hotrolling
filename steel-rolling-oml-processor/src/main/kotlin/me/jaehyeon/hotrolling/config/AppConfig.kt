package me.jaehyeon.hotrolling.config

import java.io.Serializable

data class AppConfig(
    // OML Hyperparameters
    val ewmaLambda: Double = 0.02,
    val sgdLearningRate: Double = 0.001,
    val sgdDecay: Double = 0.00001,
    // Kafka Config
    val bootstrapAddress: String = System.getenv("BOOTSTRAP") ?: "kafka-1:19092",
    val predictionRequestsTopic: String = "mill-predictions",
    val groundTruthTopic: String = "mill-groundtruth",
    val kafkaGroupId: String = "hot-rolling-oml-group",
    // Flink Config
    val jobName: String = "HotRollingOMLProcessor",
    val parallelism: Int = 3,
    val checkpointInterval: Long = 10_000,
    val checkPointTimeout: Long = 60_000,
    val minPauseBetweenCheckpoints: Long = 500,
    val maxConcurrentCheckpoints: Int = 1,
    val tolerableCheckpointFailureNumber: Int = 3,
    // ClickHouse Config (For Dashboard Metrics)
    val chEndpoint: String = System.getenv("CH_ENDPOINT") ?: "http://ch-server:8123",
    val chDatabase: String = "dev",
    val chTable: String = "oml_evaluation_metrics",
    val chUser: String = "default",
) : Serializable
