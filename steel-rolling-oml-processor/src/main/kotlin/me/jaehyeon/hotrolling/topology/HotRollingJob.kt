package me.jaehyeon.hotrolling.topology

import me.jaehyeon.hotrolling.config.AppConfig
import me.jaehyeon.hotrolling.domain.model.GroundTruthEvent
import me.jaehyeon.hotrolling.domain.model.PredictionRequestEvent
import me.jaehyeon.hotrolling.infrastructure.clickhouse.ClickHouseSinkFactory
import me.jaehyeon.hotrolling.infrastructure.kafka.KafkaSourceFactory
import me.jaehyeon.hotrolling.topology.processing.EventMatchProcessFunction
import me.jaehyeon.hotrolling.topology.processing.MoaEvaluationProcessFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 * Directed Acyclic Graph (DAG) definition for the Flink streaming application.
 *
 * This object defines the topology of the pipeline: where data comes from (Sources),
 * how it is transformed and joined (Operators), and where it goes (Sinks).
 */
object HotRollingJob {
    fun build(
        env: StreamExecutionEnvironment,
        config: AppConfig,
    ) {
        val kafkaFactory = KafkaSourceFactory(env, config)

        // 1. Define Sources
        // We ingest both the "What we expect" (Predictions) and "What happened" (Ground Truth) streams
        val predictionStream =
            kafkaFactory.createStream(
                config.predictionRequestsTopic,
                PredictionRequestEvent::class.java,
            )

        val groundTruthStream =
            kafkaFactory.createStream(
                config.groundTruthTopic,
                GroundTruthEvent::class.java,
            )

        // 2. STAGE 1: Match Streams (Joiner)
        // Keying by Slab ID and Pass Number guarantees that the Prediction and its corresponding
        // Ground Truth are routed to the exact same TaskManager node. The EventMatchProcessFunction
        // resolves race conditions and fuses them into a single MatchedEvent.
        val matchedStream =
            predictionStream
                .keyBy { "${it.identifiers.slabId}-${it.identifiers.passNumber}" }
                .connect(groundTruthStream.keyBy { "${it.identifiers.slabId}-${it.identifiers.passNumber}" })
                .process(EventMatchProcessFunction())
                .name("Event-Matcher")

        // 3. STAGE 2: Online Machine Learning Evaluation (Engine)
        // We re-key the stream by Steel Grade. This is critical. Physics between structural
        // and high alloy steel are drastically different. By keying by grade, Flink provisions
        // completely isolated AMRules models for each product line, preventing Catastrophic Forgetting.
        val evaluationStream =
            matchedStream
                .keyBy { it.prediction.identifiers.steelGrade }
                .process(
                    MoaEvaluationProcessFunction(
                        ewmaLambda = config.ewmaLambda,
                        sgdLearningRate = config.sgdLearningRate,
                        sgdDecay = config.sgdDecay,
                        overpressTolerance = config.overpressTolerance,
                        underpressTolerance = config.underpressTolerance,
                        smoothingFactor = config.smoothingFactor,
                    ),
                ).name("OML-Evaluation-And-Training")

        // 4. Define Sink (ClickHouse)
        // The resulting Evaluation Metrics (containing Safe APE and Shadow APE) are batched
        // and flushed to ClickHouse for the real-time user interface to consume.
        val clickHouseSink = ClickHouseSinkFactory.createSink(config)
        evaluationStream
            .sinkTo(clickHouseSink)
            .name("ClickHouse-Sink")
    }
}
