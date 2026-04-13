package me.jaehyeon.hotrolling.topology

import me.jaehyeon.hotrolling.config.AppConfig
import me.jaehyeon.hotrolling.domain.model.GroundTruthEvent
import me.jaehyeon.hotrolling.domain.model.PredictionRequestEvent
import me.jaehyeon.hotrolling.infrastructure.clickhouse.ClickHouseSinkFactory
import me.jaehyeon.hotrolling.infrastructure.kafka.KafkaSourceFactory
import me.jaehyeon.hotrolling.topology.processing.EventMatchProcessFunction
import me.jaehyeon.hotrolling.topology.processing.MoaEvaluationProcessFunction
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object HotRollingJob {
    fun build(
        env: StreamExecutionEnvironment,
        config: AppConfig,
    ) {
        val kafkaFactory = KafkaSourceFactory(env, config)

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

        // STAGE 1: Match Streams (Isolated by Slab & Pass to handle race conditions)
        val matchedStream =
            predictionStream
                .keyBy { "${it.identifiers.slabId}-${it.identifiers.passNumber}" }
                .connect(groundTruthStream.keyBy { "${it.identifiers.slabId}-${it.identifiers.passNumber}" })
                .process(EventMatchProcessFunction())
                .name("Event-Matcher")

        // STAGE 2: OML Evaluation (Isolated by Steel Grade to prevent Catastrophic Forgetting)
        val evaluationStream =
            matchedStream
                .keyBy { it.prediction.identifiers.steelGrade }
                .process(
                    MoaEvaluationProcessFunction(
                        ewmaLambda = config.ewmaLambda,
                        sgdLearningRate = config.sgdLearningRate,
                        sgdDecay = config.sgdDecay,
                        fallbackTolerance = config.fallbackTolerance,
                    ),
                ).name("OML-Evaluation-And-Training")

        // Sink to ClickHouse
        val clickHouseSink = ClickHouseSinkFactory.createSink(config)
        evaluationStream
            .sinkTo(clickHouseSink)
            .name("ClickHouse-Sink")
    }
}
