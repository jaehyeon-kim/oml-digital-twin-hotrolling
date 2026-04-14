package me.jaehyeon.hotrolling.infrastructure.clickhouse

import me.jaehyeon.hotrolling.config.AppConfig
import org.slf4j.LoggerFactory
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

/**
 * Utility object for bootstrapping the ClickHouse database environment.
 *
 * Ensures that the target database and MergeTree table exist before the Flink job
 * begins processing. This prevents the streaming pipeline from crashing due to
 * missing infrastructure on startup.
 */
object ClickHouseUtils {
    private val logger = LoggerFactory.getLogger(ClickHouseUtils::class.java)

    /**
     * Connects to ClickHouse via HTTP and executes Data Definition Language statements.
     *
     * The table created uses the MergeTree engine, which is optimized for massive
     * time-series data ingestion. The ORDER BY clause is critical: it physically sorts
     * the data on disk by timestamp and steel grade, making dashboard queries lightning fast.
     */
    fun ensureTableExists(config: AppConfig) {
        val client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()

        val createDbSql = "CREATE DATABASE IF NOT EXISTS ${config.chDatabase}"
        executeSql(client, config, createDbSql)

        val createTableSql =
            """
            CREATE TABLE IF NOT EXISTS ${config.chDatabase}.${config.chTable} (
                evaluation_timestamp DateTime64(3),
                steel_grade String,
                slab_id String,
                pass_number Int32,                
                baseline_roll_force_kn Float64,
                target_mean_roll_force_kn Float64,
                sgd_roll_force_kn Float64,
                am_rules_roll_force_kn Float64,
                actual_roll_force_kn Float64,                
                baseline_ape Float64,
                target_mean_ape Float64,
                sgd_ape Float64,
                am_rules_ape Float64,
                am_rules_shadow_ape Float64,
                wear_level Float64,
                is_am_rules_fallback UInt8
            ) ENGINE = MergeTree()
            ORDER BY (evaluation_timestamp, steel_grade);
            """.trimIndent()

        executeSql(client, config, createTableSql)
    }

    /**
     * Executes a raw SQL query against the ClickHouse HTTP interface.
     */
    private fun executeSql(
        client: HttpClient,
        config: AppConfig,
        sql: String,
    ) {
        val request =
            HttpRequest
                .newBuilder()
                .uri(URI.create(config.chEndpoint))
                .header("X-ClickHouse-User", config.chUser)
                .POST(HttpRequest.BodyPublishers.ofString(sql))
                .build()

        try {
            val response = client.send(request, HttpResponse.BodyHandlers.ofString())
            if (response.statusCode() == 200) {
                logger.info("Executed SQL successfully on ClickHouse.")
            } else {
                throw RuntimeException("Failed to execute SQL: ${response.body()}")
            }
        } catch (e: Exception) {
            logger.error("ClickHouse connection error: ${e.message}")
            throw e
        }
    }
}
