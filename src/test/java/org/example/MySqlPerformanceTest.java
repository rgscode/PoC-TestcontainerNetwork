package org.example;

import lombok.extern.slf4j.Slf4j;
import org.example.model.UuidRecord;
import org.example.service.DatabaseService;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@Testcontainers
public class MySqlPerformanceTest {

    private static final int TOTAL_RECORDS = 10000;
    private static final int BATCH_SIZE = 10_000;
    
    private static final String DATABASE_NAME = "testdb";
    private static final String USERNAME = "testuser";
    private static final String PASSWORD = "testpassword";
    private static final int MYSQL_PORT = 3306;
    private static final String MYSQL_HOST = "localhost";

    /**
     * Create a MySQL container using GenericContainer.
     * We configure it with:
     * 1. MySQL 8.0 image
     * 2. Host network mode to make it accessible from localhost:3306
     * 3. Environment variables for database name, username, and password
     * 4. Startup timeout of 1 minute
     */
    @Container
    private static final GenericContainer<?> mysql = new GenericContainer<>(DockerImageName.parse("mysql:8.0"))
            .withEnv("MYSQL_DATABASE", DATABASE_NAME)
            .withEnv("MYSQL_USER", USERNAME)
            .withEnv("MYSQL_PASSWORD", PASSWORD)
            .withEnv("MYSQL_ROOT_PASSWORD", PASSWORD)
            .withStartupTimeout(Duration.ofMinutes(1))
            .withNetworkMode("host")
            .waitingFor(
                Wait.forLogMessage(".*ready for connections.*\\s", 2)
            );

    @Test
    public void testInsertOneMillionUuids() throws SQLException {
        
        log.info("Starting MySQL performance test");
        log.info("MySQL container is running at: {}:{}", MYSQL_HOST, MYSQL_PORT);
        
        waitForDatabaseConnection();
        
        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s",
                MYSQL_HOST, MYSQL_PORT, DATABASE_NAME);
        
        log.info("Connecting to database at: {}", jdbcUrl);
        
        try (Connection connection = DriverManager.getConnection(
                jdbcUrl, 
                USERNAME, 
                PASSWORD)) {
            
            DatabaseService databaseService = new DatabaseService(connection);
            
            databaseService.createTable();
            
            log.info("Generating {} UUID records", TOTAL_RECORDS);
            List<UuidRecord> records = generateUuidRecords(TOTAL_RECORDS);
            
            Instant start = Instant.now();
            databaseService.insertRecords(records);
            Instant end = Instant.now();
            
            Duration duration = Duration.between(start, end);
            log.info("Inserted {} records in {} seconds", TOTAL_RECORDS, duration.getSeconds());
            log.info("Average insertion rate: {} records/second", TOTAL_RECORDS / (double) duration.getSeconds());
            
            long count = databaseService.countRecords();
            assertEquals(TOTAL_RECORDS, count, "Number of records in the database should match the number inserted");
            
            log.info("Test completed successfully");
        }
    }

    private void waitForDatabaseConnection() {
        String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s",
                MYSQL_HOST, MYSQL_PORT, DATABASE_NAME);
        
        int maxAttempts = 30; // 30 seconds maximum wait time
        int attemptDelay = 1000; // 1 second between attempts
        
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try (Connection connection = DriverManager.getConnection(
                    jdbcUrl, USERNAME, PASSWORD)) {
                log.info("Database connection established on attempt {}", attempt);
                return;
            } catch (SQLException e) {
                if (attempt == maxAttempts) {
                    throw new RuntimeException("Failed to establish database connection after " + maxAttempts + " attempts", e);
                }
                log.debug("Database connection attempt {} failed: {}", attempt, e.getMessage());
                try {
                    Thread.sleep(attemptDelay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for database connection", ie);
                }
            }
        }
    }
    
    /**
     * Generate a list of UUID records.
     *
     * @param count the number of records to generate
     * @return a list of UUID records
     */
    private List<UuidRecord> generateUuidRecords(int count) {
        List<UuidRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            records.add(new UuidRecord(UUID.randomUUID()));
            
            if ((i + 1) % BATCH_SIZE == 0) {
                log.debug("Generated {} UUID records", i + 1);
            }
        }
        return records;
    }
}