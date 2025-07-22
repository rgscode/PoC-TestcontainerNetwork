package org.example.service;

import lombok.extern.slf4j.Slf4j;
import org.example.model.UuidRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;

@Slf4j
public class DatabaseService {

    private final Connection connection;
    private static final String TABLE_NAME = "uuid_records";
    private static final int BATCH_SIZE = 10000;

    public DatabaseService(Connection connection) {
        this.connection = connection;
    }

    /**
     * Creates the table for storing UUID records.
     * The table has a single column for the UUID.
     *
     * @throws SQLException if an error occurs while creating the table
     */
    public void createTable() throws SQLException {
        log.info("Creating table: {}", TABLE_NAME);
        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
            statement.execute("CREATE TABLE " + TABLE_NAME + " (id BINARY(16) PRIMARY KEY)");
        }
        log.info("Table created successfully");
    }

    /**
     * Inserts a list of UUID records into the database.
     * The records are inserted in batches for better performance.
     *
     * @param records the list of UUID records to insert
     * @throws SQLException if an error occurs while inserting the records
     */
    public void insertRecords(List<UuidRecord> records) throws SQLException {
        log.info("Inserting {} records into table: {}", records.size(), TABLE_NAME);
        String sql = "INSERT INTO " + TABLE_NAME + " (id) VALUES (?)";
        
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int count = 0;
            
            for (UuidRecord record : records) {

                UUID uuid = record.getId();
                byte[] bytes = new byte[16];
                
                long msb = uuid.getMostSignificantBits();
                for (int i = 0; i < 8; i++) {
                    bytes[i] = (byte) (msb >>> (8 * (7 - i)));
                }
                
                long lsb = uuid.getLeastSignificantBits();
                for (int i = 8; i < 16; i++) {
                    bytes[i] = (byte) (lsb >>> (8 * (15 - i)));
                }
                
                statement.setBytes(1, bytes);
                statement.addBatch();
                count++;
                
                if (count % BATCH_SIZE == 0) {
                    statement.executeBatch();
                    log.debug("Inserted batch of {} records", BATCH_SIZE);
                }
            }
            
            if (count % BATCH_SIZE != 0) {
                statement.executeBatch();
                log.debug("Inserted remaining {} records", count % BATCH_SIZE);
            }
        }
        log.info("Records inserted successfully");
    }

    /**
     * Counts the number of records in the table.
     *
     * @return the number of records in the table
     * @throws SQLException if an error occurs while counting the records
     */
    public long countRecords() throws SQLException {
        log.info("Counting records in table: {}", TABLE_NAME);
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
            if (resultSet.next()) {
                long count = resultSet.getLong(1);
                log.info("Found {} records in table", count);
                return count;
            }
            return 0;
        }
    }
}