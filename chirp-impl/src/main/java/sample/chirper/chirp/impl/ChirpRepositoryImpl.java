package sample.chirper.chirp.impl;

import akka.stream.javadsl.Source;
import com.lightbend.lagom.javadsl.persistence.AggregateEventTag;
import com.lightbend.lagom.javadsl.persistence.ReadSide;
import com.lightbend.lagom.javadsl.persistence.ReadSideProcessor;
import com.lightbend.lagom.javadsl.persistence.jdbc.JdbcReadSide;
import com.lightbend.lagom.javadsl.persistence.jdbc.JdbcSession;
import org.pcollections.PSequence;
import org.pcollections.TreePVector;
import sample.chirper.chirp.api.Chirp;

import javax.inject.Inject;
import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;

class ChirpRepositoryImpl implements ChirpRepository {
    private static final int NUM_RECENT_CHIRPS = 10;
    private static final String SELECT_HISTORICAL_CHIRPS =
            "SELECT * FROM chirp WHERE userId IN (SELECT * FROM unnest(?)) AND timestamp >= ? ORDER BY timestamp ASC";
    private static final String SELECT_RECENT_CHIRPS =
            "SELECT * FROM chirp WHERE userId IN (SELECT * FROM unnest(?)) ORDER BY timestamp DESC LIMIT " + NUM_RECENT_CHIRPS;

    private final JdbcSession db;

    @Inject
    ChirpRepositoryImpl(JdbcSession db, ReadSide readSide) {
        this.db = db;
        readSide.register(ChirpTimelineEventReadSideProcessor.class);
    }

    public Source<Chirp, ?> getHistoricalChirps(PSequence<String> userIds, long timestamp) {
        CompletionStage<List<Chirp>> chirps = db.withConnection(connection ->
                getHistoricalChirps(connection, userIds, timestamp)
        );

        return Source.fromCompletionStage(chirps).mapConcat(list -> list);
    }

    private List<Chirp> getHistoricalChirps(Connection connection, PSequence<String> userIds, long timestamp)
            throws SQLException {
        PreparedStatement statement = connection.prepareStatement(SELECT_HISTORICAL_CHIRPS);
        Array userIdsArray = connection.createArrayOf("VARCHAR", userIds.toArray());
        statement.setArray(1, userIdsArray);
        statement.setTimestamp(2, new Timestamp(timestamp));
        return mapChirps(statement.executeQuery());
    }

    public CompletionStage<PSequence<Chirp>> getRecentChirps(PSequence<String> userIds) {
        return db.withConnection(connection -> getRecentChirps(connection, userIds))
                .thenApply(chirps -> {
                    Collections.reverse(chirps);
                    return chirps;
                })
                .thenApply(TreePVector::from);
    }

    private List<Chirp> getRecentChirps(Connection connection, PSequence<String> userIds)
            throws SQLException {
        PreparedStatement statement = connection.prepareStatement(SELECT_RECENT_CHIRPS);
        Array userIdsArray = connection.createArrayOf("VARCHAR", userIds.toArray());
        statement.setArray(1, userIdsArray);
        return mapChirps(statement.executeQuery());
    }

    private List<Chirp> mapChirps(ResultSet chirps) throws SQLException {
        List<Chirp> mappedChirps = new ArrayList<>();
        while (chirps.next()) {
            mappedChirps.add(mapChirp(chirps));
        }
        return mappedChirps;
    }

    private Chirp mapChirp(ResultSet chirp) throws SQLException {
        return new Chirp(
                chirp.getString("userId"),
                chirp.getString("message"),
                chirp.getTimestamp("timestamp").toInstant(),
                chirp.getString("uuid")
        );
    }


    private static class ChirpTimelineEventReadSideProcessor extends ReadSideProcessor<ChirpTimelineEvent> {
        private final JdbcReadSide readSide;

        @Inject
        private ChirpTimelineEventReadSideProcessor(JdbcReadSide readSide) {
            this.readSide = readSide;
        }

        @Override
        public ReadSideHandler<ChirpTimelineEvent> buildHandler() {
            return readSide.<ChirpTimelineEvent>builder("ChirpTimelineEventReadSideProcessor")
                    .setGlobalPrepare(this::createTable)
                    .setEventHandler(ChirpTimelineEvent.ChirpAdded.class,
                            (connection, event) -> insertChirp(connection, event.chirp))
                    .build();
        }

        @Override
        public PSequence<AggregateEventTag<ChirpTimelineEvent>> aggregateTags() {
            return ChirpTimelineEvent.TAG.allTags();
        }

        private void createTable(Connection connection) throws SQLException {
            connection.prepareStatement(
                    "CREATE TABLE IF NOT EXISTS chirp ("
                            + "userId VARCHAR, timestamp TIMESTAMP, uuid VARCHAR(36), message TEXT, "
                            + "PRIMARY KEY (userId, timestamp, uuid))"
            ).execute();
        }

        private void insertChirp(Connection connection, Chirp chirp) throws SQLException {
            PreparedStatement insertChirp = connection.prepareStatement(
                    "INSERT INTO chirp (userId, uuid, timestamp, message) VALUES (?, ?, ?, ?)"
            );
            insertChirp.setString(1, chirp.userId);
            insertChirp.setString(2, chirp.uuid);
            insertChirp.setTimestamp(3, Timestamp.from(chirp.timestamp));
            insertChirp.setString(4, chirp.message);
            insertChirp.execute();
        }
    }
}
