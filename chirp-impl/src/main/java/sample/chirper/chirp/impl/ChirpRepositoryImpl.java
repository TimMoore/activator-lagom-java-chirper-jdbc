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
import java.util.StringJoiner;
import java.util.concurrent.CompletionStage;

class ChirpRepositoryImpl implements ChirpRepository {
    private static final int NUM_RECENT_CHIRPS = 10;

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
        PreparedStatement statement = connection.prepareStatement(selectHistoricalChirpsStatement(userIds.size()));
        for (int i = 0; i < userIds.size(); i++) {
            statement.setObject(i + 1, userIds.get(i));
        }
        statement.setTimestamp(userIds.size() + 1, new Timestamp(timestamp));
        return mapChirps(statement.executeQuery());
    }

    private String selectHistoricalChirpsStatement(int userIdCount) {
        return "SELECT * FROM chirp WHERE userId IN " +
                bindList(userIdCount) +
                " AND timestamp >= ? ORDER BY \"timestamp\" ASC";
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
        PreparedStatement statement = connection.prepareStatement(selectRecentChirpsStatement(userIds.size()));
        for (int i = 0; i < userIds.size(); i++) {
            statement.setObject(i + 1, userIds.get(i));
        }
        return mapChirps(statement.executeQuery());
    }

    private String selectRecentChirpsStatement(int userIdCount) {
        return "SELECT * FROM chirp WHERE userId IN " +
                bindList(userIdCount) +
                " AND ROWNUM <= " + NUM_RECENT_CHIRPS +
                " ORDER BY \"timestamp\" DESC";
    }

    private String bindList(int count) {
        StringJoiner list = new StringJoiner(",", "(", ")");
        for (int i = 0; i < count; i++) {
            list.add("?");
        }
        return list.toString();
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
//                    .setGlobalPrepare(this::createTable)
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
                    "CREATE TABLE chirp ("
                            + "userId VARCHAR2(255), \"timestamp\" TIMESTAMP, uuid VARCHAR2(36), message VARCHAR2(160), "
                            + "PRIMARY KEY (userId, \"timestamp\", uuid))"
            ).execute();
        }

        private void insertChirp(Connection connection, Chirp chirp) throws SQLException {
            PreparedStatement insertChirp = connection.prepareStatement(
                    "INSERT INTO chirp (userId, uuid, \"timestamp\", message) VALUES (?, ?, ?, ?)"
            );
            insertChirp.setString(1, chirp.userId);
            insertChirp.setString(2, chirp.uuid);
            insertChirp.setTimestamp(3, Timestamp.from(chirp.timestamp));
            insertChirp.setString(4, chirp.message);
            insertChirp.execute();
        }
    }
}
