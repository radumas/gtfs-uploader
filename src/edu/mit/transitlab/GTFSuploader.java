package edu.mit.transitlab;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.postgresql.util.PSQLException;

/**
 * This program should run in a folder of GTFS files and clean and upload them
 * to the transitlab server.
 *
 * @author rad
 */
public class GTFSuploader {

    private static Connection dbConnection;
    private static PreparedStatement insertStatement;
    private static PreparedStatement createStatement;
    private static String startDate;
    private static String endDate;
    public static final String COMMA_DELIM_DBL_QUOTE_PATTERN
            = ",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))";

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            openDatabaseConnection();
        } catch (SQLException ex) {
            Logger.getLogger(GTFSuploader.class.getName()).log(Level.SEVERE, null, ex);
        }

        try {
            //        Get the start and end date from the feed info file.
            openFeedInfo();
        } catch (IOException ex) {
            System.out.println("Error opening feed_info.txt");
            System.exit(1);

        }
        try {
            readStops();
        } catch (IOException ex) {
            Logger.getLogger(GTFSuploader.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(GTFSuploader.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            closeDatabaseConnection();
        } catch (SQLException ex) {
            Logger.getLogger(GTFSuploader.class.getName()).log(Level.SEVERE, null, ex);
        }
//        try {
//            openDatabaseConnection();
//            Date now = new Date();
//            while (now.before(endTime)) {
//                try {
//                    
//                    updateDatabase();
//                    System.gc();
//                    Thread.sleep(POLLING_INTERVAL);
//                } catch (IOException | InterruptedException ex) {
//                    Logger.getLogger(GTFSRealTimeArchiver.class.getName()).log(Level.WARNING, null, ex);
//                }
//                now = new Date();
//            }
//            closeDatabaseConnection();
//        } catch (SQLException ex) {
//            Logger.getLogger(GTFSRealTimeArchiver.class.getName()).log(Level.SEVERE, null, ex);
//            System.out.println("Exit with error.");
//            System.exit(1);
//        }
//        System.out.println("Finished.");
//        String query = "INSERT INTO gtfsrealtime("
//                + "entity_id, trip_id, trip_route_id, trip_start_time, trip_start_date, "
//                + "trip_schedule_relationship, vehicle_id, lat_e6, lon_e6, current_stop_sequence, "
//                + "\"timestamp\")"
//                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
//        ps = dbConnection.prepareStatement(query);

    }

    /**
     * Opens connection to the database and prepares the INSERT statement.
     *
     * @throws SQLException
     */
    private static void openDatabaseConnection() throws SQLException {
        dbConnection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/mbta", "java", "java");

    }

    /**
     * Closes connection to the database if it is open.
     *
     * @throws SQLException
     */
    private static void closeDatabaseConnection() throws SQLException {
        if (!dbConnection.isClosed()) {
            dbConnection.close();
        }
    }

    private static void openFeedInfo() throws IOException {
        BufferedReader reader
                = new BufferedReader(
                        new InputStreamReader(
                                new FileInputStream("feed_info.txt")));
//        Skip first row
        reader.readLine();

        String[] row = reader.readLine().split(",");
        startDate = row[3].substring(1, row[3].lastIndexOf("\""));
        endDate = row[4].substring(1, row[3].lastIndexOf("\""));
        reader.close();
//        System.out.println(startDate);
//        System.out.println(endDate);
    }

    private static void readStops() throws IOException, SQLException {
        if (dbConnection.isClosed()) {
            openDatabaseConnection();
        }

        BufferedReader reader
                = new BufferedReader(
                        new InputStreamReader(
                                new FileInputStream("stops.txt")));
//        Skip first row
        reader.readLine();
        String line;
        try {
            createStopsTable();
        } catch (PSQLException e) {
        };

        if (dbConnection.isClosed()) {
            openDatabaseConnection();
        }
        String insertQuery = "INSERT INTO gtfs_stops_" + startDate + "_" + endDate + "\n"
                + "VALUES (?, ?, ?, ?, ?, ?, \n"
                + "            ?, ?, ?, ?)";
        PreparedStatement insertStops = dbConnection.prepareStatement(insertQuery);
        int j = 1;

        for (; (line = reader.readLine()) != null; j++) {
            String[] row = line.split(COMMA_DELIM_DBL_QUOTE_PATTERN, -1);
            try {
                for (int i = 1; i < 9; i++) {
                    if (row[i - 1].lastIndexOf("\"") == 1) {
                        insertStops.setNull(i, java.sql.Types.VARCHAR);
                    } else {
                        if (row[i - 1].startsWith("\"")) {
                            insertStops.setString(i, row[i - 1].substring(row[i - 1].indexOf("\"") + 1, row[i - 1].lastIndexOf("\"")));
                        } else {
                            insertStops.setNull(i, java.sql.Types.VARCHAR);
                        }
                    }
                }

                if (row[8].startsWith("\"")) {
                    insertStops.setShort(9, Short.parseShort(row[8].substring(1, row[8].lastIndexOf("\""))));
                } else {
                    if (row[8].isEmpty()) {
                        insertStops.setNull(9, java.sql.Types.SMALLINT);
                    } else {
                        insertStops.setShort(9, Short.parseShort(row[8]));
                    }
                }

                if (row[9].lastIndexOf("\"") == 1) {
                    insertStops.setNull(10, java.sql.Types.VARCHAR);
                } else {
                    if (row[9].startsWith("\"")) {
                        insertStops.setString(10, row[9].substring(row[9].indexOf("\"") + 1, row[9].lastIndexOf("\"")));
                    } else {
                        insertStops.setNull(10, java.sql.Types.VARCHAR);
                    }
                }

                
                insertStops.addBatch();
            } catch (Exception e) {
                System.out.println("Read error on line " + j + " " + e);
            }
        }
        insertStops.executeBatch();
        insertStops.close();

    }

    private static void createStopsTable() throws SQLException {
        String createStopsQuery = "CREATE TABLE gtfs_stops_" + startDate + "_" + endDate + "\n"
                + "(\n"
                + ")\n"
                + "INHERITS (gtfs_stops)\n"
                + "WITH (\n"
                + "  OIDS=FALSE\n"
                + ");\n"
                + "ALTER TABLE gtfs_stops_20130808_20131227\n"
                + "  OWNER TO radumas;\n"
                + "GRANT ALL ON TABLE gtfs_stops_20130808_20131227 TO radumas;\n"
                + "GRANT SELECT, REFERENCES ON TABLE gtfs_stops_20130808_20131227 TO mbta_researchers;";
        PreparedStatement createStops = dbConnection.prepareStatement(createStopsQuery);
        createStops.execute();

    }
}
