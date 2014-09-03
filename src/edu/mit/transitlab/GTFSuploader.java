package edu.mit.transitlab;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.postgresql.util.PSQLException;

/**
 * This program should run in a folder of GTFS files and clean and upload them
 * to the transitlab server.
 *
 * @author rad
 */
public class GTFSuploader {

    private static Connection dbConnection;

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
            System.out.print("Opening feed_info.txt...");
            openFeedInfo();
            System.out.println("done.");
        } catch (IOException | SQLException ex) {
            System.out.println("Error.");
            System.exit(1);

        }
        try {
            System.out.print("Opening stops.txt...");
            readStops();
            System.out.println("done.");

        } catch (IOException | SQLException ex) {
            System.out.println("Error.");
            Logger.getLogger(GTFSuploader.class.getName()).log(Level.SEVERE, null, ex);
        }

        try {

            System.out.print("Opening stop_times.txt...");
            readStopTimes();
            System.out.println("done.");

        } catch (IOException | SQLException ex) {
            System.out.println("Error.");
            Logger.getLogger(GTFSuploader.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            System.out.print("Opening calendar.txt...");
            readCalendar();
            System.out.println("done.");

        } catch (IOException | SQLException ex) {
            System.out.println("Error.");
            Logger.getLogger(GTFSuploader.class.getName()).log(Level.SEVERE, null, ex);
        }

        try {
            System.out.print("Opening calendar_dates.txt...");
            readCalendar_dates();
            System.out.println("done.");

        } catch (IOException | SQLException ex) {
            System.out.println("Error.");
            Logger.getLogger(GTFSuploader.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            System.out.print("Opening routes.txt...");
            readRoutes();
            System.out.println("done.");

        } catch (IOException | SQLException ex) {
            System.out.println("Error.");
            Logger.getLogger(GTFSuploader.class.getName()).log(Level.SEVERE, null, ex);
        }

        try {
            System.out.print("Opening trips.txt...");
            readTrips();
            System.out.println("done.");

        } catch (IOException | SQLException ex) {
            System.out.println("Error.");
            Logger.getLogger(GTFSuploader.class.getName()).log(Level.SEVERE, null, ex);
        }

        try {
            System.out.print("Opening transfers.txt...");
            readTransfers();
            System.out.println("done.");

        } catch (IOException | SQLException ex) {
            System.out.println("Error.");
            Logger.getLogger(GTFSuploader.class.getName()).log(Level.SEVERE, null, ex);
        }

        try {
            System.out.print("Opening frequencies.txt...");
            readFrequencies();
            System.out.println("done.");

        } catch (IOException | SQLException ex) {
            System.out.println("Error.");
            Logger.getLogger(GTFSuploader.class.getName()).log(Level.SEVERE, null, ex);
        }

        try {
            System.out.print("Opening shapes.txt...");
            readShapes();
            System.out.println("done.");

        } catch (IOException | SQLException ex) {
            System.out.println("Error.");
            Logger.getLogger(GTFSuploader.class.getName()).log(Level.SEVERE, null, ex);
        }

        try {
            System.out.print("Add primary keys and foreign keys to gtfs tables...");
            basicKeys();
            System.out.println("done.");

        } catch (SQLException ex) {
            System.out.println("Error.");
            Logger.getLogger(GTFSuploader.class.getName()).log(Level.SEVERE, null, ex);
        }

        try {
            System.out.print("Adding geometry table, nearest stop stop matrix, and bus patterns...");

            insertStopGeometry();
            insertNearestStopStopMatrix();
            insertBusPatterns();
            System.out.println("done.");

        } catch (SQLException ex) {
            System.out.println("Error.");
            Logger.getLogger(GTFSuploader.class.getName()).log(Level.SEVERE, null, ex);
        }

        try {
            System.out.print("Add primary keys and foreign keys to processed tables...");
            advancedKeys();
            System.out.println("done.");

        } catch (SQLException ex) {
            System.out.println("Error.");
            Logger.getLogger(GTFSuploader.class.getName()).log(Level.SEVERE, null, ex);
        }

        try {
            closeDatabaseConnection();
        } catch (SQLException ex) {
            Logger.getLogger(GTFSuploader.class.getName()).log(Level.SEVERE, null, ex);
        }

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

    private static void openFeedInfo() throws IOException, SQLException {
        BufferedReader reader
                = new BufferedReader(
                        new InputStreamReader(
                                new FileInputStream("feed_info.txt")));
//        Skip first row
        reader.readLine();

        String[] row = reader.readLine().split(",");
        startDate = row[3].substring(1, row[3].lastIndexOf("\""));
        endDate = row[4].substring(1, row[3].lastIndexOf("\""));
        String feed_date = row[row.length - 1].substring(0, row[row.length - 1].lastIndexOf("\"")).trim();
        String feed_version = row[row.length - 2].substring(1).trim();
        reader.close();

        if (dbConnection.isClosed()) {
            openDatabaseConnection();
        }

        try {

            insertFeedInfo(feed_date, feed_version);
        } catch (PSQLException e) {
            System.out.println(e);
        };

//        System.out.println(startDate);
//        System.out.println(endDate);
    }

    private static void insertFeedInfo(String feedDate, String feedVersion) throws SQLException {
        String insertQuery = "INSERT INTO gtfs.feed(\n"
                + "            uploaded, start_date, end_date, feed_date, version)\n"
                + "    VALUES (now(), " + startDate + ", " + endDate + ", to_date('" + feedDate + "' , 'MM/DD/YY'),'" + feedVersion + "' );";
        PreparedStatement insert = dbConnection.prepareStatement(insertQuery);
        insert.execute();
    }

    private static void readStops() throws IOException, SQLException {
        if (dbConnection.isClosed()) {
            openDatabaseConnection();
        }

        try {
            createStopsTable();
        } catch (PSQLException e) {
            System.out.println(e);
        };

        BufferedReader reader
                = new BufferedReader(
                        new InputStreamReader(
                                new FileInputStream("stops.txt")));
        PrintStream textOut
                = new PrintStream(
                        new BufferedOutputStream(
                                new FileOutputStream("stops_prepared.csv", false)));

//        Skip first row
        textOut.println(reader.readLine());

        String line;

        int j = 1;

        for (; (line = reader.readLine()) != null; j++) {
            String[] row = line.split(COMMA_DELIM_DBL_QUOTE_PATTERN, -1);
//                The one row with a number

            if (row[8].startsWith("\"")) {
                row[8] = row[8].substring(row[8].indexOf("\"") + 1, row[8].lastIndexOf("\""));
            }
            String output = "";
            for (int i = 0; i < row.length; i++) {
//                Putting empty quotes into null values.
                if (row[i].isEmpty()) {
                    row[i] = "\"\"";
                }

            }

//            Deal with quotes for numbers
            if (row[8].lastIndexOf("\"") == 1) {
                row[8] = "";
            }

            for (int i = 0; i < row.length - 1; i++) {
//                Putting empty quotes into null values.
                output += row[i] + ",";
            }
            output += row[row.length - 1];
            textOut.println(output);

        }
        textOut.close();
        textOut.flush();
        reader.close();

        CopyManager manager = new CopyManager((BaseConnection) (dbConnection));

        try {
            manager.copyIn("COPY gtfs.stops_" + startDate + "_" + endDate + " FROM STDIN WITH (FORMAT 'csv', HEADER true)", new FileReader("stops_prepared.csv"));
        } catch (PSQLException ex) {
            System.out.println(ex);
            System.out.println(ex.getServerErrorMessage().getMessage());
        }

//
//        if (dbConnection.isClosed()) {
//            openDatabaseConnection();
//        }
//        String insertQuery = "INSERT INTO gtfs.stops_" + startDate + "_" + endDate + "\n"
//                + "VALUES (?, ?, ?, ?, ?, ?, \n"
//                + "            ?, ?, ?, ?)";
//        PreparedStatement insertStops = dbConnection.prepareStatement(insertQuery);
//        int j = 1;
//
//        for (; (line = reader.readLine()) != null; j++) {
//            String[] row = line.split(COMMA_DELIM_DBL_QUOTE_PATTERN, -1);
//            try {
//                for (int i = 1; i < 9; i++) {
//                    if (row[i - 1].lastIndexOf("\"") == 1) {
//                        insertStops.setNull(i, java.sql.Types.VARCHAR);
//                    } else {
//                        if (row[i - 1].startsWith("\"")) {
//                            insertStops.setString(i, row[i - 1].substring(row[i - 1].indexOf("\"") + 1, row[i - 1].lastIndexOf("\"")));
//                        } else {
//                            insertStops.setNull(i, java.sql.Types.VARCHAR);
//                        }
//                    }
//                }
//
//                if (row[8].startsWith("\"")) {
//                    insertStops.setShort(9, Short.parseShort(row[8].substring(1, row[8].lastIndexOf("\""))));
//                } else {
//                    if (row[8].isEmpty()) {
//                        insertStops.setNull(9, java.sql.Types.SMALLINT);
//                        
//                    } else {
//                        insertStops.setShort(9, Short.parseShort(row[8]));
//                    }
//                }
//
//                if (row[9].lastIndexOf("\"") == 1) {
//                    insertStops.setNull(10, java.sql.Types.VARCHAR);
//                } else {
//                    if (row[9].startsWith("\"")) {
//                        insertStops.setString(10, row[9].substring(row[9].indexOf("\"") + 1, row[9].lastIndexOf("\"")));
//                    } else {
//                        insertStops.setNull(10, java.sql.Types.VARCHAR);
//                    }
//                }
//
//                
//                insertStops.addBatch();
//            } catch (Exception e) {
//                System.out.println("Read error on line " + j + " " + e);
//            }
//        }
//        insertStops.executeBatch();
//        insertStops.close();
//
    }

    private static void createStopsTable() throws SQLException {
        String createStopsQuery = "DROP TABLE IF EXISTS gtfs.stops_" + startDate + "_" + endDate + " CASCADE;\n"
                + "CREATE TABLE  IF NOT EXISTS gtfs.stops_" + startDate + "_" + endDate + "\n"
                + "(\n"
                + "LIKE gtfs.stops);\n"
                + "\n"
                + "ALTER TABLE gtfs.stops_" + startDate + "_" + endDate + "\n"
                + "  OWNER TO java;\n"
                + "GRANT ALL ON TABLE gtfs.stops_" + startDate + "_" + endDate + " TO radumas;\n"
                + "GRANT SELECT, REFERENCES ON TABLE gtfs.stops_" + startDate + "_" + endDate + " TO mbta_researchers;" //                + "TRUNCATE TABLE gtfs.stops_" + startDate + "_" + endDate + " CASCADE;"
                ;
        PreparedStatement createStops = dbConnection.prepareStatement(createStopsQuery);
        createStops.execute();

    }

    /*private static void addDataToInsert(PreparedStatement statement, int idx, String data, int sqlType){
     case:
        
     }*/
    private static void readStopTimes() throws IOException, SQLException {
        if (dbConnection.isClosed()) {
            openDatabaseConnection();
        }

        try {
            createStopsTimesTable();
        } catch (PSQLException e) {
            System.out.println(e);
        };

        CopyManager manager = new CopyManager((BaseConnection) (dbConnection));

        try {
            String createTempStops = "CREATE TEMP TABLE tempstops \n"
                    + "(\n"
                    + "  trip_id character varying(64) NOT NULL,\n"
                    + "  arrival_time character varying(8) NOT NULL,\n"
                    + "  departure_time character varying(8) NOT NULL,\n"
                    + "  stop_id character varying(64) NOT NULL,\n"
                    + "  stop_sequence smallint NOT NULL,\n"
                    + "  stop_headsign character varying(8),\n"
                    + "  pickup_type smallint,\n"
                    + "  drop_off_type smallint)WITH (\n"
                    + "  OIDS=FALSE\n"
                    + ");";
            PreparedStatement createTemp = dbConnection.prepareStatement(createTempStops);

            createTemp.execute();

            manager.copyIn("COPY tempstops FROM STDIN WITH (FORMAT 'csv', HEADER true)", new FileReader("stop_times.txt"));

            String insertQuery = "INSERT INTO gtfs.stop_times_" + startDate + "_" + endDate + " \n"
                    + "(SELECT trip_id, date '1899-12-31' + arrival_time::interval, date '1899-12-31' + departure_time::interval, stop_id, stop_sequence, \n"
                    + "       stop_headsign, pickup_type, drop_off_type FROM tempstops);";

            PreparedStatement insertStopTimes = dbConnection.prepareStatement(insertQuery);
            insertStopTimes.execute();
        } catch (PSQLException ex) {
            System.out.println(ex);
            System.out.println(ex.getServerErrorMessage().getMessage());
        }

    }

    private static void createStopsTimesTable() throws SQLException {
        String createStopsQuery = "DROP TABLE IF EXISTS gtfs.stop_times_" + startDate + "_" + endDate + " CASCADE;\n"
                + "CREATE TABLE  IF NOT EXISTS gtfs.stop_times_" + startDate + "_" + endDate + "\n"
                + "(\n"
                + "LIKE gtfs.stop_times)\n"
                + "\n"
                + "WITH (\n"
                + "  OIDS=FALSE\n"
                + ");\n"
                + "ALTER TABLE gtfs.stop_times_" + startDate + "_" + endDate + "\n"
                + "  OWNER TO java;\n"
                + "GRANT ALL ON TABLE gtfs.stop_times_" + startDate + "_" + endDate + " TO radumas;\n"
                + "GRANT SELECT, REFERENCES ON TABLE gtfs.stop_times_" + startDate + "_" + endDate + " TO mbta_researchers;" //                + "TRUNCATE TABLE gtfs.stop_times_" + startDate + "_" + endDate + " CASCADE;"
                ;
        PreparedStatement createStops = dbConnection.prepareStatement(createStopsQuery);
        createStops.execute();

    }

    private static void readFrequencies() throws IOException, SQLException {
        if (dbConnection.isClosed()) {
            openDatabaseConnection();
        }

        try {
            createFrequenciesTable();
        } catch (PSQLException e) {
            System.out.println(e);
        };

        CopyManager manager = new CopyManager((BaseConnection) (dbConnection));

        try {
            String createTempFreq = "CREATE TEMP TABLE tempfrequencies \n"
                    + "(\n"
                    + "  trip_id character varying(64) NOT NULL,\n"
                    + "  start_time character varying(8) NOT NULL,\n"
                    + "  end_time character varying(8) NOT NULL,\n"
                    + "  headway_secs smallint NOT NULL) WITH\n"
                    + "(  OIDS=FALSE\n"
                    + ");";
            PreparedStatement createTemp = dbConnection.prepareStatement(createTempFreq);

            createTemp.execute();

            manager.copyIn("COPY tempfrequencies FROM STDIN WITH (FORMAT 'csv', HEADER true)", new FileReader("frequencies.txt"));

            String insertQuery = "INSERT INTO gtfs.frequencies_" + startDate + "_" + endDate + " \n"
                    + "(SELECT trip_id, date '1899-12-31' + start_time::interval, date '1899-12-31' + end_time::interval, headway_secs FROM tempfrequencies);";

            PreparedStatement insertFrequencies = dbConnection.prepareStatement(insertQuery);
            insertFrequencies.execute();
        } catch (PSQLException ex) {
            System.out.println(ex);
            System.out.println(ex.getServerErrorMessage().getMessage());
        }

    }

    private static void createFrequenciesTable() throws SQLException {
        String createStopsQuery = "DROP TABLE IF EXISTS gtfs.frequencies_" + startDate + "_" + endDate + " CASCADE;\n"
                + "CREATE TABLE  IF NOT EXISTS gtfs.frequencies_" + startDate + "_" + endDate + "\n"
                + "(\n"
                + "LIKE gtfs.frequencies)\n"
                + "\n"
                + "WITH (\n"
                + "  OIDS=FALSE\n"
                + ");\n"
                + "ALTER TABLE gtfs.frequencies_" + startDate + "_" + endDate + "\n"
                + "  OWNER TO java;\n"
                + "GRANT ALL ON TABLE gtfs.frequencies_" + startDate + "_" + endDate + " TO radumas;\n"
                + "GRANT SELECT, REFERENCES ON TABLE gtfs.frequencies_" + startDate + "_" + endDate + " TO mbta_researchers;" //                + "TRUNCATE TABLE gtfs.frequencies_" + startDate + "_" + endDate + " CASCADE;"
                ;
        PreparedStatement createStops = dbConnection.prepareStatement(createStopsQuery);
        createStops.execute();

    }

    private static void readShapes() throws IOException, SQLException {
        if (dbConnection.isClosed()) {
            openDatabaseConnection();
        }

        try {
            createShapesTable();
        } catch (PSQLException e) {
            System.out.println(e);
        };

        CopyManager manager = new CopyManager((BaseConnection) (dbConnection));

        try {

            manager.copyIn("COPY gtfs.shapes_" + startDate + "_" + endDate + " FROM STDIN WITH (FORMAT 'csv', HEADER true)", new FileReader("shapes.txt"));

        } catch (PSQLException ex) {
            System.out.println(ex);
            System.out.println(ex.getServerErrorMessage().getMessage());
        }

        createShapesGeom();

    }

    private static void createShapesGeom() throws IOException, SQLException {
        try {
            createShapesGeomTable();
        } catch (PSQLException e) {
            System.out.println(e);
        };

        String insertQuery = "SELECT gtfs.gtfs.insert_shapes_geog(\n'"
                + startDate + "','" + endDate + "'\n"
                + ");";

        PreparedStatement insertShapes = dbConnection.prepareStatement(insertQuery);
        insertShapes.execute();

    }

    private static void createShapesTable() throws SQLException {
        String createStopsQuery = "DROP TABLE IF EXISTS gtfs.shapes_" + startDate + "_" + endDate + " CASCADE;\n"
                + "CREATE TABLE  IF NOT EXISTS gtfs.shapes_" + startDate + "_" + endDate + "\n"
                + "(\n"
                + "LIKE gtfs.shapes)\n"
                + "\n"
                + "WITH (\n"
                + "  OIDS=FALSE\n"
                + ");\n"
                + "ALTER TABLE gtfs.shapes_" + startDate + "_" + endDate + "\n"
                + "  OWNER TO java;\n"
                + "GRANT ALL ON TABLE gtfs.shapes_" + startDate + "_" + endDate + " TO radumas;\n"
                + "GRANT SELECT, REFERENCES ON TABLE gtfs.shapes_" + startDate + "_" + endDate + " TO mbta_researchers;" //                + "TRUNCATE TABLE gtfs.shapes_" + startDate + "_" + endDate + " CASCADE;"
                ;
        PreparedStatement createStops = dbConnection.prepareStatement(createStopsQuery);
        createStops.execute();

    }

    private static void readCalendar() throws IOException, SQLException {
        if (dbConnection.isClosed()) {
            openDatabaseConnection();
        }

        try {
            createCalendarTable();
        } catch (PSQLException e) {
            System.out.println(e);
        };

        BufferedReader reader
                = new BufferedReader(
                        new InputStreamReader(
                                new FileInputStream("calendar.txt")));
        PrintStream textOut
                = new PrintStream(
                        new BufferedOutputStream(
                                new FileOutputStream("calendar_prepared.csv", false)));

//        Skip first row
        textOut.println(reader.readLine());

        String line;

        for (; (line = reader.readLine()) != null;) {
            String[] row = line.split(COMMA_DELIM_DBL_QUOTE_PATTERN, -1);

            String output = "";

//            Convert 0 & 1 to TRUE & FALSE for the table
            for (int i = 0; i < row.length - 1; i++) {
                if (i >= 1 && i <= 7) {
                    row[i] = (row[i].equals("1")) ? "TRUE" : "FALSE";
                }

                output += row[i] + ",";
            }
            output += row[row.length - 1];
            textOut.println(output);

        }
        textOut.close();
        textOut.flush();
        reader.close();

        CopyManager manager = new CopyManager((BaseConnection) (dbConnection));

        try {
            manager.copyIn("COPY gtfs.calendar_" + startDate + "_" + endDate + "(\n"
                    + "\"service_id\",\n"
                    + "\"monday\",\n"
                    + "\"tuesday\",\n"
                    + "\"wednesday\",\n"
                    + "\"thursday\",\n"
                    + "\"friday\",\n"
                    + "\"saturday\",\n"
                    + "\"sunday\",\n"
                    + "\"start_date\",\n"
                    + "\"end_date\") FROM STDIN WITH (FORMAT 'csv', HEADER true)", new FileReader("calendar_prepared.csv"));
        } catch (PSQLException ex) {
            System.out.println(ex);
            System.out.println(ex.getServerErrorMessage().getMessage());
        }
    }

    private static void createCalendarTable() throws SQLException {
        String createStopsQuery = "DROP TABLE IF EXISTS gtfs.calendar_" + startDate + "_" + endDate + " CASCADE;\n"
                + "CREATE TABLE  IF NOT EXISTS gtfs.calendar_" + startDate + "_" + endDate + "\n"
                + "(\n"
                + "LIKE gtfs.calendar)\n"
                + "\n"
                + "WITH (\n"
                + "  OIDS=FALSE\n"
                + ");\n"
                + "ALTER TABLE gtfs.calendar_" + startDate + "_" + endDate + "\n"
                + "  OWNER TO java;\n"
                + "GRANT ALL ON TABLE gtfs.calendar_" + startDate + "_" + endDate + " TO radumas;\n"
                + "GRANT SELECT, REFERENCES ON TABLE gtfs.calendar_" + startDate + "_" + endDate + " TO mbta_researchers;" //                + "TRUNCATE TABLE gtfs.calendar_" + startDate + "_" + endDate + " CASCADE;"
                ;
        PreparedStatement createStops = dbConnection.prepareStatement(createStopsQuery);
        createStops.execute();

    }

    private static void readCalendar_dates() throws IOException, SQLException {
        if (dbConnection.isClosed()) {
            openDatabaseConnection();
        }

        try {
            createCalendar_datesTable();
        } catch (PSQLException ex) {
            System.out.println(ex.getServerErrorMessage().getMessage());
        };

        CopyManager manager = new CopyManager((BaseConnection) (dbConnection));

        try {
            manager.copyIn("COPY gtfs.calendar_dates_" + startDate + "_" + endDate + " FROM STDIN WITH (FORMAT 'csv', HEADER true)", new FileReader("calendar_dates.txt"));
        } catch (PSQLException ex) {
            System.out.println(ex.getServerErrorMessage().getMessage());
        }
    }

    private static void createCalendar_datesTable() throws SQLException {
        String createStopsQuery = "DROP TABLE IF EXISTS gtfs.calendar_dates_" + startDate + "_" + endDate + " CASCADE;\n"
                + "CREATE TABLE  IF NOT EXISTS gtfs.calendar_dates_" + startDate + "_" + endDate + "\n"
                + "(\n"
                + "LIKE gtfs.calendar_dates)\n"
                + "\n"
                + "WITH (\n"
                + "  OIDS=FALSE\n"
                + ");\n"
                + "ALTER TABLE gtfs.calendar_dates_" + startDate + "_" + endDate + "\n"
                + "  OWNER TO java;\n"
                + "GRANT ALL ON TABLE gtfs.calendar_dates_" + startDate + "_" + endDate + " TO radumas;\n"
                + "GRANT SELECT, REFERENCES ON TABLE gtfs.calendar_dates_" + startDate + "_" + endDate + " TO mbta_researchers;" //                + "TRUNCATE TABLE gtfs.calendar_dates_" + startDate + "_" + endDate + " CASCADE;"
                ;
        PreparedStatement createStops = dbConnection.prepareStatement(createStopsQuery);
        createStops.execute();

    }

    private static void readTrips() throws IOException, SQLException {
        if (dbConnection.isClosed()) {
            openDatabaseConnection();
        }

        try {
            createTripsTable();
        } catch (PSQLException e) {
            System.out.println(e);
        };

        CopyManager manager = new CopyManager((BaseConnection) (dbConnection));

        try {
            manager.copyIn("COPY gtfs.trips_" + startDate + "_" + endDate + " FROM STDIN WITH (FORMAT 'csv', HEADER true)", new FileReader("trips.txt"));
        } catch (PSQLException ex) {
            System.out.println(ex);
            System.out.println(ex.getServerErrorMessage().getMessage());
        }
    }

    private static void createTripsTable() throws SQLException {
        String createStopsQuery = "DROP TABLE IF EXISTS gtfs.trips_" + startDate + "_" + endDate + " CASCADE;\n"
                + "CREATE TABLE  IF NOT EXISTS gtfs.trips_" + startDate + "_" + endDate + "\n"
                + "(\n"
                + "LIKE gtfs.trips"
                + ");\n"
                + "ALTER TABLE gtfs.trips_" + startDate + "_" + endDate + "\n"
                + "  OWNER TO java;\n"
                + "GRANT ALL ON TABLE gtfs.trips_" + startDate + "_" + endDate + " TO radumas;\n"
                + "GRANT SELECT, REFERENCES ON TABLE gtfs.trips_" + startDate + "_" + endDate + " TO mbta_researchers;" //                + "TRUNCATE TABLE gtfs.trips_" + startDate + "_" + endDate + " CASCADE;"
                ;
        PreparedStatement createStops = dbConnection.prepareStatement(createStopsQuery);
        createStops.execute();

    }

    private static void readTransfers() throws IOException, SQLException {
        if (dbConnection.isClosed()) {
            openDatabaseConnection();
        }

        try {
            createTransfersTable();
        } catch (PSQLException e) {
            System.out.println(e);
        };

        CopyManager manager = new CopyManager((BaseConnection) (dbConnection));

        try {
            manager.copyIn("COPY gtfs.transfers_" + startDate + "_" + endDate + " FROM STDIN WITH (FORMAT 'csv', HEADER true)", new FileReader("transfers.txt"));
        } catch (PSQLException ex) {
            System.out.println(ex);
            System.out.println(ex.getServerErrorMessage().getMessage());
        }
    }

    private static void createTransfersTable() throws SQLException {
        String createStopsQuery = "DROP TABLE IF EXISTS gtfs.transfers_" + startDate + "_" + endDate + " CASCADE;\n"
                + "CREATE TABLE  IF NOT EXISTS gtfs.transfers_" + startDate + "_" + endDate + "\n"
                + "(\n"
                + "LIKE gtfs.transfers)\n"
                + "\n"
                + "WITH (\n"
                + "  OIDS=FALSE\n"
                + ");\n"
                + "ALTER TABLE gtfs.transfers_" + startDate + "_" + endDate + "\n"
                + "  OWNER TO java;\n"
                + "GRANT ALL ON TABLE gtfs.transfers_" + startDate + "_" + endDate + " TO radumas;\n"
                + "GRANT SELECT, REFERENCES ON TABLE gtfs.transfers_" + startDate + "_" + endDate + " TO mbta_researchers;" //                + "TRUNCATE TABLE gtfs.transfers_" + startDate + "_" + endDate + " CASCADE;"
                ;
        PreparedStatement createStops = dbConnection.prepareStatement(createStopsQuery);
        createStops.execute();

    }

    private static void readRoutes() throws IOException, SQLException {
        if (dbConnection.isClosed()) {
            openDatabaseConnection();
        }

        try {
            createRoutesTable();
        } catch (PSQLException e) {
            System.out.println(e);
        };

        CopyManager manager = new CopyManager((BaseConnection) (dbConnection));

        try {
            manager.copyIn("COPY gtfs.routes_" + startDate + "_" + endDate + "(\n"
                    + "route_id,\n"
                    + "agency_id,\n"
                    + "route_short_name,\n"
                    + "route_long_name,\n"
                    + "route_desc,\n"
                    + "route_type,\n"
                    + "route_url,\n"
                    + "route_color,\n"
                    + "route_text_color\n"
                    + ")  FROM STDIN WITH (FORMAT 'csv', HEADER true)", new FileReader("routes.txt"));
        } catch (PSQLException ex) {
            System.out.println(ex);
            System.out.println(ex.getServerErrorMessage().getMessage());
        }
    }

    private static void createRoutesTable() throws SQLException {
        String createStopsQuery = "DROP TABLE IF EXISTS gtfs.routes_" + startDate + "_" + endDate + " CASCADE;\n"
                + "CREATE TABLE  IF NOT EXISTS gtfs.routes_" + startDate + "_" + endDate + "\n"
                + "(\n"
                + "LIKE gtfs.routes)\n"
                + "\n"
                + "WITH (\n"
                + "  OIDS=FALSE\n"
                + ");\n"
                + "ALTER TABLE gtfs.routes_" + startDate + "_" + endDate + "\n"
                + "  OWNER TO java;\n"
                + "GRANT ALL ON TABLE gtfs.routes_" + startDate + "_" + endDate + " TO radumas;\n"
                + "GRANT SELECT, REFERENCES ON TABLE gtfs.routes_" + startDate + "_" + endDate + " TO mbta_researchers;" //                + "TRUNCATE TABLE gtfs.routes_" + startDate + "_" + endDate + " CASCADE;"
                ;
        PreparedStatement createStops = dbConnection.prepareStatement(createStopsQuery);
        createStops.execute();

    }

    private static void insertStopGeometry() throws SQLException {
        if (dbConnection.isClosed()) {
            openDatabaseConnection();
        }

        try {
            createStopGeometryTable();
        } catch (PSQLException e) {
            System.out.println(e);
        };

        try {
            String insertQuery = "INSERT INTO gtfs.stops_geog_" + startDate + "_" + endDate + "\n"
                    + "    (SELECT stop_id, parent_station, ST_GeogFromText('SRID=4326;POINT(' || stop_lon || ' ' || stop_lat || ')')\n"
                    + ",ST_GeomFromText('POINT(' || stop_lon || ' ' || stop_lat || ')',4326) "
                    + "FROM gtfs.stops_" + startDate + "_" + endDate + " );"
                    + "ALTER TABLE gtfs.stops_geog_" + startDate + "_" + endDate + "\n"
                    + " ADD CONSTRAINT geog_" + startDate + "_" + endDate + "_stop_id PRIMARY KEY (stop_id);\n"
                    + " ALTER TABLE gtfs.stops_geog_" + startDate + "_" + endDate + "\n"
                    + " ADD CONSTRAINT _" + startDate + "_" + endDate + "_stop_id FOREIGN KEY (stop_id)\n"
                    + "      REFERENCES gtfs.stops_" + startDate + "_" + endDate + " (stop_id) MATCH SIMPLE\n"
                    + "      ON UPDATE NO ACTION ON DELETE NO ACTION\n"
                    + "      NOT VALID;\n"
                    + "      CREATE INDEX geom_index_" + startDate + "_" + endDate + "\n"
                    + "  ON gtfs.stops_geog_" + startDate + "_" + endDate + "\n"
                    + "  USING gist\n"
                    + "  (geom);";

            PreparedStatement insert = dbConnection.prepareStatement(insertQuery);
            insert.execute();
        } catch (PSQLException ex) {
            System.out.println(ex);
            System.out.println(ex.getServerErrorMessage().getMessage());
        }

    }

    private static void createStopGeometryTable() throws SQLException {
        String createStopsQuery = "DROP TABLE IF EXISTS gtfs.stops_geog_" + startDate + "_" + endDate + " CASCADE;\n"
                + "CREATE TABLE  IF NOT EXISTS gtfs.stops_geog_" + startDate + "_" + endDate + "\n"
                + "(\n"
                + "LIKE gtfs.stops_geog)\n"
                + "\n"
                + "WITH (\n"
                + "  OIDS=FALSE\n"
                + ");\n"
                + "ALTER TABLE gtfs.stops_geog_" + startDate + "_" + endDate + "\n"
                + "  OWNER TO java;\n"
                + "GRANT ALL ON TABLE gtfs.stops_geog_" + startDate + "_" + endDate + " TO radumas;\n"
                + "GRANT SELECT, REFERENCES ON TABLE gtfs.stops_geog_" + startDate + "_" + endDate + " TO mbta_researchers;" //                + "TRUNCATE TABLE gtfs.stops_geog_" + startDate + "_" + endDate + " CASCADE;"
                ;
        PreparedStatement createStops = dbConnection.prepareStatement(createStopsQuery);
        createStops.execute();

    }

    private static void insertNearestStopStopMatrix() throws SQLException {
        if (dbConnection.isClosed()) {
            openDatabaseConnection();
        }

        try {
            createNearestStopStopMatrixTable();
        } catch (PSQLException e) {
            System.out.println(e);
        };

        String insertQuery = "WITH stops AS(\n"
                + "SELECT DISTINCT stops.stop_id, geog_latlon\n"
                + "FROM gtfs.stops_" + startDate + "_" + endDate + " stops \n"
                + "INNER JOIN gtfs.stop_times_" + startDate + "_" + endDate + " stop_times ON stop_times.stop_id = stops.stop_id\n"
                + "INNER JOIN gtfs.trips_" + startDate + "_" + endDate + " trips ON trips.trip_id = stop_times.trip_id\n"
                + "INNER JOIN gtfs.routes_" + startDate + "_" + endDate + " routes ON routes.route_id = trips.route_id\n"
                + "INNER JOIN gtfs.stops_geog_" + startDate + "_" + endDate + " ON gtfs.stops_geog_" + startDate + "_" + endDate + ".stop_id = stops.stop_id\n"
                + "WHERE route_type = 3\n"
                + ")\n"
                + ", nearest_matrix AS (\n"
                + "\n"
                + "SELECT stops_geog.stop_id AS next_o, \n"
                + "		(SELECT nearest.stop_id FROM stops nearest WHERE nearest.stop_id != main.stop_id\n"
                + "			ORDER BY CAST(nearest.geog_latlon AS geometry) <-> CAST(stops_geog.geog_latlon AS geometry)\n"
                + "		LIMIT 1) AS nearest_stop\n"
                + "FROM stops main\n"
                + "INNER JOIN gtfs.stops_geog_" + startDate + "_" + endDate + " stops_geog ON stops_geog.stop_id = main.stop_id\n"
                + ")\n"
                + "\n"
                + "INSERT INTO gtfs.stop_stop_matrix_" + startDate + "_" + endDate + "\n"
                + "(SELECT next_o, nearest_stop, CAST(ST_DISTANCE(stops.geog_latlon, o.geog_latlon) AS INT)as nearest_distance\n"
                + "FROM nearest_matrix\n"
                + "INNER JOIN stops ON stops.stop_id = nearest_matrix.next_o\n"
                + "INNER JOIN gtfs.stops_geog_" + startDate + "_" + endDate + " o ON o.stop_id = next_o);";

        try {
            PreparedStatement insert = dbConnection.prepareStatement(insertQuery);
            insert.execute();
        } catch (PSQLException ex) {
            System.out.println();
            System.out.println("Error with: insertNearestStopStopMatrix()");
            System.out.println(ex);
            ex.printStackTrace();

        }

    }

    private static void createNearestStopStopMatrixTable() throws SQLException {
        String createStopsQuery = "DROP TABLE IF EXISTS gtfs.stop_stop_matrix_" + startDate + "_" + endDate + " CASCADE;\n"
                + "CREATE TABLE  IF NOT EXISTS gtfs.stop_stop_matrix_" + startDate + "_" + endDate + "\n"
                + "(\n"
                + "LIKE gtfs.stop_stop_matrix)\n"
                + "\n"
                + "WITH (\n"
                + "  OIDS=FALSE\n"
                + ");\n"
                + "ALTER TABLE gtfs.stop_stop_matrix_" + startDate + "_" + endDate + "\n"
                + "  OWNER TO java;\n"
                + "GRANT ALL ON TABLE gtfs.stop_stop_matrix_" + startDate + "_" + endDate + " TO radumas;\n"
                + "GRANT SELECT, REFERENCES ON TABLE gtfs.stop_stop_matrix_" + startDate + "_" + endDate + " TO mbta_researchers;" //                + "TRUNCATE TABLE gtfs.stop_stop_matrix_" + startDate + "_" + endDate + " CASCADE;"
                ;
        PreparedStatement createStops = dbConnection.prepareStatement(createStopsQuery);
        createStops.execute();

    }

    private static void insertBusPatterns() throws SQLException {
        if (dbConnection.isClosed()) {
            openDatabaseConnection();
        }

        try {
            createBus_patterns_Table();
        } catch (PSQLException e) {
            System.out.println(e);
        };

        String insertQuery = "SELECT gtfs.insert_bus_patterns(\n'"
                + startDate + "','" + endDate + "'\n"
                + ");";

        try {
            PreparedStatement insert = dbConnection.prepareStatement(insertQuery);
            insert.execute();
        } catch (PSQLException ex) {
            System.out.println(ex);
            System.out.println(ex.getServerErrorMessage().getMessage());
        }

    }

    private static void createBus_patterns_Table() throws SQLException {
        String createStopsQuery = "DROP TABLE IF EXISTS gtfs.bus_patterns_" + startDate + "_" + endDate + " CASCADE;\n"
                + "CREATE TABLE  IF NOT EXISTS gtfs.bus_patterns_" + startDate + "_" + endDate + "\n"
                + "(\n"
                + "LIKE gtfs.bus_patterns)\n"
                + "\n"
                + "WITH (\n"
                + "  OIDS=FALSE\n"
                + ");\n"
                + "ALTER TABLE gtfs.bus_patterns_" + startDate + "_" + endDate + "\n"
                + "  OWNER TO java;\n"
                + "GRANT ALL ON TABLE gtfs.bus_patterns_" + startDate + "_" + endDate + " TO radumas;\n"
                + "GRANT SELECT, REFERENCES ON TABLE gtfs.bus_patterns_" + startDate + "_" + endDate + " TO mbta_researchers;" //                + "TRUNCATE TABLE gtfs.bus_patterns_" + startDate + "_" + endDate + " CASCADE;"
                ;
        PreparedStatement createStops = dbConnection.prepareStatement(createStopsQuery);
        createStops.execute();

    }

    private static void basicKeys() throws SQLException {
        String constraintsQuery = "SELECT gtfs.add_primary_keys(\n'"
                + startDate + "','" + endDate + "'\n"
                + ");";
        try {
            PreparedStatement constraints = dbConnection.prepareStatement(constraintsQuery);
            constraints.execute();
        } catch (PSQLException ex) {
            System.out.println(ex);
            System.out.println(ex.getServerErrorMessage().getMessage());
        }
    }

    private static void advancedKeys() throws SQLException {
        String constraintsQuery = "SELECT gtfs.add_advanced_primary_keys(\n'"
                + startDate + "','" + endDate + "'\n"
                + ");";
        try {
            PreparedStatement constraints = dbConnection.prepareStatement(constraintsQuery);
            constraints.execute();
        } catch (PSQLException ex) {
            System.out.println(ex);
            System.out.println(ex.getServerErrorMessage().getMessage());
        }
    }

    private static void createShapesGeomTable() throws SQLException {
        String createShapesGeom = "DROP TABLE IF EXISTS gtfs.shapes_geog_" + startDate + "_" + endDate + " CASCADE;\n"
                + "CREATE TABLE  IF NOT EXISTS gtfs.shapes_geog_" + startDate + "_" + endDate + "\n"
                + "(\n"
                + "LIKE gtfs.shapes_geog)\n"
                + "\n"
                + "WITH (\n"
                + "  OIDS=FALSE\n"
                + ");\n"
                + "ALTER TABLE gtfs.shapes_geog_" + startDate + "_" + endDate + "\n"
                + "  OWNER TO java;\n"
                + "GRANT ALL ON TABLE gtfs.shapes_geog_" + startDate + "_" + endDate + " TO radumas;\n"
                + "GRANT SELECT, REFERENCES ON TABLE gtfs.shapes_geog_" + startDate + "_" + endDate + " TO mbta_researchers;" //                + "TRUNCATE TABLE gtfs.shapes_geog_" + startDate + "_" + endDate + " CASCADE;"
                ;
        PreparedStatement createStops = dbConnection.prepareStatement(createShapesGeom);
        createStops.execute();
    }
}
