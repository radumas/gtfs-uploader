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
            readStopTimes();
        } catch (IOException ex) {
            Logger.getLogger(GTFSuploader.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(GTFSuploader.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            readCalendar();
        } catch (IOException ex) {
            Logger.getLogger(GTFSuploader.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(GTFSuploader.class.getName()).log(Level.SEVERE, null, ex);
        }

        try {
            readCalendar_dates();
        } catch (IOException ex) {
            Logger.getLogger(GTFSuploader.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(GTFSuploader.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            readRoutes();
        } catch (IOException ex) {
            Logger.getLogger(GTFSuploader.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(GTFSuploader.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
        
        try {
            readTrips();
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
        String createStopsQuery = "CREATE TABLE gtfs.stops_" + startDate + "_" + endDate + "\n"
                + "(\n"
                + ")\n"
                + "INHERITS (gtfs.stops)\n"
                + "WITH (\n"
                + "  OIDS=FALSE\n"
                + ");\n"
                + "ALTER TABLE gtfs.stops_" + startDate + "_" + endDate + "\n"
                + "  OWNER TO java;\n"
                + "GRANT ALL ON TABLE gtfs.stops_" + startDate + "_" + endDate + " TO radumas;\n"
                + "GRANT SELECT, REFERENCES ON TABLE gtfs.stops_" + startDate + "_" + endDate + " TO mbta_researchers;";
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
        String createStopsQuery = "CREATE TABLE gtfs.stop_times_" + startDate + "_" + endDate + "\n"
                + "(\n"
                + ")\n"
                + "INHERITS (gtfs.stop_times)\n"
                + "WITH (\n"
                + "  OIDS=FALSE\n"
                + ");\n"
                + "ALTER TABLE gtfs.stop_times_" + startDate + "_" + endDate + "\n"
                + "  OWNER TO java;\n"
                + "GRANT ALL ON TABLE gtfs.stop_times_" + startDate + "_" + endDate + " TO radumas;\n"
                + "GRANT SELECT, REFERENCES ON TABLE gtfs.stop_times_" + startDate + "_" + endDate + " TO mbta_researchers;";
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

        int j = 1;

        for (; (line = reader.readLine()) != null; j++) {
            String[] row = line.split(COMMA_DELIM_DBL_QUOTE_PATTERN, -1);

            String output = "";

//            Convert 0 & 1 to TRUE & FALSE for the table
            for (int i = 0; i < row.length - 1; i++) {
                if (i >= 1 && 1 <= 7) {
                    row[i] = (row[i].equals(1)) ? "TRUE" : "FALSE";
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
        String createStopsQuery = "CREATE TABLE gtfs.calendar_" + startDate + "_" + endDate + "\n"
                + "(\n"
                + ")\n"
                + "INHERITS (gtfs.calendar)\n"
                + "WITH (\n"
                + "  OIDS=FALSE\n"
                + ");\n"
                + "ALTER TABLE gtfs.calendar_" + startDate + "_" + endDate + "\n"
                + "  OWNER TO java;\n"
                + "GRANT ALL ON TABLE gtfs.calendar_" + startDate + "_" + endDate + " TO radumas;\n"
                + "GRANT SELECT, REFERENCES ON TABLE gtfs.calendar_" + startDate + "_" + endDate + " TO mbta_researchers;";
        PreparedStatement createStops = dbConnection.prepareStatement(createStopsQuery);
        createStops.execute();

    }

    private static void readCalendar_dates() throws IOException, SQLException {
        if (dbConnection.isClosed()) {
            openDatabaseConnection();
        }

        try {
            createCalendar_datesTable();
        } catch (PSQLException e) {
            System.out.println(e);
        };

        CopyManager manager = new CopyManager((BaseConnection) (dbConnection));

        try {
            manager.copyIn("COPY gtfs.calendar_dates_" + startDate + "_" + endDate + " FROM STDIN WITH (FORMAT 'csv', HEADER true)", new FileReader("calendar_dates.txt"));
        } catch (PSQLException ex) {
            System.out.println(ex);
            System.out.println(ex.getServerErrorMessage().getMessage());
        }
    }

    private static void createCalendar_datesTable() throws SQLException {
        String createStopsQuery = "CREATE TABLE gtfs.calendar_dates_" + startDate + "_" + endDate + "\n"
                + "(\n"
                + ")\n"
                + "INHERITS (gtfs.calendar_dates)\n"
                + "WITH (\n"
                + "  OIDS=FALSE\n"
                + ");\n"
                + "ALTER TABLE gtfs.calendar_dates_" + startDate + "_" + endDate + "\n"
                + "  OWNER TO java;\n"
                + "GRANT ALL ON TABLE gtfs.calendar_dates_" + startDate + "_" + endDate + " TO radumas;\n"
                + "GRANT SELECT, REFERENCES ON TABLE gtfs.calendar_dates_" + startDate + "_" + endDate + " TO mbta_researchers;";
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
        String createStopsQuery = "CREATE TABLE gtfs.trips_" + startDate + "_" + endDate + "\n"
                + "(\n"
                + ")\n"
                + "INHERITS (gtfs.trips)\n"
                + "WITH (\n"
                + "  OIDS=FALSE\n"
                + ");\n"
                + "ALTER TABLE gtfs.trips_" + startDate + "_" + endDate + "\n"
                + "  OWNER TO java;\n"
                + "GRANT ALL ON TABLE gtfs.trips_" + startDate + "_" + endDate + " TO radumas;\n"
                + "GRANT SELECT, REFERENCES ON TABLE gtfs.trips_" + startDate + "_" + endDate + " TO mbta_researchers;";
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
        String createStopsQuery = "CREATE TABLE gtfs.routes_" + startDate + "_" + endDate + "\n"
                + "(\n"
                + ")\n"
                + "INHERITS (gtfs.routes)\n"
                + "WITH (\n"
                + "  OIDS=FALSE\n"
                + ");\n"
                + "ALTER TABLE gtfs.routes_" + startDate + "_" + endDate + "\n"
                + "  OWNER TO java;\n"
                + "GRANT ALL ON TABLE gtfs.routes_" + startDate + "_" + endDate + " TO radumas;\n"
                + "GRANT SELECT, REFERENCES ON TABLE gtfs.routes_" + startDate + "_" + endDate + " TO mbta_researchers;";
        PreparedStatement createStops = dbConnection.prepareStatement(createStopsQuery);
        createStops.execute();

    }
}
