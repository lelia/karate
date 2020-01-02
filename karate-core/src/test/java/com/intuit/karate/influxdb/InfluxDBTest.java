package com.intuit.karate.influxdb;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.InfluxDBIOException;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Series;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/**
 *
 * @author lelia
 */
public class InfluxDBTest {

    private InfluxDB influxDB;
    private String dbName = "karate";
    private String ptName = "features";
    private String ansiCyan = "\u001B[36m";
    private String ansiReset = "\u001B[0m";

    private static final int INFLUX_PORT = 8086;
    private static final String INFLUX_HOST = "localhost";
    private static final String INFLUX_USER = "root";
    private static final String INFLUX_PASS = "root";

    private static final String INFLUX_SERVER = "http://" + INFLUX_HOST + ":" + INFLUX_PORT;

    /**
     * Connect to InfluxDB and create a new database
     */
    @Before
    public void setupInflux() {
        try {
            this.influxDB = InfluxDBFactory.connect(INFLUX_SERVER, INFLUX_USER, INFLUX_PASS);
            this.influxDB.query(new Query("CREATE DATABASE " + this.dbName));
        } catch (InfluxDBIOException ioe) {
            System.out.println("\n------------------------------------------------------------------------");
            System.out.println(ansiCyan + "  INFLUX CONNECTION ERROR  " + ansiReset);
            System.out.println("------------------------------------------------------------------------");
            System.out.println("  Failed to connect to local InfluxDB server at " + ansiCyan + INFLUX_SERVER + ansiReset);
            System.out.println("  Please ensure you have Influx running on port " + ansiCyan + INFLUX_PORT + ansiReset + " and try again!");
            System.out.println("------------------------------------------------------------------------\n");
            this.influxDB = null;
        }
    }

    /**
     * Test writing a point and querying the measurement on InfluxDB
     */
    @Test
    public void testInfluxMeasurement() {
        assertNotNull(this.influxDB);
        this.influxDB.setDatabase(this.dbName);

        // Write a sample measurement point to InfluxDB using fake test data
        this.influxDB.write(Point.measurement(this.ptName)
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .tag("feature", "tests/karate/demos/tags.feature")
                .tag("protocol", "graphql")
                .tag("env", "dev")
                .tag("api", "v3")
                .addField("passed", 43)
                .addField("failed", 5)
                .addField("skipped", 12)
                .addField("total", 60)
                .addField("duration", 619)
                .addField("status", "failed")
                .build());

        QueryResult query = this.influxDB.query(new Query("SELECT * FROM " + this.ptName, this.dbName));
        Series series = query.getResults().get(0).getSeries().get(0);

        String name = series.getName();
        assertEquals(this.ptName, name);

        List<String> columns = series.getColumns();
        assertThat(columns, hasItems("passed", "failed", "skipped"));
    }

    /**
     * Delete the test database and disconnect from InfluxDB
     */
    @After
    public void teardownInflux() {
        if (this.influxDB != null) {
            this.influxDB.query(new Query("DROP DATABASE " + this.dbName));
            this.influxDB.close();
        }
    }
}
