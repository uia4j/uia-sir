package uia.sir.ds.hana.db;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import uia.dao.where.Where;
import uia.sir.ds.ClientFactory;
import uia.sir.ds.hana.AbstractTest;

public class HanaYesDaoTest extends AbstractTest {

    public HanaYesDaoTest() throws Exception {
    }

    @Test
    public void testSelect() throws Exception {
        try (HanaClient client = (HanaClient) ClientFactory.client("KS")) {
            // fields
            List<String> fields = Arrays.asList("id", "order_id");

            // where
            Where w = Where.simpleAnd()
                    .between("log_time", new Date(System.currentTimeMillis() - 3600000L * 16), new Date());

            // method
            List<Map<String, Object>> data = client.dao("wafer_ct_done_log").select(fields, w);

            // result
            data.forEach(System.out::println);
        }
    }

    @Test
    public void testAggregate() throws Exception {
        try (HanaClient client = (HanaClient) ClientFactory.client("KS")) {
            // fields
            List<String> fields = Arrays.asList(
                    "partition_key",
                    "stage_name");

            // where
            Where w = Where.simpleAnd()
                    .between("log_time", new Date(System.currentTimeMillis() - 3600000L * 24), new Date());

            // accumulator
            HanaAccumulator acc = new HanaAccumulator()
                    .sum("waiting_time", "waiting_time")
                    .sum("cycle_time", "cycle_time");

            // method
            List<Map<String, Object>> data = client.dao("wafer_ct_done_log").aggregate(fields, w, acc);

            // result
            data.forEach(System.out::println);
        }
    }

    @Test
    public void testDaily() throws Exception {
        try (HanaClient client = (HanaClient) ClientFactory.client("KS")) {
            // fields
            List<String> fields = Arrays.asList(
                    "stage_name");

            // where
            Where w = Where.simpleAnd()
                    .between("log_time", new Date(System.currentTimeMillis() - 3600000L * 24), new Date());

            // accumulator
            HanaAccumulator acc = new HanaAccumulator()
                    .countDistinct("lot_id", "lot_id")
                    .countDistinct("wafer_id", "wafer_id");

            // method
            List<Map<String, Object>> data = client.dao("wafer_ct_done_log").daily(fields, "log_time", w, acc);

            // result
            data.forEach(System.out::println);
        }
    }

    @Test
    public void testWeekly() throws Exception {
        try (HanaClient client = (HanaClient) ClientFactory.client("KS")) {
            // fields
            List<String> fields = Arrays.asList(
                    "stage_name");

            // where
            Where w = Where.simpleAnd()
                    .between("log_time", new Date(System.currentTimeMillis() - 86400000L * 60), new Date());

            // accumulator
            HanaAccumulator acc = new HanaAccumulator()
                    .count("output");

            // method
            List<Map<String, Object>> data = client.dao("wafer_ct_done_log").weekly(fields, "log_time", w, acc);

            // result
            data.forEach(System.out::println);
        }
    }

    @Test
    public void testMonthly() throws Exception {
        try (HanaClient client = (HanaClient) ClientFactory.client("KS")) {
            // fields
            List<String> fields = Arrays.asList(
                    "stage_name");

            // where
            Where w = Where.simpleAnd()
                    .between("log_time", new Date(System.currentTimeMillis() - 86400000L * 60), new Date());

            // accumulator
            HanaAccumulator acc = new HanaAccumulator()
                    .count("output");

            // method
            List<Map<String, Object>> data = client.dao("wafer_ct_done_log").monthly(fields, "log_time", w, acc);

            // result
            data.forEach(System.out::println);
        }
    }

    @Test
    public void testQuarter() throws Exception {
        try (HanaClient client = (HanaClient) ClientFactory.client("KS")) {
            // fields
            List<String> fields = Arrays.asList(
                    "stage_name");

            // where
            Where w = Where.simpleAnd()
                    .between("log_time", new Date(System.currentTimeMillis() - 86400000L * 60), new Date());

            // accumulator
            HanaAccumulator acc = new HanaAccumulator()
                    .count("output");

            // method
            List<Map<String, Object>> data = client.dao("wafer_ct_done_log").quarter(fields, "log_time", w, acc);

            // result
            data.forEach(System.out::println);
        }
    }
}
