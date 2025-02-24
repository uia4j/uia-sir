package uia.sir.ds.mgo.db;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.junit.Test;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;

import uia.sir.ds.ClientFactory;
import uia.sir.ds.mgo.AbstractTest;

public class MgoYesDaoTest extends AbstractTest {

    public MgoYesDaoTest() throws Exception {
    }

    @Test
    public void testSelect() throws Exception {
        try (MgoClient client = (MgoClient) ClientFactory.client("TREK")) {
            MgoYesDao dao = client.dao("report_info");

            // fields
            List<String> fs = Arrays.asList("report_id");
            String order = "report_id";

            // filter
            MgoWhere w = MgoWhere.and()
                    .eq("fab_lot_name", "SFCBO:1020,BF2AM008.1");

            // accumulator

            // method
            FindIterable<Document> docs = dao.select(fs, w, order);

            // result
            docs.forEach(d -> {
                System.out.println(d.get("report_id") + ", " + d.get("fab_lot_name"));
            });
        }
    }

    @Test
    public void testDistinct() throws Exception {
        try (MgoClient client = (MgoClient) ClientFactory.client("TREK")) {
            MgoYesDao dao = client.dao("report_info");

            // fields
            String f = "fab_lot_name";

            // filter
            MgoWhere w = MgoWhere.and()
                    .gte("upload_time", new Date(System.currentTimeMillis() - 86400000))
                    .lt("upload_time", new Date(System.currentTimeMillis()));

            // accumulator

            // method
            DistinctIterable<String> docs = dao.distinctStr(f, w);

            // result
            docs.forEach(d -> {
                System.out.println(new String(d.getBytes()));
            });
        }
    }

    @Test
    public void testAggregation() throws Exception {
        try (MgoClient client = (MgoClient) ClientFactory.client("TREK")) {
            MgoYesDao dao = client.dao("report_info");

            // fields
            List<String> fields = Arrays
                    .asList("fab_op_name", "sub_lot_id");

            // filter
            MgoWhere w = MgoWhere.and()
                    .eq("fab_lot_name", "SFCBO:1020,WF1NM027.1");

            // accumulator
            MgoAccumulator acc = new MgoAccumulator()
                    .count("count")
                    .sum("qty", "num_good");

            // method
            AggregateIterable<Document> docs = dao.aggreagte(fields, w, acc);

            // result
            docs.forEach(d -> {
                System.out.println(MgoYesDao.toRow(d, "count", "qty"));
            });
        }
    }

    @Test
    public void testDaily() throws Exception {
        try (MgoClient client = (MgoClient) ClientFactory.client("TREK")) {
            MgoYesDao dao = client.dao("report_info");

            // fields
            List<String> fields = Arrays
                    .asList("fab_op_name");

            // filter
            MgoWhere w = MgoWhere.and()
                    .eq("fab_lot_name", "SFCBO:1020,WF2BM011.2");

            // accumulator
            MgoAccumulator acc = new MgoAccumulator()
                    .count("output");

            // method
            AggregateIterable<Document> docs = dao.daily(fields, "upload_time", w, acc);

            // result
            docs.forEach(d -> {
                System.out.println(MgoYesDao.toRow(d, "output"));
            });
        }
    }

    @Test
    public void testWeekly() throws Exception {
        try (MgoClient client = (MgoClient) ClientFactory.client("TREK")) {
            MgoYesDao dao = client.dao("report_info");

            // fields
            List<String> fields = Arrays
                    .asList("fab_op_name");

            // filter
            MgoWhere w = MgoWhere.and()
                    .eq("fab_lot_name", "SFCBO:1020,WF2BM011.2");

            // accumulator
            MgoAccumulator acc = new MgoAccumulator()
                    .count("output");

            // method
            AggregateIterable<Document> docs = dao.weekly(fields, "upload_time", w, acc);

            // result
            docs.forEach(d -> {
                System.out.println(MgoYesDao.toRow(d, "output"));
            });
        }
    }

    @Test
    public void testMonthly() throws Exception {
        try (MgoClient client = (MgoClient) ClientFactory.client("TREK")) {
            MgoYesDao dao = client.dao("report_info");

            // fields
            List<String> fields = Arrays
                    .asList("fab_op_name");

            // filter
            MgoWhere w = MgoWhere.and()
                    .between("upload_time", new Date(System.currentTimeMillis() - 86400000L * 90), new Date(System.currentTimeMillis()));

            // accumulator
            MgoAccumulator acc = new MgoAccumulator()
                    .count("output");

            // method
            AggregateIterable<Document> docs = dao.monthly(fields, "upload_time", w, acc);

            // result
            docs.forEach(d -> {
                System.out.println(MgoYesDao.toRow(d, "output"));
            });
        }
    }

    @Test
    public void testQuarter() throws Exception {
        try (MgoClient client = (MgoClient) ClientFactory.client("TREK")) {
            MgoYesDao dao = client.dao("report_info");

            // fields
            List<String> fields = Arrays
                    .asList("fab_op_name");

            // filter
            MgoWhere w = MgoWhere.and()
                    .eq("fab_lot_name", "SFCBO:1020,WF2BM011.2");

            // accumulator
            MgoAccumulator acc = new MgoAccumulator()
                    .count("output");

            // method
            AggregateIterable<Document> docs = dao.quarter(fields, "upload_time", w, acc);

            // result
            docs.forEach(d -> {
                System.out.println(MgoYesDao.toRow(d, "output"));
            });
        }
    }
}
