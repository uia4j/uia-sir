package uia.sir;

import org.junit.Test;

import uia.dao.Database;
import uia.dao.hana.Hana;
import uia.sir.db.DataSource;
import uia.sir.db.DataSource.Host;
import uia.sir.db.Dataset;
import uia.sir.db.dao.DataSourceDao;
import uia.sir.db.dao.DatasetDao;
import uia.sir.db.dao.SIR;
import uia.sir.db.dao.SIRClient;

public class MetadataBuilderTest extends AbstractTest {

    public MetadataBuilderTest() throws Exception {
    }

    @Test
    public void testListDataSource() throws Exception {
        try (SIRClient client = SIR.create()) {
            DataSourceDao dsDao = client.dataSource();
            dsDao.selectAll().forEach(d -> {
                System.out.printf("%-10s, %-10s, %s\n", d.getName(), d.getType(), d.getAccumulators());
            });
        }

    }

    @Test
    public void testCreateDataSource() throws Exception {
        MetadataBuilder builder = new MetadataBuilder();

        DataSource ks = builder.hana("KS.ROAD");
        ks.getHosts().add(new Host("10.160.2.38", 30015));
        ks.setUsr("ROAD");
        ks.setPwd("Road12345");
        builder.save(ks);

        DataSource js = builder.hana("JS.ROAD");
        js.getHosts().add(new Host("10.170.110.50", 30015));
        js.setUsr("ROAD");
        js.setPwd("Road12345");
        builder.save(js);

        DataSource trek1 = builder.mongo("KS.TREK");
        trek1.getHosts().add(new Host("10.160.240.174", 27017));
        trek1.getHosts().add(new Host("10.160.240.175", 27017));
        trek1.getHosts().add(new Host("10.160.240.176", 27017));
        trek1.setUsr("trekro");
        trek1.setPwd("trek2024ro");
        builder.save(trek1);

        DataSource trek2 = builder.mongo("JS.TREK");
        trek2.getHosts().add(new Host("10.170.110.59", 27017));
        trek2.setUsr("trek");
        trek2.setPwd("trek12345");
        builder.save(trek2);
    }

    @Test
    public void testListDataset() throws Exception {
        try (SIRClient client = SIR.create()) {
            DatasetDao dsDao = client.dataset();
            dsDao.selectAll().forEach(d -> {
                System.out.printf("%s, %s\n", d.getTableName(), d.getDataSources());
                d.getColumns().forEach(c -> System.out.printf(" %-2s %-20s, %s\n",
                        c.isPk() ? "PK" : "",
                        c.getName(),
                        c.getDataType()));
            });
        }
    }

    @Test
    public void testCreateDataset() throws Exception {
        try (Database db = new Hana("10.160.2.38", "30015", "ROAD", "ROAD", "Road12345")) {
            MetadataBuilder builder = new MetadataBuilder();

            Dataset dataset1 = builder.dataset(db, "lot_ct_done_log");
            dataset1.getDataSources().add("KS.ROAD");
            dataset1.getDataSources().add("JS.ROAD");
            builder.save(dataset1);

            Dataset dataset2 = builder.dataset(db, "wafer_ct_done_log");
            dataset2.getDataSources().add("KS.ROAD");
            dataset2.getDataSources().add("JS.ROAD");
            builder.save(dataset2);

            Dataset dataset3 = builder.dataset(db, "lot_stage_done_log");
            dataset3.getDataSources().add("KS.ROAD");
            dataset3.getDataSources().add("JS.ROAD");
            builder.save(dataset3);

            Dataset dataset4 = builder.dataset(db, "oee_equip_state");
            dataset4.getDataSources().add("KS.ROAD");
            dataset4.getDataSources().add("JS.ROAD");
            builder.save(dataset4);
        }
    }
}
