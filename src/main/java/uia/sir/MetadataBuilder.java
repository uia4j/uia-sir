package uia.sir;

import java.sql.SQLException;

import uia.dao.ColumnType;
import uia.dao.Database;
import uia.dao.TableType;
import uia.sir.db.DataSource;
import uia.sir.db.Dataset;
import uia.sir.db.dao.DataSourceDao;
import uia.sir.db.dao.DatasetDao;
import uia.sir.db.dao.SIR;
import uia.sir.db.dao.SIRClient;
import uia.sir.simple.Accumulator;

public class MetadataBuilder {

    public DataSource hana(String serviceName) {
        DataSource ds = new DataSource();
        ds.setName(serviceName);
        ds.setType("HANA");
        ds.getAccumulators().add(Accumulator.count);
        ds.getAccumulators().add(Accumulator.countDistinct);
        ds.getAccumulators().add(Accumulator.sum);
        return ds;
    }

    public DataSource mongo(String serviceName) {
        DataSource ds = new DataSource();
        ds.setName(serviceName);
        ds.setType("MGODB");
        ds.getAccumulators().add(Accumulator.count);
        ds.getAccumulators().add(Accumulator.sum);
        return ds;
    }

    public Dataset dataset(Database db, String tableName) throws SQLException {
        TableType tt = db.selectTable(tableName, false);

        Dataset one = new Dataset();
        one.setName(tt.getTableName());
        one.setTableName(tt.getTableName());
        one.setDescription(tt.getRemark());

        for (ColumnType ct : tt.getColumns()) {
            Dataset.ColumnInfo ci = new Dataset.ColumnInfo();
            ci.setName(ct.getColumnName());
            ci.setDisplayName(ct.getColumnName());
            ci.setEnabled(true);
            ci.setAlignment("L");
            ci.setPk(ct.isPk());
            if (ct.isNumericType()) {
                ci.setDataType("N");
                ci.setAlignment("R");
            }
            else if (ct.isStringType()) {
                ci.setDataType("S");
            }
            else if (ct.isDateTimeType()) {
                ci.setDataType("T");
            }
            else {
                ci.setDataType("O");
            }
            one.getColumns().add(ci);
        }

        return one;
    }

    public void save(DataSource dataSource) throws Exception {
        try (SIRClient client = SIR.create()) {
            DataSourceDao dsDao = client.dataSource();
            dsDao.delete(dataSource.getName());
            dsDao.insert(dataSource);
        }
    }

    public void save(Dataset dataset) throws Exception {
        try (SIRClient client = SIR.create()) {
            DatasetDao dsDao = client.dataset();
            dsDao.delete(dataset.getName());
            dsDao.insert(dataset);
        }
    }
}
