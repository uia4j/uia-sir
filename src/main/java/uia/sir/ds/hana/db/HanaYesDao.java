package uia.sir.ds.hana.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

import uia.dao.where.Where;
import uia.sir.simple.ValueReader;

public class HanaYesDao {

    private Connection hana;

    private String tableName;

    private boolean saveUTC;

    public HanaYesDao(Connection hana, String tableName, boolean saveUTC) {
        this.hana = hana;
        this.tableName = tableName;
        this.saveUTC = saveUTC;
    }

    public boolean isSaveUTC() {
        return this.saveUTC;
    }

    public void setSaveUTC(boolean saveUTC) {
        this.saveUTC = saveUTC;
    }

    public List<Map<String, Object>> select(List<String> fields, Where where, Map<String, ValueReader> readers, String... orders) throws SQLException {
        String sql = String.format("SELECT %s FROM %s", String.join(",", fields), this.tableName);
        if (where.hasConditions()) {
            sql += " WHERE " + where.generate();
        }
        return run(sql, where, fields, readers);
    }

    public List<Map<String, Object>> aggregate(List<String> fields, Where where, Map<String, ValueReader> readers, HanaAccumulator acc) throws SQLException {
        String fs = String.join(",", fields);
        String sql = String.format("SELECT %s,%s FROM %s",
                fs,
                acc.build(),
                this.tableName);
        if (where.hasConditions()) {
            sql += " WHERE " + where.generate();
        }
        sql += " GROUP BY " + fs;

        List<String> cols = new ArrayList<>(fields);
        cols.addAll(acc.getNewNames());
        return run(sql, where, cols, readers);
    }

    public List<Map<String, Object>> daily(List<String> fields, String timeField, Where where, Map<String, ValueReader> readers, HanaAccumulator acc) throws SQLException {
        fields.remove(timeField);
        String fs = String.join(",", fields);
        String sql1 = acc.getRef().isEmpty()
                ? String.format("SELECT TO_VARCHAR(%s,'YYYY-MM-DD') AS %s,%s FROM %s",
                        timeField,
                        timeField,
                        fs,
                        this.tableName)
                : String.format("SELECT TO_VARCHAR(%s,'YYYY-MM-DD') AS %s,%s,%s FROM %s",
                        timeField,
                        timeField,
                        fs,
                        String.join(",", acc.getRef()),
                        this.tableName);
        if (where.hasConditions()) {
            sql1 += " WHERE " + where.generate();
        }

        String sql = String.format("SELECT %s,%s,%s FROM (%s) GROUP BY %s,%s",
                timeField,
                fs,
                acc.build(),
                sql1,
                timeField,
                fs);

        List<String> cols = new ArrayList<>(fields);
        cols.addAll(acc.getNewNames());
        cols.add(timeField);
        return run(sql, where, cols, readers);
    }

    public List<Map<String, Object>> weekly(List<String> fields, String timeField, Where where, Map<String, ValueReader> readers, HanaAccumulator acc) throws SQLException {
        fields.remove(timeField);
        String fs = String.join(",", fields);
        String sql1 = acc.getRef().isEmpty()
                ? String.format("SELECT TO_VARCHAR(%s,'YYYY-WW') AS %s,%s FROM %s",
                        timeField,
                        timeField,
                        fs,
                        this.tableName)
                : String.format("SELECT TO_VARCHAR(%s,'YYYY-WW') AS %s,%s,%s FROM %s",
                        timeField,
                        timeField,
                        fs,
                        String.join(",", acc.getRef()),
                        this.tableName);
        if (where.hasConditions()) {
            sql1 += " WHERE " + where.generate();
        }

        String sql = String.format("SELECT %s,%s,%s FROM (%s) GROUP BY %s,%s",
                timeField,
                fs,
                acc.build(),
                sql1,
                timeField,
                fs);

        List<String> cols = new ArrayList<>(fields);
        cols.addAll(acc.getNewNames());
        cols.add(timeField);
        return run(sql, where, cols, readers);
    }

    public List<Map<String, Object>> monthly(List<String> fields, String timeField, Where where, Map<String, ValueReader> readers, HanaAccumulator acc) throws SQLException {
        fields.remove(timeField);
        String fs = String.join(",", fields);
        String sql1 = acc.getRef().isEmpty()
                ? String.format("SELECT TO_VARCHAR(%s,'YYYY-Q') AS %s,%s FROM %s",
                        timeField,
                        timeField,
                        fs,
                        this.tableName)
                : String.format("SELECT TO_VARCHAR(%s,'YYYY-Q') AS %s,%s,%s FROM %s",
                        timeField,
                        timeField,
                        fs,
                        String.join(",", acc.getRef()),
                        this.tableName);
        if (where.hasConditions()) {
            sql1 += " WHERE " + where.generate();
        }

        String sql = String.format("SELECT %s,%s,%s FROM (%s) GROUP BY %s,%s",
                timeField,
                fs,
                acc.build(),
                sql1,
                timeField,
                fs);

        List<String> cols = new ArrayList<>(fields);
        cols.addAll(acc.getNewNames());
        cols.add(timeField);
        return run(sql, where, cols, readers);
    }

    public List<Map<String, Object>> quarter(List<String> fields, String timeField, Where where, Map<String, ValueReader> readers, HanaAccumulator acc) throws SQLException {
        fields.remove(timeField);
        String fs = String.join(",", fields);
        String sql1 = acc.getRef().isEmpty()
                ? String.format("SELECT TO_VARCHAR(%s,'YYYY-MM') AS %s,%s FROM %s",
                        timeField,
                        timeField,
                        fs,
                        this.tableName)
                : String.format("SELECT TO_VARCHAR(%s,'YYYY-MM') AS %s,%s,%s FROM %s",
                        timeField,
                        timeField,
                        fs,
                        String.join(",", acc.getRef()),
                        this.tableName);
        if (where.hasConditions()) {
            sql1 += " WHERE " + where.generate();
        }

        String sql = String.format("SELECT %s,%s,%s FROM (%s) GROUP BY %s,%s",
                timeField,
                fs,
                acc.build(),
                sql1,
                timeField,
                fs);

        List<String> cols = new ArrayList<>(fields);
        cols.addAll(acc.getNewNames());
        cols.add(timeField);
        return run(sql, where, cols, readers);
    }

    private List<Map<String, Object>> run(String sql, Where where, List<String> fs, Map<String, ValueReader> readers) throws SQLException {
        List<Map<String, Object>> result = new ArrayList<>();
        try (PreparedStatement ps = this.hana.prepareStatement(sql)) {
            where.accept(ps, 1);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Map<String, Object> row = new TreeMap<>();
                    for (String f : fs) {
                        Object v = rs.getObject(f);
                        if (this.saveUTC && v instanceof Date) {
                            v = new Date(((Date) v).getTime() + TimeZone.getDefault().getRawOffset());
                        }
                        row.put(f, readers.getOrDefault(f, ValueReader.simple).apply(v));
                    }
                    result.add(row);
                }
            }
        }
        return result;
    }
}
