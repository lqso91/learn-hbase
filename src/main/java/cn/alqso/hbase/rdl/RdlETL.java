package cn.alqso.hbase.rdl;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import cn.alqso.hbase.util.HBaseClient;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class RdlETL {
    protected Logger logger = LoggerFactory.getLogger(RdlETL.class);

    public static String url = "jdbc:oracle:thin:@192.168.10.91:1521/dbgx";
    public static String user = "prod_yx";
    public static String password = "prod_yx";

    public static void main(String[] args) throws IOException {
        RdlETL rdlETL = new RdlETL();

        DateTime start = DateTime.parse("2019-05-01");
        DateTime end = DateTime.parse("2019-08-01");

        for (int d = 1; d <= 15 ; d++) {
            System.out.println("--------> " + d);
            Map<String, String> map = rdlETL.loadCache(d);

            for (int i = 0; start.plusDays(i).compareTo(end) <= 0; i++) {
                rdlETL.imp(d, start.plusDays(i), map);
            }

            map.clear();
        }
    }

    private Map<String, String> loadCache(int d) throws IOException {
        Map<String, String> map = Maps.newHashMap();
        HBaseClient hBaseClient = new HBaseClient();

        String tableName = "fact_rdl_bmz";
        Table table = hBaseClient.getTable(tableName);

        Scan scan = new Scan();
        // scan.setLimit(100);
        scan.addColumn("info".getBytes(), "cldbs".getBytes());
        scan.withStartRow(String.format("04%02d", d).getBytes());
        scan.withStopRow(String.format("04%02d", d + 1).getBytes());

        ResultScanner scanner = table.getScanner(scan);
        scanner.forEach(i -> {
            byte[] cldbs = i.getValue("info".getBytes(), "cldbs".getBytes());
            byte[] row = i.getRow();
            map.put(Bytes.toString(cldbs), Bytes.toString(row));
        });

        scanner.close();
        table.close();
        hBaseClient.close();

        logger.info("cache size : {}", map.size());
        return map;
    }

    private void imp(int d, DateTime dateTime, Map<String, String> map) throws IOException {
        HBaseClient hBaseClient = new HBaseClient();
        String tableName = "fact_rdl_bmz";
        String colFamily = "bmz";

        String sql = "SELECT DNBZCH CLDBS, ZXYGZ FROM CJ_RDJDNL_MAC " +
                String.format(" WHERE SJSJ = TO_DATE('%s', 'yyyy-MM-dd') ", dateTime.toString("yyyy-MM-dd"))
                + String.format(" AND SUBSTR(GDDWBM, 1, 4) = '04%02d'", d);

        logger.info("\n" + sql);

        JdbcTemplate template = new JdbcTemplate(getDataSource());

        template.query(con -> {
            PreparedStatement preparedStatement = con.prepareStatement(sql,
                            ResultSet.TYPE_FORWARD_ONLY,
                            ResultSet.CONCUR_READ_ONLY);
            preparedStatement.setFetchSize(1000);
            preparedStatement.setFetchDirection(ResultSet.FETCH_FORWARD);
            return preparedStatement;
        }, rs -> {
            int rowCount = 0;
            int batchSize = 200;

            List<HBaseClient.RowData> rows = Lists.newArrayListWithExpectedSize(batchSize);
            Set<String> rk = Sets.newHashSetWithExpectedSize(batchSize);

            while (rs.next()) {
                try {
                    String cldbs = rs.getString("CLDBS");
                    String rowKey = map.get(cldbs);
                    if(rowKey == null || rowKey.length() == 0 || rk.contains(rowKey)){
                        continue;
                    }

                    rowCount++;

                    BigDecimal zxygz = rs.getBigDecimal("ZXYGZ");

                    List<Pair<String, String>> pairs = Lists.newArrayList();
                    pairs.add(Pair.newPair(dateTime.toString("yyyyMMdd"), zxygz == null ? "" : zxygz.toString()));
                    pairs.add(Pair.newPair("DATA", "1"));

                    HBaseClient.RowData rowData = new HBaseClient.RowData(rowKey, colFamily, pairs);
                    rows.add(rowData);
                    rk.add(rowKey);

                    if(rows.size() == batchSize){
                        hBaseClient.putData(tableName, rows);
                        rows.clear();
                    }

                    if(rowCount % 30000 == 0){
                        logger.info("{}, rowCount:{}", dateTime.toString("yyyyMMdd"), rowCount);
                        Thread.sleep(1000);
                    }

                    if(rowCount % 300000 == 0){
                        Thread.sleep(10000);
                    }
                } catch (Exception e) {
                    logger.error("", e);
                }
            }

            if(rows.size() > 0){
                try {
                    hBaseClient.putData(tableName, rows);
                    logger.info("{}, rowCount:{}", dateTime.toString("yyyyMMdd"), rowCount);
                } catch (IOException e) {
                    logger.error("", e);
                }
                rows.clear();
            }

            rk.clear();
        });

        hBaseClient.close();
    }

    private DataSource getDataSource(){
        Properties properties = new Properties();
        properties.put(DruidDataSourceFactory.PROP_URL, url);
        properties.put(DruidDataSourceFactory.PROP_USERNAME, user);
        properties.put(DruidDataSourceFactory.PROP_PASSWORD, password);

        DataSource dataSource = null;
        try {
            dataSource = DruidDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return dataSource;
    }
}