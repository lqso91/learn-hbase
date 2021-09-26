package cn.alqso.hbase.rdl;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.google.common.collect.Lists;
import cn.alqso.hbase.util.HBaseClient;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Properties;

public class YhCldETL {
    protected Logger logger = LoggerFactory.getLogger(YhCldETL.class);

    public static String url = "jdbc:mysql://192.168.10.171:9030/guangxi_db?sessionVariables=query_timeout=3600";
    public static String user = "guangxi_user";
    public static String password = "guangxi_user";

    public static void main(String[] args) throws IOException {

        YhCldETL etl = new YhCldETL();
        for (int i = 1; i <= 15 ; i++) {
            etl.imp(40000 + i * 100);
        }
    }

    private void test() throws IOException {
        HBaseClient hBaseClient = new HBaseClient();

        logger.info(">>>>>>>>>");
        byte[] data = hBaseClient.getData("dim_yhcld", "000002237929",
                "info", "xxx");
        logger.info("<<<<<<<<<<");

        hBaseClient.close();
    }

    private void imp(int dqbh) throws IOException {
        HBaseClient hBaseClient = new HBaseClient();

        hBaseClient.status();

        String tableName = "fact_rdl_bmz";

        hBaseClient.createTable(tableName, new String[]{"info", "bmz", "rdl"});

        JdbcTemplate template = new JdbcTemplate(getDataSource());

        template.query(con -> {
            PreparedStatement preparedStatement =
                    con.prepareStatement("SELECT GDFJ_KEY, CLDBS, YHBH, DNB_ZHBL ZHBL FROM DIM_YHCLD " +
                                    "WHERE DQBH = " + dqbh +
                                    " AND GDFJ_KEY IS NOT NULL " +
//                                    "WHERE GDFJ_KEY IS NOT NULL " +
                                    "AND YHBH IS NOT NULL " +
                                    "AND DNB_ZHBL IS NOT NULL ",
                            ResultSet.TYPE_FORWARD_ONLY,
                            ResultSet.CONCUR_READ_ONLY);
            preparedStatement.setFetchSize(Integer.MIN_VALUE);
            preparedStatement.setFetchDirection(ResultSet.FETCH_FORWARD);
            return preparedStatement;
        }, rs -> {
            int rowCount = 0;
            int batchSize = 500;

            List<HBaseClient.RowData> rows = Lists.newArrayListWithExpectedSize(batchSize);

            while (rs.next()) {
                try {
                    rowCount++;

                    String gdfj_key = rs.getString("GDFJ_KEY");
                    String cldbs = rs.getString("CLDBS");
                    String yhbh = rs.getString("YHBH");
                    BigDecimal zhbl = rs.getBigDecimal("ZHBL");

                    String rowKey = gdfj_key + "_" + cldbs;

                    List<Pair<String, String>> pairs = Lists.newArrayList();
                    pairs.add(Pair.newPair("cldbs", cldbs == null ? "" : cldbs));
                    pairs.add(Pair.newPair("yhbh", yhbh == null ? "" : yhbh));
                    pairs.add(Pair.newPair("zhbl", zhbl == null ? "" : zhbl.toString()));

                    HBaseClient.RowData rowData = new HBaseClient.RowData(rowKey, "info", pairs);
                    rows.add(rowData);

                    if(rows.size() == batchSize){
                        hBaseClient.putData(tableName, rows);
                        rows.clear();
                    }

                    if(rowCount % 50000 == 0){
                        logger.info("dqbh:{}, rowCount:{}", dqbh, rowCount);
                        Thread.sleep(1000);
                    }
                } catch (Exception e) {
                    logger.error("", e);
                }
            }

            if(rows.size() > 0){
                try {
                    hBaseClient.putData(tableName, rows);
                    logger.info("dqbh:{}, rowCount:{}", dqbh, rowCount);
                } catch (IOException e) {
                    logger.error("", e);
                }
                rows.clear();
            }
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