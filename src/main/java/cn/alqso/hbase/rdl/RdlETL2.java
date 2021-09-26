package cn.alqso.hbase.rdl;

import com.google.common.collect.Lists;
import cn.alqso.hbase.util.HBaseClient;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

public class RdlETL2 {
    protected Logger logger = LoggerFactory.getLogger(RdlETL2.class);

    public static void main(String[] args) throws Exception {
        RdlETL2 rdlETL = new RdlETL2();

        for (int i = 1; i <= 15; i++) {
            rdlETL.cal(String.format("04%02d", i));
        }
    }

    private void cal(String gdj) throws IOException {
        HBaseClient hBaseClient = new HBaseClient();
        String tableName = "fact_rdl_bmz";
        String colFamily_info = "info";
        String colFamily_bmz = "bmz";
        String colFamily_rdl = "rdl";

        Table table = hBaseClient.getTable(tableName);
        Scan scan = new Scan();

        // 过滤DATA == 1的行
        SingleColumnValueFilter filter = new SingleColumnValueFilter(colFamily_bmz.getBytes(),
                "DATA".getBytes(), CompareOperator.EQUAL,
                new BinaryComparator("1".getBytes()));
        // 不含含 没有DATA列的行
        filter.setFilterIfMissing(true);

        scan.setFilter(filter);

        // 仅查询需要的列族
        scan.addFamily(colFamily_info.getBytes());
        scan.addFamily(colFamily_bmz.getBytes());

        // scan.setLimit(5);

        scan.withStartRow(Bytes.toBytes(gdj), true);
        scan.withStopRow(Bytes.toBytes("0" + (NumberUtils.toInt(gdj) + 1)), false);
//         scan.setLimit(10);

        ResultScanner results = table.getScanner(scan);

        int rowCount = 0;
        int batchSize = 100;
        String dt_format = "yyyyMMdd";

        List<HBaseClient.RowData> rows = Lists.newArrayListWithExpectedSize(batchSize);
        for (Result rs : results) {
            try {
                rowCount++;

                String rowKey = Bytes.toString(rs.getRow());
                String zhbl_str = Bytes.toString(rs.getValue(colFamily_info.getBytes(), "zhbl".getBytes()));
                BigDecimal zhbl = BigDecimal.valueOf(NumberUtils.toDouble(zhbl_str, 1L));

                List<Pair<String, String>> pairs = Lists.newArrayList();

                DateTime start = DateTime.parse("2019-05-01");
                DateTime end = DateTime.parse("2019-08-01");

                for (int i = 0; start.plusDays(i).compareTo(end) <= 0; i++) {
                    DateTime dt_cur = start.plusDays(i);
                    DateTime dt_tom = start.plusDays(i + 1);

                    String cur_bmz_str = Bytes.toString(rs.getValue(colFamily_bmz.getBytes(),
                            dt_cur.toString(dt_format).getBytes()));
                    BigDecimal cur_bmz = BigDecimal.valueOf(NumberUtils.toDouble(cur_bmz_str, 0L));

                    String tom_bmz_str = Bytes.toString(rs.getValue(colFamily_bmz.getBytes(),
                            dt_tom.toString(dt_format).getBytes()));
                    BigDecimal tom_bmz = BigDecimal.valueOf(NumberUtils.toDouble(tom_bmz_str, 0L));

                    BigDecimal rdl = null;
                    if(cur_bmz.compareTo(BigDecimal.ZERO) > 0
                            && tom_bmz.compareTo(BigDecimal.ZERO) > 0
                            && tom_bmz.compareTo(cur_bmz) >= 0){
                        rdl = tom_bmz.subtract(cur_bmz).multiply(zhbl);
                    }

                    pairs.add(Pair.newPair(dt_cur.toString(dt_format),
                            rdl == null ? "" : String.valueOf(rdl.doubleValue())));
                }

                HBaseClient.RowData rowData = new HBaseClient.RowData(rowKey, colFamily_rdl, pairs);
                rows.add(rowData);

                if(rows.size() == batchSize){
                    hBaseClient.putData(tableName, rows);
                    rows.clear();
                    logger.info("{}, rowCount:{}", gdj, rowCount);
                    Thread.sleep(300);
                }

                if(rowCount % 1000 == 0){
                    logger.info("{}, rowCount:{}", gdj, rowCount);
                    Thread.sleep(1000);
                }
                if(rowCount % 10000 == 0){
                    Thread.sleep(2000);
                }
            } catch (Exception e) {
                logger.error("", e);
            }
        }

        if(rows.size() > 0){
            try {
                hBaseClient.putData(tableName, rows);
                logger.info("{}, rowCount:{}", gdj, rowCount);
            } catch (IOException e) {
                logger.error("", e);
            }
            rows.clear();
        }

        results.close();
        table.close();
        hBaseClient.close();
    }
}