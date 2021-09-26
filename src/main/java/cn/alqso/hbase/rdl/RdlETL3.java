package cn.alqso.hbase.rdl;

import cn.alqso.hbase.util.HBaseClient;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class RdlETL3 {
    protected Logger logger = LoggerFactory.getLogger(RdlETL3.class);

    public static void main(String[] args) throws Exception {
        RdlETL3 rdlETL = new RdlETL3();

        for (int i = 1; i <= 15; i++) {
            rdlETL.exp(String.format("04%02d", i));
        }
    }

    private void exp(String gdj) throws IOException {
        String csvPath = String.format("/home/oo/data/gxrdl2/%s", gdj);
        File file = new File(csvPath);
        if(file.exists()){
           logger.error("{} exists!!!", csvPath);
           deleteDir(file);
           file.mkdirs();
        }else {
            file.mkdirs();
        }

        DateTime start = DateTime.parse("2019-05-01");
        DateTime end = DateTime.parse("2019-07-31");

        String title = getTitle(start, end);

        AtomicInteger fileId = new AtomicInteger();
        File dFile = new File(String.format("%s%s%s-%d.csv", csvPath, File.separator, gdj, fileId.get()));

        HBaseClient hBaseClient = new HBaseClient();
        String tableName = "fact_rdl_bmz";
        String colFamily_info = "info";
        String colFamily_rdl = "rdl";
        String colFamily_bmz = "bmz";

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
        //scan.addFamily(colFamily_info.getBytes());
        //scan.addFamily(colFamily_rdl.getBytes());

        scan.withStartRow(Bytes.toBytes(gdj), true);
        scan.withStopRow(Bytes.toBytes("0" + (NumberUtils.toInt(gdj) + 1)), false);
//        scan.setLimit(1000);

        ResultScanner results = table.getScanner(scan);

        FileOutputStream out = null;

        try {
            out = FileUtils.openOutputStream(dFile, true);
            IOUtils.write(title + "\n", out, "UTF-8");
        } catch (Exception e){
            logger.error("92, ", e);
        }

        int rowCount = 0;
        for (Result rs : results) {
            try {
                rowCount++;

//                logger.info(String.valueOf(rowCount));

                String yhbh = Bytes.toString(rs.getValue(colFamily_info.getBytes(), "yhbh".getBytes()));
                if(StringUtils.isBlank(yhbh)){
                    continue;
                }

                StringBuilder rowData = new StringBuilder(yhbh);
                for (int i = 0; start.plusDays(i).compareTo(end) <= 0; i++) {
                    String rdl = Bytes.toString(rs.getValue(colFamily_rdl.getBytes(),
                            start.plusDays(i).toString("yyyyMMdd").getBytes()));
                    rowData.append(",").append(rdl == null ? "" : rdl);
                }

                try {
                    IOUtils.write(rowData.toString() + "\n", out, "UTF-8");
                } catch (IOException e) {
                    logger.error("114, ", e);
                }

                // 50w line per file
                if (rowCount % 500000 == 0) {
                    try {
                        IOUtils.closeQuietly(out);
                        dFile = new File(String.format("%s%s%s-%d.csv",
                                csvPath, File.separator, gdj, fileId.incrementAndGet()));
                        out = FileUtils.openOutputStream(dFile, true);
                        IOUtils.write(title + "\n", out, "UTF-8");
                    }catch (Exception e){
                        logger.error("126, ", e);
                    }
                }

                if(rowCount % 30000 == 0){
                    logger.error("rowCount{}, file:{}", rowCount, dFile.getName());
                }

            } catch (Exception e) {
                logger.error("", e);
            }
        }

        IOUtils.closeQuietly(out);

        results.close();
        table.close();
        hBaseClient.close();
    }

    private void deleteDir(File file) {
        if(file.exists() && file.isDirectory()){
            File[] files = file.listFiles();
            for (File file1 : files) {
                file1.delete();
            }
        }
    }

    private String getTitle(DateTime start, DateTime end) {
        StringBuilder sb = new StringBuilder("YHBH,");

        for (int i = 0; start.plusDays(i).compareTo(end) <= 0; i++) {
            sb.append(start.plusDays(i).toString("yyyyMMdd")).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);

        return sb.toString();
    }
}