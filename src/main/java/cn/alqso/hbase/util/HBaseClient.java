package cn.alqso.hbase.util;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;


public class HBaseClient {
    private Logger logger = LoggerFactory.getLogger(HBaseClient.class);

    private Connection connection;
    private HBaseAdmin admin;

    public HBaseClient() {
        init();
    }

    public static void main(String[] args)throws IOException{
        HBaseClient hBaseClient = new HBaseClient();
        hBaseClient.createTable("student",new String[]{"score"});
        hBaseClient.putData("student","zhangsan","score","English","69");
        hBaseClient.putData("student","zhangsan","score","Math","86");
        hBaseClient.putData("student","zhangsan","score","Computer","77");

        byte[] data = hBaseClient.getData("student", "zhangsan", "score", "English");
        System.out.println(Bytes.toString(data));

        hBaseClient.close();
    }

    public void init(){
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");

        try{
            connection = ConnectionFactory.createConnection(configuration);
            admin = (HBaseAdmin) connection.getAdmin();
        }catch (Exception e){
            logger.error("", e);
        }
    }

    public void close(){
        try{
            if(admin != null){
                admin.close();
            }
            if(null != connection){
                connection.close();
            }
        }catch (Exception e){
            logger.error("", e);
        }
    }

    public void status() throws IOException {
        ClusterMetrics clusterMetrics = admin.getClusterMetrics();
        Map<ServerName, ServerMetrics> liveServerMetrics = clusterMetrics.getLiveServerMetrics();
        liveServerMetrics.forEach((k, v) -> {
            logger.info("{} -> {}", k.getServerName(), v.getUsedHeapSize().get(Size.Unit.MEGABYTE));
        });
    }

    public boolean exists(String myTableName) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        return admin.tableExists(tableName);
    }

    public void createTable(String myTableName,String[] colFamily) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        if(admin.tableExists(tableName)){
            logger.info("TALBE [{}] IS EXISTS!", myTableName);
        }else {
            TableDescriptorBuilder tableDescriptor = TableDescriptorBuilder.newBuilder(tableName);
            for(String str:colFamily){
                ColumnFamilyDescriptor family = ColumnFamilyDescriptorBuilder
                        .newBuilder(Bytes.toBytes(str)).build();
                tableDescriptor.setColumnFamily(family);
            }
            admin.createTable(tableDescriptor.build());
        }
    }

    public void putData(String tableName, List<RowData> rows) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        List<Put> puts = Lists.newArrayList();
        // 行
        rows.forEach(i -> {
            Put put = new Put(i.getRowKey().getBytes());
            // 列
            i.getPairs().forEach(c -> {
                put.addColumn(i.getColFamily().getBytes(), Bytes.toBytes(c.getFirst()), Bytes.toBytes(c.getSecond()));
            });
            puts.add(put);
        });

        table.put(puts);
        table.close();
    }

    public void putData(String tableName, String rowKey, String columnFamily, List<Pair<String, String>> pairs) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        Put put = new Put(rowKey.getBytes());

        pairs.forEach(i -> {
            put.addColumn(columnFamily.getBytes(), i.getFirst().getBytes(), i.getSecond().getBytes());
        });

        table.put(put);
        table.close();
    }

    @SafeVarargs
    public final void putData(String tableName, String rowKey, String columnFamily, Pair<String, String>... data) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        Put put = new Put(rowKey.getBytes());
        Stream.of(data).forEach(i -> {
            put.addColumn(columnFamily.getBytes(), i.getFirst().getBytes(), i.getSecond().getBytes());
        });

        table.put(put);
        table.close();
    }

    public void putData(String tableName, String rowKey, String columnFamily, String columnName, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        Put put = new Put(rowKey.getBytes());
        put.addColumn(columnFamily.getBytes(), columnName.getBytes(), value.getBytes());

        table.put(put);
        table.close();
    }

    public byte[] getData(String tableName, String rowKey, String colFamily, String col) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        get.addColumn(colFamily.getBytes(), col.getBytes());
        Result result = table.get(get);
        byte[] resultValue = result.getValue(colFamily.getBytes(), col.getBytes());
        table.close();

        return resultValue;
    }

    public Table getTable(String tableName) throws IOException {
        return connection.getTable(TableName.valueOf(tableName));
    }

    public static class RowData {
        private String rowKey;
        private String colFamily;
        private List<Pair<String, String>> pairs;

        public RowData(String rowKey, String colFamily, List<Pair<String, String>> pairs) {
            this.rowKey = rowKey;
            this.colFamily = colFamily;
            this.pairs = pairs;
        }

        public String getRowKey() {
            return rowKey;
        }

        public String getColFamily() {
            return colFamily;
        }

        public void setRowKey(String rowKey) {
            this.rowKey = rowKey;
        }

        public void setColFamily(String colFamily) {
            this.colFamily = colFamily;
        }

        public List<Pair<String, String>> getPairs() {
            return pairs;
        }

        public void setPairs(List<Pair<String, String>> pairs) {
            this.pairs = pairs;
        }
    }
}