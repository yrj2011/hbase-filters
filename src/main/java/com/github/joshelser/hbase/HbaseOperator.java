package com.github.joshelser.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class HbaseOperator {

    /**
     * 配置ss
     */
    private Configuration config = null;
    private Connection connection = null;
    private Table table = null;

    /**
     * 格式化输出封装对象
     * @param result
     * @return
     */
    private static Map<String,String> getValue(Result result) {
        Map<String,String> object =  new HashMap<>();
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            String rowname = new String(CellUtil.cloneQualifier(cell));
            String rowvalue = new String(CellUtil.cloneValue(cell));
            object.put(rowname,rowvalue);
        }
        return object;
    }

    public static boolean insert(Table table,String rowKey , String cf, Map<String,String> object) throws IOException {
        if(table == null || rowKey == null || cf == null || object == null){
            return false;
        }
        Put put = new Put(Bytes.toBytes(rowKey));
        for(String key:object.keySet()){
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(key), Bytes.toBytes(object.get(key)));
        }
        long start = System.currentTimeMillis();
        table.put(put);
        System.out.println("water:"+(System.currentTimeMillis()-start));
        return true;
    }


     /**
      * 统计表行数
      */
     public static long rowCount(Configuration configuration, Table table, String family) throws Throwable {
        AggregationClient ac = new AggregationClient(configuration);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(family));
        scan.setFilter(new FirstKeyOnlyFilter());
        long rowCount = 0;
        try {
            rowCount = ac.rowCount(table, new LongColumnInterpreter(), scan);
        } catch (Throwable e) {
            throw e;
        }
        return rowCount;
    }

    /** 求和*/
    public static double sum(Configuration configuration, Table table, String family, String qualifier) throws Throwable {
        AggregationClient ac = new AggregationClient(configuration);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        double sum = 0;
        try {
            sum = ac.sum(table, new LongColumnInterpreter(), scan);
        } catch (Throwable e) {
            throw e;
        }
        return sum;
    }

    /** 求平均值*/
    public static double avg(Configuration configuration, Table table, String family, String qualifier) throws Throwable {
        AggregationClient ac = new AggregationClient(configuration);
        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        double avg = 0;
        try {
            avg = ac.avg(table, new LongColumnInterpreter(), scan);
        } catch (Throwable e) {
            throw e;
        }
        return avg;
    }

    public static void main( String[] args ) {
        System.setProperty("hadoop.home.dir","c:/");
        Configuration config = null;
        Connection connection = null;
        Table table = null;
        String columnFamily = "cf";
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "192.168.1.228");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            String tableName = "short_link_mobile_log";
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf(tableName));

            //通过HBaseAdmin来完成为表添加配置
            HBaseAdmin hbaseAdmin = new HBaseAdmin(connection);
            hbaseAdmin.disableTable(tableName);
            //通过表的描述来为表添加AggregationImplementation
            HTableDescriptor htd = hbaseAdmin.getTableDescriptor(Bytes.toBytes(tableName));
            htd.addCoprocessor("org.apache.hadoop.hbase.coprocessor.AggregateImplementation");
            hbaseAdmin.modifyTable(tableName, htd);
            hbaseAdmin.enableTable(tableName);
            hbaseAdmin.close();


            long start = System.currentTimeMillis();
            long count = rowCount(config,table,columnFamily);
            System.out.println("waste:"+(System.currentTimeMillis()-start));
            RowFilter rf = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("88"));
            Scan scan = new Scan();
            scan.setFilter(rf);
            ResultScanner rs = table.getScanner(scan);

            System.out.println(rs);
            for(Result res:rs){
                Map<String,String> value =  getValue(res);
                System.out.println("--------------:"+value);
            }
            rs.close();
            /* SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            for(int i=0;i<=10000000;i++) {
                String short_code_mobile ="scm_"+i;
                String short_code ="sc_"+i;
                String url = "http://www.baidu.com/test_"+i;
                String mobile ="138"+i;
                String date = sdf.format(new Date());
                String rowKey = i+"_"+date+"_"+url;
                System.out.println("rowKey:"+ rowKey);
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(PhoneUtil.HBASE_COLFAMILY), Bytes.toBytes("short_code_mobile"), Bytes.toBytes(short_code_mobile));
                put.addColumn(Bytes.toBytes(PhoneUtil.HBASE_COLFAMILY), Bytes.toBytes("short_code"), Bytes.toBytes(String.valueOf(short_code)));
                put.addColumn(Bytes.toBytes(PhoneUtil.HBASE_COLFAMILY), Bytes.toBytes("original_url"), Bytes.toBytes(url));
                put.addColumn(Bytes.toBytes(PhoneUtil.HBASE_COLFAMILY), Bytes.toBytes("user_id"), Bytes.toBytes(i));
                put.addColumn(Bytes.toBytes(PhoneUtil.HBASE_COLFAMILY), Bytes.toBytes("mobile"), Bytes.toBytes(mobile));
                put.addColumn(Bytes.toBytes(PhoneUtil.HBASE_COLFAMILY), Bytes.toBytes("create_time"), Bytes.toBytes(String.valueOf(date)));
                put.addColumn(Bytes.toBytes(PhoneUtil.HBASE_COLFAMILY), Bytes.toBytes("sms_status"), Bytes.toBytes(String.valueOf(1)));
                long start = System.currentTimeMillis();
                table.put(put);
                System.out.println("water:"+(System.currentTimeMillis()-start));

            }*/
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        } finally {
            try {
                if (null != table) { table.close(); }
                if (null != connection) { connection.close(); }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
