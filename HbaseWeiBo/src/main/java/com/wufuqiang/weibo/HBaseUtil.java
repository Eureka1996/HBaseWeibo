package com.wufuqiang.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ author wufuqiang
 * @ date 2019/3/16/016 - 15:12
 **/
public class HBaseUtil {
    public static Configuration conf ;

    static{
        conf = HBaseConfiguration.create() ;
    }


    public static boolean isExist(String tableName) throws IOException {
//        老API
//        HBaseAdmin admin = new HBaseAdmin(conf) ;

        Connection connection = ConnectionFactory.createConnection(conf) ;
        Admin admin = connection.getAdmin() ;
        return admin.tableExists(TableName.valueOf(tableName)) ;
    }


    //    表的创建
    public static void createTable(String tableName , String ... columnFamily) throws IOException{
        Connection connection = ConnectionFactory.createConnection(conf) ;
        Admin admin = connection.getAdmin() ;
        if(admin.tableExists(TableName.valueOf(tableName))){
            System.out.println("表已经存在，请不要重复创建。");
        }else{
            HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName) ) ;
            for(String cf : columnFamily){
                htd.addFamily(new HColumnDescriptor(cf)) ;
            }
            admin.createTable(htd);
            System.out.println(tableName + "表创建成功。");

        }
        admin.close();
        connection.close();
    }

    //    表的删除
    public static void deleteTable(String tableName) throws IOException{
        Connection connection = ConnectionFactory.createConnection(conf) ;
        Admin admin = connection.getAdmin() ;
        if(admin.tableExists(TableName.valueOf(tableName))){
            if(!admin.isTableDisabled(TableName.valueOf(tableName)))
                admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println("已经成功删除表："+tableName) ;
        }else{
            System.out.println("要删除的表不存在。") ;
        }
        admin.close() ;
        connection.close() ;

    }

    //    添加一行数据
    public static void addRow(String tableName , String rowKey , String cf , String column , String value)throws IOException{
        Connection connection = ConnectionFactory.createConnection(conf) ;
        Table table = connection.getTable(TableName.valueOf(tableName))  ;
        Put put = new Put(Bytes.toBytes(rowKey)) ;
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column),Bytes.toBytes(value)) ;
        table.put(put) ;
        System.out.println("添加数据成功。");
        table.close() ;
        connection.close() ;
    }

    public static void addRow(String tableName ,List<Put> puts)throws IOException{
        Connection connection = ConnectionFactory.createConnection(conf) ;
        Table table = connection.getTable(TableName.valueOf(tableName)) ;
        table.put(puts) ;
        table.close() ;
        connection.close();

    }

    //    删除一行数据
    public static void deleteRow(String tableName, String rowKey,String cf , String column)throws IOException{
        Connection connection = ConnectionFactory.createConnection(conf) ;
        Table table = connection.getTable(TableName.valueOf(tableName) )  ;
        Delete delete = new Delete(Bytes.toBytes(rowKey)) ;
        delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(column)) ;
        table.delete(delete);
        table.close() ;
        connection.close() ;
        System.out.println("删除数据成功.") ;

    }

    public static void deleteRows(String tableName , List<Delete> deletes)throws IOException{
        Connection connection = ConnectionFactory.createConnection(conf)  ;
        Table table = connection.getTable(TableName.valueOf(tableName) ) ;
        table.delete(deletes) ;
        table.close() ;
        connection.close() ;
    }

    public static void getAllRows(String tableName)throws IOException{
        Connection connection = ConnectionFactory.createConnection(conf) ;
        Table table = connection.getTable(TableName.valueOf(tableName)) ;
        Scan scan = new Scan() ;
        ResultScanner resultScanner = table.getScanner(scan) ;
        for(Result result : resultScanner){
            Cell[] cells = result.rawCells() ;
            for(Cell cell : cells){
                System.out.println("行健："+Bytes.toString(CellUtil.cloneRow(cell))) ;
                System.out.println("列族："+Bytes.toString(CellUtil.cloneFamily(cell))) ;
                System.out.println("列："+Bytes.toString(CellUtil.cloneQualifier(cell))) ;
                System.out.println("值："+Bytes.toString(CellUtil.cloneValue(cell))) ;

            }
            System.out.println("---------------------------------------------");

        }
        table.close() ;
        connection.close() ;
    }

    public static List<Result> getRows(String tableName , RowFilter filter) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf) ;
        Table table = connection.getTable(TableName.valueOf(tableName)) ;
        Scan scan = new Scan() ;
        scan.setFilter(filter) ;
        ResultScanner resultScanner = table.getScanner(scan) ;


        List<Result> results = new ArrayList<Result>() ;
        for(Result result : resultScanner){
            results.add(result) ;
        }
        table.close() ;
        connection.close() ;
        return results ;
    }

    public static Result[] getRows(String tableName,List<Get> gets)throws IOException{
        Connection connection = ConnectionFactory.createConnection(conf) ;
        Table table = connection.getTable(TableName.valueOf(tableName)) ;
        Result[] results = table.get(gets) ;
        table.close() ;
        connection.close() ;
        return results ;

    }

    public static Result getARowByRowKey(String tableName,String rowKey,String cf)throws IOException{
        Connection connection = ConnectionFactory.createConnection(conf) ;
        Table table = connection.getTable(TableName.valueOf(tableName)) ;

        Get get = new Get(Bytes.toBytes(rowKey)) ;
        get.setMaxVersions(5) ;
        get.addFamily(Bytes.toBytes(cf)) ;
        Result result = table.get(get) ;
        table.close() ;
        connection.close();
        return result ;
    }

    private static void initNameSpace() throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf) ;
        Admin admin = connection.getAdmin() ;
        NamespaceDescriptor ns_weibo = NamespaceDescriptor
                .create("ns_wufuqiang")
                .addConfiguration("creator","wufuqiang")
                .addConfiguration("create_time",String.valueOf(System.currentTimeMillis()))
                .build() ;

        admin.createNamespace(ns_weibo);
        admin.close() ;
        connection.close() ;
    }
}
