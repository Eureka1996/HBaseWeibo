package com.wufuqiang.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * @ author wufuqiang
 * @ date 2019/3/15/015 - 10:26
 *
 **/
public class WeiBo {
//    HBase的配置对象
    private Configuration conf = HBaseConfiguration.create() ;

    //创建weibo这个业务 的命名空间，3张表
    private static final byte[] NS_WEIBO = Bytes.toBytes("ns_weibo") ;
    private static final byte[] TABLE_CONTENT = Bytes.toBytes("ns_weibo:content") ;
    private static final byte[] TABLE_RELATION = Bytes.toBytes("ns_weibo:relation") ;
    private static final byte[] TABLE_INBOX = Bytes.toBytes("ns_weibo:inbox") ;



//    初始化命名空间和3张表
    private void init() throws IOException {
//        创建微博业务 命名空间
        initNameSpace() ;
//        创建微博内容表
        initTableContent() ;
//        创建微博关系表
        initTableRelation() ;
//        创建微博收件箱表
        initTableInbox() ;
    }

    private void createTable(byte[] tableName , int version , String ... columnFamily) throws IOException{
        Connection connection = ConnectionFactory.createConnection(conf) ;
        Admin admin = connection.getAdmin() ;
        if(admin.tableExists(TableName.valueOf(tableName))){
            System.out.println(new String(tableName) + "表已经存在，请不要重复创建。");
        }else{
            //            创建表描述器
            HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName) ) ;
            for(String cf : columnFamily){
                //创建列描述器
                HColumnDescriptor infoColumnDescriptor = new HColumnDescriptor(cf) ;
                //设置块缓存
                infoColumnDescriptor.setBlockCacheEnabled(true) ;
                //设置块缓存 大小为：2M
                infoColumnDescriptor.setBlocksize(2*1024*1024);
                //设置版本确界
                infoColumnDescriptor.setMinVersions(version) ;
                infoColumnDescriptor.setMaxVersions(version) ;
                htd.addFamily(infoColumnDescriptor) ;
            }
            admin.createTable(htd);
            System.out.println(new String(tableName) + "表创建成功。");
        }
        admin.close() ;
        connection.close() ;
    }


    /*
    * 表名：ns_weibo:content
    * 列族名：info
    * 列名：content
    * rowkey：用户ID_时间戳
    * value：微博内容（文字 内容，图片URL，视频 URL，语音URL）
    * versions:1
    * */
    private void initTableContent() throws IOException {
        createTable(TABLE_CONTENT,1,"info");
    }

    /*
     * 表名：ns_weibo:relation
     * 列族名：attends,fans
     * 列名：用户ID
     * rowkey：当前操作人的用户ID
     * versions:1
     *
     * */

    private void initTableRelation() throws IOException {
        createTable(TABLE_RELATION,1,"attends","fans");
    }
    /*
     * 表名：ns_weibo:inbox
     * 列族名：info
     * 列名：当前用户所关注的人的用户ID
     * rowkey：当前用户ID
     * value：微博rowkey
     * versions:10
     * */
    private void initTableInbox() throws IOException {
        createTable(TABLE_INBOX,10,"info");
    }

    private void initNameSpace() throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf) ;
        Admin admin = connection.getAdmin() ;
        NamespaceDescriptor ns_weibo = NamespaceDescriptor
                .create("ns_weibo")
                .addConfiguration("creator","wufuqiang")
                .addConfiguration("create_time",String.valueOf(System.currentTimeMillis()))
                .build() ;
        try {
            admin.createNamespace(ns_weibo);
        }catch (Exception e){
            System.out.println("ns_weibo表空间已经创建.") ;
        }

        admin.close() ;
        connection.close() ;
    }


    public void publishContent(String uid , String content)throws IOException{

        long ts = System.currentTimeMillis() ;

        String rowKey = uid + "_" +System.currentTimeMillis() ;
//        添加微博内容到微博表
        HBaseUtil.addRow(new String(TABLE_CONTENT),rowKey,"info","content",content) ;
//        查询用户关系表，得到当前用户的fans用户id
        List<byte[]> fansId = new ArrayList<byte[]>() ;
        Result result = HBaseUtil.getARowByRowKey(new String(TABLE_RELATION),uid,"fans") ;
        Cell[] cells = result.rawCells() ;
        for(Cell cell : cells){
            fansId.add(CellUtil.cloneValue(cell)) ;
        }
        if(fansId.size() <=0) return ;

//        开始操作收件箱表
        List<Put> puts = new ArrayList<Put>() ;
        for(byte[] fansRoeKey : fansId){
            Put inboxPut = new Put(fansRoeKey) ;
            inboxPut.addColumn(Bytes.toBytes("info"),Bytes.toBytes(uid),ts,Bytes.toBytes(rowKey)) ;
            puts.add(inboxPut) ;
        }

        HBaseUtil.addRow(new String(TABLE_INBOX),puts);
    }

    /*
    * a、在用户关系表中，对当前 主动操作的用户id进行添加关注的操作
    * b、在用户关系表中，对被关注的人的用户id添加粉丝操作
    * c、对当前操作的用户的收件箱表中，添加他所关注的人的最近 的微博rowkey
    * */
    public void addAttends(String uid , String ... attends) throws IOException{
        if(attends == null || attends.length <= 0 || uid == null) return ;

        List<Put> puts = new ArrayList<Put>() ;
//        在微博用户关系表中，添加新关注的好友
        Put attendPut = new Put(Bytes.toBytes(uid)) ;
        for(String attend : attends ){
//            为当前用户添加关注人
            attendPut.addColumn(Bytes.toBytes("attends"),Bytes.toBytes(attend),Bytes.toBytes(attend)) ;
//          被关注的人添加粉丝
            Put fanPut = new Put(Bytes.toBytes(attend)) ;
            fanPut.addColumn(Bytes.toBytes("fans"),Bytes.toBytes(uid),Bytes.toBytes(uid)) ;
            puts.add(fanPut) ;
        }
        puts.add(attendPut) ;
        HBaseUtil.addRow(new String(TABLE_RELATION),puts);
//        用户存放扫描出来的我所关注的人的微博rowkey
        List<byte[]> rowkeys = new ArrayList<byte[]>() ;
        for(String attend : attends){
//            扫描微博rowkey，使用rowfilter过滤器
            RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator(attend+"_")) ;
            List<Result> results = HBaseUtil.getRows(new String(TABLE_CONTENT),filter) ;
            for(Result result : results){
                rowkeys.add(result.getRow()) ;
            }
        }
//        将取出的微博rowkey放置于当前操作的这个用户的收件箱表中

//        如果所关注的人没有一条微博，直接返回
        if(rowkeys.size() <= 0) return ;

        List<Put> inboxPuts = new ArrayList<Put>() ;
        Put put = new Put(Bytes.toBytes(uid)) ;
        for(byte[] rowkey : rowkeys){
            String rowkeyString = Bytes.toString(rowkey) ;
            String attendUID = rowkeyString.split("_")[0] ;
            String attendWeiboTS = rowkeyString.split("_")[1] ;
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes(attendUID),Long.valueOf(attendWeiboTS),rowkey) ;
        }
        inboxPuts.add(put) ;

        HBaseUtil.addRow(new String(TABLE_INBOX),inboxPuts);

    }

    /*
    * 取关操作
    * 1、在用户关系表中，删除你要取关的那个人的用户id
    * 2、在用户关系表中，删除被你取关 的那个的粉丝中的你的uid
    * 3、删除微博收件 箱表中你取关 的人所发布的微博的rowkey
    * */
    public static void removeAttends(String uid , String ... attends) throws IOException{


        List<Delete> deletes = new ArrayList<Delete>() ;
        Delete attendDelete = new Delete(Bytes.toBytes(uid)) ;

        List<Delete> fansDeletes = new ArrayList<Delete>() ;

        List<Delete> inboxDeletes = new ArrayList<Delete>() ;
        Delete inboxDelete = new Delete(Bytes.toBytes(uid)) ;

        for(String attend : attends){
            attendDelete.addColumn(Bytes.toBytes("attends"),Bytes.toBytes(attend)) ;

            Delete fansDelete = new Delete(Bytes.toBytes(attend)) ;
            fansDelete.addColumns(Bytes.toBytes("fans"),Bytes.toBytes(uid)) ;
            fansDeletes.add(fansDelete) ;

            inboxDelete.addColumns(Bytes.toBytes("info"),Bytes.toBytes(attend)) ;

        }
        deletes.add(attendDelete) ;
        inboxDeletes.add(inboxDelete) ;
        HBaseUtil.deleteRows(new String(TABLE_RELATION),deletes) ;
        HBaseUtil.deleteRows(new String(TABLE_RELATION) ,fansDeletes);
        HBaseUtil.deleteRows(new String(TABLE_INBOX),inboxDeletes) ;


    }

    public List<Message> getAttendsContent(String uid) throws IOException{
        List<Message> messages = new ArrayList<Message>() ;
        List<byte[]> contentRowkeys = new ArrayList<byte[]>() ;


//        从收件箱表中获取微博的rowkey
        Result result = HBaseUtil.getARowByRowKey(new String(TABLE_INBOX),uid,"info") ;
        Cell[] inboxCells = result.rawCells() ;

        for(Cell cell : inboxCells){
            contentRowkeys.add(CellUtil.cloneValue(cell)) ;
        }

        List<Get> contentGets = new ArrayList<Get>() ;

        for(byte[] contentRowkey : contentRowkeys){
            Get contentGet = new Get(contentRowkey) ;

            contentGets.add(contentGet) ;

        }
        Result[] contentResults = HBaseUtil.getRows(new String(TABLE_CONTENT),contentGets) ;

        for(Result contentResult : contentResults){

            for(Cell cell : contentResult.rawCells()){
                Message message = new Message() ;
                message.setContent(new String(CellUtil.cloneValue(cell)));
                message.setUid(new String(CellUtil.cloneRow(cell)).split("_")[0]);
                message.setTimestamp(Long.valueOf(new String(CellUtil.cloneRow(cell)).split("_")[1]));
                messages.add(message) ;
            }


        }

        return messages;
    }


//    发布微博
    public static void publishWeiBo(WeiBo wb , String uid , String content)throws IOException{

        wb.publishContent(uid,content);
    }
//    关注
    public static void addAttendTest(WeiBo wb , String uid , String ... attends)throws IOException{

        wb.addAttends(uid,attends);
    }
//    取关
    public static void removeAttendTest(WeiBo wb , String uid , String ... attends)throws IOException{

        wb.removeAttends(uid,attends);
    }

//    刷微博
    public static void scanWeiBoContentTest(WeiBo wb , String uid)throws IOException{
        List<Message> messages = wb.getAttendsContent(uid) ;
        for(Message message : messages){
            System.out.println(message) ;
        }
    }


    public static void main(String[] args) throws IOException {
        WeiBo weibo = new WeiBo() ;
        /*weibo.init() ;
        publishWeiBo(weibo,"1002","哈哈哈");
        publishWeiBo(weibo,"1002","中国1");
        publishWeiBo(weibo,"1002","中国2");
        publishWeiBo(weibo,"1003","中国3");
        publishWeiBo(weibo,"1003","中国4");
        publishWeiBo(weibo,"1003","buaa");*/

//        addAttendTest(weibo,"1001","1002","1003");
/*        scanWeiBoContentTest(weibo,"1001");
        removeAttendTest(weibo,"1001","1002");
        System.out.println("--------------------------------------") ;
        scanWeiBoContentTest(weibo,"1001");*/
//        addAttendTest(weibo,"1003","1001");
/*        publishWeiBo(weibo,"1001","大美女1") ;
        publishWeiBo(weibo,"1001","大美女2") ;
        publishWeiBo(weibo,"1001","大美女3") ;
        publishWeiBo(weibo,"1001","大美女4") ;
        publishWeiBo(weibo,"1001","大美女5") ;
        publishWeiBo(weibo,"1001","大美女6") ;
        publishWeiBo(weibo,"1001","大美女7") ;
        publishWeiBo(weibo,"1001","大美女8") ;*/
        scanWeiBoContentTest(weibo,"1003");
    }
}
