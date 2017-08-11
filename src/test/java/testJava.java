/*
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import sun.misc.BASE64Encoder;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class testJava {
    public static String EncoderByMd5(String str) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        //确定计算方法
        MessageDigest md5=MessageDigest.getInstance("MD5");
        BASE64Encoder base64en = new BASE64Encoder();
        //加密后的字符串
     //   String newstr=md5.digest(str.getBytes("utf-8"));
        return null;
    }

    public static void main(String[] args) throws Exception{

        String str="0123456789";
        System.out.println(EncoderByMd5(str));

        Configuration conf =  HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","andlinksmaster,andlinksslave1,andlinksslave2");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.set("zookeeper.znode.parent","/hbase-unsecure");
        Connection connection  = ConnectionFactory.createConnection(conf);

        Table resultHTable =connection.getTable(TableName.valueOf("cc:wb"));
        Put put=new Put(Bytes.toBytes("12334"));
        put.addColumn( Bytes.toBytes("info"),  Bytes.toBytes("a"), Bytes.toBytes("123"));
resultHTable.put(put);

        resultHTable.close();

    }
}
*/
