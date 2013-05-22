package com.xingcloud.qt.utils;

import com.xingcloud.mysql.MySql_fixseqid;
import com.xingcloud.mysql.UserProp;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-5-13
 * Time: 下午3:31
 * To change this template use File | Settings | File Templates.
 */
public class QueryUtils {
    private static Log LOG = LogFactory.getLog(QueryUtils.class);

    private static long BYTES_4 = 0xffffffffl;

    public static List<UserProp> getUserProps(String pID) throws SQLException {
        return MySql_fixseqid.getInstance().getUserProps(pID);
    }

    public static Map<String, Integer> attrMap = new HashMap<String, Integer>();

    public static Long getDateValInMySql(String date, boolean isBegin) {
        date = date.replace("-", "");
        if (isBegin) {
            date = date + "000000";
        } else {
            date = date + "235959";
        }
        return Long.parseLong(date);
    }

    public static int getInnerUidFromSamplingUid(long suid) {
        return (int) (BYTES_4 & suid);
    }

    public static String changeToReadableDateFormat(String dateInMysql) {
        dateInMysql = dateInMysql.substring(0, 4) + "-" + dateInMysql.substring(4, 6) + "-" + dateInMysql.substring(6, 8);
        return dateInMysql;
    }

    public static byte[] getUIRowKey(long suid) {
        byte[] rk = new byte[5];
        rk[0] = (byte) (suid>>32 & 0xff);
        rk[1] = (byte) (suid>>24 & 0xff);
        rk[2] = (byte) (suid>>16 & 0xff);
        rk[3] = (byte) (suid>>8 & 0xff);
        rk[4] = (byte) (suid & 0xff);
        return rk;
    }

    public static byte[] getUIIndexRowKey(String attrName, byte[] attrVal) {
        int index = attrMap.get(attrName);
        byte[] rk = combineIndexRowKey(index, attrVal);
        return rk;
    }
    
    public static byte[] getUIIndexRowKey(String attrName){
      int index = attrMap.get(attrName);
      return Bytes.toBytes(index);
    }
    
    public static byte[] getUINextIndexRowKey(String attrName){
      int index = attrMap.get(attrName)+1;
      return Bytes.toBytes(index);
    }
    public static String getUIIndexTableName(String pID, String attrName) {
        return "property_" + pID + "_index";
    }

    public static String getUITableName(String pID, String attrName) {
        int index = attrMap.get(attrName);
        return "property_" + pID + "_" + index;
    }

    public static byte[] combineIndexRowKey(int propertyID, byte[] value){
        return bytesCombine(Bytes.toBytes((short)propertyID), value);

    }
    public static byte[] bytesCombine(byte[]... bytesArrays){
        int length = 0;
        for (byte[] bytes: bytesArrays){
            length += bytes.length;
        }
        byte[] combinedBytes = new byte[length];
        int index = 0;
        for (byte[] bytes: bytesArrays){
            for(byte b: bytes){
                combinedBytes[index] = b;
                index++;
            }
        }
        return combinedBytes;
    }


    public static int getInnerUid(byte[] uid) {
        uid[0] = (byte) (uid[0] & 0);
        return Bytes.toInt(uid);
    }



    public static String getAttrValFromIndexRK(byte[] rk) {
        byte[] val = Arrays.copyOfRange(rk, 2, rk.length);
        return Bytes.toString(val);
    }

    public static String getAttrFromVal(byte[] val, boolean isLong) {
        if (isLong) {
            return String.valueOf(Bytes.toLong(val));
        } else {
            return Bytes.toString(val);
        }
    }

    public static Pair<Long, Long> getLocalSEUidOfBucket(int bucketNum, int offsetBucket) {
        long startBucket = offsetBucket;
        startBucket = startBucket << 32;
        long endBucket = 0;
        if (offsetBucket + bucketNum >= 256) {
            endBucket = (1l << 40) - 1l;
        } else {
            endBucket = offsetBucket + bucketNum;
            endBucket = endBucket << 32;
        }

        return new Pair<Long, Long>(startBucket, endBucket);
    }

    public static void initAttrMap() {
        Configuration conf = HBaseConfiguration.create();
        HTable hTable = null;

        try {
            hTable = new HTable(conf, "properties");
            Scan scan = new Scan();
            ResultScanner rs = hTable.getScanner(scan);
            for (Result r : rs) {
                KeyValue[] kv = r.raw();
                for (int i=0; i<kv.length; i++) {
                    byte[] rk = kv[i].getRow();
                    String attr = Bytes.toString(rk);
                    byte[] val = kv[i].getValue();
                    int index = Bytes.toInt(val);
                    attrMap.put(attr, index);
                    LOG.info(attr + ": " + index);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } finally {
            if (hTable != null) {
                try {
                    hTable.close();
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }

    }


}
