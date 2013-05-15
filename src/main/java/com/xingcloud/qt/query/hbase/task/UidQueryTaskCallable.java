package com.xingcloud.qt.query.hbase.task;

import com.xingcloud.qt.query.hbase.HBaseResourceManager;
import com.xingcloud.qt.model.SmartSet;
import com.xingcloud.qt.utils.QueryUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.util.concurrent.Callable;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-5-14
 * Time: 下午11:40
 * To change this template use File | Settings | File Templates.
 */
public class UidQueryTaskCallable implements Callable<SmartSet> {

    private static Log LOG = LogFactory.getLog(UidQueryTaskCallable.class);

    private String tableName;
    private Scan scan;
    private String pID;


    public UidQueryTaskCallable(String pID, String attrName, Scan scan) {
        this.pID = pID;
        this.tableName = QueryUtils.getUIIndexTableName(pID, attrName);
        this.scan = scan;
    }

    @Override
    public SmartSet call() throws Exception {
        SmartSet ss = new SmartSet();

        HTablePool.PooledHTable hTable = HBaseResourceManager.getInstance().getTable(tableName);
        try {
            ResultScanner rs = hTable.getScanner(scan);
            for (Result r : rs) {
                KeyValue[] kv = r.raw();
                for (int i=0; i<kv.length; i++) {
                    byte[] qualifier = kv[i].getQualifier();
                    int innerUid = QueryUtils.getInnerUid(qualifier);
                    ss.add(innerUid);
                }
            }
        } finally {
            HBaseResourceManager.getInstance().putTable(hTable);
        }

        return ss;
    }
}
