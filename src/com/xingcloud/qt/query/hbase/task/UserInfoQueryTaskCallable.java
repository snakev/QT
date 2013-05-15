package com.xingcloud.qt.query.hbase.task;

import com.xingcloud.qt.query.hbase.HBaseResourceManager;
import com.xingcloud.qt.model.DictCompressMap;
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
 * Date: 13-5-15
 * Time: 上午12:05
 * To change this template use File | Settings | File Templates.
 */
public class UserInfoQueryTaskCallable implements Callable<DictCompressMap> {

    private static Log LOG = LogFactory.getLog(UserInfoQueryTaskCallable.class);

    private Scan scan;
    private String tableName;
    private boolean useIndex;
    private String attrName;
    private String pID;


    public UserInfoQueryTaskCallable(String pID, String attrName, Scan scan, boolean useIndex) {
        this.pID = pID;
        this.attrName = attrName;
        this.scan = scan;
        this.useIndex = useIndex;
        if (useIndex) {
            this.tableName = QueryUtils.getUIIndexTableName(pID, attrName);
        } else {
            this.tableName = QueryUtils.getUITableName(pID, attrName);
        }
    }

    @Override
    public DictCompressMap call() throws Exception {
        boolean isLong = false;
        if (attrName.endsWith("_time")) {
            isLong = true;
        }

        DictCompressMap dm = new DictCompressMap();
        HTablePool.PooledHTable hTable = HBaseResourceManager.getInstance().getTable(tableName);
        try {
            ResultScanner rs = hTable.getScanner(scan);
            for (Result r : rs) {
                KeyValue[] kv = r.raw();
                for (int i=0; i<kv.length; i++) {
                    byte[] rk = kv[i].getRow();
                    if (useIndex) {
                        String val = QueryUtils.getAttrValFromIndexRK(rk);
                        byte[] qualifier = kv[i].getQualifier();
                        int innerUid = QueryUtils.getInnerUid(qualifier);
                        dm.put(innerUid, val);
                    } else {
                        String val = QueryUtils.getAttrFromVal(kv[i].getValue(), isLong);
                        int innerUid = QueryUtils.getInnerUid(kv[i].getRow());
                        dm.put(innerUid, val);
                    }
                }
            }
        } finally {
            HBaseResourceManager.getInstance().putTable(hTable);
        }


        return dm;
    }
}
