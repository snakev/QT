package com.xingcloud.qt.mainrun;

import com.xingcloud.qt.query.QueryBase;
import com.xingcloud.qt.query.hbase.QueryHBase;
import com.xingcloud.qt.query.mysql.QueryMysql;
import com.xingcloud.qt.utils.QueryUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Pair;

import java.util.concurrent.CountDownLatch;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-5-16
 * Time: 下午1:55
 * To change this template use File | Settings | File Templates.
 */
public class QueryTask extends Thread {
    private static Log LOG = LogFactory.getLog(QueryTask.class);

    private String pID;
    private String segment;
    private String groupByAttr;
    private String type;
    private int bucketNum;
    private int times;

    private CountDownLatch threadsSignal;

    public QueryTask(String pID, String segment, String groupByAttr,
                     String type, int bucketNum, int times, CountDownLatch threadsSignal) {
        this.pID = pID;
        this.segment = segment;
        this.groupByAttr = groupByAttr;
        this.type = type;
        this.bucketNum = bucketNum;
        this.times = times;
        this.threadsSignal = threadsSignal;
    }

    @Override
    public void run() {
        Pair<Long, Long> uidPair = QueryUtils.getLocalSEUidOfBucket(bucketNum, 0);
        QueryBase qb = null;
        if (type.equals("hbase")) {
            qb = new QueryHBase();
        } else {
            qb = new QueryMysql();
        }
        long st = System.nanoTime();
        for (int i=0; i<times; i++) {
            if (groupByAttr.equals("NA")) {
                qb.getSegmentUidSet(pID, segment, uidPair.getFirst(), uidPair.getSecond());
            } else {
                qb.getUserInfo(pID, segment, groupByAttr, uidPair.getFirst(), uidPair.getSecond());
            }
        }
        LOG.info(Thread.currentThread().getId() + " Taken: " + (System.nanoTime()-st)/1.0e9 + " sec");
        threadsSignal.countDown();
    }
}
