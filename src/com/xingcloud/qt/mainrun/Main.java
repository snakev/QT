package com.xingcloud.qt.mainrun;

import com.xingcloud.qt.model.DictCompressMap;
import com.xingcloud.qt.model.SmartSet;
import com.xingcloud.qt.query.QueryBase;
import com.xingcloud.qt.query.hbase.QueryHBase;
import com.xingcloud.qt.utils.QueryUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-5-15
 * Time: 下午3:49
 * To change this template use File | Settings | File Templates.
 */
public class Main {

    private static Log LOG = LogFactory.getLog(Main.class);

    public static void main(String[] args) {
        String pID = args[0];
        String segment = args[1];
        String groupByAttr = args[2];
        String type = args[3];
        int bucketNum = Integer.parseInt(args[4]);

        Pair<Long, Long> uidPair = QueryUtils.getLocalSEUidOfBucket(bucketNum, 0);

        long st = System.nanoTime();
        if (type.equals("hbase")) {
            QueryUtils.initAttrMap();
            QueryBase qb = new QueryHBase();
            if (!groupByAttr.equals("NA")) {
                DictCompressMap result = qb.getUserInfo(pID, segment, groupByAttr, uidPair.getFirst(), uidPair.getSecond());
            } else {
                SmartSet result = qb.getSegmentUidSet(pID, segment, uidPair.getFirst(), uidPair.getSecond());
            }
            LOG.info("Taken: " + (System.nanoTime()-st)/1.0e9 + " sec");

        } else if (type.equals("mysql")) {

        }


    }
}
