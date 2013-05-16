package com.xingcloud.qt.mainrun;

import com.xingcloud.qt.model.DictCompressMap;
import com.xingcloud.qt.model.SmartSet;
import com.xingcloud.qt.query.QueryBase;
import com.xingcloud.qt.query.hbase.QueryHBase;
import com.xingcloud.qt.utils.ConfigReader;
import com.xingcloud.qt.utils.Dom;
import com.xingcloud.qt.utils.QueryUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

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

        Dom dom = ConfigReader.getDom("query.xml");
        List<Dom> indexes = dom.elements("index");

        List<Thread> threads = new ArrayList<Thread>();

        CountDownLatch threadsSignal = new CountDownLatch(indexes.size());

        for (Dom index : indexes) {
            String pID = index.elementText("pid");
            String segment = index.elementText("segment");
            String groupByAttr = index.elementText("group_by_attr");
            String type = index.elementText("type");
            int bucketNum = Integer.parseInt(index.elementText("bucket_num"));
            int times = Integer.parseInt(index.elementText("times"));

            LOG.info("Init Thread: " + pID + " " + segment + " " + groupByAttr + " " + type + " " + type + " " + bucketNum);
            Thread thread = new QueryTask(pID, segment, groupByAttr, type, bucketNum, times, threadsSignal);
            threads.add(thread);
        }

        long st = System.nanoTime();
        for (Thread thread : threads) {
            thread.start();
        }

        try {
            threadsSignal.await();
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        LOG.info("------All taken: " + (System.nanoTime()-st)/1.0e9 + " sec");
    }

}
