package com.xingcloud.qt.query.hbase;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.xingcloud.mysql.PropType;
import com.xingcloud.qt.model.BitSet;
import com.xingcloud.qt.model.DictCompressMap;
import com.xingcloud.qt.model.SmartSet;
import com.xingcloud.qt.query.QueryBase;
import com.xingcloud.qt.query.hbase.filter.QualifierUidFilter;
import com.xingcloud.qt.query.hbase.task.UidQueryTaskCallable;
import com.xingcloud.qt.query.hbase.task.UserInfoQueryTaskCallable;
import com.xingcloud.qt.query.pool.UIQueryPool;
import com.xingcloud.qt.redis.RedisOperate;
import com.xingcloud.qt.utils.Constants;
import com.xingcloud.qt.utils.DateManager;
import com.xingcloud.qt.utils.QueryUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-5-13
 * Time: 下午4:38
 * To change this template use File | Settings | File Templates.
 */
public class QueryHBase implements QueryBase {

  private static Log LOG = LogFactory.getLog(QueryHBase.class);

  private final int TASK_TIMEOUT = 300;

  @Override
  public SmartSet getSegmentUidSet(String pID, String segmentJson, long startUid, long endUid) {
    long startTime = System.nanoTime();

    List<Pair<String, Scan>> scans = null;
    try {
      scans = parseToScans(pID, segmentJson, startUid, endUid, null);
    } catch (ParseException e) {
      e.printStackTrace();
    }
    List<FutureTask<SmartSet>> futures = new ArrayList<FutureTask<SmartSet>>();
    for (Pair<String, Scan> pair : scans) {
      UidQueryTaskCallable task = new UidQueryTaskCallable(pID, pair.getFirst(), pair.getSecond());
      FutureTask<SmartSet> futureTask = (FutureTask<SmartSet>) UIQueryPool.addTask(task);
      futures.add(futureTask);
    }

    SmartSet finalBitSet = null;
    try {
      for (int i = 0; i < futures.size(); i++) {
        try {
          if (futures.get(i).get(TASK_TIMEOUT, TimeUnit.SECONDS) != null) {
            SmartSet tmp = futures.get(i).get();
            if (finalBitSet == null) {
              finalBitSet = tmp;
            } else {
              finalBitSet = SmartSet.and(finalBitSet, tmp);
            }
          }
        } catch (TimeoutException e) {
          futures.get(i).cancel(true);
          Thread.currentThread().interrupt();
          LOG.error("uid query task timeout "
            + futures.get(i).toString());
        }
      }

    } catch (InterruptedException e) {
      try {
        Thread.currentThread().interrupt();
      } catch (Exception e1) {
        LOG.error(
          "Dealing with uid query task got interruptedException!",
          e1);

      }
    } catch (ExecutionException e) {
      LOG.error(
        "Dealing with uid query task got ExecutionException",
        e);
    }


    LOG.info("------" + pID + "\n" +
      segmentJson + "\n" +
      startUid + "\n" +
      endUid + "\n" +
      "Query uid taken " + (System.nanoTime() - startTime) / 1.0e9 + "\n" +
      "Size: " + finalBitSet.size());

    return finalBitSet;

  }

  @Override
  public DictCompressMap getUserInfo(String pID, String segmentJson, String groupByAttr, long startUid, long endUid) {
    long startTime = System.nanoTime();

    List<Pair<String, Scan>> scans = null;
    try {
      scans = parseToScans(pID, segmentJson, startUid, endUid, groupByAttr);
    } catch (ParseException e) {
      e.printStackTrace();
    }

    List<FutureTask<SmartSet>> futures = new ArrayList<FutureTask<SmartSet>>(scans.size());

    FutureTask<DictCompressMap> userInfoFuture = null;

    boolean segmentContainsAttr = segmentJson.contains(groupByAttr);

    for (Pair<String, Scan> pair : scans) {
      String attrName = pair.getFirst();
      Scan scan = pair.getSecond();

      if (attrName.equals(groupByAttr)) {
        UserInfoQueryTaskCallable task = new UserInfoQueryTaskCallable(pID, attrName, scan, true);
        userInfoFuture = (FutureTask<DictCompressMap>) UIQueryPool.addTask(task);
      } else {
        UidQueryTaskCallable task = new UidQueryTaskCallable(pID, pair.getFirst(), pair.getSecond());
        FutureTask<SmartSet> futureTask = (FutureTask<SmartSet>) UIQueryPool.addTask(task);
        futures.add(futureTask);
      }
    }

    DictCompressMap finalUserInfo = new DictCompressMap();

    /*we have segment json*/
    if (!Constants.TOTAL_USER_IDENTIFIER.equals(segmentJson) && !(scans.size() == 1 && groupByAttr != null)) {
      SmartSet ss = null;
      try {
        for (int i = 0; i < futures.size(); i++) {
          try {
            if (futures.get(i).get(TASK_TIMEOUT, TimeUnit.SECONDS) != null) {
              SmartSet tmp = futures.get(i).get();
              if (ss == null) {
                ss = tmp;
              } else {
                ss = SmartSet.and(ss, tmp);
              }
            }
          } catch (TimeoutException e) {
            futures.get(i).cancel(true);
            Thread.currentThread().interrupt();
            LOG.error("uid query task timeout "
              + futures.get(i).toString());
          }
        }

      } catch (InterruptedException e) {
        try {
          Thread.currentThread().interrupt();
        } catch (Exception e1) {
          LOG.error(
            "Dealing with uid query task got interruptedException!",
            e1);

        }
      } catch (ExecutionException e) {
        LOG.error(
          "Dealing with uid query task got ExecutionException",
          e);
      }

      DictCompressMap userInfo = null;
      try {
        userInfo = userInfoFuture.get(TASK_TIMEOUT, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        userInfoFuture.cancel(true);
        Thread.currentThread().interrupt();
        LOG.error("Dealing with user info query got timeout exception!", e);
      } catch (InterruptedException e) {
        try {
          Thread.currentThread().interrupt();
        } catch (Exception e1) {
          LOG.error(
            "Dealing with uid query task got interruptedException!",
            e1);

        }
      } catch (ExecutionException e) {
        LOG.error(
          "Dealing with uid query task got ExecutionException",
          e);
      }


      if (ss.isUseBitSet()) {
        BitSet bs = ss.getBs();
        for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i + 1)) {
          String val = userInfo.get(i);
          if (val != null) {
            finalUserInfo.put(i, val);
          } else if (!segmentContainsAttr) {
                        /* Don't have attribute info */
            finalUserInfo.put(i, Constants.NA);
          }
        }
      } else {
        Set<Integer> set = ss.getSet();
        for (int i : set) {
          String val = userInfo.get(i);
          if (val != null) {
            finalUserInfo.put(i, val);
          } else if (!segmentContainsAttr) {
                        /* Don't have attribute info */
            finalUserInfo.put(i, Constants.NA);
          }
        }
      }

    } else { /* Only have group by query */
      try {
        finalUserInfo = userInfoFuture.get(TASK_TIMEOUT, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        userInfoFuture.cancel(true);
        Thread.currentThread().interrupt();
        LOG.error("Dealing with user info query got timeout exception!", e);
      } catch (InterruptedException e) {
        try {
          Thread.currentThread().interrupt();
        } catch (Exception e1) {
          LOG.error(
            "Dealing with uid query task got interruptedException!",
            e1);

        }
      } catch (ExecutionException e) {
        LOG.error(
          "Dealing with uid query task got ExecutionException",
          e);
      }
    }
    LOG.info("------" + pID + "\n" +
      segmentJson + "\n" +
      groupByAttr + "\n" +
      startUid + "\n" +
      endUid + "\n" +
      "Query uid taken " + (System.nanoTime() - startTime) / 1.0e9 + "\n" +
      "Size: " + finalUserInfo.size());
    return finalUserInfo;

  }
  
  /*use segment to get uid set, use group by attribute to get uid-->value, then join the result*/
  public List<Pair<String, Scan>> parseToScans(String pID, String segmentJson, long startUid, long endUid, String attrName) throws ParseException {
    List<Pair<String, Scan>> queryScanList = new ArrayList<Pair<String, Scan>>();
      
    /*construct a scan to get property value for the user if we need group by*/
    if (attrName != null && !segmentJson.contains(attrName)) {
      Scan scan = new Scan();
      FilterList filters = new FilterList();
      QualifierUidFilter qualifierUidFilter = new QualifierUidFilter(startUid, endUid);
      filters.addFilter(qualifierUidFilter);
      scan.setFilter(filters);
      scan.setStartRow(QueryUtils.getUIIndexRowKey(attrName));
      scan.setStopRow(QueryUtils.getUINextIndexRowKey(attrName));
      Pair<String, Scan> pair = new Pair<String, Scan>(attrName, scan);
      queryScanList.add(pair);
    }

    /*we need group by only*/
    if (segmentJson.equals(Constants.TOTAL_USER_IDENTIFIER)) {
      return queryScanList;
    }
    
    /*construct scans on segment json to get matched uids*/
    DBObject queryObj = (DBObject) JSON.parse(segmentJson);
    for (String key : queryObj.keySet()) {
      Scan scan = new Scan();
      FilterList filters = new FilterList();
      QualifierUidFilter qualifierUidFilter = new QualifierUidFilter(startUid, endUid);
      filters.addFilter(qualifierUidFilter);

      Object obj = queryObj.get(key);
      if (obj instanceof Integer || obj instanceof Long) { //{"grade":10}
        byte[] rk;
        if (obj instanceof Integer) {
          rk = QueryUtils.getUIIndexRowKey(key, Bytes.toBytes((Integer) obj));
        } else {
          rk = QueryUtils.getUIIndexRowKey(key, Bytes.toBytes((Long) obj));
        }

        scan.setStartRow(rk);
        scan.setStopRow(rk);
      } else if (obj instanceof String) {//{"register_time":"20121212"}
        if (RedisOperate.getPropInfo(pID, key).getFirst() == PropType.sql_datetime) {
          /* Date type */
          String date = (String) obj;
          String nextDay = DateManager.getInstance().calDay(date, 1);
          long dateInMySqlBegin = QueryUtils.getDateValInMySql(date, true);
          long dateInMySqlEnd = QueryUtils.getDateValInMySql(nextDay, true);

          byte[] srk = QueryUtils.getUIIndexRowKey(key, Bytes.toBytes(dateInMySqlBegin));
          byte[] erk = QueryUtils.getUIIndexRowKey(key, Bytes.toBytes(dateInMySqlEnd));
          scan.setStartRow(srk);
          scan.setStopRow(erk);
          
        } else {
          byte[] rk = QueryUtils.getUIIndexRowKey(key, Bytes.toBytes(obj.toString()));
          scan.setStartRow(rk);
          scan.setStopRow(rk);
        }
      } else if (obj instanceof Pattern) {
        /*To do*/

      } else if (obj instanceof DBObject) {
        Set<String> subKeys = ((DBObject) obj).keySet();
        for (String keySub : subKeys) {
          Object val = ((DBObject) obj).get(keySub);
          if (keySub.equals("$gt")) {
            if (val instanceof Integer || val instanceof Long) {
              byte[] srk = null;
              if (val instanceof Integer) {
                srk = QueryUtils.getUIIndexRowKey(key, Bytes.toBytes((Integer) val + 1));
              } else {
                srk = QueryUtils.getUIIndexRowKey(key, Bytes.toBytes((Long) val + 1l));
              }
              scan.setStartRow(srk);
            } else {
              if (RedisOperate.getPropInfo(pID, key).getFirst() == PropType.sql_datetime) {
                String date = (String) val;
                String newDate = DateManager.getInstance().calDay(date, 1);
                long dateInMySql = QueryUtils.getDateValInMySql(newDate, true);
                byte[] srk = QueryUtils.getUIIndexRowKey(key, Bytes.toBytes(dateInMySql));
                scan.setStartRow(srk);
              } else {
                String valStr = val.toString();
                byte[] srk = Bytes.toBytes(valStr + 1);
                scan.setStartRow(srk);
              }
            }

          } else if (keySub.equals("$gte")) {
            if (val instanceof Integer || val instanceof Long) {
              byte[] srk = null;
              if (val instanceof Integer) {
                srk = QueryUtils.getUIIndexRowKey(key, Bytes.toBytes((Integer) val));
              } else {
                srk = QueryUtils.getUIIndexRowKey(key, Bytes.toBytes((Long) val));
              }
              scan.setStartRow(srk);
            } else {
              if (RedisOperate.getPropInfo(pID, key).getFirst() == PropType.sql_datetime) {
                String date = (String) val;
                long dateInMySql = QueryUtils.getDateValInMySql(date, true);
                byte[] srk = QueryUtils.getUIIndexRowKey(key, Bytes.toBytes(dateInMySql));
                scan.setStartRow(srk);
              } else {
                byte[] srk = QueryUtils.getUIIndexRowKey(key, Bytes.toBytes(val.toString()));
                scan.setStartRow(srk);
              }
            }

          } else if (keySub.equals("$lt")) {
            if (val instanceof Integer || val instanceof Long) {
              byte[] erk;
              if (val instanceof Integer) {
                erk = QueryUtils.getUIIndexRowKey(key, Bytes.toBytes((Integer) val + 1));
              } else {
                erk = QueryUtils.getUIIndexRowKey(key, Bytes.toBytes((Long) val + 1l));
              }
              scan.setStopRow(erk);
            } else {
              if (RedisOperate.getPropInfo(pID, key).getFirst() == PropType.sql_datetime) {
                String date = (String) val;
                long dateInMySql = QueryUtils.getDateValInMySql(date, false);
                byte[] erk = QueryUtils.getUIIndexRowKey(key, Bytes.toBytes(dateInMySql));
                scan.setStopRow(erk);
              } else {
                byte[] erk = QueryUtils.getUIIndexRowKey(key, Bytes.toBytes(val.toString()));
                scan.setStopRow(erk);
              }
            }

          } else if (keySub.equals("$lte")) {

            byte[] erk;
            if (val instanceof Integer || val instanceof Long) {
              if (val instanceof Integer) {
                erk = QueryUtils.getUIIndexRowKey(key, Bytes.toBytes((Integer) val));
              } else {
                erk = QueryUtils.getUIIndexRowKey(key, Bytes.toBytes((Long) val));
              }
            } else {
              if (RedisOperate.getPropInfo(pID, key).getFirst() == PropType.sql_datetime) {
                String date = (String) val;
                long dateInMySql = QueryUtils.getDateValInMySql(date, false);
                erk = QueryUtils.getUIIndexRowKey(key, Bytes.toBytes(dateInMySql));
              } else {
                erk = QueryUtils.getUIIndexRowKey(key, Bytes.toBytes(val.toString()));
              }
            }
            scan.setStopRow(erk);
            InclusiveStopFilter filter = new InclusiveStopFilter(erk);
            filters.addFilter(filter);
          }

        }
      }

      scan.setFilter(filters);
      Pair<String, Scan> pair = new Pair<String, Scan>(key, scan);
      queryScanList.add(pair);
    }

    return queryScanList;
  }

}
