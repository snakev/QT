package com.xingcloud.qt.query.mysql;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.xingcloud.mysql.PropType;
import com.xingcloud.qt.model.BitSet;
import com.xingcloud.qt.model.DictCompressMap;
import com.xingcloud.qt.model.SmartSet;
import com.xingcloud.qt.query.pool.UIQueryPool;
import com.xingcloud.qt.query.mysql.task.UidQueryTaskCallable;
import com.xingcloud.qt.query.mysql.task.UserInfoQueryTaskCallable;
import com.xingcloud.qt.query.QueryBase;
import com.xingcloud.qt.redis.RedisOperate;
import com.xingcloud.qt.utils.Constants;
import com.xingcloud.qt.utils.QueryUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-5-13
 * Time: 下午3:30
 * To change this template use File | Settings | File Templates.
 */
public class QueryMysql implements QueryBase {
    private static Log LOG = LogFactory.getLog(QueryMysql.class);

    private final int TASK_TIMEOUT = 300;

    @Override
    public SmartSet getSegmentUidSet(String pID, String segmentJson, long startUid, long endUid) {

        long startTime = System.nanoTime();


        List<String> sqllist = parseToSQL(pID, segmentJson, startUid, endUid, null);

        List<FutureTask<SmartSet>> futures = new ArrayList<FutureTask<SmartSet>>(sqllist.size());

        for (String sql : sqllist) {
            UidQueryTaskCallable task = new UidQueryTaskCallable(pID, sql);
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
    public DictCompressMap getUserInfo(String pID, String segmentJson, String attrName, long startUid, long endUid) {
        long startTime = System.nanoTime();

        List<String> sqllist = parseToSQL(pID, segmentJson, startUid, endUid, attrName);

        List<FutureTask<SmartSet>> futures = new ArrayList<FutureTask<SmartSet>>(sqllist.size());

        FutureTask<DictCompressMap> userInfoFuture = null;
        for (String sql : sqllist) {
            if (sql.contains("val FROM")) {
                UserInfoQueryTaskCallable task = new UserInfoQueryTaskCallable(pID, sql, attrName, true);
                userInfoFuture = (FutureTask<DictCompressMap>)UIQueryPool.addTask(task);
            } else {
                UidQueryTaskCallable task = new UidQueryTaskCallable(pID, sql);
                FutureTask<SmartSet> futureTask = (FutureTask<SmartSet>)UIQueryPool.addTask(task);
                futures.add(futureTask);
            }
        }
        DictCompressMap finalUserInfo = new DictCompressMap();

        if (!Constants.TOTAL_USER_IDENTIFIER.equals(segmentJson) && !(sqllist.size()==1 && attrName != null)) {
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

            boolean segmentContainsAttr = segmentJson.contains(attrName);

            if (ss.isUseBitSet()) {
                BitSet bs = ss.getBs();
                for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1)) {
                    String val = userInfo.get(i);
                    if (val != null) {
                        finalUserInfo.put(i, val);
                    } else if (!segmentContainsAttr){
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
                    } else if (!segmentContainsAttr){
                        /* Don't have attribute info */
                        finalUserInfo.put(i, Constants.NA);
                    }
                }
            }

        } else {
        	/* Only have group by query */
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
                attrName + "\n" +
                startUid + "\n" +
                endUid + "\n" +
                "Query uid taken " + (System.nanoTime() - startTime) / 1.0e9 + "\n" +
                "Size: " + finalUserInfo.size());
        return finalUserInfo;

    }

    public List<String> parseToSQL(String pID, String segmentJson, long startUid, long endUid, String attrName) {
        List<String> querySqlList = new ArrayList<String>();
        boolean isAlreadyQuery = false;

        if (segmentJson.equals(Constants.TOTAL_USER_IDENTIFIER)) {
        	 /*Only group by*/
            StringBuilder sqlBuilder = new StringBuilder("SELECT uid, val FROM " + attrName + " WHERE uid>=" + startUid + " AND uid<=" + endUid);
            querySqlList.add(sqlBuilder.toString());
            return querySqlList;
        }

        DBObject queryObj = (DBObject) JSON.parse(segmentJson);

        for (String key : queryObj.keySet()) {
            StringBuilder sqlBuilder = new StringBuilder();
            if (attrName != null && attrName.equals(key)) {
                sqlBuilder.append("SELECT uid,val FROM " + key + " WHERE uid>=" + startUid + " AND uid<=" + endUid + " AND ");
                isAlreadyQuery = true;
            } else {
                sqlBuilder.append("SELECT uid FROM " + key + " WHERE uid>=" + startUid + " AND uid<=" + endUid + " AND ");
            }

            Object obj = queryObj.get(key);
            if (obj instanceof Integer || obj instanceof Long) {
                sqlBuilder.append("val="+obj);

            } else if (obj instanceof String) {
                if (RedisOperate.getPropInfo(pID, key).getFirst() == PropType.sql_datetime) {
            		 /* Date type */
                    String date = (String)obj;
                    long dateInMySqlBegin = QueryUtils.getDateValInMySql(date, true);
                    long dateInMySqlEnd = QueryUtils.getDateValInMySql(date, false);
                    sqlBuilder.append("val>="+dateInMySqlBegin+" AND val<="+dateInMySqlEnd);
                } else {
                    sqlBuilder.append("val='"+obj+"'");
                }
            } else if (obj instanceof Pattern) {
                 /*To do*/

            } else if (obj instanceof DBObject) {
                Set<String> subKeys = ((DBObject)obj).keySet();
                int counter = 0;
                for (String keySub : subKeys) {
                    Object val = ((DBObject)obj).get(keySub);
                    if (keySub.equals("$gt")) {
                        if (val instanceof Integer || val instanceof Long) {
                            sqlBuilder.append("val>"+val);
                        } else {
                            if (RedisOperate.getPropInfo(pID, key).getFirst() == PropType.sql_datetime) {
                                String date = (String)val;
                                long dateInMySql = QueryUtils.getDateValInMySql(date, true);
                                sqlBuilder.append("val>"+dateInMySql);
                            }
                            else {
                                sqlBuilder.append("val>'"+val+"'");
                            }
                        }

                    } else if (keySub.equals("$gte")) {
                        if (val instanceof Integer || val instanceof Long) {
                            sqlBuilder.append("val>="+val);
                        } else {
                            if (RedisOperate.getPropInfo(pID, key).getFirst() == PropType.sql_datetime) {
                                String date = (String)val;
                                long dateInMySql = QueryUtils.getDateValInMySql(date, true);
                                sqlBuilder.append("val>="+dateInMySql);
                            } else {
                                sqlBuilder.append("val>='"+val+"'");
                            }
                        }

                    } else if (keySub.equals("$lt")) {
                        if (val instanceof Integer || val instanceof Long) {
                            sqlBuilder.append("val<"+val);
                        } else {
                            if (RedisOperate.getPropInfo(pID, key).getFirst() == PropType.sql_datetime) {
                                String date = (String)val;
                                long dateInMySql = QueryUtils.getDateValInMySql(date, false);
                                sqlBuilder.append("val<"+dateInMySql);
                            } else {
                                sqlBuilder.append("val<'"+val+"'");
                            }
                        }

                    } else if (keySub.equals("$lte")) {
                        if (val instanceof Integer || val instanceof Long) {
                            sqlBuilder.append("val<="+val);
                        } else {
                            if (RedisOperate.getPropInfo(pID, key).getFirst() == PropType.sql_datetime) {
                                String date = (String)val;
                                long dateInMySql = QueryUtils.getDateValInMySql(date, false);
                                sqlBuilder.append("val<="+dateInMySql);
                            } else {
                                sqlBuilder.append("val<='"+val+"'");
                            }
                        }
                    }

                    counter++;
                    if (counter != subKeys.size()) {
                        sqlBuilder.append(" AND ");
                    } else {
                        sqlBuilder.append(" ");
                    }
                }
            }

            querySqlList.add(sqlBuilder.toString());
        }

        if (attrName != null && !isAlreadyQuery) {
            StringBuilder sqlBuilder = new StringBuilder("SELECT uid,val" + " FROM " + attrName + " WHERE uid>=" + startUid + " AND uid<=" + endUid);
            querySqlList.add(sqlBuilder.toString());
        }

        return querySqlList;

    }

}
