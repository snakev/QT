package com.xingcloud.qt.query.mysql.task;

import com.xingcloud.mysql.MySql_fixseqid;
import com.xingcloud.mysql.PropType;
import com.xingcloud.qt.model.DictCompressMap;
import com.xingcloud.qt.redis.RedisOperate;
import com.xingcloud.qt.utils.QueryUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.Callable;

public class UserInfoQueryTaskCallable implements Callable<DictCompressMap> {
    private static Log LOG = LogFactory.getLog(UserInfoQueryTaskCallable.class);
    
    private String pID;
    private String sql;
    private String attr;
    private boolean formatDate;
    
    public UserInfoQueryTaskCallable(String pID, String sql, String attr, boolean formatDate) {
        super();
        this.pID = pID;
        this.sql = sql;
        this.attr = attr;
        this.formatDate = formatDate;
    }

    @Override
    public DictCompressMap call() throws Exception {
        long startTime = System.nanoTime();
        boolean isDateFormat = RedisOperate.getPropInfo(pID, attr).getFirst() == PropType.sql_datetime;
        DictCompressMap uidMap = new DictCompressMap();
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = MySql_fixseqid.getInstance().getConnLocalNode(pID);
            if (conn.isClosed()) {
                LOG.info("Can't connection to the Database!");
                return uidMap;
            }

            Statement statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            statement.setFetchSize(Integer.MIN_VALUE);
            rs = statement.executeQuery(sql);
            
            while(rs.next()) {
                long uid = rs.getLong("uid");
                int innerUid = QueryUtils.getInnerUidFromSamplingUid(uid);
                String val = rs.getString("val");
                if (val != null) {
                    if (isDateFormat && formatDate) {
                        val = QueryUtils.changeToReadableDateFormat(val);
                    }
                    uidMap.put(innerUid, val);
                }

            }
        } catch (Exception e) {
            LOG.error("Exception in UserInfoQueryTaskCallable. MSG: " + e.getMessage() + "\tSQL: " + sql, e);
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (conn != null) {
                conn.close();
            }
        }

        LOG.info(pID + " group by query taken: " + (System.nanoTime() - startTime) / 1.0e9 + " size: " + uidMap.size());

        return uidMap;
    }

    

}
