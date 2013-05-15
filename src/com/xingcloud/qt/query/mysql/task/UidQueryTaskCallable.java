package com.xingcloud.qt.query.mysql.task;

import com.xingcloud.mysql.MySql_fixseqid;
import com.xingcloud.qt.model.SmartSet;
import com.xingcloud.qt.utils.QueryUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.Callable;

public class UidQueryTaskCallable implements Callable<SmartSet> {
    private static Log LOG = LogFactory.getLog(UidQueryTaskCallable.class);
    
    private String sql;
    private String pID;
    
    public UidQueryTaskCallable(String pID, String sql) {
        super();
        this.sql = sql;
        this.pID = pID;
    }

    @Override
    public SmartSet call() throws Exception {
        long startTime = System.nanoTime();

        SmartSet uidSet = new SmartSet();
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = MySql_fixseqid.getInstance().getConnLocalNode(pID);
            if (conn.isClosed()) {
                LOG.info("Can't connection to the Database!");
                return uidSet;
            }

            Statement statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            statement.setFetchSize(Integer.MIN_VALUE);
            rs = statement.executeQuery(sql);
            while(rs.next()) {
                Long suid = rs.getLong("uid");
                long innerUid = QueryUtils.getInnerUidFromSamplingUid(suid);
                uidSet.add((int)innerUid);
            }
        } catch (Exception e){
            LOG.error("Exception in UidQueryTaskCallable. MSG: " + e.getMessage() + "\tSQL: " + sql, e);
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (conn != null) {
                conn.close();
            }

        }

        LOG.info(pID + " query uid set size: " + uidSet.size() + " taken: " + (System.nanoTime() - startTime) / 1.0e9);

        return uidSet;
    }

    

}
