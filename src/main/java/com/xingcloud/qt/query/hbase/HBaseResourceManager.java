package com.xingcloud.qt.query.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;

import java.io.IOException;


public class HBaseResourceManager {
    private static Log logger = LogFactory.getLog(HBaseResourceManager.class);
    private Configuration conf = HBaseConfiguration.create();
    private HTablePool pool;
    private final int max_size = 200;
    private static HBaseResourceManager m_instance;

    public synchronized static HBaseResourceManager getInstance() throws IOException {
        if (m_instance == null) {
            m_instance = new HBaseResourceManager();
        }
        return m_instance; 
    }
    
    
    private HBaseResourceManager() throws IOException {
        this.pool = new HTablePool(conf, max_size);
    }
    
    public HTablePool.PooledHTable getTable(byte[] tableName) throws IOException {
        return (HTablePool.PooledHTable) pool.getTable(tableName);
    }
    
    public HTablePool.PooledHTable getTable(String tableName) throws IOException {
        HTablePool.PooledHTable htable = null;
        try {
            htable = (HTablePool.PooledHTable) pool.getTable(tableName);
        } catch (Exception e) {
            logger.error("Get htable got exception! MSG: " + e.getMessage());
            e.printStackTrace();
            throw new IOException("HTable pool get table got exception! " + tableName);
        }
        return htable;
    }
    
    public void putTable(HTableInterface htable) throws IOException {
        if (htable != null) {
            htable.close();
        }
    }
    
    public void closeAll() throws IOException {
        this.pool.close();
    }
    
    public void closeAll(String projectId) throws IOException {
        this.pool.closeTablePool(projectId + "_deu"); 
    }
    
    public void closeAllConnections() {
       HConnectionManager.deleteAllConnections(true);
    }
}
