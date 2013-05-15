package com.xingcloud.qt.query.pool;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-5-13
 * Time: 下午4:13
 * To change this template use File | Settings | File Templates.
 */
public class UIQueryPool {

    private static Log logger = LogFactory.getLog(UIQueryPool.class);
    private ThreadPoolExecutor executor;
    private int DEFAULT_THREAD_NUM = 24;
    private long TIMEOUT = 60*60;

    private static UIQueryPool m_instance;

    private UIQueryPool() {
        logger.info("First time init ui query task pool");
        ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
        builder.setNameFormat("UI query task pool");
        builder.setDaemon(true);
        ThreadFactory factory = builder.build();
        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(DEFAULT_THREAD_NUM, factory);

    }

    public synchronized static UIQueryPool getInstance() {
        if (m_instance == null) {
            m_instance = new UIQueryPool();
        }
        return m_instance;
    }

    public ThreadPoolExecutor getPool() {
        return executor;
    }

    public static FutureTask<?> addTask(Callable<?> task) {
        return  (FutureTask<?>)UIQueryPool.getInstance().getPool().submit(task);
    }

    public synchronized void shutDownNow() {
        logger.info("------Shut down all tasks in thread pool------");
        executor.shutdown();
        /*Wait for all the tasks to finish*/
        try {
            boolean stillRunning = !executor.awaitTermination(TIMEOUT, TimeUnit.MILLISECONDS);
            if (stillRunning) {
                try {
                    executor.shutdownNow();
                } catch (Exception e) {
                    logger.error("Thread pool remain query tasks' time out of time for 30 seconds.", e);
                }
            }
        } catch (InterruptedException e) {
            try {
                Thread.currentThread().interrupt();
            } catch (Exception e1) {
                logger.error("Thread pool has been interrupted!", e);
            }
        }
    }

    public static synchronized void shutDownAllTasks() {
        UIQueryPool.getInstance().shutDownNow();
    }



}