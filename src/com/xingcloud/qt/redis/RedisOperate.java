package com.xingcloud.qt.redis;

import com.xingcloud.mysql.PropType;
import com.xingcloud.mysql.UpdateFunc;
import com.xingcloud.mysql.UserProp;
import com.xingcloud.qt.utils.Constants;
import com.xingcloud.qt.utils.QueryUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Pair;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-5-13
 * Time: 下午3:45
 * To change this template use File | Settings | File Templates.
 */
public class RedisOperate {
    private static Log logger = LogFactory.getLog(RedisOperate.class);


    public static Pair<PropType, UpdateFunc> getPropInfo(String pID, String propName) {
        /* Default properties */
        Pair<PropType, UpdateFunc> propInfo = Constants.DEFAULT_ATTR_INFO.get(propName);
        /* Query from mysql */
        if (propInfo == null) {
            ShardedJedis jedis = null;
            try {
                jedis = RedisShardedPoolResourceManager.getInstance().getCache(Constants.REDIS_DB_SEGMENT_INCREMENTAL);
                String key = new String(pID + Constants.CONTENT_SPLIT_STRING + propName);
                String typeAndFunc = jedis.get(key);

                if (typeAndFunc == null) {
                    /* Not in redis either, reload all attributes from mysql */
                    List<UserProp> propList = QueryUtils.getUserProps(pID);
                    ShardedJedisPipeline pipeLine = jedis.pipelined();
                    for (UserProp prop : propList) {
                        PropType propType = prop.getPropType();
                        UpdateFunc up = prop.getPropFunc();
                        String name = prop.getPropName();
                        pipeLine.set(pID + "-" + name, propType.toString() + Constants.CONTENT_SPLIT_STRING + up.toString());
                        if (name.equals(propName)) {
                            propInfo = new Pair<PropType, UpdateFunc>(propType, up);
                        }
                    }
                    pipeLine.sync();
                    jedis.expire(key, Constants.CACHE_DURATION_10MIN);
                } else {
                    /* Read from redis */
                    String[] fields = typeAndFunc.split(Constants.CONTENT_SPLIT_STRING);
                    if (fields.length < 2) {
                        logger.error("Get property info error!!! " + pID + " " + propName);
                        return null;
                    }
                    PropType pt = PropType.valueOf(fields[0]);
                    UpdateFunc uf = UpdateFunc.valueOf(fields[1]);
                    propInfo = new Pair<PropType, UpdateFunc>(pt, uf);
                }
                return propInfo;
            } catch (Exception e) {
                RedisShardedPoolResourceManager.getInstance().returnBrokenResource(jedis);
                jedis = null;
                logger.error("Exception when getPropInfo " + pID + " " + propName + " " + e.getMessage());
            } finally {
                if (jedis != null) {
                    RedisShardedPoolResourceManager.getInstance().returnResource(jedis);
                }
            }


        }
        return propInfo;
    }

}
