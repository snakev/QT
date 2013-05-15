package com.xingcloud.qt.utils;

import com.xingcloud.mysql.PropType;
import com.xingcloud.mysql.UpdateFunc;
import org.apache.hadoop.hbase.util.Pair;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-5-13
 * Time: 下午3:40
 * To change this template use File | Settings | File Templates.
 */
public class Constants {
    public static final String TOTAL_USER_IDENTIFIER = "TOTAL_USER";

    /* Default attribute info */
    public static Map<String, Pair<PropType, UpdateFunc>> DEFAULT_ATTR_INFO = new HashMap<String, Pair<PropType,UpdateFunc>>(){
        {
            put("register_time", new Pair(PropType.sql_datetime, UpdateFunc.once));
            put("last_login_time", new Pair(PropType.sql_datetime, UpdateFunc.cover));
            put("first_pay_time", new Pair(PropType.sql_datetime, UpdateFunc.once));
            put("last_pay_time", new Pair(PropType.sql_datetime, UpdateFunc.cover));
            put("grade", new Pair(PropType.sql_bigint, UpdateFunc.cover));
            put("game_time", new Pair(PropType.sql_bigint, UpdateFunc.inc));
            put("pay_amount", new Pair(PropType.sql_bigint, UpdateFunc.inc));
            put("language", new Pair(PropType.sql_string, UpdateFunc.cover));
            put("version", new Pair(PropType.sql_string, UpdateFunc.cover));
            put("platform", new Pair(PropType.sql_string, UpdateFunc.cover));
            put("identifier", new Pair(PropType.sql_string, UpdateFunc.cover));
            put("ref", new Pair(PropType.sql_string, UpdateFunc.once));
            put("ref0", new Pair(PropType.sql_string, UpdateFunc.once));
            put("ref1", new Pair(PropType.sql_string, UpdateFunc.once));
            put("ref2", new Pair(PropType.sql_string, UpdateFunc.once));
            put("ref3", new Pair(PropType.sql_string, UpdateFunc.once));
            put("ref4", new Pair(PropType.sql_string, UpdateFunc.once));
            put("nation", new Pair(PropType.sql_string, UpdateFunc.once));
        }
    };

    public static final int REDIS_DB_SEGMENT_INCREMENTAL = 9;
    public static final String CONTENT_SPLIT_STRING = "#";
    public static final int CACHE_DURATION_10MIN = 600;
    public static final String NA = "XA-NA";

}
