package com.xingcloud.qt.query;

import com.xingcloud.qt.model.DictCompressMap;
import com.xingcloud.qt.model.SmartSet;

import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-5-13
 * Time: 下午3:24
 * To change this template use File | Settings | File Templates.
 */
public interface QueryBase {

    public enum ATTR_TYPE {
        DATE, STRING, NUMERIC
    }

    public SmartSet getSegmentUidSet(String pID, String segmentJson, long startUid, long endUid);
    public DictCompressMap getUserInfo(String pID, String segmentJson, String attr, long startUid, long endUid);

}
