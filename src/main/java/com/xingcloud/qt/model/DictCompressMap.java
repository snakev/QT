package com.xingcloud.qt.model;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-4-26
 * Time: 下午7:20
 * To change this template use File | Settings | File Templates.
 */
public class DictCompressMap implements Serializable {
    private Log LOG = LogFactory.getLog(DictCompressMap.class);

    private AtomicInteger innerID = new AtomicInteger(1);
    private Map<Integer, String> dictIO = new HashMap<Integer, String>();
    private Map<String, Integer> dictOI = new HashMap<String, Integer>();

    private Int2IntOpenHashMap info = new Int2IntOpenHashMap();

    protected int index = 0;

    public void put(int key, String val) {
        Integer inner = dictOI.get(val);
        if (inner == null) {
            info.put(key, innerID.get());
            dictIO.put(innerID.get(), val);
            dictOI.put(val, innerID.getAndIncrement());
        } else {
            info.put(key, inner.intValue());
        }
    }

    public boolean containsKey(int key) {
        return info.containsKey(key);
    }

    public String get(int key) {
        int inner = info.get(key);
        if (inner==0) {
            return null;
        }
        return dictIO.get(inner);
    }

    public int[] keys() {
        return info.keySet().toIntArray();
    }

    public int size() {
        return info.size();
    }
}
