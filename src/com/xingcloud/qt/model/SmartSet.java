package com.xingcloud.qt.model;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-5-13
 * Time: 下午3:59
 * To change this template use File | Settings | File Templates.
 */
public class SmartSet implements Serializable {

    private static Log LOG = LogFactory.getLog(SmartSet.class);

    // 最多在BitSet和HashSet中切换的次数
    private static final int MAX_SWITCH = 128;

    private int switched = 0;
    // record max & min key in the set
    // 计算bitset理论值大小的时候用
    private int min = Integer.MAX_VALUE;
    private int max = Integer.MIN_VALUE;

    // 记录一共put了多少个key
    // 计算HashSet理论值大小的时候用
    private int keyPutted = 0;

    private BitSet bs = new BitSet();
    private Set<Integer> set = null;

//    private boolean isUseBitSet = false;

    public SmartSet() {
    }

    public void initWithBS(BitSet bs) {
        max = bs.previousSetBit(bs.length());
        min = bs.nextSetBit(0);
        keyPutted = bs.cardinality();
        switched = 0;
        this.bs = bs;
        trySwitchSet();
    }

    public void add(int val) {
        updateMaxMinPutted(val);
        trySwitchSet();

        if(isUseBitSet()){
            bs.set(val);
        }else{
            set.add(val);
        }
    }

    private void trySwitchSet(){
        boolean useBitset =  (computeBitSetBytes() <= computeHashSetBytes());
        if(switched > MAX_SWITCH){
            return;
        }
        if(useBitset && this.bs == null){
            switched ++;
            bs = new BitSet();
            if (set != null) {
			   /* Move hashset data to bitmap */
                for (int value : set) {
                    bs.set(value);
                }
                set = null;
            }
        }else if(!useBitset && this.set == null){
            switched ++;
            set = new HashSet<Integer>();
            if (bs != null) {
				   /* Move bitmap data to hashset */
                for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i + 1)) {
                    set.add(i);
                }
                bs = null;
            }
        }
    }

    public boolean contains(int val) {
        if (isUseBitSet()) {
            if (bs == null) {
                return false;
            }
            return bs.get(val);
        } else {
            if (set == null) {
                return false;
            }
            return set.contains(val);
        }
    }

    public int size() {
        if (isUseBitSet()) {
            if (bs == null) {
                return 0;
            }
            return bs.cardinality();
        } else {
            if (set == null) {
                return 0;
            }
            return set.size();
        }
    }

    public int getMin() {
        return min;
    }

    public int getMax() {
        return max;
    }

    public BitSet getBs() {
        return bs;
    }

    public Set<Integer> getSet() {
        return set;
    }

    public boolean isUseBitSet() {
        return this.bs != null;
    }

    public static SmartSet and(SmartSet ss1, SmartSet ss2) {
        SmartSet res = new SmartSet();
        if (ss1.isUseBitSet()) {
            BitSet anotherSS1 = ss1.getBs();
            if (anotherSS1 == null) {
                return res;
            }
            if (ss2.isUseBitSet()) {
                BitSet bs = (BitSet) anotherSS1.clone();
                BitSet anotherSS2 = ss2.getBs();
                if (anotherSS2 == null) {
                    return res;
                }
                bs.and(anotherSS2);
                res.initWithBS(bs);
            } else {
                Set<Integer> anotherSS2 = ss2.getSet();
                if (anotherSS2 == null) {
                    return res;
                }
                int sizeSS1 = anotherSS1.cardinality();
                int sizeSS2 = anotherSS2.size();

                if (sizeSS1 < sizeSS2) {
                    for (int i = anotherSS1.nextSetBit(0); i >= 0; i = anotherSS1.nextSetBit(i+1)) {
                        if (anotherSS2.contains(i)) {
                            res.add(i);
                        }
                    }
                } else {
                    for (int val : anotherSS2) {
                        if (anotherSS1.get(val)) {
                            res.add(val);
                        }
                    }
                }

            }
        } else {
            Set<Integer> anotherSS1 = ss1.getSet();
            if (anotherSS1 == null) {
                return res;
            }
            if (ss2.isUseBitSet()) {
                BitSet anotherSS2 = ss2.getBs();
                if (anotherSS2 == null) {
                    return res;
                }

                int sizeSS1 = anotherSS1.size();
                int sizeSS2 = anotherSS2.cardinality();

                if (sizeSS1 < sizeSS2) {
                    for (int val : anotherSS1) {
                        if (anotherSS2.get(val)) {
                            res.add(val);
                        }
                    }
                } else {
                    for (int i=anotherSS2.nextSetBit(0); i>=0; i=anotherSS2.nextSetBit(i+1)) {
                        if (anotherSS1.contains(i)) {
                            res.add(i);
                        }
                    }
                }
            } else {
                Set<Integer> anotherSS2 = ss2.getSet();
                if (anotherSS2 == null) {
                    return res;
                }
                if (anotherSS2.size() > anotherSS1.size()) {
                    for (int val : anotherSS1) {
                        if (anotherSS2.contains(val)) {
                            res.add(val);
                        }
                    }
                } else {
                    for (int val : anotherSS2) {
                        if (anotherSS1.contains(val)) {
                            res.add(val);
                        }
                    }
                }
            }
        }
        return res;
    }

    private void updateMaxMinPutted(int val) {
        if (val > max) {
            max = val;
        }
        if (val < min) {
            min = val;
        }
        keyPutted ++;
    }

    private int computeBitSetBytes() {
        return max/8+1;
    }

    private int computeHashSetBytes() {
        return keyPutted * 32; // estimated space of HashMpa.Entry<K, V>
		/*
        if (set != null) {
            return set.contains(val) ? set.size()*4 : (set.size()+1)*4;
        } else {
            return 4;
        }
        */
    }

    @Override
    public String toString() {
        if (isUseBitSet()) {
            if (bs != null) {
                return "MIN: " + min + "\tMAX: " + max + "\tBitmap size: " + bs.cardinality();
            } else {
                return "MIN: " + min + "\tMAX: " + max + "\tBitmap size: 0";
            }
        } else {
            if (set != null) {
                return "MIN: " + min + "\tMAX: " + max + "\tHashset size: " + set.size();
            } else {
                return "MIN: " + min + "\tMAX: " + max + "\tHashset size: 0";
            }
        }
    }

    public static void main(String[] args) {
        SmartSet smartSet1 = new SmartSet();
        for (int i=0; i<8; i++) {
            smartSet1.add(i);
        }

        System.out.println(smartSet1);
        System.out.println(smartSet1.contains(3));
        System.out.println(smartSet1.contains(8));

        SmartSet smartSet2 = new SmartSet();
        smartSet2.add(2);
        smartSet2.add(6);
        smartSet2.add(10000);
        System.out.println(smartSet2);
        System.out.println(smartSet2.contains(999));

        SmartSet smartSet3 = SmartSet.and(smartSet1, smartSet2);
        System.out.println(smartSet3);
        smartSet1 = new SmartSet();
        for (int i=100000; i<100040; i++) {
            smartSet1.add(i);
        }
        System.out.println(smartSet1.contains(1));
        System.out.println(smartSet1.contains(100004));
        System.out.println(smartSet1.contains(100040));
        System.out.println(smartSet1.contains(100050));
        System.out.println(smartSet1);
    }
}
