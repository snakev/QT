package com.xingcloud.qt.query.hbase.filter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: hadoop
 * Date: 5/17/13
 * Time: 3:33 AM
 * To change this template use File | Settings | File Templates.
 */
public class QualifierUidFilter extends FilterBase {
  private static Log LOG = LogFactory.getLog(QualifierUidFilter.class);

  private byte[] startUidOfBytes5;
  private byte[] endUidOfBytes5;


  public QualifierUidFilter(long startUid, long endUid) {
    byte[] sub = Bytes.toBytes(startUid);
    byte[] eub = Bytes.toBytes(endUid);

    this.startUidOfBytes5 = Arrays.copyOfRange(sub, 3, sub.length);
    this.endUidOfBytes5 = Arrays.copyOfRange(eub, 3, eub.length);
  }

  @Override
  public Filter.ReturnCode filterKeyValue(KeyValue kv) {
    byte[] uid = kv.getQualifier();
    if(Bytes.compareTo(uid, endUidOfBytes5) > 0){
      LOG.info(kv.getRow()+"\t"+kv.getQualifier());
      return Filter.ReturnCode.NEXT_ROW;
    }else if (Bytes.compareTo(uid, startUidOfBytes5) < 0 ){
      return ReturnCode.SEEK_NEXT_USING_HINT;
    }
    return Filter.ReturnCode.INCLUDE;
  }

  @Override
  public KeyValue getNextKeyHint(KeyValue kv) {
      KeyValue newKV = new KeyValue(kv.getRow(), kv.getFamily(), startUidOfBytes5);
      return KeyValue.createFirstOnRow(newKV.getBuffer(), newKV.getRowOffset(), newKV
        .getRowLength(), newKV.getBuffer(), newKV.getFamilyOffset(), newKV
        .getFamilyLength(), null, 0, 0);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    startUidOfBytes5 = Bytes.readByteArray(in);
    endUidOfBytes5 = Bytes.readByteArray(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, startUidOfBytes5);
    Bytes.writeByteArray(out, endUidOfBytes5);
  } 
}
