package com.ebay.hadoop.arrow.executor.agg;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.reader.FieldReader;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class HashAggregator {
  private final Map<Integer, Integer> result = new HashMap<Integer, Integer>(1024);
  private Integer nullSum = null;
  
  public void update(ValueVector keys, ValueVector values) {
    FieldReader keyReader = keys.getReader();
    FieldReader valueReader = values.getReader();
    
    
    for (int i=0; i<keys.getValueCount(); i++) {
      keyReader.setPosition(i);
      valueReader.setPosition(i);
      Integer key = keyReader.readInteger();
      Integer value = valueReader.readInteger();
      if (key == null) {
        if (value != null) {
          if (nullSum != null) {
            nullSum = value + nullSum;
          } else {
            nullSum = value;
          }
        }
      } else {
        if (value != null) {
          if (result.containsKey(key)) {
            result.put(key, result.get(key)+value);
          } else {
            result.put(key, value);
          }
        }
      }
    }
  }
  
  public Iterator<Map.Entry<Integer, Integer>> toIterator() {
    return result.entrySet().iterator();
  }
}
