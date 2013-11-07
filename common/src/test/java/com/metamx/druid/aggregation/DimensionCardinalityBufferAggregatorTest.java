package com.metamx.druid.aggregation;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.common.collect.Lists;
import com.metamx.druid.processing.ObjectColumnSelector;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 *
 */
public class DimensionCardinalityBufferAggregatorTest
{
  @Test
  public void testSanity() throws Exception
  {
    final Object[] val = new Object[]{0};

    DimensionCardinalityBufferAggregator agg = new DimensionCardinalityBufferAggregator(new ObjectColumnSelector()
    {
      @Override
      public Class classOfObject()
      {
        return Object.class;
      }

      @Override
      public Object get()
      {
        return val[0];
      }
    });

    int offset = 1029;
    ByteBuffer buf = ByteBuffer.allocate(new DimensionCardinalityAggregatorFactory("", "").getMaxIntermediateSize() + offset);
    agg.init(buf, offset);
    for (int i = 0; i < 1000; ++i) {
      val[0] = i;
      agg.aggregate(buf, offset);
    }
    val[0] = null;
    agg.aggregate(buf, offset);
    Assert.assertEquals(1023, ((HyperLogLogPlus) agg.get(buf, offset)).cardinality());
  }
}
