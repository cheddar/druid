package com.metamx.druid.index.v1.serde;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.aggregation.DimensionCardinalityAggregatorFactory;
import com.metamx.druid.index.v1.IncrementalIndex;
import com.metamx.druid.input.MapBasedInputRow;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 *
 */
public class HllPlusComplexMetricSerdeTest
{

  @Before
  public void setUp() throws Exception
  {
    ComplexMetricSerdes.registerDefaultSerdes();
  }

  @Test
  public void testSanity() throws Exception
  {
    IncrementalIndex rows = new IncrementalIndex(
        System.currentTimeMillis(),
        QueryGranularity.DAY,
        new AggregatorFactory[]{new DimensionCardinalityAggregatorFactory("billy", "billy")}
    );

    List<String> dimensions = Lists.newArrayList("yay");

    Random random = new Random(1234);
    boolean toggle = true;
    for (int i = 0; i < 100; ++i) {
      HyperLogLogPlus hll = new HyperLogLogPlus(11, 0);
      List<String> vals = Lists.newArrayList();
      for (int y = 0; y < random.nextInt(4) + 1; ++y) {
        String val = String.format("%d-howdy-%d", i, y);
        vals.add(val);
        hll.offer(val);
      }

      Map<String, Object> map = Maps.newHashMap();
      map.put("billy", toggle ? vals : hll);
      map.put("yay", random.nextBoolean() ? "hi" : "ho");

      rows.add(new MapBasedInputRow(System.currentTimeMillis(), dimensions, map));
      toggle = !toggle;
    }
  }
}
