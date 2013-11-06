package com.metamx.druid.aggregation;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.processing.ObjectColumnSelector;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public class DimensionCardinalityAggregator implements Aggregator
{
  static final int MAX_SIZE_BYTES = 1381;

  static final HyperLogLogPlus makeHllPlus()
  {
    return new HyperLogLogPlus(11, 0);
  }

  static final HyperLogLogPlus fromBytes(byte[] bytes)
  {
    try {
      return HyperLogLogPlus.Builder.build(bytes);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private final String name;
  private final ObjectColumnSelector selector;

  private volatile HyperLogLogPlus hllPlus = null;

  public DimensionCardinalityAggregator(
      String name,
      ObjectColumnSelector selector
  )
  {
    this.name = name;
    this.selector = selector;

    this.hllPlus = makeHllPlus();
  }

  @Override
  public void aggregate()
  {
    Object obj = selector.get();
    if (obj instanceof List) {
      for (Object element : ((List) obj)) {
        hllPlus.offer(element);
      }
    } else {
      hllPlus.offer(obj);
    }
  }

  @Override
  public void reset()
  {
    hllPlus = makeHllPlus();
  }

  @Override
  public Object get()
  {
    return hllPlus;
  }

  @Override
  public float getFloat()
  {
    return hllPlus.cardinality();
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public void close()
  {

  }
}
