package com.metamx.druid.aggregation;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.common.base.Throwables;
import com.metamx.druid.processing.ObjectColumnSelector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 *
 */
public class DimensionCardinalityBufferAggregator implements BufferAggregator
{
  private static final byte[] emptyBytes = new byte[DimensionCardinalityAggregator.MAX_SIZE_BYTES];

  private final String name;
  private final ObjectColumnSelector selector;

  public DimensionCardinalityBufferAggregator(
      String name,
      ObjectColumnSelector selector
  )
  {
    this.name = name;
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    ByteBuffer duplicate = buf.duplicate();
    duplicate.position(position);
    duplicate.put(emptyBytes);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    HyperLogLogPlus hll = (HyperLogLogPlus) get(buf, position);

    Object obj = selector.get();
    if (obj instanceof List) {
      for (Object element : ((List) obj)) {
        hll.offer(element);
      }
    } else {
      hll.offer(obj);
    }

    writeHll(buf, hll);
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    ByteBuffer duplicate = buf.duplicate();
    duplicate.position(position);
    int size = duplicate.getInt();

    byte[] bytes = new byte[size];
    duplicate.get(bytes);
    return DimensionCardinalityAggregator.fromBytes(bytes);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return ((HyperLogLogPlus) get(buf, position)).cardinality();
  }

  @Override
  public void close()
  {
  }

  private void writeHll(ByteBuffer buf, HyperLogLogPlus hll)
  {
    try {
      byte[] outBytes = hll.getBytes();
      ByteBuffer outBuf = buf.duplicate();
      outBuf.putInt(outBytes.length);
      outBuf.put(outBytes);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
