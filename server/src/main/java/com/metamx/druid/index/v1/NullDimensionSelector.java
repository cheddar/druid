package com.metamx.druid.index.v1;

import com.metamx.druid.index.v1.processing.DimensionSelector;
import com.metamx.druid.kv.IndexedInts;
import com.metamx.druid.kv.SingleIndexedInts;

public class NullDimensionSelector implements DimensionSelector
{
  @Override
  public IndexedInts getRow()
  {
    return new SingleIndexedInts(0);
  }

  @Override
  public int getValueCardinality()
  {
    return 1;
  }

  @Override
  public String lookupName(int id)
  {
    return null;
  }

  @Override
  public int lookupId(String name)
  {
    return 0;
  }
}
