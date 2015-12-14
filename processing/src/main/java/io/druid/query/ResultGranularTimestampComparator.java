/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query;

import com.google.common.primitives.Longs;
import io.druid.granularity.QueryGranularity;

import java.util.Comparator;

/**
 */
public class ResultGranularTimestampComparator<T> implements Comparator<Result<T>>
{
  private final QueryGranularity gran;
  private final boolean descending;

  public ResultGranularTimestampComparator(QueryGranularity granularity, boolean descending)
  {
    this.gran = granularity;
    this.descending = descending;
  }

  @Override
  public int compare(Result<T> r1, Result<T> r2)
  {
    long t1 = r1.getTimestamp().getMillis();
    long t2 = r2.getTimestamp().getMillis();
    if (t1 == t2) {
      return 0;
    }
    int compare = Longs.compare(gran.truncate(t1), gran.truncate(t2));
    return descending ? -compare : compare;
  }
}
