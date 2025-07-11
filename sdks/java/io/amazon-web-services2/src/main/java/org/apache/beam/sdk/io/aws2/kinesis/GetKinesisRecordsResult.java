/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.aws2.kinesis;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.stream.Collectors;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

/** Represents the output of 'get' operation on Kinesis stream. */
@SuppressFBWarnings(value = "CT_CONSTRUCTOR_THROW", justification = "Initialization is safe.")
class GetKinesisRecordsResult {

  private final List<KinesisRecord> records;
  private final String nextShardIterator;
  private final long millisBehindLatest;

  public GetKinesisRecordsResult(
      List<KinesisClientRecord> records,
      String nextShardIterator,
      long millisBehindLatest,
      final String streamName,
      final String shardId) {
    this.records =
        records.stream()
            .map(
                input -> {
                  assert input != null; // to make FindBugs happy
                  return new KinesisRecord(input, streamName, shardId);
                })
            .collect(Collectors.toList());
    this.nextShardIterator = nextShardIterator;
    this.millisBehindLatest = millisBehindLatest;
  }

  public List<KinesisRecord> getRecords() {
    return records;
  }

  public String getNextShardIterator() {
    return nextShardIterator;
  }

  public long getMillisBehindLatest() {
    return millisBehindLatest;
  }
}
