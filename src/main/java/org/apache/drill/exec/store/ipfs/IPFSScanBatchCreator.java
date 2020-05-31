/*
 * Copyright (c) 2018-2020 Bowen Ding, Yuedong Xu, Liang Wang
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.drill.exec.store.ipfs;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import io.ipfs.multihash.Multihash;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.ExecutorFragmentContext;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;
import java.util.LinkedList;
import java.util.List;

public class IPFSScanBatchCreator implements BatchCreator<IPFSSubScan> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSScanBatchCreator.class);

  @Override
  public ScanBatch getBatch(ExecutorFragmentContext context, IPFSSubScan subScan, List<RecordBatch> children)
      throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    List<RecordReader> readers = new LinkedList<>();
    List<SchemaPath> columns = null;
    logger.debug(String.format("subScanSpecList.size = %d", subScan.getIPFSSubScanSpecList().size()));

    for (Multihash scanSpec : subScan.getIPFSSubScanSpecList()) {
      try {
        //FIXME what are columns and what are they for?
        if ((columns = subScan.getColumns())==null) {
          columns = GroupScan.ALL_COLUMNS;
        }
        RecordReader reader;
        //if (subScan.getFormat() == IPFSScanSpec.Format.JSON) {
        reader = new IPFSJSONRecordReader(context, subScan.getIPFSContext(), scanSpec.toString(), columns);
        readers.add(reader);
        //}
      } catch (Exception e1) {
        throw new ExecutionSetupException(e1);
      }
    }
    return new ScanBatch(subScan, context, readers);
  }
}
