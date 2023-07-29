<<<<<<< HEAD
package org.apache.drill.exec.store.ipfs;

import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Writer;
import org.apache.drill.exec.planner.logical.CreateTableEntry;
=======
/*
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

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
>>>>>>> a989ec4 ('FMT')
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;
<<<<<<< HEAD
import org.apache.drill.exec.store.StorageStrategy;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
=======
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
>>>>>>> a989ec4 ('FMT')
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

<<<<<<< HEAD
public class IPFSSchemaFactory implements SchemaFactory{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSSchemaFactory.class);
=======
public class IPFSSchemaFactory implements SchemaFactory {
  private static final Logger logger = LoggerFactory.getLogger(IPFSSchemaFactory.class);
>>>>>>> a989ec4 ('FMT')

  final String schemaName;
  final IPFSContext context;

<<<<<<< HEAD
  public IPFSSchemaFactory(IPFSContext context, String name) throws IOException {
=======
  public IPFSSchemaFactory(IPFSContext context, String name) {
>>>>>>> a989ec4 ('FMT')
    this.context = context;
    this.schemaName = name;
  }

  @Override
<<<<<<< HEAD
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
=======
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) {
>>>>>>> a989ec4 ('FMT')
    logger.debug("registerSchemas {}", schemaName);
    IPFSTables schema = new IPFSTables(schemaName);
    SchemaPlus hPlus = parent.add(schemaName, schema);
    schema.setHolder(hPlus);
  }

  class IPFSTables extends AbstractSchema {
<<<<<<< HEAD
    private Set<String> tableNames = Sets.newHashSet();
    private final ConcurrentMap<String, Table> tables = new ConcurrentSkipListMap<>(String::compareToIgnoreCase);
    public IPFSTables (String name) {
      super(ImmutableList.<String>of(), name);
      tableNames.add(name);
    }

    public void setHolder(SchemaPlus pulsOfThis) {
=======
    private final Set<String> tableNames = Sets.newHashSet();
    private final ConcurrentMap<String, Table> tables = new ConcurrentSkipListMap<>(String::compareToIgnoreCase);

    public IPFSTables(String name) {
      super(ImmutableList.of(), name);
      tableNames.add(name);
    }

    public void setHolder(SchemaPlus plusOfThis) {
>>>>>>> a989ec4 ('FMT')
    }

    @Override
    public String getTypeName() {
      return IPFSStoragePluginConfig.NAME;
    }

    @Override
    public Set<String> getTableNames() {
      return Collections.emptySet();
    }

    @Override
    public Table getTable(String tableName) {
<<<<<<< HEAD
      //TODO: better handling of table names
=======
      //DRILL-7766: handle placeholder table name when the table is yet to create
>>>>>>> a989ec4 ('FMT')
      logger.debug("getTable in IPFSTables {}", tableName);
      if (tableName.equals("create")) {
        return null;
      }

      IPFSScanSpec spec = new IPFSScanSpec(context, tableName);
      return tables.computeIfAbsent(name,
          n -> new DynamicDrillTable(context.getStoragePlugin(), schemaName, spec));
    }

    @Override
    public AbstractSchema getSubSchema(String name) {
      return null;
    }

    @Override
    public Set<String> getSubSchemaNames() {
      return Collections.emptySet();
    }

    @Override
<<<<<<< HEAD
    public CreateTableEntry createNewTable(String tableName, List<String> partitionColumns, StorageStrategy storageStrategy) {
      return createNewTable(tableName, partitionColumns);
    }

    @Override
    public CreateTableEntry createNewTable(final String tableName, List<String> partitionColumns) {
      return new CreateTableEntry(){

        @Override
        public Writer getWriter(PhysicalOperator child) throws IOException {
          return new IPFSWriter(child, tableName, context);
        }

        @Override
        public List<String> getPartitionColumns() {
          return Collections.emptyList();
        }

      };
    }

    @Override
=======
>>>>>>> a989ec4 ('FMT')
    public boolean isMutable() {
      logger.debug("IPFS Schema isMutable called");
      return true;
    }
  }
}
