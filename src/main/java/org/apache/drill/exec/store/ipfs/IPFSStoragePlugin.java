<<<<<<< HEAD
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


>>>>>>> a989ec4 ('FMT')
package org.apache.drill.exec.store.ipfs;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
<<<<<<< HEAD
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;
import io.ipfs.api.IPFS;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.planner.PlannerPhase;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class IPFSStoragePlugin extends AbstractStoragePlugin {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSStoragePlugin.class);
=======
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class IPFSStoragePlugin extends AbstractStoragePlugin {
  private static final Logger logger = LoggerFactory.getLogger(IPFSStoragePlugin.class);
>>>>>>> a989ec4 ('FMT')

  private final IPFSContext ipfsContext;
  private final IPFSStoragePluginConfig pluginConfig;
  private final IPFSSchemaFactory schemaFactory;
<<<<<<< HEAD
  private final IPFS ipfsClient;

  public IPFSStoragePlugin(IPFSStoragePluginConfig config, DrillbitContext context, String name) throws IOException {
    super(context, name);
    this.ipfsClient = new IPFS(config.getHost(), config.getPort());
    this.ipfsContext = new IPFSContext(config, this, ipfsClient);
=======

  public IPFSStoragePlugin(IPFSStoragePluginConfig config, DrillbitContext context, String name) throws IOException {
    super(context, name);
    this.ipfsContext = new IPFSContext(config, this);
>>>>>>> a989ec4 ('FMT')
    this.schemaFactory = new IPFSSchemaFactory(this.ipfsContext, name);
    this.pluginConfig = config;
  }

  @Override
  public boolean supportsRead() {
    return true;
  }

  @Override
  public boolean supportsWrite() {
    return true;
  }

  @Override
  public IPFSGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
<<<<<<< HEAD
    logger.debug("IPFSStoragePlugin before getPhysicalScan");
    IPFSScanSpec spec = selection.getListWith(new ObjectMapper(), new TypeReference<IPFSScanSpec>() {});
    logger.debug("IPFSStoragePlugin getPhysicalScan with selection {}", selection);
    return new IPFSGroupScan(ipfsContext, spec, null);
=======
    return getPhysicalScan(userName, selection, (List<SchemaPath>) null);
>>>>>>> a989ec4 ('FMT')
  }

  @Override
  public IPFSGroupScan getPhysicalScan(String userName, JSONOptions selection, List<SchemaPath> columns) throws IOException {
    logger.debug("IPFSStoragePlugin before getPhysicalScan");
    IPFSScanSpec spec = selection.getListWith(new ObjectMapper(), new TypeReference<IPFSScanSpec>() {});
    logger.debug("IPFSStoragePlugin getPhysicalScan with selection {}, columns {}", selection, columns);
    return new IPFSGroupScan(ipfsContext, spec, columns);
  }

  @Override
<<<<<<< HEAD
  public Set<? extends RelOptRule> getOptimizerRules(OptimizerRulesContext optimizerContext, PlannerPhase phase) {
    switch (phase) {
      case PHYSICAL:
        return ImmutableSet.of(IPFSAggWriterPrule.INSTANCE);
      default:
        return ImmutableSet.of();
    }
  }

  public IPFS getIPFSClient() {
    return ipfsClient;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
=======
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) {
>>>>>>> a989ec4 ('FMT')
    schemaFactory.registerSchemas(schemaConfig, parent);
  }

  @Override
  public IPFSStoragePluginConfig getConfig() {
    return pluginConfig;
  }

  public IPFSContext getIPFSContext() {
    return ipfsContext;
  }
<<<<<<< HEAD

=======
>>>>>>> a989ec4 ('FMT')
}
