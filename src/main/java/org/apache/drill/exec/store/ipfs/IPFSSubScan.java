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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
<<<<<<< HEAD
import com.fasterxml.jackson.core.JsonProcessingException;
=======
>>>>>>> a989ec4 ('FMT')
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
<<<<<<< HEAD
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;
import io.ipfs.multihash.Multihash;
import org.apache.drill.common.exceptions.ExecutionSetupException;
=======
import io.ipfs.cid.Cid;
import io.ipfs.multihash.Multihash;
import org.apache.drill.common.PlanStringBuilder;
>>>>>>> a989ec4 ('FMT')
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;
<<<<<<< HEAD
import org.apache.drill.exec.store.StoragePluginRegistry;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/*import org.apache.drill.common.expression.SchemaPath;*/

@JsonTypeName("ipfs-sub-scan")
public class IPFSSubScan extends AbstractBase implements SubScan {
  private static int IPFS_SUB_SCAN_VALUE = 19155;
=======
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


@JsonTypeName("ipfs-sub-scan")
public class IPFSSubScan extends AbstractBase implements SubScan {
>>>>>>> a989ec4 ('FMT')
  private final IPFSContext ipfsContext;
  private final List<Multihash> ipfsSubScanSpecList;
  private final IPFSScanSpec.Format format;
  private final List<SchemaPath> columns;


  @JsonCreator
  public IPFSSubScan(@JacksonInject StoragePluginRegistry registry,
                     @JsonProperty("IPFSStoragePluginConfig") IPFSStoragePluginConfig ipfsStoragePluginConfig,
<<<<<<< HEAD
                     @JsonProperty("IPFSSubScanSpec") @JsonDeserialize(using=MultihashDeserializer.class) List<Multihash> ipfsSubScanSpecList,
                     @JsonProperty("format") IPFSScanSpec.Format format,
                     @JsonProperty("columns") List<SchemaPath> columns
                     ) throws ExecutionSetupException {
    super((String) null);
    IPFSStoragePlugin plugin = (IPFSStoragePlugin) registry.getPlugin(ipfsStoragePluginConfig);
=======
                     @JsonProperty("IPFSSubScanSpec") @JsonDeserialize(using = MultihashDeserializer.class) List<Multihash> ipfsSubScanSpecList,
                     @JsonProperty("format") IPFSScanSpec.Format format,
                     @JsonProperty("columns") List<SchemaPath> columns
  ) {
    super((String) null);
    IPFSStoragePlugin plugin = registry.resolve(ipfsStoragePluginConfig, IPFSStoragePlugin.class);
>>>>>>> a989ec4 ('FMT')
    ipfsContext = plugin.getIPFSContext();
    this.ipfsSubScanSpecList = ipfsSubScanSpecList;
    this.format = format;
    this.columns = columns;
  }

  public IPFSSubScan(IPFSContext ipfsContext, List<Multihash> ipfsSubScanSpecList, IPFSScanSpec.Format format, List<SchemaPath> columns) {
    super((String) null);
    this.ipfsContext = ipfsContext;
    this.ipfsSubScanSpecList = ipfsSubScanSpecList;
    this.format = format;
    this.columns = columns;
  }

  @JsonIgnore
  public IPFSContext getIPFSContext() {
    return ipfsContext;
  }

  @JsonProperty("IPFSStoragePluginConfig")
  public IPFSStoragePluginConfig getIPFSStoragePluginConfig() {
    return ipfsContext.getStoragePluginConfig();
  }

  @JsonProperty("columns")
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonProperty("format")
  public IPFSScanSpec.Format getFormat() {
    return format;
  }

<<<<<<< HEAD
=======
  @Override
  public String toString() {
    return new PlanStringBuilder(this)
        .field("scan spec", ipfsSubScanSpecList)
        .field("format", format)
        .field("columns", columns)
        .toString();
  }

>>>>>>> a989ec4 ('FMT')
  @JsonSerialize(using = MultihashSerializer.class)
  @JsonProperty("IPFSSubScanSpec")
  public List<Multihash> getIPFSSubScanSpecList() {
    return ipfsSubScanSpecList;
  }

  @Override
  public <T, X, E extends Throwable> T accept(
      PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return ImmutableSet.<PhysicalOperator>of().iterator();
  }

  @Override
  public int getOperatorType() {
<<<<<<< HEAD
    return IPFS_SUB_SCAN_VALUE;
=======
    return UserBitShared.CoreOperatorType.IPFS_SUB_SCAN_VALUE;
>>>>>>> a989ec4 ('FMT')
  }

  @Override
  public boolean isExecutable() {
    return false;
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    return new IPFSSubScan(ipfsContext, ipfsSubScanSpecList, format, columns);
  }

  public static class IPFSSubScanSpec {
    private final String targetHash;

    @JsonCreator
    public IPFSSubScanSpec(@JsonProperty("targetHash") String targetHash) {
      this.targetHash = targetHash;
    }

    @JsonProperty
    public String getTargetHash() {
      return targetHash;
    }
  }

  static class MultihashSerializer extends JsonSerializer<List<Multihash>> {

    @Override
    public void serialize(List<Multihash> value, JsonGenerator jgen,
<<<<<<< HEAD
                          SerializerProvider provider) throws IOException, JsonProcessingException {
=======
                          SerializerProvider provider) throws IOException {
>>>>>>> a989ec4 ('FMT')
      jgen.writeStartArray();
      for (Multihash hash : value) {
        jgen.writeString(hash.toString());
      }
      jgen.writeEndArray();
<<<<<<< HEAD

=======
>>>>>>> a989ec4 ('FMT')
    }
  }

  static class MultihashDeserializer extends JsonDeserializer<List<Multihash>> {
    @Override
    public List<Multihash> deserialize(JsonParser jp, DeserializationContext ctxt)
<<<<<<< HEAD
      throws IOException, JsonProcessingException {
      assert jp.nextToken() == JsonToken.START_ARRAY;

      List<Multihash> multihashList = new LinkedList<>();
      while (jp.nextToken() != JsonToken.END_ARRAY) {
        String hash = jp.getValueAsString();
        multihashList.add(Multihash.fromBase58(hash));
=======
        throws IOException {
      assert jp.currentToken() == JsonToken.START_ARRAY;

      List<Multihash> multihashList = new ArrayList<>();
      while (jp.nextToken() != JsonToken.END_ARRAY) {
        String hash = jp.getValueAsString();
        multihashList.add(Cid.decode(hash));
>>>>>>> a989ec4 ('FMT')
      }
      return multihashList;
    }
  }
}
