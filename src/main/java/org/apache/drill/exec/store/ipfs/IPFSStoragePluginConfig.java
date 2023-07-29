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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
<<<<<<< HEAD
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfigBase;
=======
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfigBase;
import org.apache.drill.shaded.guava.com.google.common.base.Objects;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableMap;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
>>>>>>> a989ec4 ('FMT')

import java.security.InvalidParameterException;
import java.util.Map;

@JsonTypeName(IPFSStoragePluginConfig.NAME)
<<<<<<< HEAD
public class IPFSStoragePluginConfig extends StoragePluginConfigBase{
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSStoragePluginConfig.class);

    public static final String NAME = "ipfs";

    private final String host;
    private final int port;

    @JsonProperty("max-nodes-per-leaf")
    private final int maxNodesPerLeaf;

    //TODO add more specific timeout configs fot different operations in IPFS,
    // eg. provider resolution, data read, etc.
    @JsonProperty("ipfs-timeouts")
    private final Map<IPFSTimeOut, Integer> ipfsTimeouts;

    @JsonIgnore
    private static final Map<IPFSTimeOut, Integer> ipfsTimeoutDefaults = ImmutableMap.of(
        IPFSTimeOut.FIND_PROV, 4,
        IPFSTimeOut.FIND_PEER_INFO, 4,
        IPFSTimeOut.FETCH_DATA, 6
    );

    public enum IPFSTimeOut {
        @JsonProperty("find-provider")
        FIND_PROV("find-provider"),
        @JsonProperty("find-peer-info")
        FIND_PEER_INFO("find-peer-info"),
        @JsonProperty("fetch-data")
        FETCH_DATA("fetch-data");

        @JsonProperty("type")
        private String which;
        IPFSTimeOut(String which) {
            this.which = which;
        }

        @JsonCreator
        public static IPFSTimeOut of(String which) {
            switch (which) {
                case "find-provider":
                    return FIND_PROV;
                case "find-peer-info":
                    return FIND_PEER_INFO;
                case "fetch-data":
                    return FETCH_DATA;
                default:
                    throw new InvalidParameterException("Unknown key for IPFS timeout config entry: " + which);
            }
        }

        @Override
        public String toString() {
            return this.which;
        }
    }

    @JsonProperty("groupscan-worker-threads")
    private final int numWorkerThreads;

    @JsonProperty
    private final Map<String, FormatPluginConfig> formats;

    @JsonCreator
    public IPFSStoragePluginConfig(
        @JsonProperty("host") String host,
        @JsonProperty("port") int port,
        @JsonProperty("max-nodes-per-leaf") int maxNodesPerLeaf,
        @JsonProperty("ipfs-timeouts") Map<IPFSTimeOut, Integer> ipfsTimeouts,
        @JsonProperty("groupscan-worker-threads") int numWorkerThreads,
        @JsonProperty("formats") Map<String, FormatPluginConfig> formats) {
        this.host = host;
        this.port = port;
        this.maxNodesPerLeaf = maxNodesPerLeaf > 0 ? maxNodesPerLeaf : 1;
        //TODO Jackson failed to deserialize the ipfsTimeouts map causing NPE
        if (ipfsTimeouts != null) {
            ipfsTimeoutDefaults.forEach(ipfsTimeouts::putIfAbsent);
        } else {
            ipfsTimeouts = ipfsTimeoutDefaults;
        }
        this.ipfsTimeouts = ipfsTimeouts;
        this.numWorkerThreads = numWorkerThreads > 0 ? numWorkerThreads : 1;
        this.formats = formats;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getMaxNodesPerLeaf() {
        return maxNodesPerLeaf;
    }

    public int getIpfsTimeout(IPFSTimeOut which) {
        return ipfsTimeouts.get(which);
    }

    public Map<IPFSTimeOut, Integer> getIpfsTimeouts() {
        return ipfsTimeouts;
    }

    public int getNumWorkerThreads() {
        return numWorkerThreads;
    }

    public Map<String, FormatPluginConfig> getFormats() {
        return formats;
    }

    @Override
    public int hashCode() {
        String host_port = String.format("%s:%d[%d,%s]", host, port, maxNodesPerLeaf, ipfsTimeouts);
        final int prime = 31;
        int result = 1;
        result = prime * result + ((host_port == null) ? 0 : host_port.hashCode());
        result = prime * result + ((formats == null) ? 0 : formats.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        IPFSStoragePluginConfig other = (IPFSStoragePluginConfig) obj;
        if (formats == null) {
            if (other.formats != null) {
                return false;
            }
        } else if (!formats.equals(other.formats)) {
            return false;
        }
        if (host == null) {
            if (other.host != null) {
                return false;
            }
        } else if (!host.equals(other.host)
            || port != other.port
            || maxNodesPerLeaf != other.maxNodesPerLeaf
            || ipfsTimeouts != other.ipfsTimeouts
            || numWorkerThreads != other.numWorkerThreads ) {
            return false;
        }
        return true;
    }
=======
public class IPFSStoragePluginConfig extends StoragePluginConfigBase {
  public static final String NAME = "ipfs";

  @JsonProperty
  private final String host;

  @JsonProperty
  private final int port;

  @JsonProperty("max-nodes-per-leaf")
  private final int maxNodesPerLeaf;

  @JsonProperty("distributed-mode")
  private final boolean distributedMode;

  @JsonProperty("modified-Merkle-tree")
  private final boolean modifiedTree;

  @JsonProperty("top-hash-data-size")
  private final int topHahsDataSize;

  @JsonProperty("ipfs-timeouts")
  private final Map<IPFSTimeOut, Integer> ipfsTimeouts;

  @JsonProperty("ipfs-caches")
  private final Map<IPFSCacheType, IPFSCache> ipfsCaches;

  @JsonIgnore
  public static final Map<IPFSTimeOut, Integer> ipfsTimeoutDefaults = ImmutableMap.of(
      IPFSTimeOut.FIND_PROV, 4,
      IPFSTimeOut.FIND_PEER_INFO, 4,
      IPFSTimeOut.FETCH_DATA, 6
  );

  @JsonIgnore
  public static final Map<IPFSCacheType, IPFSCache> ipfsCacheDefaults = ImmutableMap.of(
      IPFSCacheType.PEER, new IPFSCache(1000, 600),
      IPFSCacheType.PROVIDER, new IPFSCache(1000, 600)
  );

  public enum IPFSTimeOut {
    @JsonProperty("find-provider")
    FIND_PROV("find-provider"),
    @JsonProperty("find-peer-info")
    FIND_PEER_INFO("find-peer-info"),
    @JsonProperty("fetch-data")
    FETCH_DATA("fetch-data");

    @JsonProperty("type")
    private final String which;

    IPFSTimeOut(String which) {
      this.which = which;
    }

    @JsonCreator
    public static IPFSTimeOut of(String which) {
      switch (which) {
        case "find-provider":
          return FIND_PROV;
        case "find-peer-info":
          return FIND_PEER_INFO;
        case "fetch-data":
          return FETCH_DATA;
        default:
          throw new InvalidParameterException("Unknown key for IPFS timeout config entry: " + which);
      }
    }

    @Override
    public String toString() {
      return this.which;
    }
  }

  public enum IPFSCacheType {
    @JsonProperty("peer")
    PEER("peer"),
    @JsonProperty("provider")
    PROVIDER("provider");

    @JsonProperty("type")
    private final String which;

    IPFSCacheType(String which) {
      this.which = which;
    }

    @JsonCreator
    public static IPFSCacheType of(String which) {
      switch (which) {
        case "peer":
          return PEER;
        case "provider":
          return PROVIDER;
        default:
          throw new InvalidParameterException("Unknown key for cache config entry: " + which);
      }
    }

    @Override
    public String toString() {
      return this.which;
    }
  }

  public static class IPFSCache {
    @JsonProperty
    public final int size;
    @JsonProperty
    public final int ttl;

    @JsonCreator
    public IPFSCache(@JsonProperty("size") int size, @JsonProperty("ttl") int ttl) {
      Preconditions.checkArgument(size >= 0 && ttl > 0);
      this.size = size;
      this.ttl = ttl;
    }
  }

  @JsonProperty("groupscan-worker-threads")
  private final int numWorkerThreads;

  @JsonProperty
  private final Map<String, FormatPluginConfig> formats;

  @JsonCreator
  public IPFSStoragePluginConfig(
      @JsonProperty("host") String host,
      @JsonProperty("port") int port,
      @JsonProperty("max-nodes-per-leaf") int maxNodesPerLeaf,
      @JsonProperty("distributed-mode") boolean distributedMode,
      @JsonProperty("modified-Merkle-tree") boolean ModifiedTree,
      @JsonProperty("top-hash-data-size") int topHahsDataSize,
      @JsonProperty("ipfs-timeouts") Map<IPFSTimeOut, Integer> ipfsTimeouts,
      @JsonProperty("ipfs-caches") Map<IPFSCacheType, IPFSCache> ipfsCaches,
      @JsonProperty("groupscan-worker-threads") int numWorkerThreads,
      @JsonProperty("formats") Map<String, FormatPluginConfig> formats) {
    this.host = host;
    this.port = port;
    this.maxNodesPerLeaf = maxNodesPerLeaf > 0 ? maxNodesPerLeaf : 1;
    this.distributedMode = distributedMode;
    this.modifiedTree=ModifiedTree;
    this.ipfsTimeouts = applyDefaultMap(ipfsTimeouts, ipfsTimeoutDefaults);
    this.ipfsCaches = applyDefaultMap(ipfsCaches, ipfsCacheDefaults);
    this.numWorkerThreads = numWorkerThreads > 0 ? numWorkerThreads : 1;
    this.formats = formats;
    this.topHahsDataSize = topHahsDataSize;
  }

  private static <K, V> Map<K, V> applyDefaultMap(Map<K, V> supplied, Map<K, V> defaults) {
    Map<K, V> ret;
    if (supplied == null) {
      ret = defaults;
    } else {
      ret = Maps.newHashMap();
      supplied.forEach(ret::put);
      defaults.forEach(ret::putIfAbsent);
    }
    return ret;
  }

  @JsonProperty
  public String getHost() {
    return host;
  }

  @JsonProperty
  public int getPort() {
    return port;
  }

  @JsonProperty("max-nodes-per-leaf")
  public int getMaxNodesPerLeaf() {
    return maxNodesPerLeaf;
  }

  @JsonProperty("distributed-mode")
  public boolean isDistributedMode() {
    return distributedMode;
  }

  @JsonIgnore
  public int getIPFSTimeout(IPFSTimeOut which) {
    return ipfsTimeouts.get(which);
  }

  @JsonIgnore
  public int getTopHahsDataSize() {return topHahsDataSize;}

  @JsonIgnore
  public IPFSCache getIPFSCache(IPFSCacheType which) {
    return ipfsCaches.get(which);
  }

  @JsonProperty("ipfs-timeouts")
  public Map<IPFSTimeOut, Integer> getIPFSTimeouts() {
    return ipfsTimeouts;
  }

  @JsonProperty("ipfs-caches")
  public Map<IPFSCacheType, IPFSCache> getIPFSCaches() {
    return ipfsCaches;
  }

  @JsonProperty("modified-Merkle-tree")
  public boolean getModifiedMerkleTree(){return modifiedTree;}

  @JsonProperty("groupscan-worker-threads")
  public int getNumWorkerThreads() {
    return numWorkerThreads;
  }

  @JsonProperty
  public Map<String, FormatPluginConfig> getFormats() {
    return formats;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(host, port, maxNodesPerLeaf, distributedMode, ipfsTimeouts, ipfsCaches, formats);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    IPFSStoragePluginConfig other = (IPFSStoragePluginConfig) obj;
    return Objects.equal(formats, other.formats)
        && Objects.equal(host, other.host)
        && Objects.equal(ipfsTimeouts, other.ipfsTimeouts)
        && Objects.equal(ipfsCaches, other.ipfsTimeouts)
        && port == other.port
        && maxNodesPerLeaf == other.maxNodesPerLeaf
        && distributedMode == other.distributedMode
        && numWorkerThreads == other.numWorkerThreads;
  }
>>>>>>> a989ec4 ('FMT')
}
