<<<<<<< HEAD
package org.apache.drill.exec.store.ipfs;


import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.shaded.guava.com.google.common.cache.LoadingCache;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.ArrayListMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.ListMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import io.ipfs.api.MerkleNode;
import io.ipfs.multihash.Multihash;
import org.apache.drill.common.exceptions.ExecutionSetupException;
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


import com.fasterxml.jackson.annotation.*;
import io.ipfs.api.MerkleNode;
import io.ipfs.cid.Cid;
import io.ipfs.multihash.Multihash;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.exceptions.UserException;
>>>>>>> a989ec4 ('FMT')
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.util.DrillVersionInfo;
import org.apache.drill.exec.coord.ClusterCoordinator;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
<<<<<<< HEAD
import org.apache.drill.exec.store.schedule.AffinityCreator;
import org.apache.drill.exec.store.schedule.AssignmentCreator;
import org.apache.drill.exec.store.schedule.CompleteWork;
import org.apache.drill.exec.store.schedule.EndpointByteMap;
import org.apache.drill.exec.store.schedule.EndpointByteMapImpl;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
=======
import org.apache.drill.exec.store.schedule.*;
import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.shaded.guava.com.google.common.cache.LoadingCache;
import org.apache.drill.shaded.guava.com.google.common.collect.ArrayListMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.ListMultimap;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
>>>>>>> a989ec4 ('FMT')
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

<<<<<<< HEAD
import static org.apache.drill.exec.store.ipfs.IPFSStoragePluginConfig.IPFSTimeOut.FETCH_DATA;

@JsonTypeName("ipfs-scan")
public class IPFSGroupScan extends AbstractGroupScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSGroupScan.class);
  private IPFSContext ipfsContext;
  private IPFSScanSpec ipfsScanSpec;
  private IPFSStoragePluginConfig config;
  private List<SchemaPath> columns;

  private static long DEFAULT_NODE_SIZE = 1000l;

  private ListMultimap<Integer, IPFSWork> assignments;
  private List<IPFSWork> ipfsWorkList = Lists.newArrayList();
  private Map<String, List<IPFSWork>> endpointWorksMap;
  private List<EndpointAffinity> affinities;

  @JsonCreator
  public IPFSGroupScan(@JsonProperty("IPFSScanSpec") IPFSScanSpec ipfsScanSpec,
                       @JsonProperty("IPFSStoragePluginConfig") IPFSStoragePluginConfig ipfsStoragePluginConfig,
                       @JsonProperty("columns") List<SchemaPath> columns,
                       @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException, ExecutionSetupException {
    this(
        ((IPFSStoragePlugin) pluginRegistry.getPlugin(ipfsStoragePluginConfig)).getIPFSContext(),
        ipfsScanSpec,
        columns
    );
  }

  public IPFSGroupScan(IPFSContext ipfsContext,
                       IPFSScanSpec ipfsScanSpec,
                       List<SchemaPath> columns) {
    super((String) null);
    this.ipfsContext = ipfsContext;
    this.ipfsScanSpec = ipfsScanSpec;
    this.config = ipfsContext.getStoragePluginConfig();
    logger.debug("GroupScan constructor called with columns {}", columns);
    this.columns = columns == null || columns.size() == 0? ALL_COLUMNS : columns;
    init();
  }

  private void init() {
    IPFSHelper ipfsHelper = ipfsContext.getIPFSHelper();
    ipfsHelper.setMaxPeersPerLeaf(config.getMaxNodesPerLeaf());
    ipfsHelper.setTimeouts(config.getIpfsTimeouts());
    endpointWorksMap = new HashMap<>();

    Multihash topHash = ipfsScanSpec.getTargetHash(ipfsHelper);
    LoadingCache<Multihash, IPFSPeer> peerMap = ipfsContext.getIPFSPeerCache();

    try {
      //TODO detect and warn about loops/recursions in a malformed tree
      class IPFSTreeFlattener extends RecursiveTask<Map<Multihash, String>> {
        private Multihash hash;
        private boolean isProvider;
        private Map<Multihash, String> ret = new LinkedHashMap<>();

        public IPFSTreeFlattener(Multihash hash, boolean isProvider) {
          this.hash = hash;
          this.isProvider = isProvider;
        }

        @Override
        public Map<Multihash, String> compute() {
          try {
            if (isProvider) {
              IPFSPeer peer = peerMap.getUnchecked(hash);
              ret.put(hash, peer.hasDrillbitAddress() ? peer.getDrillbitAddress().get() : null);
              return ret;
            }

            MerkleNode metaOrSimpleNode = ipfsHelper.timedFailure(ipfsHelper.getClient().object::links, hash, config.getIpfsTimeout(FETCH_DATA));
            if (metaOrSimpleNode.links.size() > 0) {
              logger.debug("{} is a meta node", hash);
              //TODO do something useful with leaf size, e.g. hint Drill about operation costs
              List<Multihash> intermediates = metaOrSimpleNode.links.stream().map(x -> x.hash).collect(Collectors.toList());

              ImmutableList.Builder<IPFSTreeFlattener> builder = ImmutableList.builder();
              for (Multihash intermediate : intermediates.subList(1, intermediates.size())) {
                builder.add(new IPFSTreeFlattener(intermediate, false));
              }
              ImmutableList<IPFSTreeFlattener> subtasks = builder.build();
              subtasks.forEach(IPFSTreeFlattener::fork);

              IPFSTreeFlattener first = new IPFSTreeFlattener(intermediates.get(0), false);
              ret.putAll(first.compute());
              subtasks.reverse().forEach(
                  subtask -> ret.putAll(subtask.join())
              );

            } else {
              logger.debug("{} is a simple node", hash);
              List<IPFSPeer> providers = ipfsHelper.findprovsTimeout(hash).stream()
                  .map(id ->
                    peerMap.getUnchecked(id)
                  )
                  .collect(Collectors.toList());
              //FIXME isDrillReady may block threads
              providers = providers.stream()
                  .filter(IPFSPeer::isDrillReady)
                  .collect(Collectors.toList());
              if (providers.size() < 1) {
                logger.warn("No drill-ready provider found for leaf {}, adding foreman as the provider", hash);
                providers.add(ipfsContext.getMyself());
              }

              logger.debug("Got {} providers for {} from IPFS", providers.size(), hash);
              ImmutableList.Builder<IPFSTreeFlattener> builder = ImmutableList.builder();
              for (IPFSPeer provider : providers.subList(1, providers.size())) {
                builder.add(new IPFSTreeFlattener(provider.getId(), true));
              }
              ImmutableList<IPFSTreeFlattener> subtasks = builder.build();
              subtasks.forEach(IPFSTreeFlattener::fork);

              List<String> possibleAddrs = new LinkedList<>();
              Multihash firstProvider = providers.get(0).getId();
              IPFSTreeFlattener firstTask = new IPFSTreeFlattener(firstProvider, true);
              String firstAddr = firstTask.compute().get(firstProvider);
              if (firstAddr != null) {
                possibleAddrs.add(firstAddr);
              }

              subtasks.reverse().forEach(
                  subtask -> {
                    String addr = subtask.join().get(subtask.hash);
                    if (addr != null) {
                      possibleAddrs.add(addr);
                    }
                  }
              );

              if (possibleAddrs.size() < 1) {
                logger.error("All attempts to find an appropriate provider address for {} have failed", hash);
                throw new RuntimeException("No address found for any provider for leaf " + hash);
              } else {
                Random random = new Random();
                String chosenAddr = possibleAddrs.get(random.nextInt(possibleAddrs.size()));
                ret.clear();
                ret.put(hash, chosenAddr);
                logger.debug("Got peer host {} for leaf {}", chosenAddr, hash);
              }
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          return ret;
        }
      }

      logger.debug("start to recursively expand nested IPFS hashes, topHash={}", topHash);

      Stopwatch watch = Stopwatch.createStarted();
      //FIXME parallelization width magic number, maybe a config entry?
      ForkJoinPool forkJoinPool = new ForkJoinPool(config.getNumWorkerThreads());
      ipfsHelper.setExecutorService(Executors.newCachedThreadPool());
      IPFSTreeFlattener topTask = new IPFSTreeFlattener(topHash, false);
      Map<Multihash, String> leafAddrMap = forkJoinPool.invoke(topTask);

      logger.debug("Took {} ms to expand hash leaves", watch.elapsed(TimeUnit.MILLISECONDS));
      logger.debug("Iterating on {} leaves...", leafAddrMap.size());
      ClusterCoordinator coordinator = ipfsContext.getStoragePlugin().getContext().getClusterCoordinator();
      for (Multihash leaf : leafAddrMap.keySet()) {
        String peerHostname = leafAddrMap.get(leaf);

        Optional<DrillbitEndpoint> oep = coordinator.getAvailableEndpoints()
            .stream()
            .filter(a -> a.getAddress().equals(peerHostname))
            .findAny();
        DrillbitEndpoint ep;
        if (oep.isPresent()) {
          ep = oep.get();
          logger.debug("Using existing endpoint {}", ep.getAddress());
        } else {
          logger.debug("created new endpoint on the fly {}", peerHostname);
          //TODO read ports & version info from IPFS instead of hard-coded
          ep = DrillbitEndpoint.newBuilder()
              .setAddress(peerHostname)
              .setUserPort(31010)
              .setControlPort(31011)
              .setDataPort(31012)
              .setHttpPort(8047)
              .setVersion(DrillVersionInfo.getVersion())
              .setState(DrillbitEndpoint.State.ONLINE)
              .build();
          //TODO how to safely remove endpoints that are no longer needed once the query is completed?
          ClusterCoordinator.RegistrationHandle handle = coordinator.register(ep);
        }

        IPFSWork work = new IPFSWork(leaf.toBase58());
        logger.debug("added endpoint {} to work {}", ep.getAddress(), work);
        work.getByteMap().add(ep, DEFAULT_NODE_SIZE);
        work.setOnEndpoint(ep);

        if(endpointWorksMap.containsKey(ep.getAddress())) {
          endpointWorksMap.get(ep.getAddress()).add(work);
        } else {
          List<IPFSWork> ipfsWorks = Lists.newArrayList();
          ipfsWorks.add(work);
          endpointWorksMap.put(ep.getAddress(), ipfsWorks);
        }
        ipfsWorkList.add(work);
      }
    }catch (Exception e) {
      logger.debug("exception in init");
      throw new RuntimeException(e);
    }
  }

  private IPFSGroupScan(IPFSGroupScan that) {
    super(that);
    this.ipfsContext = that.ipfsContext;
    this.ipfsScanSpec = that.ipfsScanSpec;
    this.config = that.config;
    this.assignments = that.assignments;
    this.ipfsWorkList = that.ipfsWorkList;
    this.endpointWorksMap = that.endpointWorksMap;
    this.columns = that.columns;
  }

  @JsonProperty
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonIgnore
  public IPFSStoragePlugin getStoragePlugin() {
    return ipfsContext.getStoragePlugin();
  }

  @JsonProperty
  public IPFSScanSpec getIPFSScanSpec() {
    return ipfsScanSpec;
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    if (affinities == null) {
      affinities = AffinityCreator.getAffinityMap(ipfsWorkList);
    }
    return affinities;
  }

  @Override
  public int getMaxParallelizationWidth() {
    DrillbitEndpoint myself = ipfsContext.getStoragePlugin().getContext().getEndpoint();
    int width;
    if (endpointWorksMap.containsKey(myself.getAddress())) {
      // the foreman is also going to be a minor fragment worker under a UnionExchange operator
      width = ipfsWorkList.size();
    } else {
      // the foreman does not hold data, so we have to force parallelization
      // to make sure there is a UnionExchange operator
      width = ipfsWorkList.size() + 1;
    }
    logger.debug("getMaxParallelizationWidth: {}", width);
    return width;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> incomingEndpoints) {
    logger.debug("ipfsWorkList.size() = {}", ipfsWorkList.size());
    logger.debug("endpointWorksMap: {}", endpointWorksMap);
    if (endpointWorksMap.size()>1) { //偶尔还会出错？
      //incomingEndpoints是已经排好顺序的endpoints,和fragment 顺序对应
      logger.debug("Use manual assignment");
      assignments = ArrayListMultimap.create();
      for (int fragmentId = 0; fragmentId < incomingEndpoints.size(); fragmentId++) {
        String address = incomingEndpoints.get(fragmentId).getAddress();
        if (endpointWorksMap.containsKey(address)) { //如果对应的节点有工作
          for (IPFSWork work : endpointWorksMap.get(address)) {
            assignments.put(fragmentId, work);
          }
        } else //如果对应的节点没有工作安排，分配一个空work
        {

        }
      }
    }
    else //如果出问题，按照系统默认分配模式？
    {
     logger.debug("Use AssignmentCreator");
      assignments = AssignmentCreator.getMappings(incomingEndpoints, ipfsWorkList);
    }

    for (int i = 0; i < incomingEndpoints.size(); i++) {
      logger.debug("Fragment {} on endpoint {} is assigned with works: {}", i, incomingEndpoints.get(i).getAddress(), assignments.get(i));
    }
  }

  @Override
  public IPFSSubScan getSpecificScan(int minorFragmentId) {
    logger.debug(String.format("getSpecificScan: minorFragmentId = %d", minorFragmentId));
    List<IPFSWork> workList = assignments.get(minorFragmentId);
    logger.debug("workList == null: " + (workList == null? "true": "false"));
    logger.debug(String.format("workList.size(): %d", workList.size()));

    List<Multihash> scanSpecList = Lists.newArrayList();

    for (IPFSWork work : workList) {
      scanSpecList.add(work.getPartialRootHash());
    }

    return new IPFSSubScan(ipfsContext, scanSpecList, ipfsScanSpec.getFormatExtension(), columns);
  }

  @Override
  public ScanStats getScanStats() {
    //FIXME why 100000 * size?
    long recordCount = 100000 * endpointWorksMap.size();
    return new ScanStats(ScanStats.GroupScanProperty.NO_EXACT_ROW_COUNT, recordCount, 1, recordCount);
  }

  @Override
  public IPFSGroupScan clone(List<SchemaPath> columns){
    logger.debug("IPFSGroupScan clone {}", columns);
    IPFSGroupScan cloned = new IPFSGroupScan(this);
    cloned.columns = columns;
    return cloned;
  }

  @Override
  @JsonIgnore
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    //FIXME what does this mean?
    return true;
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    logger.debug("getNewWithChildren called");
    return new IPFSGroupScan(this);
  }



  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public String toString() {
    return "IPFSGroupScan [IPFSScanSpec=" + ipfsScanSpec + ", columns=" + columns + "]";
  }

  private class IPFSWork implements CompleteWork {
    private EndpointByteMapImpl byteMap = new EndpointByteMapImpl();
    private Multihash partialRoot;
    private DrillbitEndpoint onEndpoint = null;


    public IPFSWork(String root) {
      this.partialRoot = Multihash.fromBase58(root);
    }

    public IPFSWork(Multihash root) {
      this.partialRoot = root;
    }

    public Multihash getPartialRootHash() {return partialRoot;}

    public void setOnEndpoint(DrillbitEndpoint endpointAddress) {
      this.onEndpoint = endpointAddress;
    }

    @Override
    public long getTotalBytes() {
      return DEFAULT_NODE_SIZE;
    }

    @Override
    public EndpointByteMap getByteMap() {
      return byteMap;
    }

    @Override
    public int compareTo(CompleteWork o) {
      return 0;
=======

@JsonTypeName("ipfs-scan")
public class IPFSGroupScan extends AbstractGroupScan {
    public static final int DEFAULT_USER_PORT = 31010;
    public static final int DEFAULT_CONTROL_PORT = 31011;
    public static final int DEFAULT_DATA_PORT = 31012;
    public static final int DEFAULT_HTTP_PORT = 8047;
    private static final Logger logger = LoggerFactory.getLogger(IPFSGroupScan.class);
    private static final long DEFAULT_NODE_SIZE = 1000L;
    private final IPFSContext ipfsContext;
    private final IPFSScanSpec ipfsScanSpec;
    private final IPFSStoragePluginConfig config;
    private List<SchemaPath> columns;
    private ListMultimap<Integer, IPFSWork> assignments;
    private List<IPFSWork> ipfsWorkList = Lists.newArrayList();
    private ListMultimap<String, IPFSWork> endpointWorksMap;
    private List<EndpointAffinity> affinities;

    @JsonCreator
    public IPFSGroupScan(@JsonProperty("IPFSScanSpec") IPFSScanSpec ipfsScanSpec,
                         @JsonProperty("IPFSStoragePluginConfig") IPFSStoragePluginConfig ipfsStoragePluginConfig,
                         @JsonProperty("columns") List<SchemaPath> columns,
                         @JacksonInject StoragePluginRegistry pluginRegistry) {
        this(
                pluginRegistry.resolve(ipfsStoragePluginConfig, IPFSStoragePlugin.class).getIPFSContext(),
                ipfsScanSpec,
                columns
        );
    }

    public IPFSGroupScan(IPFSContext ipfsContext,
                         IPFSScanSpec ipfsScanSpec,
                         List<SchemaPath> columns) {
        super((String) null);
        this.ipfsContext = ipfsContext;
        this.ipfsScanSpec = ipfsScanSpec;
        this.config = ipfsContext.getStoragePluginConfig();
        logger.debug("GroupScan constructor called with columns {}", columns);
        this.columns = columns == null || columns.size() == 0 ? ALL_COLUMNS : columns;
        init();
    }

    private IPFSGroupScan(IPFSGroupScan that) {
        super(that);
        this.ipfsContext = that.ipfsContext;
        this.ipfsScanSpec = that.ipfsScanSpec;
        this.config = that.config;
        this.assignments = that.assignments;
        this.ipfsWorkList = that.ipfsWorkList;
        this.endpointWorksMap = that.endpointWorksMap;
        this.columns = that.columns;
    }

    private void init() {
        IPFSHelper ipfsHelper = ipfsContext.getIPFSHelper();
        endpointWorksMap = ArrayListMultimap.create();

        Multihash topHash = ipfsScanSpec.getTargetHash(ipfsHelper);
        try {
            Map<Multihash, IPFSPeer> leafPeerMap = getLeafPeerMappings(topHash);
            logger.debug("Iterating on {} leaves...", leafPeerMap.size());

            ClusterCoordinator coordinator = ipfsContext.getStoragePlugin().getContext().getClusterCoordinator();
            for (Multihash leaf : leafPeerMap.keySet()) {
                DrillbitEndpoint ep;
                if (config.isDistributedMode()) {
                    logger.debug("distribution {}", leafPeerMap.size());
                    String peerHostname = leafPeerMap
                            .get(leaf)
                            .getDrillbitAddress()
                            .orElseThrow(() -> new RuntimeException("Chosen IPFS peer does not have drillbit address"));
                    ep = registerEndpoint(coordinator, peerHostname);
                } else {
                    // the foreman is used to execute the plan
                    ep = ipfsContext.getStoragePlugin().getContext().getEndpoint();
                }

                IPFSWork work = new IPFSWork(leaf);
                logger.debug("added endpoint {} to work {}", ep.getAddress(), work);
                work.getByteMap().add(ep, DEFAULT_NODE_SIZE);
                work.setOnEndpoint(ep);
                endpointWorksMap.put(ep.getAddress(), work);
                ipfsWorkList.add(work);
            }
        } catch (Exception e) {
            throw UserException
                    .planError(e)
                    .message("Exception during initialization of IPFS GroupScan")
                    .build(logger);
        }
    }

    private DrillbitEndpoint registerEndpoint(ClusterCoordinator coordinator, String peerHostname) {
        Optional<DrillbitEndpoint> oep = coordinator.getAvailableEndpoints()
                .stream()
                .filter(ep -> ep.getAddress().equals(peerHostname))
                .findAny();
        DrillbitEndpoint ep;
        if (oep.isPresent()) {
            ep = oep.get();
            logger.debug("Using existing endpoint {}", ep.getAddress());
        } else {
            logger.debug("created new endpoint on the fly {}", peerHostname);
            //DRILL-7754: read ports & version info from IPFS instead of hard-coded
            ep = DrillbitEndpoint.newBuilder()
                    .setAddress(peerHostname)
                    .setUserPort(DEFAULT_USER_PORT)
                    .setControlPort(DEFAULT_CONTROL_PORT)
                    .setDataPort(DEFAULT_DATA_PORT)
                    .setHttpPort(DEFAULT_HTTP_PORT)
                    .setVersion(DrillVersionInfo.getVersion())
                    .setState(DrillbitEndpoint.State.ONLINE)
                    .build();
            //DRILL-7777: how to safely remove endpoints that are no longer needed once the query is completed?
            ClusterCoordinator.RegistrationHandle handle = coordinator.register(ep);
        }

        return ep;
    }

    Map<Multihash, IPFSPeer> getLeafPeerMappings(Multihash topHash) {
        logger.debug("start to recursively expand nested IPFS hashes, topHash={}", topHash);
        Stopwatch watch = Stopwatch.createStarted();
        ForkJoinPool forkJoinPool = new ForkJoinPool(config.getNumWorkerThreads());
        IPFSTreeFlattener topTask = new IPFSTreeFlattener(topHash, ipfsContext);
        List<Multihash> leaves = forkJoinPool.invoke(topTask);
        logger.debug("Took {} ms to expand hash leaves", watch.elapsed(TimeUnit.MILLISECONDS));

        logger.debug("Start to resolve providers");
        watch.reset().start();
        Map<Multihash, IPFSPeer> leafPeerMap;
        Map<String, Integer> load = new HashMap<>();
        if (config.isDistributedMode()) {
            int topHashThreshold = config.getTopHahsDataSize();
            IPFSPeer chosen = null;

            if(topHashThreshold!=-1 && leaves.size()<topHashThreshold){
                List<IPFSPeer> providers = ipfsContext.getProviderCache().getUnchecked(topHash).parallelStream()
                        .map(ipfsContext.getIPFSPeerCache()::getUnchecked)
                        .filter(IPFSPeer::isDrillReady)
                        .filter(IPFSPeer::hasDrillbitAddress)
                        .collect(Collectors.toList());
                chosen = providers.get(0);
            }

            leafPeerMap = forkJoinPool.invoke(new IPFSProviderResolver(leaves, ipfsContext, load, chosen));
        } else {
            leafPeerMap = new HashMap<>();
            for (Multihash leaf : leaves) {
                leafPeerMap.put(leaf, ipfsContext.getMyself());
            }
        }
        logger.debug("Took {} ms to resolve providers", watch.elapsed(TimeUnit.MILLISECONDS));

        return leafPeerMap;
    }

    @JsonProperty
    public List<SchemaPath> getColumns() {
        return columns;
    }

    @JsonIgnore
    public IPFSStoragePlugin getStoragePlugin() {
        return ipfsContext.getStoragePlugin();
    }

    @JsonProperty
    public IPFSScanSpec getIPFSScanSpec() {
        return ipfsScanSpec;
    }

    @Override
    public List<EndpointAffinity> getOperatorAffinity() {
        if (affinities == null) {
            affinities = AffinityCreator.getAffinityMap(ipfsWorkList);
        }
        return affinities;
    }

    @Override
    public int getMaxParallelizationWidth() {
        DrillbitEndpoint myself = ipfsContext.getStoragePlugin().getContext().getEndpoint();
        int width;
        if (endpointWorksMap.containsKey(myself.getAddress())) {
            // the foreman is also going to be a minor fragment worker under a UnionExchange operator
            width = ipfsWorkList.size();
        } else {
            // the foreman does not hold data, so we have to force parallelization
            // to make sure there is a UnionExchange operator
            width = ipfsWorkList.size() + 1;
        }
        logger.debug("getMaxParallelizationWidth: {}", width);
        return width;
    }

    @Override
    public void applyAssignments(List<DrillbitEndpoint> incomingEndpoints) {
        logger.debug("Applying assignments: endpointWorksMap = {}", endpointWorksMap);
        assignments = AssignmentCreator.getMappings(incomingEndpoints, ipfsWorkList);
    }

    @Override
    public IPFSSubScan getSpecificScan(int minorFragmentId) {
        logger.debug(String.format("getSpecificScan: minorFragmentId = %d", minorFragmentId));
        List<IPFSWork> workList = assignments.get(minorFragmentId);
        List<Multihash> scanSpecList = Lists.newArrayList();
        if (workList != null) {
            logger.debug("workList.size(): {}", workList.size());

            for (IPFSWork work : workList) {
                scanSpecList.add(work.getPartialRootHash());
            }
        }

        return new IPFSSubScan(ipfsContext, scanSpecList, ipfsScanSpec.getFormatExtension(), columns);
    }

    @Override
    public ScanStats getScanStats() {
        long recordCount = 100000 * endpointWorksMap.size();
        return new ScanStats(ScanStats.GroupScanProperty.NO_EXACT_ROW_COUNT, recordCount, 1, recordCount);
    }

    @Override
    public IPFSGroupScan clone(List<SchemaPath> columns) {
        logger.debug("IPFSGroupScan clone {}", columns);
        IPFSGroupScan cloned = new IPFSGroupScan(this);
        cloned.columns = columns;
        return cloned;
    }

    @Override
    @JsonIgnore
    public boolean canPushdownProjects(List<SchemaPath> columns) {
        return true;
    }

    @Override
    @JsonIgnore
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
        Preconditions.checkArgument(children.isEmpty());
        logger.debug("getNewWithChildren called");
        return new IPFSGroupScan(this);
    }

    @Override
    public String getDigest() {
        return toString();
>>>>>>> a989ec4 ('FMT')
    }

    @Override
    public String toString() {
<<<<<<< HEAD
      return "IPFSWork [root = " + partialRoot.toString() + "]";
    }
  }
=======
        return new PlanStringBuilder(this)
                .field("scan spec", ipfsScanSpec)
                .field("columns", columns)
                .toString();
    }

    private static class IPFSWork implements CompleteWork {
        private final EndpointByteMapImpl byteMap = new EndpointByteMapImpl();
        private final Multihash partialRoot;
        private DrillbitEndpoint onEndpoint = null;


        public IPFSWork(String root) {
            this.partialRoot = Cid.decode(root);
        }

        public IPFSWork(Multihash root) {
            this.partialRoot = root;
        }

        public Multihash getPartialRootHash() {
            return partialRoot;
        }

        public void setOnEndpoint(DrillbitEndpoint endpointAddress) {
            this.onEndpoint = endpointAddress;
        }

        @Override
        public long getTotalBytes() {
            return DEFAULT_NODE_SIZE;
        }

        @Override
        public EndpointByteMap getByteMap() {
            return byteMap;
        }

        @Override
        public int compareTo(CompleteWork o) {
            return 0;
        }

        @Override
        public String toString() {
            return new PlanStringBuilder(this)
                    .field("partial root", partialRoot)
                    .toString();
        }
    }

    static class IPFSProviderResolver extends RecursiveTask<Map<Multihash, IPFSPeer>> {
        private final List<Multihash> leaves;
        private final Map<Multihash, IPFSPeer> ret = new LinkedHashMap<>();
        private final IPFSPeer myself;
        private final IPFSHelper helper;
        private final LoadingCache<Multihash, IPFSPeer> peerCache;
        private final LoadingCache<Multihash, List<Multihash>> providerCache;
        private final Map<String, Integer> load;
        private final IPFSPeer chosen;

        public IPFSProviderResolver(List<Multihash> leaves, IPFSContext context, Map<String, Integer> load, IPFSPeer chosen) {
            this(leaves, context.getMyself(), chosen, context.getIPFSHelper(), context.getIPFSPeerCache(), context.getProviderCache(), load);
        }

        public IPFSProviderResolver(IPFSProviderResolver reference, List<Multihash> leaves, IPFSPeer chosen) {
            this(leaves, reference.myself, chosen,reference.helper, reference.peerCache, reference.providerCache, reference.load);
        }

        IPFSProviderResolver(List<Multihash> leaves, IPFSPeer myself, IPFSPeer chosen, IPFSHelper helper, LoadingCache<Multihash, IPFSPeer> peerCache, LoadingCache<Multihash, List<Multihash>> providerCache, Map<String, Integer> load) {
            this.leaves = leaves;
            this.myself = myself;
            this.helper = helper;
            this.peerCache = peerCache;
            this.providerCache = providerCache;
            this.load = load;
            this.chosen = chosen;
        }

        private int selectByRandom(List<IPFSPeer> providers) {
            Random random = new Random();
            return random.nextInt(providers.size());
        }

        private int selectByResponse(List<IPFSPeer> providers) {
            return 0; //return the first of all
        }

        private int selectByLoad(List<IPFSPeer> providers) {
            int minLoad = 100000;
            int selected = 0;
            int i = 0;

            for (IPFSPeer peer : providers) {
                int workload;
                String prints = peer.getId().toBase58();
                if (!load.containsKey(prints)) {
                    load.put(prints, 0);
                }
                workload = load.get(prints);

                if (minLoad > workload) {
                    minLoad = workload;
                    selected = i;
                }
                i++;
            }

            return selected;
        }

        @Override
        protected Map<Multihash, IPFSPeer> compute() {
            if(chosen!=null){
                for(Multihash hash : leaves){
                    ret.put(hash, chosen);
                }
                return ret;
            }

            int totalLeaves = leaves.size();
            if (totalLeaves == 1) {
                Multihash hash = leaves.get(0);
                List<IPFSPeer> providers = providerCache.getUnchecked(hash).parallelStream()
                        .map(peerCache::getUnchecked)
                        .filter(IPFSPeer::isDrillReady)
                        .filter(IPFSPeer::hasDrillbitAddress)
                        .collect(Collectors.toList());
                if (providers.size() < 1) {
                    logger.warn("No drill-ready provider found for leaf {}, adding foreman as the provider", hash);
                    providers.add(myself);
                }
                logger.debug("Got {} providers for {} from IPFS", providers.size(), hash);

                //DRILL-7753: better peer selection algorithm
                IPFSPeer chosenPeer;
                chosenPeer = providers.get(selectByRandom(providers));
                ret.put(hash, chosenPeer);
                logger.debug("Use peer {} for leaf {}", chosenPeer, hash);
                return ret;
            }

            int firstHalf = totalLeaves / 2;
            ImmutableList<IPFSProviderResolver> resolvers = ImmutableList.of(
                    new IPFSProviderResolver(this, leaves.subList(0, firstHalf), chosen),
                    new IPFSProviderResolver(this, leaves.subList(firstHalf, totalLeaves), chosen)
            );
            resolvers.forEach(ForkJoinTask::fork);
            resolvers.reverse().forEach(resolver -> ret.putAll(resolver.join()));
            return ret;
        }
    }

    //DRILL-7756: detect and warn about loops/recursions in case of a malformed tree
    static class IPFSTreeFlattener extends RecursiveTask<List<Multihash>> {
        private final Multihash hash;
        private final List<Multihash> ret = new LinkedList<>();
        private final IPFSPeer myself;
        private final IPFSHelper helper;
        private final boolean modified;
        private final int height;

        public IPFSTreeFlattener(Multihash hash, IPFSContext context) {
            this(
                    hash,
                    context.getMyself(),
                    context.getIPFSHelper(),
                    context.getStoragePluginConfig().getModifiedMerkleTree(),
                    0
            );
        }

        IPFSTreeFlattener(Multihash hash, IPFSPeer myself, IPFSHelper ipfsHelper, boolean modified, int height) {
            this.hash = hash;
            this.myself = myself;
            this.helper = ipfsHelper;

            this.modified = modified;
            this.height = height;
        }

        public IPFSTreeFlattener(IPFSTreeFlattener reference, Multihash hash) {
            this(hash, reference.myself, reference.helper, reference.modified, reference.height + 1);
        }

        @Override
        public List<Multihash> compute() {
            if(height == 2 && modified){
                logger.debug("due to height equals 2. {} is a simple node", hash);
                ret.add(hash);
                return ret;
            }

            try {
                MerkleNode metaOrSimpleNode = helper.getObjectLinksTimeout(hash);
                if (metaOrSimpleNode.links.size() > 0) {
                    logger.debug("{} is a meta node", hash);
                    //DRILL-7755: do something useful with leaf size, e.g. hint Drill about operation costs
                    List<Multihash> intermediates = metaOrSimpleNode.links.stream().map(x -> x.hash).collect(Collectors.toList());

                    ImmutableList.Builder<IPFSTreeFlattener> builder = ImmutableList.builder();
                    for (Multihash intermediate : intermediates.subList(1, intermediates.size())) {
                        builder.add(new IPFSTreeFlattener(this, intermediate));
                    }
                    ImmutableList<IPFSTreeFlattener> subtasks = builder.build();
                    subtasks.forEach(IPFSTreeFlattener::fork);

                    IPFSTreeFlattener first = new IPFSTreeFlattener(this, intermediates.get(0));
                    ret.addAll(first.compute());
                    subtasks.reverse().forEach(
                            subtask -> ret.addAll(subtask.join())
                    );
                } else {
                    logger.debug("{} is a simple node", hash);
                    ret.add(hash);
                }
            } catch (IOException e) {
                throw UserException.planError(e).message("Exception during planning").build(logger);
            }
            return ret;
        }
    }
>>>>>>> a989ec4 ('FMT')
}
