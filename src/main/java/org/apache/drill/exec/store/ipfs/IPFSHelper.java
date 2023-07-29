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

import io.ipfs.api.IPFS;
import io.ipfs.api.MerkleNode;
<<<<<<< HEAD
=======
import io.ipfs.cid.Cid;
>>>>>>> a989ec4 ('FMT')
import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.store.ipfs.IPFSStoragePluginConfig.IPFSTimeOut;
<<<<<<< HEAD
import org.bouncycastle.util.Strings;

import java.io.IOException;
import java.lang.ref.WeakReference;
=======
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.bouncycastle.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Inet6Address;
>>>>>>> a989ec4 ('FMT')
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
<<<<<<< HEAD
import java.util.concurrent.Executors;
=======
>>>>>>> a989ec4 ('FMT')
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

<<<<<<< HEAD


public class IPFSHelper {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(IPFSHelper.class);

  public static final String IPFS_NULL_OBJECT_HASH = "QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n";
  public static final Multihash IPFS_NULL_OBJECT = Multihash.fromBase58(IPFS_NULL_OBJECT_HASH);

  private WeakReference<ExecutorService> executorService;
  private static ExecutorService DEFAULT_EXECUTOR = Executors.newSingleThreadExecutor();
  private IPFS client;
=======
import static org.apache.drill.exec.store.ipfs.IPFSStoragePluginConfig.IPFSTimeOut.FETCH_DATA;
import static org.apache.drill.exec.store.ipfs.IPFSStoragePluginConfig.IPFSTimeOut.FIND_PEER_INFO;

/**
 * Helper class with some utilities that are specific to Drill with an IPFS storage
 *
 * DRILL-7778: refactor to support CIDv1
 */
public class IPFSHelper {
  private static final Logger logger = LoggerFactory.getLogger(IPFSHelper.class);

  public static final String IPFS_NULL_OBJECT_HASH = "QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n";
  public static final Multihash IPFS_NULL_OBJECT = Cid.decode(IPFS_NULL_OBJECT_HASH);

  private ExecutorService executorService;
  private final IPFS client;
  private final IPFSCompat clientCompat;
>>>>>>> a989ec4 ('FMT')
  private IPFSPeer myself;
  private int maxPeersPerLeaf;
  private Map<IPFSTimeOut, Integer> timeouts;

<<<<<<< HEAD
  class DefaultWeakReference<T> extends WeakReference<T> {
    private T default_;
    public DefaultWeakReference(T referent, T default_) {
      super(referent);
      this.default_ = default_;
    }

    @Override
    public T get() {
      T ret = super.get();
      if (ret == null) {
        return default_;
      } else {
        return ret;
      }
    }
  }

  public IPFSHelper(IPFS ipfs) {
    executorService = new DefaultWeakReference<>(DEFAULT_EXECUTOR, DEFAULT_EXECUTOR);
    this.client = ipfs;
  }

  public void setExecutorService(ExecutorService executorService) {
    this.executorService = new DefaultWeakReference<>(executorService, DEFAULT_EXECUTOR);
=======
  public IPFSHelper(IPFS ipfs) {
    this.client = ipfs;
    this.clientCompat = new IPFSCompat(ipfs);
  }

  public IPFSHelper(IPFS ipfs, ExecutorService executorService) {
    this(ipfs);
    this.executorService = executorService;
>>>>>>> a989ec4 ('FMT')
  }

  public void setTimeouts(Map<IPFSTimeOut, Integer> timeouts) {
    this.timeouts = timeouts;
  }

  public void setMyself(IPFSPeer myself) {
    this.myself = myself;
  }

<<<<<<< HEAD
=======
  /**
   * Set maximum number of providers per leaf node. The more providers, the more time it takes to do DHT queries, while
   * it is more likely we can find an optimal peer.
   *
   * @param maxPeersPerLeaf max number of providers to search per leaf node
   */
>>>>>>> a989ec4 ('FMT')
  public void setMaxPeersPerLeaf(int maxPeersPerLeaf) {
    this.maxPeersPerLeaf = maxPeersPerLeaf;
  }

  public IPFS getClient() {
    return client;
  }

<<<<<<< HEAD
  public List<Multihash> findprovsTimeout(Multihash id) throws IOException {
    List<String> providers;
    providers = client.dht.findprovsListTimeout(id, maxPeersPerLeaf, timeouts.get(IPFSTimeOut.FIND_PROV), executorService.get());

    List<Multihash> ret = providers.stream().map(str -> Multihash.fromBase58(str)).collect(Collectors.toList());
    return ret;
  }

  public List<MultiAddress> findpeerTimeout(Multihash peerId) throws IOException {
    // trying to resolve addresses of a node itself will always hang
    // so we treat it specially
    if(peerId.equals(myself.getId())) {
=======
  public IPFSCompat getClientCompat() {
    return clientCompat;
  }

  public List<Multihash> findprovsTimeout(Multihash id) {
    List<String> providers;
    providers = clientCompat.dht.findprovsListTimeout(id, maxPeersPerLeaf, timeouts.get(IPFSTimeOut.FIND_PROV), executorService);

    return providers.stream().map(Cid::decode).collect(Collectors.toList());
  }

  public List<MultiAddress> findpeerTimeout(Multihash peerId) {
    // trying to resolve addresses of a node itself will always hang
    // so we treat it specially
    if (peerId.equals(myself.getId())) {
>>>>>>> a989ec4 ('FMT')
      return myself.getMultiAddresses();
    }

    List<String> addrs;
<<<<<<< HEAD
    addrs = client.dht.findpeerListTimeout(peerId, timeouts.get(IPFSTimeOut.FIND_PEER_INFO), executorService.get());
    List<MultiAddress>
        ret = addrs
        .stream()
        .filter(addr -> !addr.equals(""))
        .map(str -> new MultiAddress(str)).collect(Collectors.toList());
    return ret;
  }

  @FunctionalInterface
  public interface ThrowingFunction<T, R, E extends Exception>{
=======
    addrs = clientCompat.dht.findpeerListTimeout(peerId, timeouts.get(IPFSTimeOut.FIND_PEER_INFO), executorService);
    return addrs.stream()
        .filter(addr -> !addr.equals(""))
        .map(MultiAddress::new).collect(Collectors.toList());
  }

  public byte[] getObjectDataTimeout(Multihash object) throws IOException {
    return timedFailure(client.object::data, object, timeouts.get(IPFSTimeOut.FETCH_DATA));
  }

  public MerkleNode getObjectLinksTimeout(Multihash object) throws IOException {
    return timedFailure(client.object::links, object, timeouts.get(IPFSTimeOut.FETCH_DATA));
  }

  public IPFSPeer getMyself() throws IOException {
    if (this.myself != null) {
      return this.myself;
    }

    Map res = timedFailure(client::id, timeouts.get(FIND_PEER_INFO));
    Multihash myID = Cid.decode((String) res.get("ID"));
    // Rule out any non-local addresses as they might be NAT-ed external
    // addresses that are not always reachable from the inside.
    // But is it safe to assume IPFS always listens on loopback and local addresses?
    List<MultiAddress> myAddrs = ((List<String>) res.get("Addresses"))
        .stream()
        .map(MultiAddress::new)
        .filter(addr -> {
          try {
            InetAddress inetAddress = InetAddress.getByName(addr.getHost());
            if (inetAddress instanceof Inet6Address) {
              return false;
            }
            return inetAddress.isSiteLocalAddress()
                || inetAddress.isLinkLocalAddress()
                || inetAddress.isLoopbackAddress();
          } catch (UnknownHostException e) {
            return false;
          }
        })
        .collect(Collectors.toList());
    this.myself = new IPFSPeer(this, myID, myAddrs);

    return this.myself;
  }

  public Multihash resolve(String prefix, String path, boolean recursive) {
    Map<String, String> result = timedFailure(
        (args) -> clientCompat.resolve((String) args.get(0), (String) args.get(1), (boolean) args.get(2)),
        ImmutableList.<Object>of(prefix, path, recursive),
        timeouts.get(IPFSTimeOut.FIND_PEER_INFO)
    );
    if (!result.containsKey("Path")) {
      return null;
    }

    // the path returned is of form /ipfs/Qma...
    String hashString = result.get("Path").split("/")[2];
    return Cid.decode(hashString);
  }

  @FunctionalInterface
  public interface ThrowingFunction<T, R, E extends Exception> {
>>>>>>> a989ec4 ('FMT')
    R apply(final T in) throws E;
  }

  @FunctionalInterface
  public interface ThrowingSupplier<R, E extends Exception> {
    R get() throws E;
  }

  /**
<<<<<<< HEAD
   * Execute a time-critical operation op within time timeout. Throws TimeoutException, so the
   * caller has a chance to recover from a timeout.
   * @param op a Function that represents the operation to perform
   * @param in the parameter for op
   * @param timeout consider the execution has timed out after this amount of time in seconds
   * @param <T>
   * @param <R>
   * @param <E>
   * @return R the result of the operation
   * @throws TimeoutException
   * @throws E
   */
  public <T, R, E extends Exception> R timed(ThrowingFunction<T, R, E> op, T in, int timeout) throws TimeoutException, E {
    Callable<R> task = () -> op.apply(in);
    Future<R> res = executorService.get().submit(task);
    try {
      return res.get(timeout, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      throw (E) e.getCause();
    } catch (CancellationException | InterruptedException e) {
      throw UserException.executionError(e).build(logger);
    }
  }

  /**
   * Execute a time-critical operation op within time timeout. Causes the query to fail completely
   * if the operation times out.
   * @param op a Function that represents the operation to perform
   * @param in the parameter for op
   * @param timeout consider the execution has timed out after this amount of time in seconds
   * @param <T>
   * @param <R>
   * @param <E>
   * @return R the result of the operation
   * @throws E
=======
   * Execute a time-critical operation op within time timeout. Causes the query to fail completely
   * if the operation times out.
   *
   * @param op      a Function that represents the operation to perform
   * @param in      the parameter for op
   * @param timeout consider the execution has timed out after this amount of time in seconds
   * @param <T>     Input type
   * @param <R>     Return type
   * @param <E>     Type of checked exception op throws
   * @return R the result of the operation
   * @throws E when the function throws an E
>>>>>>> a989ec4 ('FMT')
   */
  public <T, R, E extends Exception> R timedFailure(ThrowingFunction<T, R, E> op, T in, int timeout) throws E {
    Callable<R> task = () -> op.apply(in);
    return timedFailure(task, timeout, TimeUnit.SECONDS);
  }

  public <R, E extends Exception> R timedFailure(ThrowingSupplier<R, E> op, int timeout) throws E {
    Callable<R> task = op::get;
    return timedFailure(task, timeout, TimeUnit.SECONDS);
  }

  private <R, E extends Exception> R timedFailure(Callable<R> task, int timeout, TimeUnit timeUnit) throws E {
<<<<<<< HEAD
    Future<R> res = executorService.get().submit(task);
=======
    Future<R> res = executorService.submit(task);
>>>>>>> a989ec4 ('FMT')
    try {
      return res.get(timeout, timeUnit);
    } catch (ExecutionException e) {
      throw (E) e.getCause();
    } catch (TimeoutException e) {
      throw UserException.executionError(e).message("IPFS operation timed out").build(logger);
    } catch (CancellationException | InterruptedException e) {
<<<<<<< HEAD
      throw UserException.executionError(e).build(logger);
    }
  }

=======
      throw UserException.executionError(e).message("IPFS operation was cancelled or interrupted").build(logger);
    }
  }

  /*
   * DRILL-7753: implement a more advanced algorithm that picks optimal addresses. Maybe check reachability, latency
   * and bandwidth?
   */

  /**
   * Choose a peer's network address from its advertised Multiaddresses.
   * Prefer globally routable address over local addresses.
   *
   * @param peerAddrs Multiaddresses obtained from IPFS.DHT.findprovs
   * @return network address
   */
>>>>>>> a989ec4 ('FMT')
  public static Optional<String> pickPeerHost(List<MultiAddress> peerAddrs) {
    String localAddr = null;
    for (MultiAddress addr : peerAddrs) {
      String host = addr.getHost();
      try {
        InetAddress inetAddress = InetAddress.getByName(host);
<<<<<<< HEAD
        if (inetAddress.isLoopbackAddress()) {
          continue;
        }
        if (inetAddress.isSiteLocalAddress() || inetAddress.isLinkLocalAddress()) {
          //FIXME we don't know which local address can be reached; maybe check with InetAddress.isReachable?
=======
        if (inetAddress instanceof Inet6Address) {
          // ignore IPv6 addresses
          continue;
        }
//        if (inetAddress.getHostAddress().equals("127.0.0.1")){
//          continue;
//        }
        if (inetAddress.isSiteLocalAddress() || inetAddress.isLinkLocalAddress()) {
>>>>>>> a989ec4 ('FMT')
          localAddr = host;
        } else {
          return Optional.of(host);
        }
<<<<<<< HEAD
      } catch (UnknownHostException e) {
        continue;
=======
      } catch (UnknownHostException ignored) {
>>>>>>> a989ec4 ('FMT')
      }
    }

    return Optional.ofNullable(localAddr);
  }

  public Optional<String> getPeerDrillHostname(Multihash peerId) {
    return getPeerData(peerId, "drill-hostname").map(Strings::fromByteArray);
  }

<<<<<<< HEAD
=======
  /**
   * Check if an IPFS peer is also running a Drillbit so that it can be used to execute a part of a query.
   *
   * @param peerId the id of the peer
   * @return if the peer is Drill-ready
   */
>>>>>>> a989ec4 ('FMT')
  public boolean isDrillReady(Multihash peerId) {
    try {
      return getPeerData(peerId, "drill-ready").isPresent();
    } catch (RuntimeException e) {
      return false;
    }
  }

  public Optional<Multihash> getIPNSDataHash(Multihash peerId) {
    Optional<List<MerkleNode>> links = getPeerLinks(peerId);
    if (!links.isPresent()) {
      return Optional.empty();
    }

    return links.get().stream()
        .filter(l -> l.name.equals(Optional.of("drill-data")))
        .findFirst()
        .map(l -> l.hash);
  }

<<<<<<< HEAD

=======
  /**
   * Get from IPFS data under a peer's ID, i.e. the data identified by /ipfs/{ID}/key.
   *
   * @param peerId the peer's ID
   * @param key    key
   * @return data in bytes
   */
>>>>>>> a989ec4 ('FMT')
  private Optional<byte[]> getPeerData(Multihash peerId, String key) {
    Optional<List<MerkleNode>> links = getPeerLinks(peerId);
    if (!links.isPresent()) {
      return Optional.empty();
    }

<<<<<<< HEAD
    return links.get().stream()
        .filter(l -> l.name.equals(Optional.of(key)))
        .findFirst()
        .map(l -> {
          try {
            return client.object.data(l.hash);
          } catch (IOException e) {
            return null;
          }
        });
  }

  private Optional<List<MerkleNode>> getPeerLinks(Multihash peerId) {
    try {
      Optional<String> optionalPath = client.name.resolve(peerId, 30);
=======
    for (MerkleNode link : links.get()) {
      if (link.name.equals(Optional.of(key))) {
        try {
          byte[] result = timedFailure(client.object::data, link.hash, timeouts.get(FETCH_DATA));
          return Optional.of(result);
        } catch (IOException e) {
          return Optional.empty();
        }
      }
    }
    return Optional.empty();
  }

  /**
   * Get all the links under a peer's ID.
   *
   * @param peerId peer's ID
   * @return List of links
   */
  private Optional<List<MerkleNode>> getPeerLinks(Multihash peerId) {
    try {
      Optional<String> optionalPath = clientCompat.name.resolve(peerId, timeouts.get(FIND_PEER_INFO), executorService);
>>>>>>> a989ec4 ('FMT')
      if (!optionalPath.isPresent()) {
        return Optional.empty();
      }
      String path = optionalPath.get().substring(6); // path starts with /ipfs/Qm...

<<<<<<< HEAD
      List<MerkleNode> links = client.object.get(Multihash.fromBase58(path)).links;
      if (links.size() < 1) {
        return Optional.empty();
      } else {
        return Optional.of(links);
      }
    } catch (IOException e) {
      return Optional.empty();
    }
=======
      List<MerkleNode> links = timedFailure(
          client.object::get,
          Cid.decode(path),
          timeouts.get(FETCH_DATA)
      ).links;
      if (links.size() > 0) {
        return Optional.of(links);
      }
    } catch (IOException ignored) {
    }
    return Optional.empty();
>>>>>>> a989ec4 ('FMT')
  }
}
