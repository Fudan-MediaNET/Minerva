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

import io.ipfs.multiaddr.MultiAddress;
import io.ipfs.multihash.Multihash;

<<<<<<< HEAD
import java.io.IOException;
=======
>>>>>>> a989ec4 ('FMT')
import java.util.List;
import java.util.Optional;

public class IPFSPeer {
<<<<<<< HEAD
  private IPFSHelper helper;

  private Multihash id;
=======
  private final IPFSHelper helper;

  private final Multihash id;
>>>>>>> a989ec4 ('FMT')
  private List<MultiAddress> addrs;
  private boolean isDrillReady;
  private boolean isDrillReadyChecked = false;
  private Optional<String> drillbitAddress = Optional.empty();
  private boolean drillbitAddressChecked = false;


  public IPFSPeer(IPFSHelper helper, Multihash id) {
    this.helper = helper;
    this.id = id;
  }

  IPFSPeer(IPFSHelper helper, Multihash id, List<MultiAddress> addrs) {
    this.helper = helper;
    this.id = id;
    this.addrs = addrs;
    this.isDrillReady = helper.isDrillReady(id);
    this.isDrillReadyChecked = true;
    this.drillbitAddress = IPFSHelper.pickPeerHost(addrs);
    this.drillbitAddressChecked = true;
  }

  public boolean isDrillReady() {
    if (!isDrillReadyChecked) {
      isDrillReady = helper.isDrillReady(id);
      isDrillReadyChecked = true;
    }
    return isDrillReady;
  }

  public boolean hasDrillbitAddress() {
<<<<<<< HEAD
    findDrillbitAddress();
    return drillbitAddress.isPresent();
=======
    return getDrillbitAddress().isPresent();
>>>>>>> a989ec4 ('FMT')
  }

  public Optional<String> getDrillbitAddress() {
    findDrillbitAddress();
    return drillbitAddress;
  }

  public List<MultiAddress> getMultiAddresses() {
    findDrillbitAddress();
    return addrs;
  }

  public Multihash getId() {
    return id;
  }


  private void findDrillbitAddress() {
    if (!drillbitAddressChecked) {
<<<<<<< HEAD
      try {
        addrs = helper.findpeerTimeout(id);
        drillbitAddress = IPFSHelper.pickPeerHost(addrs);
      } catch (IOException e) {
        drillbitAddress = Optional.empty();
      }
=======
      addrs = helper.findpeerTimeout(id);
      drillbitAddress = IPFSHelper.pickPeerHost(addrs);
>>>>>>> a989ec4 ('FMT')
      drillbitAddressChecked = true;
    }
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public String toString() {
    return String.format("IPFSPeer(%s)", id.toBase58());
  }
<<<<<<< HEAD

=======
>>>>>>> a989ec4 ('FMT')
}
