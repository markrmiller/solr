/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.common.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.apache.solr.common.ParWork;
import org.apache.solr.common.util.CloseTracker;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// we use this class to expose nasty stuff for tests
@SuppressWarnings({"try"})
public class SolrZooKeeper extends ZooKeeperAdmin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private CloseTracker closeTracker;

  public SolrZooKeeper(String connectString, int sessionTimeout, Watcher watcher) throws IOException {
    super(connectString, sessionTimeout, watcher);
    assert (closeTracker = new CloseTracker()) != null;
  }

  public ClientCnxn getConnection() {
    return cnxn;
  }

  public SocketAddress getSocketAddress() {
    return testableLocalSocketAddress();
  }

  public void closeCnxn() {
    final Runnable t = new Runnable() {
      @Override public void run() {
        AccessController.doPrivileged((PrivilegedAction<Void>) this::closeZookeeperChannel);
      }

      @SuppressForbidden(reason = "Hack for Zookeper needs access to private methods.") private Void closeZookeeperChannel() {
        final ClientCnxn cnxn = getConnection();

        try {
          final Field sendThreadFld = cnxn.getClass().getDeclaredField("sendThread");
          sendThreadFld.setAccessible(true);
          Object sendThread = sendThreadFld.get(cnxn);
          if (sendThread != null) {
            Method method = sendThread.getClass().getDeclaredMethod("testableCloseSocket");
            method.setAccessible(true);
            try {
              method.invoke(sendThread);
            } catch (InvocationTargetException e) {
              // is fine
            }
          }
        } catch (Exception e) {
          ParWork.propagateInterrupt(e);
          throw new RuntimeException("Closing Zookeeper send channel failed.", e);
        }

        return null; // Void
      }
    };

    ParWork.submit("ZkCloseConnectTestThread", t);
  }

  @Override
  public synchronized void close() {
    assert closeTracker != null ? closeTracker.close() : true;
//    try {
//      RequestHeader h = new RequestHeader();
//      h.setType(ZooDefs.OpCode.closeSession);
//      cnxn.submitRequest(h, null, null, null);
//    } catch (InterruptedException e) {
//      // ignore, close the send/event threads
//    } finally {
//      ZooKeeperExposed zk = new ZooKeeperExposed(this, cnxn);
//      try (ParWork work = new ParWork(this, true)) {
//        work.collect("", () -> {
//          zk.closeCnxn();
//        });
//        work.collect("", () -> {
//          zk.interruptSendThread();
//          zk.interruptSendThread();
//        });
//      }
//    }
    try {
      super.close();
    } catch (InterruptedException e) {

    }
  }

}

