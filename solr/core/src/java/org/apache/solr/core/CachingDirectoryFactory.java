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
package org.apache.solr.core;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link DirectoryFactory} impl base class for caching Directory instances
 * per path. Most DirectoryFactory implementations will want to extend this
 * class and simply implement {@link DirectoryFactory#create(String, LockFactory, DirContext)}.
 * <p>
 * This is an expert class and these API's are subject to change.
 */
public abstract class CachingDirectoryFactory extends DirectoryFactory {
  protected static class CacheValue {
    final public String path;
    final public Directory directory;
    // for debug
    //final Exception originTrace;
    // use the setter!
    private boolean deleteOnClose = false;

    public int refCnt = 1;
    // has doneWithDirectory(Directory) been called on this?
    public boolean closeCacheValueCalled = false;
    public boolean doneWithDir = false;
    private boolean deleteAfterCoreClose = false;
    public final Set<CacheValue> removeEntries = new HashSet<>();
    public final Set<CacheValue> closeEntries = new HashSet<>();

    public CacheValue(String path, Directory directory) {
      this.path = path;
      this.directory = directory;
      this.closeEntries.add(this);
      // for debug
      // this.originTrace = new RuntimeException("Originated from:");
    }



    public void setDeleteOnClose(boolean deleteOnClose, boolean deleteAfterCoreClose) {
      if (log.isDebugEnabled()) {
        log.debug("setDeleteOnClose(boolean deleteOnClose={}, boolean deleteAfterCoreClose={}) - start", deleteOnClose, deleteAfterCoreClose);
      }

      if (deleteOnClose) {
        removeEntries.add(this);
      }
      this.deleteOnClose = deleteOnClose;
      this.deleteAfterCoreClose = deleteAfterCoreClose;

      if (log.isDebugEnabled()) {
        log.debug("setDeleteOnClose(boolean, boolean) - end");
      }
    }

    @Override
    public String toString() {
      return "CachedDir<<" + "refCount=" + refCnt + ";path=" + path + ";done=" + doneWithDir + ">>";
    }
  }

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final boolean DEBUG_GET_RELEASE = false;

  protected final Map<String, CacheValue> byPathCache = new ConcurrentHashMap<>();

  protected final Map<Directory, List<CloseListener>> closeListeners = new ConcurrentHashMap<>();

  private volatile Double maxWriteMBPerSecFlush;

  private volatile Double maxWriteMBPerSecMerge;

  private volatile Double maxWriteMBPerSecRead;

  private volatile Double maxWriteMBPerSecDefault;

  private volatile boolean closed;

  public interface CloseListener {
    public void postClose();

    public void preClose();
  }

  @Override
  public void addCloseListener(Directory dir, CloseListener closeListener) {


      List<CloseListener> listeners = closeListeners.get(dir);
      if (listeners == null) {
        listeners = new ArrayList<>();
        closeListeners.put(dir, listeners);
      }
      listeners.add(closeListener);

      closeListeners.put(dir, listeners);



  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.solr.core.DirectoryFactory#close()
   */
  @Override
  public void close() throws IOException {
    if (log.isTraceEnabled()) log.trace("close() - start");
    closed = true;
    byPathCache.forEach((s, cacheValue) -> org.apache.solr.common.util.IOUtils.closeQuietly(cacheValue.directory));
  }

  @Override
  public boolean exists(String path) throws IOException {
    if (log.isTraceEnabled()) log.trace("exists(String path={}) - start", path);

    // back compat behavior
    File dirFile = new File(path);
    boolean returnboolean = dirFile.canRead() && dirFile.list().length > 0;

    if (log.isTraceEnabled()) log.trace("exists(String) - end");

    return returnboolean;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.solr.core.DirectoryFactory#get(java.lang.String,
   * java.lang.String, boolean)
   */
  @Override
  public final Directory get(String path, DirContext dirContext, String rawLockType)
          throws IOException {
    if (log.isTraceEnabled()) log.trace("get(String path={}, DirContext dirContext={}, String rawLockType={}) - start", path, dirContext, rawLockType);

    String fullPath = normalize(path);

    final CacheValue cacheValue = byPathCache.get(fullPath);
    Directory directory = null;
    if (cacheValue != null) {
      directory = cacheValue.directory;
    }

    if (directory == null) {
      directory = create(fullPath, createLockFactory(rawLockType), dirContext);
      //   assert !directory.getClass().getSimpleName().equals("MockDirectoryWrapper") ? ObjectReleaseTracker.track(directory) : true;
      boolean success = false;
      try {
        CacheValue newCacheValue = new CacheValue(fullPath, directory);
        byPathCache.put(fullPath, newCacheValue);
        if (log.isDebugEnabled())
          log.debug("return new directory for {}", newCacheValue, DEBUG_GET_RELEASE && newCacheValue.path.equals("data/index") ? new RuntimeException() : null);
        success = true;
      } finally {
        if (!success) {
          IOUtils.closeWhileHandlingException(directory);
          remove(fullPath);

        }
      }
    } else {
      cacheValue.refCnt++;
      if (log.isDebugEnabled())
        log.debug("Reusing cached directory: {}", cacheValue, DEBUG_GET_RELEASE && cacheValue.path.equals("data/index") ? new RuntimeException() : null);
    }
    //  if (cacheValue.path.equals("data/index")) {
    //    log.info("getDir " + path, new RuntimeException("track get " + fullPath)); // MRM TODO:
    // }

    if (log.isTraceEnabled()) log.trace("get(String, DirContext, String) - end");

    return directory;

  }



  @Override
  public void init(NamedList args) {
    if (log.isTraceEnabled()) log.trace("init(NamedList args={}) - start", args);

    maxWriteMBPerSecFlush = (Double) args.get("maxWriteMBPerSecFlush");
    maxWriteMBPerSecMerge = (Double) args.get("maxWriteMBPerSecMerge");
    maxWriteMBPerSecRead = (Double) args.get("maxWriteMBPerSecRead");
    maxWriteMBPerSecDefault = (Double) args.get("maxWriteMBPerSecDefault");

    // override global config
    if (args.get(SolrXmlConfig.SOLR_DATA_HOME) != null) {
      dataHomePath = Paths.get((String) args.get(SolrXmlConfig.SOLR_DATA_HOME));
    }
    if (dataHomePath != null) {
      log.info(SolrXmlConfig.SOLR_DATA_HOME + "=" + dataHomePath);
    }

    if (log.isTraceEnabled()) log.trace("init(NamedList) - end");
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.solr.core.DirectoryFactory#release(org.apache.lucene.store.Directory
   * )
   */
  @Override
  public void release(String String) throws IOException {

//      if (cacheValue.path.equals("data/index")) {
//        log.info(
//            "Releasing directory: " + cacheValue.path + " " + (cacheValue.refCnt - 1) + " " + cacheValue.doneWithDir,
//            new RuntimeException("Fake to find stack trace")); // MRM TODO:
//      } else {

      //    }



  }

  @Override
  public void remove(String path) throws IOException {
    if (log.isTraceEnabled()) log.trace("remove(String path={}) - start", path);

    remove(path, false);

    if (log.isTraceEnabled()) log.trace("remove(String) - end");
  }

  @Override
  public void remove(String path, boolean deleteAfterCoreClose) throws IOException {
    if (log.isTraceEnabled()) log.trace("remove(String path={}, boolean deleteAfterCoreClose={}) - start", path, deleteAfterCoreClose);
    CacheValue val = byPathCache.get(normalize(path));
    if (val == null) {
      throw new IllegalArgumentException("Unknown directory " + path);
    }
    // MRM TODO: the actual delete
    org.apache.solr.common.util.IOUtils.closeQuietly(val.directory);
  }


  @Override
  public String normalize(String path) throws IOException {
    if (log.isTraceEnabled()) log.trace("normalize(String path={}) - start", path);


    path = stripTrailingSlash(path);

    return path;
  }

  protected String stripTrailingSlash(String path) {
    if (log.isTraceEnabled()) log.trace("stripTrailingSlash(String path={}) - start", path);

    if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }

    return path;
  }

  /**
   * Method for inspecting the cache
   *
   * @return paths in the cache which have not been marked "done"
   */
  public Set<String> getLivePaths() {
    if (log.isTraceEnabled()) log.trace("getLivePaths() - start");

    HashSet<String> livePaths = new HashSet<>(byPathCache.size());
    for (CacheValue val : byPathCache.values()) {
      if (!val.doneWithDir) {
        livePaths.add(val.path);
      }
    }

    if (log.isTraceEnabled()) log.trace("getLivePaths() - end");

    return livePaths;
  }

  @Override
  protected boolean deleteOldIndexDirectory(String oldDirPath) throws IOException {
    if (log.isTraceEnabled()) log.trace("deleteOldIndexDirectory(String oldDirPath={}) - start", oldDirPath);

    Set<String> livePaths = getLivePaths();
    if (livePaths.contains(oldDirPath)) {
      log.warn("Cannot delete directory {} as it is still being referenced in the cache!", oldDirPath);
      return false;
    }

    return super.deleteOldIndexDirectory(oldDirPath);
  }

}
