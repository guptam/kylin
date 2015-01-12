/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.metadata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.persistence.JsonSerializer;
import com.kylinolap.common.persistence.ResourceStore;
import com.kylinolap.common.persistence.Serializer;
import com.kylinolap.common.restclient.Broadcaster;
import com.kylinolap.common.restclient.SingleValueCache;
import com.kylinolap.common.util.JsonUtil;
import com.kylinolap.metadata.model.DataModelDesc;
import com.kylinolap.metadata.model.TableDesc;

/**
 * Serves (and caches) metadata for Kylin instance.
 * <p/>
 * Also provides a ResourceStore for general purpose data persistence. 
 * Metadata is serialized as JSON and stored in ResourceStore.
 * 
 * @author yangli9
 */
public class MetadataManager {

    private static final Logger logger = LoggerFactory.getLogger(MetadataManager.class);

    private static final Serializer<TableDesc> TABLE_SERIALIZER = new JsonSerializer<TableDesc>(TableDesc.class);
    private static final Serializer<DataModelDesc> MODELDESC_SERIALIZER = new JsonSerializer<DataModelDesc>(DataModelDesc.class);

    // static cached instances
    private static final ConcurrentHashMap<KylinConfig, MetadataManager> CACHE = new ConcurrentHashMap<KylinConfig, MetadataManager>();

    public static MetadataManager getInstance(KylinConfig config) {
        MetadataManager r = CACHE.get(config);
        if (r != null) {
            return r;
        }

        synchronized (MetadataManager.class) {
            r = CACHE.get(config);
            if (r != null) {
                return r;
            }
            try {
                r = new MetadataManager(config);
                CACHE.put(config, r);
                if (CACHE.size() > 1) {
                    logger.warn("More than one singleton exist");
                }

                return r;
            } catch (IOException e) {
                throw new IllegalStateException("Failed to init MetadataManager from " + config, e);
            }
        }
    }

    public static synchronized void removeInstance(KylinConfig config) {
        CACHE.remove(config);
    }

    public static void dropCache() {
        CACHE.clear();
    }

    // ============================================================================

    private KylinConfig config;
    // table name ==> SourceTable
    private SingleValueCache<String, TableDesc> srcTableMap = new SingleValueCache<String, TableDesc>(Broadcaster.TYPE.METADATA);
    // name => value
    private SingleValueCache<String, Map<String, String>> srcTableExdMap = new SingleValueCache<String, Map<String, String>>(Broadcaster.TYPE.METADATA);
    // name => DataModelDesc
    private SingleValueCache<String, DataModelDesc> dataModelDescMap = new SingleValueCache<String, DataModelDesc>(Broadcaster.TYPE.METADATA);

    private MetadataManager(KylinConfig config) throws IOException {
        init(config);
    }

    /**
     * Tell MetadataManager that the instance has changed. The cube info will
     * be stored Reload the cube desc and source table A broadcast must be sent
     * out
     * 
     * @return
     * @throws IOException
     */
    public void reload() {
        removeInstance(config);
        getInstance(config);
    }

    public KylinConfig getConfig() {
        return config;
    }

    public ResourceStore getStore() {
        return ResourceStore.getStore(this.config);
    }

    public List<TableDesc> listAllTables() {
        return Lists.newArrayList(srcTableMap.values());
    }

    public Map<String, TableDesc> getAllTablesMap() {
        return Collections.unmodifiableMap(srcTableMap.getMap());
    }

    public Map<String, Map<String, String>> listAllTableExdMap() {
        return srcTableExdMap.getMap();
    }

    /**
     * Get Table Desc object
     * 
     * @param tableName
     * @return
     */
    public TableDesc getTableDesc(String tableName) {
        return srcTableMap.get(tableName.toUpperCase());
    }

    /**
     * Get table extended info. Keys are defined in {@link MetadataConstances}
     * 
     * @param tableName
     * @return
     */
    public Map<String, String> getTableDescExd(String tableName) {
        String tableIdentity = tableName;
        Map<String, String> result = new HashMap<String, String>();
        if (srcTableExdMap.containsKey(tableIdentity)) {
            Map<String, String> tmp = srcTableExdMap.get(tableIdentity);
            Iterator<Entry<String, String>> it = tmp.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, String> entry = it.next();
                result.put(entry.getKey(), entry.getValue());
            }
            result.put(MetadataConstances.TABLE_EXD_STATUS_KEY, "true");
        } else {
            result.put(MetadataConstances.TABLE_EXD_STATUS_KEY, "false");
        }
        return result;
    }

    public void saveSourceTable(TableDesc srcTable) throws IOException {
        if (srcTable.getUuid() == null || srcTable.getIdentity() == null) {
            throw new IllegalArgumentException();
        }

        srcTable.init();
        
        String path = srcTable.getResourcePath();
        getStore().putResource(path, srcTable, TABLE_SERIALIZER);

        srcTableMap.put(srcTable.getIdentity(), srcTable);
    }

    private void init(KylinConfig config) throws IOException {
        this.config = config;
        reloadAllSourceTable();
        reloadAllSourceTableExd();
        reloadAllDataModel();
    }

    private void reloadAllSourceTableExd() throws IOException {
        ResourceStore store = getStore();
        logger.debug("Reloading SourceTable exd info from folder " + store.getReadableResourcePath(ResourceStore.TABLE_EXD_RESOURCE_ROOT));

        srcTableExdMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.TABLE_EXD_RESOURCE_ROOT, MetadataConstances.FILE_SURFIX);
        for (String path : paths) {
            reloadSourceTableExdAt(path);
        }
        
        logger.debug("Loaded " + srcTableExdMap.size() + " SourceTable EXD(s)");
    }

    public Map<String, String> reloadSourceTableExt(String tableIdentity) throws IOException {
        return reloadSourceTableExdAt(TableDesc.concatExdResourcePath(tableIdentity));
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> reloadSourceTableExdAt(String path) throws IOException {
        Map<String, String> attrs = Maps.newHashMap();

        ResourceStore store = getStore();
        InputStream is = store.getResource(path);
        if (is == null) {
            logger.warn("Failed to get table exd info from " + path);
            return null;
        }

        try {
            attrs.putAll(JsonUtil.readValue(is, HashMap.class));
        } finally {
            if (is != null)
                is.close();
        }

        // parse table identity from file name
        String file = path;
        if (file.indexOf("/") > -1) {
            file = file.substring(file.lastIndexOf("/") + 1);
        }
        String tableIdentity = file.substring(0, file.length() - MetadataConstances.FILE_SURFIX.length());
        
        checkNoDupName(tableIdentity, srcTableExdMap.containsKey(tableIdentity), "SourceTableExd", path);

        srcTableExdMap.putLocal(tableIdentity, attrs);
        return attrs;
    }

    private void reloadAllSourceTable() throws IOException {
        ResourceStore store = getStore();
        logger.debug("Reloading SourceTable from folder " + store.getReadableResourcePath(ResourceStore.TABLE_RESOURCE_ROOT));

        srcTableMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.TABLE_RESOURCE_ROOT, MetadataConstances.FILE_SURFIX);
        for (String path : paths) {
            reloadSourceTableAt(path);
        }

        logger.debug("Loaded " + srcTableMap.size() + " SourceTable(s)");
    }

    public TableDesc reloadSourceTable(String tableIdentity) throws IOException {
        return reloadSourceTableAt(TableDesc.concatResourcePath(tableIdentity));
    }

    private TableDesc reloadSourceTableAt(String path) throws IOException {
        ResourceStore store = getStore();
        TableDesc t = store.getResource(path, TableDesc.class, TABLE_SERIALIZER);
        t.init();

        String tableIdentity = t.getIdentity();
        checkNoDupName(tableIdentity, srcTableMap.containsKey(tableIdentity), "SourceTable", path);

        srcTableMap.putLocal(tableIdentity, t);
        return t;
    }

    public DataModelDesc getDataModelDesc(String name) {
        return dataModelDescMap.get(name);
    }

    private void reloadAllDataModel() throws IOException {
        ResourceStore store = getStore();
        logger.debug("Reloading DataModel from folder " + store.getReadableResourcePath(ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT));

        dataModelDescMap.clear();

        List<String> paths = store.collectResourceRecursively(ResourceStore.DATA_MODEL_DESC_RESOURCE_ROOT, MetadataConstances.FILE_SURFIX);
        for (String path : paths) {
            reloadDataModelDescAt(path);
        }

        logger.debug("Loaded " + dataModelDescMap.size() + " DataModel(s)");
    }

    public DataModelDesc reloadDataModelDesc(String name) {
        return reloadDataModelDescAt(DataModelDesc.concatResourcePath(name));
    }

    private DataModelDesc reloadDataModelDescAt(String path) {
        ResourceStore store = getStore();
        DataModelDesc ndesc = null;
        try {
            ndesc = store.getResource(path, DataModelDesc.class, MODELDESC_SERIALIZER);
        } catch (IOException e) {
            throw new IllegalStateException("Error to load" + path, e);
        }

        ndesc.init(this.getAllTablesMap());

        if (ndesc.getError().isEmpty() == false) {
            throw new IllegalStateException("DataModelDesc at " + path + " has issues: " + ndesc.getError());
        }

        String name = ndesc.getName();
        checkNoDupName(name, dataModelDescMap.containsKey(name), "DataModel", path);

        dataModelDescMap.putLocal(name, ndesc);
        return ndesc;
    }

    public DataModelDesc createDataModelDesc(DataModelDesc dataModelDesc) throws IOException {
        String name = dataModelDesc.getName();
        if (dataModelDescMap.containsKey(name))
            throw new IllegalArgumentException("DataModelDesc '" + name + "' already exists");

        return saveDataModelDesc(dataModelDesc);
    }

    public DataModelDesc updateDataModelDesc(DataModelDesc desc) throws IOException {
        String name = desc.getName();
        if (!dataModelDescMap.containsKey(name)) {
            throw new IllegalArgumentException("DataModelDesc '" + name + "' does not exist.");
        }

        return saveDataModelDesc(desc);
    }

    private DataModelDesc saveDataModelDesc(DataModelDesc dataModelDesc) throws IOException {
        try {
            dataModelDesc.init(this.getAllTablesMap());
        } catch (Exception e) {
            dataModelDesc.addError(e.getMessage(), true);
        }

        if (!dataModelDesc.getError().isEmpty()) {
            return dataModelDesc;
        }

        String path = dataModelDesc.getResourcePath();
        getStore().putResource(path, dataModelDesc, MODELDESC_SERIALIZER);
        dataModelDescMap.put(dataModelDesc.getName(), dataModelDesc);

        return dataModelDesc;
    }

    public void deleteDataModelDesc(DataModelDesc dataModelDesc) throws IOException {
        // remove dataModelDesc
        String path = dataModelDesc.getResourcePath();
        getStore().deleteResource(path);
        dataModelDescMap.remove(dataModelDesc.getName());
    }
    
    public void saveTableExd(String tableId, Map<String, String> tableExdProperties) throws IOException {
        if (tableId == null) {
            throw new IllegalArgumentException("tableId couldn't be null");
        }
        TableDesc srcTable = srcTableMap.get(tableId);
        if(srcTable == null) {
            throw new IllegalArgumentException("Couldn't find Source Table with identifier: " + tableId);
        }

        String path = TableDesc.concatExdResourcePath(tableId);
        
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        JsonUtil.writeValueIndent(os, tableExdProperties);
        os.flush();
        InputStream is = new ByteArrayInputStream(os.toByteArray());
        getStore().putResource(path, is, System.currentTimeMillis());

        srcTableExdMap.putLocal(tableId, tableExdProperties);
    }

    private void checkNoDupName(String name, boolean containsKey, String entityType, String path) {
        if (StringUtils.isBlank(name)) {
            throw new IllegalStateException(entityType + " name at " + path + ", must not be blank");
        }
        if (containsKey) {
            throw new IllegalStateException("Dup " + entityType + " name '" + name + "' at " + path);
        }
    }

}
