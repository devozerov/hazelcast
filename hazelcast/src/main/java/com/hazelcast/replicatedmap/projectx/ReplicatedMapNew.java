package com.hazelcast.replicatedmap.projectx;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.core.EntryListener;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.MapEntrySimple;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.query.Predicate;
import com.hazelcast.replicatedmap.LocalReplicatedMapStats;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ReplicatedMapNew<K, V> implements ReplicatedMap<K, V> {

    private final String name;
    private final MapProxyImpl<K, V> delegate;

    public ReplicatedMapNew(String name, MapProxyImpl<K, V> delegate) {
        this.name = name;
        this.delegate = delegate;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }

    @Override
    public String getPartitionKey() {
        return getName();
    }

    @Override
    public void destroy() {
        delegate.destroy();
    }

    @Override
    public V get(Object key) {
        return delegate.get(key);
    }

    @Nullable
    @Override
    public V put(K key, V value) {
        return delegate.put(key, value);
    }

    @Override
    public V put(@NotNull K key, @NotNull V value, long ttl, @NotNull TimeUnit timeUnit) {
        return delegate.put(key, value, ttl, timeUnit);
    }

    @Override
    public void putAll(@NotNull Map<? extends K, ? extends V> map) {
        delegate.putAll(map);
    }

    @Override
    public V remove(Object key) {
        return delegate.remove(key);
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @NotNull
    @Override
    public Set<Entry<K, V>> entrySet() {
        Set<Entry<K, V>> res = new HashSet<>();

        InternalSerializationService ss = (InternalSerializationService) delegate.getNodeEngine().getSerializationService();

        long now = System.currentTimeMillis();

        for (int i = 0; i < delegate.getNodeEngine().getPartitionService().getPartitionCount(); i++) {
            RecordStore store = delegate.getService().getMapServiceContext().getExistingRecordStore(i, name);

            if (store == null) {
                continue;
            }

            Iterator<Map.Entry> storeIterator = store.getStorage().mutationTolerantIterator();

            while (storeIterator.hasNext()) {
                Map.Entry<Data, Record<Object>> entry = storeIterator.next();

                if (!store.isExpired(entry.getValue(), now, false)) {
                    Object key = entry.getKey();
                    Object value = entry.getValue().getValue();

                    if (key instanceof Data) {
                        key = ss.toObject(key);
                    }

                    if (value instanceof Data) {
                        value = ss.toObject(value);
                    }

                    res.add(new MapEntrySimple<>((K) key, (V) value));
                }
            }
        }

        return res;
    }

    @NotNull
    @Override
    public Set<K> keySet() {
        Set<K> res = new HashSet<>();

        for (Entry<K, V> entry : entrySet()) {
            res.add(entry.getKey());
        }

        return res;
    }

    @NotNull
    @Override
    public Collection<V> values() {
        List<V> res = new ArrayList<>();

        for (Entry<K, V> entry : entrySet()) {
            res.add(entry.getValue());
        }

        return res;
    }

    @NotNull
    @Override
    public Collection<V> values(@Nullable Comparator<V> comparator) {
        List<V> values = new ArrayList<>(values());

        values.sort(comparator);

        return values;
    }

    @Override
    public int size() {
        return entrySet().size();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    @SuppressWarnings("SuspiciousMethodCalls")
    @Override
    public boolean containsValue(Object value) {
        return values().contains(value);
    }

    @Override
    public void addIndex(IndexConfig indexConfig) {
        delegate.addIndex(indexConfig);
    }

    @NotNull
    @Override
    public UUID addEntryListener(@NotNull EntryListener<K, V> listener) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public UUID addEntryListener(@NotNull EntryListener<K, V> listener, @Nullable K key) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public UUID addEntryListener(@NotNull EntryListener<K, V> listener, @NotNull Predicate<K, V> predicate) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public UUID addEntryListener(@NotNull EntryListener<K, V> listener, @NotNull Predicate<K, V> predicate, @Nullable K key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeEntryListener(@NotNull UUID id) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public LocalReplicatedMapStats getReplicatedMapStats() {
        throw new UnsupportedOperationException();
    }
}
