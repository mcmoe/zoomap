package com.mcmoe;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;

import java.io.Closeable;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;


public class ZooMap implements Map<String, String>, Closeable {
    private final CuratorFramework client;
    private final String connectionString;
    private final String root;

    public static ZooMap newMap(String connectionString) {
        return newBuilder(connectionString).build();
    }

    public static ZooMap newMap(String connectionString, String root) {
        return newBuilder(connectionString).withRoot(root).build();
    }

    public static Builder newBuilder(String connectionString) {
        return new Builder(connectionString);
    }

    private ZooMap(Builder builder) {
        this.connectionString = builder.connectionString;
        client = CuratorFrameworkFactory.newClient(connectionString, builder.retryPolicy);
        this.root = builder.root;
        startAndBlock();
        if(!root.isEmpty()) {
            tryIt(() -> client.createContainers(root));
        }
    }

    private void startAndBlock() {
        client.start();

        tryIt(() -> {
            if(!client.blockUntilConnected(1, TimeUnit.SECONDS)) {
                throw new RuntimeException("Did not connect in time");
            }
        });
    }

    private void tryIt(ThrowingRunner r) {
        try {
            r.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private <R> R tryIt(ThrowingSupplier<R> s) {
        try {
            return s.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int size() {
        return tryIt(() -> getKeys().size());
    }

    @Override
    public boolean isEmpty() {
        return tryIt(() -> getKeys().isEmpty());
    }

    @Override
    public boolean containsKey(Object key) {
        if(key instanceof String) {
            return tryIt(() -> client.checkExists().forPath(keyPath((String) key)) != null);
        }
        throw new IllegalArgumentException("key must be of type String");
    }

    private List<String> getKeys() throws Exception {
        return client.getChildren().forPath(root);
    }

    @Override
    public boolean containsValue(Object value) {
        if(value instanceof String) {
            return tryIt(() -> getKeys().stream().map(this::getValue).anyMatch(b -> value.equals(new String(b))));
        }
        throw new IllegalArgumentException("value must be of type String");
    }

    private byte[] getValue(String key) {
        try {
            return client.getData().forPath(keyPath(key));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String keyPath(String key) {
        return root + "/" + key;
    }

    @Override
    public String get(Object key) {
        if(key instanceof String) {
            if(!containsKey(key)) {
                return null;
            }
            byte[] value = tryIt(() -> client.getData().forPath(keyPath((String) key)));
            return value != null ? new String(value) : null;
        }

        throw new IllegalArgumentException("key must be of type String");
    }

    @Override
    public String put(String key, String value) {
        String previousValue = get(key);
        tryIt(() -> client.createContainers(keyPath(key)));
        tryIt(() -> client.setData().forPath(keyPath(key), value.getBytes()));
        return previousValue;
    }

    @Override
    public String remove(Object key) {
        if(key instanceof String) {
            String previousValue = get(key);
            if(previousValue != null) {
                tryIt(() -> client.delete().forPath(keyPath((String) key)));
            }
            return previousValue;
        } else {
            throw new IllegalArgumentException("key must be of type String");
        }
    }

    @Override
    public void putAll(Map<? extends String, ? extends String> m) {
        m.forEach(this::put);
    }

    @Override
    public void clear() {
        tryIt(() -> client.delete().deletingChildrenIfNeeded().forPath(root));
        tryIt(() -> client.create().forPath(root));
    }

    @Override
    public Set<String> keySet() {
        return new HashSet<>(tryIt(() -> client.getChildren().forPath(root)));
    }

    @Override
    public Collection<String> values() {
        return tryIt(() -> getKeys().stream().map(this::getValue).map(String::new).collect(Collectors.toList()));
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        return tryIt(() -> getKeys().stream().map(entryFromKey())).collect(Collectors.toSet());
    }

    private Function<String, Entry<String, String>> entryFromKey() {
        return k -> new AbstractMap.SimpleEntry<>(k, tryIt(() -> new String(getValue(k))));
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;

        if (!(o instanceof ZooMap))
            return false;

        ZooMap otherZooMap = (ZooMap) o;
        return root.equals(otherZooMap.root) && connectionString.equals(otherZooMap.connectionString);
    }

    @Override
    public int hashCode() {
        return this.connectionString.hashCode() + this.root.hashCode();
    }

    @Override
    public void replaceAll(BiFunction<? super String, ? super String, ? extends String> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        if(client != null) {
            client.close();
        }
    }

    public static class Builder {
        private final String connectionString;
        private String root = "";
        private RetryPolicy retryPolicy = new RetryOneTime(1000);

        private Builder(String connectionString) {
            this.connectionString = connectionString;
            this.root = root;
        }

        public Builder withRetryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = retryPolicy;
            return this;
        }

        public Builder withRoot(String root) {
            if("/".equals(root) || root == null) {
                this.root = "";
            } else {
                this.root = root;
            }
            return this;
        }

        public ZooMap build() {
            return new ZooMap(this);
        }
    }
}

interface ThrowingSupplier<R> {
    R get() throws Exception;
}

interface ThrowingRunner {
    void run() throws Exception;
}