package com.mcmoe;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.client.ConnectStringParser;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
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
        ConnectStringParser connectStringParser = new ConnectStringParser(connectionString);
        if(connectStringParser.getChrootPath() != null) {
            final String connectionStringForChrootCreation = connectStringParser.getServerAddresses().stream().map(InetSocketAddress::toString).collect(Collectors.joining(","));
            try(final CuratorFramework clientForChrootCreation = CuratorFrameworkFactory.newClient(connectionStringForChrootCreation, builder.retryPolicy)) {
                startAndBlock(clientForChrootCreation);
                tryIt(() -> clientForChrootCreation.createContainers(connectStringParser.getChrootPath()));
            }
        }
        client = CuratorFrameworkFactory.newClient(connectionString, builder.retryPolicy);
        this.root = builder.root;
        startAndBlock(client);
        if(!root.isEmpty()) {
            tryIt(() -> client.createContainers(root));
        }
    }

    private static void startAndBlock(CuratorFramework c) {
        c.start();

        tryIt(() -> {
            if(!c.blockUntilConnected(1, TimeUnit.SECONDS)) {
                throw new IOException("Did not connect in time");
            }
        });
    }

    private static void tryIt(ThrowingRunner r) {
        try {
            r.run();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static <R> R tryIt(ThrowingSupplier<R> s) {
        try {
            return s.get();
        } catch (RuntimeException e) {
            throw e;
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
        return client.getChildren().forPath(emptyToSlash(root));
    }

    @Override
    public boolean containsValue(Object value) {
        if(value instanceof String) {
            return tryIt(() -> getKeys().stream().map(this::getValue).anyMatch(value::equals));
        }
        throw new IllegalArgumentException("value must be of type String");
    }

    private String getValue(String key) {
        return tryIt(() -> {
            final byte[] bytes = client.getData().forPath(keyPath(key));
            return bytes != null ? new String(bytes, StandardCharsets.UTF_8) : null;
        });
    }

    private String keyPath(String key) {
        return root + "/" + key;
    }

    private static String emptyToSlash(String path) {
        return path.isEmpty() ? "/" : path;
    }

    @Override
    public String get(Object key) {
        if(key instanceof String) {
            if(!containsKey(key)) {
                return null;
            }
            return getValue((String)key);
        }

        throw new IllegalArgumentException("key must be of type String");
    }

    @Override
    public String put(String key, String value) {
        if(key != null && !key.isEmpty()) {
            String previousValue = get(key);
            tryIt(() -> client.createContainers(keyPath(key)));
            tryIt(() -> client.setData().forPath(keyPath(key), value != null ? value.getBytes(StandardCharsets.UTF_8) : null));
            return previousValue;
        }
        throw new IllegalArgumentException("Key should not be empty nor null (" + key + ")");
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
        tryIt(() -> client.delete().deletingChildrenIfNeeded().forPath(emptyToSlash(root)));
        tryIt(() -> client.create().forPath(emptyToSlash(root)));
    }

    @Override
    public Set<String> keySet() {
        return new HashSet<>(tryIt(() -> client.getChildren().forPath(emptyToSlash(root))));
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
        return k -> new AbstractMap.SimpleEntry<>(k, tryIt(() -> getValue(k)));
    }

    @Override
    public final boolean equals(Object o) {
        if (o == this)
            return true;

        if (!(o instanceof ZooMap))
            return false;

        ZooMap otherZooMap = (ZooMap) o;
        return root.equals(otherZooMap.root) && connectionString.equals(otherZooMap.connectionString);
    }

    @Override
    public final int hashCode() {
        return this.connectionString.hashCode() + this.root.hashCode();
    }

    @Override
    public void replaceAll(BiFunction<? super String, ? super String, ? extends String> function) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        client.close();
    }

    public static class Builder {
        private final String connectionString;
        private String root = "";
        private RetryPolicy retryPolicy = new RetryOneTime(1000);

        private Builder(String connectionString) {
            this.connectionString = connectionString;
        }

        public Builder withRetryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = retryPolicy;
            return this;
        }

        public Builder withRoot(String root) {
            if(root == null) {
                this.root = "";
            } else if(root.endsWith("/")) {
                this.root = root.substring(0, root.length() - 1);
            } else {
                this.root = root;
            }
            if(!this.root.isEmpty() && !this.root.startsWith("/")) {
                throw new IllegalArgumentException("Root path should start with \"/\"");
            }
            return this;
        }

        public ZooMap build() {
            return new ZooMap(this);
        }
    }
}

@FunctionalInterface
interface ThrowingSupplier<R> {
    R get() throws Exception;
}

@FunctionalInterface
interface ThrowingRunner {
    void run() throws Exception;
}