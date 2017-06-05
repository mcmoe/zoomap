package com.mcmoe;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Consumer;

import static com.google.common.truth.Truth.assertThat;


public class ZooMapTest {

    @Test(expected = RuntimeException.class)
    public void creating_a_new_map_with_a_downed_server_should_fail() {
        ZooMap.newMap("lalalala:12345", "/test/map");
    }

    @Test(expected = RuntimeException.class)
    public void creating_a_new_map_with_retry_policy_with_a_downed_server_should_fail() {
        ZooMap.newBuilder("lalalala:12345").withRoot("/test/map").withRetryPolicy(new RetryOneTime(10)).build();
    }

    @Test
    public void creating_a_new_map_with_empty_root_should_not_fail() {
        withServer((server) -> {
            ZooMap.newBuilder(server.getConnectString()).withRoot("").build();
        });
    }

    @Test
    public void creating_a_new_map_with_slash_root_should_not_fail() {
        withServer((server) -> {
            ZooMap.newBuilder(server.getConnectString()).withRoot("/").build();
        });
    }

    @Test
    public void creating_a_new_map_with_a_running_server_should_succeed() throws Exception {
        withMap((zooMap) -> assertThat(zooMap).isNotNull());
    }

    @Test
    public void contains_on_non_existing_key_should_return_false() {
        withMap((zooMap) -> assertThat(zooMap.containsKey("bla")).isFalse());
    }

    @Test
    public void get_on_non_existing_key_should_return_null() {
        withMap((zooMap) -> assertThat(zooMap.get("bla")).isNull());
    }

    @Test
    public void contains_on_existing_key_should_return_true() {
        withMap((zooMap) -> {
            zooMap.put("bla", "");
            assertThat(zooMap.containsKey("bla")).isTrue();
        });
    }

    @Test
    public void first_put_should_return_null() {
        withMap((zooMap) -> assertThat(zooMap.put("ka", "")).isNull());
    }

    @Test
    public void get_of_empty_string_put_should_return_empty_string() {
        withMap((zooMap) -> {
            zooMap.put("ka", "");
            assertThat(zooMap.get("ka")).isEmpty();
        });
    }

    @Test
    public void i_can_get_what_i_put() throws Exception {
        withMap((zooMap) -> {
            zooMap.put("ka", "va");
            assertThat(zooMap.get("ka")).isEqualTo("va");
        });
    }

    @Test
    public void my_map_should_equal_itself() {
        withMap((zooMap) -> assertThat(zooMap).isEqualTo(zooMap));
    }

    @Test
    public void my_map_should_equal_another_with_same_connection_and_root() {
        withServer((server) -> {
            Map<String, String> zooMap1 = ZooMap.newMap(server.getConnectString(), "/test/map");
            Map<String, String> zooMap2 = ZooMap.newMap(server.getConnectString(), "/test/map");
            assertThat(zooMap1).isEqualTo(zooMap2);
        });
    }

    @Test
    public void my_map_should_not_equal_another_with_same_connection_but_different_root() {
        withServer((server) -> {
            Map<String, String> zooMap1 = ZooMap.newMap(server.getConnectString(), "/test/map1");
            Map<String, String> zooMap2 = ZooMap.newMap(server.getConnectString(), "/test/map2");
            assertThat(zooMap1).isNotEqualTo(zooMap2);
        });
    }

    @Test
    public void my_map_should_not_equal_another_with_different_connection_but_same_root() {
        withServer((server) -> {
            Map<String, String> zooMap1 = ZooMap.newMap(server.getConnectString(), "/test/map");
            withServer((s2) -> {
                Map<String, String> zooMap2 = ZooMap.newMap(s2.getConnectString(), "/test/map");
                assertThat(zooMap1).isNotEqualTo(zooMap2);
            });
        });
    }

    @Test
    public void my_map_should_not_equal_another_with_different_connection_and_root() {
        withServer((server) -> {
            Map<String, String> zooMap1 = ZooMap.newMap(server.getConnectString(), "/test/map1");
            withServer((s2) -> {
                Map<String, String> zooMap2 = ZooMap.newMap(s2.getConnectString(), "/test/map2");
                assertThat(zooMap1).isNotEqualTo(zooMap2);
            });
        });
    }

    @Test
    public void my_map_should_serialize_values_as_utf8_byte_array() {
        withServer(server -> {
            final ZooMap zooMap = ZooMap.newBuilder(server.getConnectString()).withRoot("").build();
            try(CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(100))) {
                client.start();
                zooMap.put("Roger", "Fedérer");
                final byte[] bytes = client.getData().forPath("/Roger");
                assertThat(new String(bytes, StandardCharsets.ISO_8859_1)).isNotEqualTo("Fedérer");
                assertThat(new String(bytes, StandardCharsets.UTF_8)).isEqualTo("Fedérer");
            }
        });
    }

    private void withMap(Consumer<Map<String, String>> testBlock) {
        withServer((server) -> {
            Map<String, String> zooMap = ZooMap.newMap(server.getConnectString(), "/test/map");
            testBlock.accept(zooMap);
        });
    }

    private void withServer(ThrowingConsumer<TestingServer> testBlock) {
        try(TestingServer server = new TestingServer()) {
            server.start();
            testBlock.accept(server);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}

@FunctionalInterface
interface ThrowingConsumer<T> {
    void accept(T t) throws Exception;
}