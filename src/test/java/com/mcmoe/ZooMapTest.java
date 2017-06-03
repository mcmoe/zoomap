package com.mcmoe;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import javax.naming.NamingException;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.function.Consumer;

import static com.google.common.truth.Truth.assertThat;


public class ZooMapTest {

    @Test(expected = RuntimeException.class)
    public void creating_a_new_map_with_a_downed_server_should_fail() throws InterruptedException {
        ZooMap.newBuilder("lalalala:12345").withConnectionTimeout(Duration.ofSeconds(1)).build();
    }

    @Test(expected = RuntimeException.class)
    public void creating_a_new_map_with_retry_policy_with_a_downed_server_should_fail() {
        ZooMap.newBuilder("lalalala:12345").withConnectionTimeout(Duration.ofSeconds(1)).withRetryPolicy(new RetryOneTime(10)).build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void creating_a_new_map_with_non_absolute_root_should_fail() {
        ZooMap.newBuilder("lalalala:12345").withRoot("test/map").build();
    }

    @Test(expected = RuntimeException.class)
    public void creating_a_new_chrooted_map_with_a_downed_server_should_fail() {
        ZooMap.newBuilder("lalalala:12345/test/map").withConnectionTimeout(Duration.ofSeconds(1)).build();
    }

    @Test
    public void create_a_new_zoo_map_without_a_root_should_use_the_top_level_zookeeper_root() {
        withServer((server) -> {
            ZooMap zooMap = ZooMap.newMap(server.getConnectString());
            assertThat(zooMap).doesNotContainKey(("myKey"));
            try (CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(100))) {
                client.start();
                client.createContainers("/myKey");
                assertThat(zooMap).containsKey(("myKey"));
            }
        });
    }

    @Test
    public void creating_a_new_map_with_empty_root_should_not_fail() {
        withServer((server) -> ZooMap.newBuilder(server.getConnectString()).withRoot("").build());
    }

    @Test
    public void creating_a_new_map_with_null_root_should_not_fail() {
        withServer((server) -> ZooMap.newBuilder(server.getConnectString()).withRoot(null).build());
    }

    @Test
    public void creating_a_new_map_with_slash_root_should_not_fail() {
        withServer((server) -> ZooMap.newBuilder(server.getConnectString()).withRoot("/").build());
    }

    @Test(expected = IllegalArgumentException.class)
    public void creating_a_new_map_with_invalid_root_should_not_fail() {
        withServer((server) -> ZooMap.newBuilder(server.getConnectString()).withRoot("///").build());
    }

    @Test
    public void creating_a_new_map_with_a_running_server_should_succeed() throws Exception {
        withMap((zooMap) -> assertThat(zooMap).isNotNull());
    }

    @Test(expected = IllegalArgumentException.class)
    public void contains_on_non_string_key_should_fail() {
        withMap((zoomap) -> zoomap.containsKey(new Integer(0)));
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

    @Test(expected = IllegalArgumentException.class)
    public void contains_on_non_string_value_should_fail() {
        withMap((zoomap) -> zoomap.containsValue(new Integer(0)));
    }

    @Test
    public void contains_on_existing_value_should_return_true() {
        withMap((zooMap) -> {
            zooMap.put("Roger", "Federer");
            zooMap.put("Raphael", "Nadal");
            assertThat(zooMap.containsValue("Federer")).isTrue();
            assertThat(zooMap.containsValue("Nadal")).isTrue();
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void remove_with_non_string_key_should_fail() {
        withMap(zooMap -> zooMap.remove(new Integer(0)));
    }

    @Test
    public void remove_on_existing_keys_should_return_previous_value_and_remove_entry() {
        withMap(zooMap -> {
            zooMap.put("Roger", "Federer");
            assertThat(zooMap.remove("Roger")).isEqualTo("Federer");
            assertThat(zooMap.values()).doesNotContain("Federer");
            assertThat(zooMap.keySet()).doesNotContain("Roger");
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void put_with_an_empty_key_should_fail() {
        withMap(zooMap -> zooMap.put(null, "tupac"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void put_with_a_null_key_should_fail() {
        withMap(zooMap -> zooMap.put("", "biggie"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void put_with_a_illegal_key_should_fail() {
        withMap(zooMap -> zooMap.put("\\//", "biggie"));
    }

    @Test
    public void put_on_existing_keys_should_return_previous_value_and_put_new_value() {
        withMap(zooMap -> {
            zooMap.put("Roger", "Federer");
            assertThat(zooMap.put("Roger", "Moore")).isEqualTo("Federer");
            assertThat(zooMap).containsEntry("Roger", "Moore");
        });
    }

    @Test
    public void first_put_should_return_null() {
        withMap((zooMap) -> assertThat(zooMap.put("ka", "")).isNull());
    }

    @Test
    public void put_all_should_add_all_entries_to_zoomap() {
        withMap(zooMap -> {
            Map<String, String> map = new HashMap<>();
            map.put("a", "a");
            map.put("b", "b");
            map.put("c", "c");

            zooMap.putAll(map);
            assertThat(zooMap).hasSize(3);
            map.forEach((k, v) ->  {
                assertThat(zooMap).containsKey(k);
                assertThat(zooMap.containsValue(v)).isTrue();
            });
        });
    }

    @Test
    public void get_of_empty_string_put_should_return_empty_string() {
        withMap((zooMap) -> {
            zooMap.put("ka", "");
            assertThat(zooMap.get("ka")).isEmpty();
        });
    }

    @Test
    public void get_of_null_string_put_should_return_null_string() {
        withMap((zooMap) -> {
            zooMap.put("null", null);
            assertThat(zooMap.get("null")).isNull();
        });
    }

    @Test
    public void removing_missing_key_should_return_null() {
        withMap((zooMap) -> {
            assertThat(zooMap.remove("missing")).isNull();
        });
    }

    @Test
    public void i_can_get_what_i_put() throws Exception {
        withMap((zooMap) -> {
            zooMap.put("ka", "va");
            assertThat(zooMap.get("ka")).isEqualTo("va");
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void a_get_with_non_string_key_should_fail() {
        withMap((zoomap) -> zoomap.get(new Integer(0)));
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
    public void my_map_should_not_equal_another_object_type() {
        withServer((server) -> {
            Map<String, String> zooMap1 = ZooMap.newMap(server.getConnectString(), "/test/map");
            assertThat(zooMap1).isNotEqualTo(new Object());
        });
    }

    @Test
    public void my_map_should_return_values_and_keys_with_empty_root() {
        withServer(server -> {
            final ZooMap zooMap = ZooMap.newBuilder(server.getConnectString()).withRoot("").build();
            zooMap.put("Roger", "Federer");
            zooMap.put("Raphael", "Nadal");
            assertThat(zooMap.values()).containsAllOf("Federer", "Nadal");
            assertThat(zooMap.keySet()).containsAllOf("Roger", "Raphael");
        });
    }

    @Test(expected = RuntimeException.class)
    public void clearing_a_zoomap_whose_path_has_been_deleted_should_fail() {
        withServer(server -> {
            final ZooMap zooMap = ZooMap.newBuilder(server.getConnectString()).withRoot("/some/root").build();
            try (CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(100))) {
                client.start();
                client.delete().forPath("/some/root");
            }
            zooMap.clear();
        });
    }

    @Test
    public void my_map_should_clear() {
        withServer(server -> {
            final ZooMap zooMap = ZooMap.newBuilder(server.getConnectString()).withRoot("/foo").build();
            zooMap.put("Roger", "Federer");
            zooMap.clear();
            assertThat(zooMap.values()).isEmpty();
            assertThat(zooMap.keySet()).isEmpty();
        });
    }

    @Test(expected = RuntimeException.class)
    public void my_map_should_not_clear_with_empty_root() {
        withServer(server -> {
            final ZooMap zooMap = ZooMap.newBuilder(server.getConnectString()).withRoot("").build();
            zooMap.put("Roger", "Federer");
            zooMap.clear();
        });
    }

    @Test
    public void my_map_should_serialize_values_as_utf8_byte_array() {
        withServer(server -> {
            final ZooMap zooMap = ZooMap.newBuilder(server.getConnectString()).withRoot("").build();
            try (CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new RetryOneTime(100))) {
                client.start();
                zooMap.put("Roger", "Fedérer");
                final byte[] bytes = client.getData().forPath("/Roger");
                assertThat(new String(bytes, StandardCharsets.ISO_8859_1)).isNotEqualTo("Fedérer");
                assertThat(new String(bytes, StandardCharsets.UTF_8)).isEqualTo("Fedérer");
            }
        });
    }

    @Test
    public void verify_equals_and_hashcode() {
        EqualsVerifier.forClass(ZooMap.class).withIgnoredFields("client").withNonnullFields("root", "connectionString").verify();
    }

    @Test
    public void an_empty_zoomap_should_respect_the_is_empty_contract() {
        withMap(zoomap -> assertThat(zoomap).isEmpty());
    }

    @Test
    public void a_non_empty_zoomap_should_respect_the_is_empty_contract() {
        withMap(zoomap -> {
            zoomap.put("yo", "lo");
            assertThat(zoomap).isNotEmpty();
        });
    }

    @Test
    public void an_empty_zoomap_should_have_a_size_of_zero() {
        withMap(zoomap -> assertThat(zoomap).hasSize(0));
    }

    @Test
    public void a_zoomap_with_items_should_return_the_correct_size() {
        withMap(zoomap -> {
            zoomap.put("Mambo", "a");
            zoomap.put("Jambo", "b");
            zoomap.put("Rambo", "c");
            assertThat(zoomap).hasSize(3);
        });
    }

    @Test(expected = UnsupportedOperationException.class)
    public void a_zoomap_should_not_support_replace_all() {
        withMap(zoomap -> zoomap.replaceAll(null));
    }

    @Test
    public void my_map_should_create_chroot_without_removing_existing_root_if_exists() {
        withServer((server) -> {
            try(ZooMap zooMap = ZooMap.newMap(server.getConnectString() + "/test/map")) {
                zooMap.put("Roger", "Federer");
            }
            /* Create chroot a second time */
            try(ZooMap zooMap = ZooMap.newMap(server.getConnectString() + "/test/map")) {
                assertThat(zooMap).containsEntry("Roger", "Federer");
            }
        });
    }

    @Test
    public void garbaged_map_should_close_curator_client() throws NamingException {
        withServer(srv -> {
            WeakReference<ZooMap> zooMap = new WeakReference<>(ZooMap.newMap(srv.getConnectString()));
            final CuratorFramework client = ZooMap.Periscope.client(zooMap.get());
            System.gc();
            assertThat(zooMap.get()).isNull();
            assertThat(client.getState()).isEqualTo(CuratorFrameworkState.STOPPED);
        });
    }

    private void withMap(Consumer<Map<String, String>> testBlock) {
        withServer((server) -> {
            try(ZooMap zooMap = ZooMap.newMap(server.getConnectString(), "/test/map")) {
                testBlock.accept(zooMap);
            }
        });
    }

    private void withServer(ThrowingConsumer<TestingServer> testBlock) {
        try(TestingServer server = new TestingServer()) {
            server.start();
            testBlock.accept(server);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}

@FunctionalInterface
interface ThrowingConsumer<T> {
    void accept(T t) throws Exception;
}