package com.ignite;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;

public final class SimpleCacheExample {
    /** Cache name. */
    private static final String CACHE_NAME = SimpleCacheExample.class.getSimpleName();

    /** Number of keys. */
    private static final int KEY_CNT = 1000;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) throws InterruptedException {

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")){
            System.out.println();
            System.out.println(">>> Cache affinity example started.");

            try (IgniteCache<Integer, String> cache = ignite.getOrCreateCache(CACHE_NAME)) {
                // Clear caches before running example.
                cache.clear();

                for (int i = 0; i < KEY_CNT; i++)
                    cache.put(i, Integer.toString(i));

                // Co-locates jobs with data using GridCompute.affinityRun(...) method.
                visitUsingAffinityRun();

            }
        }
    }

    /**
     * Collocates jobs with keys they need to work on using
     * {@link IgniteCompute#affinityRun(String, Object, IgniteRunnable)} method.
     */
    private static void visitUsingAffinityRun() throws InterruptedException {
        Ignite ignite = Ignition.ignite();

        final IgniteCache<Integer, String> cache = ignite.cache(CACHE_NAME);

        for (int i = 0; i < KEY_CNT; i++) {
            final int key = i;

            if (i % 20 == 0){
                Thread.sleep(2000);
            }
            // This runnable will execute on the remote node where
            // data with the given key is located. Since it will be co-located
            // we can use local 'peek' operation safely.
            ignite.compute().affinityRun(CACHE_NAME, key, new IgniteRunnable() {
                @Override public void run() {
                    // Peek is a local memory lookup, however, value should never be 'null'
                    // as we are co-located with node that has a given key.
                    System.out.println("Co-located using affinityRun [key= " + key +
                            ", value=" + cache.localPeek(key) + ']');
                }
            });
        }
    }

    /*
    private static void visitUsingMapKeysToNodes() {
        final Ignite ignite = Ignition.ignite();

        Collection<Integer> keys = new ArrayList<>(KEY_CNT);

        for (int i = 0; i < KEY_CNT; i++)
            keys.add(i);

        // Map all keys to nodes.
        Map<ClusterNode, Collection<Integer>> mappings = ignite.cluster().mapKeysToNodes(CACHE_NAME, keys);

        for (Map.Entry<ClusterNode, Collection<Integer>> mapping : mappings.entrySet()) {
            ClusterNode node = mapping.getKey();

            final Collection<Integer> mappedKeys = mapping.getValue();

            if (node != null) {
                // Create cluster group with one node.
                ClusterGroup grp = ignite.cluster().forNode(node);

                // Bring computations to the nodes where the data resides (i.e. collocation).
                ignite.compute(grp).run(new IgniteRunnable() {
                    @Override public void run() {
                        IgniteCache<Integer, String> cache = ignite.cache(CACHE_NAME);

                        // Peek is a local memory lookup, however, value should never be 'null'
                        // as we are co-located with node that has a given key.
                        for (Integer key : mappedKeys)
                            System.out.println("Co-located using mapKeysToNodes [key= " + key +
                                    ", value=" + cache.localPeek(key) + ']');
                    }
                });
            }
        }
    }
*/

}

