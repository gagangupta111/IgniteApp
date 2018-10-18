package com.ignite.weatherwarning;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import javax.cache.Cache;

public class WeatherWarning {

    public static void main(String[] args) throws IgniteException {

        Ignite ignite =  Ignition.start("examples/config/example-ignite.xml");

        long cityId = 2; // Id for Denver

        // Sending the logic to a cluster node that stores Denver and its residents.
        ignite.compute().affinityRun("SQL_PUBLIC_CITY", cityId, new IgniteRunnable() {

            @IgniteInstanceResource
            Ignite ignite;

            @Override
            public void run() {

                IgniteCache<BinaryObject, BinaryObject> people = ignite.cache(
                        "Person").withKeepBinary();

                ScanQuery<BinaryObject, BinaryObject> query =
                        new ScanQuery <BinaryObject, BinaryObject>();

                try (QueryCursor<Cache.Entry<BinaryObject, BinaryObject>> cursor =
                             people.query(query)) {

                    for (Cache.Entry<BinaryObject, BinaryObject> entry : cursor) {
                        BinaryObject personKey = entry.getKey();

                        if (personKey.<Long>field("CITY_ID") == cityId) {

                            BinaryObject person = entry.getValue();
                            System.out.println("Sending the warning message to person : key : " + personKey + " value : " + person);

                        }
                    }
                }
            }
        });

    }

}
