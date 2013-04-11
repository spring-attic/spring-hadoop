/*
 * Copyright 2011-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;
import org.springframework.util.Assert;

import java.nio.charset.Charset;
import java.util.List;

/**
 * Provides functionality to access Hbase over REST by wrapping Hbase {@link org.apache.hadoop.hbase.rest.client.Client}
 *
 * @author Muhammad Ashraf
 * @since 4/9/13
 */
public class HBaseRestProtocol implements HBaseProtocol {
    /**
     * Thread safe HTTP Client for Hbase
     */
    private final Client client;

    public HBaseRestProtocol(List<String> addresses) {
        Assert.notNull(addresses, " remote HBase address is required");
        final Cluster cluster = createHBaseRemoteCluster(addresses);
        client = new Client(cluster);

    }

    private Cluster createHBaseRemoteCluster(List<String> addresses) {
        final Cluster cluster = new Cluster();
        for (String address : addresses) {

            cluster.add(address);
        }
        return cluster;
    }


    @Override
    public HTableInterface getHTable(String tableName, Configuration configuration, Charset charset, HTableInterfaceFactory tableFactory) {
        try {
            return new RemoteHTable(client, tableName);
        } catch (Exception e) {
            throw HbaseUtils.convertHbaseException(e);
        }
    }

    @Override
    public void releaseTable(String tableName, HTableInterface table, HTableInterfaceFactory tableFactory) {
        /**
         * NO OP operation as calling table.close() on RemoteHTable shuts down underlying http client.
         */
    }

    public void shutdown() {
        client.shutdown();
    }

}
