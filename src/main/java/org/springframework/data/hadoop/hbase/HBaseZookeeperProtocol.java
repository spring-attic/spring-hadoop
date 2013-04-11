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

import java.nio.charset.Charset;

/**
 * Provides functionality to access Hbase through Zookeeper
 * @author Muhammad Ashraf
 * @since 4/9/13
 */
public class HBaseZookeeperProtocol implements HBaseProtocol {

    @Override
    public HTableInterface getHTable(String tableName, Configuration configuration, Charset charset, HTableInterfaceFactory tableFactory) {
        return HbaseUtils.getHTable(tableName, configuration, charset, tableFactory);
    }

    @Override
    public void releaseTable(String tableName, HTableInterface table, HTableInterfaceFactory tableFactory) {
        HbaseUtils.releaseTable(tableName, table, tableFactory);
    }
}
