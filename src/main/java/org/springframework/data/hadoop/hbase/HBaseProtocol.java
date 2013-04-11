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
 * @author Muhammad Ashraf
 * @since 4/9/13
 */
public interface HBaseProtocol {


    /**
     * Retrieves an Hbase table instance identified by its name and charset using the given table factory.
     *
     * @param tableName     table name
     * @param configuration Hbase configuration object
     * @param charset       name charset (may be null)
     * @param tableFactory  table factory (may be null)
     * @return table instance
     */
    public HTableInterface getHTable(String tableName, Configuration configuration, Charset charset, HTableInterfaceFactory tableFactory);

    /**
     * Releases (or closes) the given table, created via the given configuration if it is not managed externally (or bound to the thread).
     *
     * @param tableName
     * @param table
     */
    public  void releaseTable(String tableName, HTableInterface table, HTableInterfaceFactory tableFactory);
}
