/*
 *  Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.ballerinalang.jdbc;

import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import org.ballerinalang.sql.nativeimpl.ClientProcessor;
import org.ballerinalang.sql.utils.ErrorGenerator;

import java.util.Locale;

/**
 * This class will include the native method implementation for the JDBC client.
 *
 * @since 1.2.0
 */
public class NativeImpl {

    public static Object createClient(BObject client, BMap<BString, Object> clientConfig,
                                      BMap<BString, Object> globalPool) {
        BMap<BString, Object> connectionParameters = ValueCreator.createMapValue();
        BString url = clientConfig.getStringValue(Constants.ClientConfiguration.URL);
        if (!isJdbcUrlValid(url.getValue())) {
            return ErrorGenerator.getSQLApplicationError("Invalid JDBC URL: " + url);
        }
        connectionParameters.put(org.ballerinalang.sql.Constants.SQLParamsFields.URL, url);
        BString user = clientConfig.getStringValue(Constants.ClientConfiguration.USER);
        connectionParameters.put(org.ballerinalang.sql.Constants.SQLParamsFields.USER, user);
        BString password = clientConfig.getStringValue(Constants.ClientConfiguration.PASSWORD);
        connectionParameters.put(org.ballerinalang.sql.Constants.SQLParamsFields.PASSWORD, password);
        BMap options = clientConfig.getMapValue(Constants.ClientConfiguration.OPTIONS);
        BMap properties = null;
        BString datasourceName = null;
        BMap<BString, Object> poolProperties = null;
        if (options != null) {
            properties = options.getMapValue(Constants.ClientConfiguration.PROPERTIES);
            System.out.println("properties: "+properties+"\n");
            datasourceName = options.getStringValue(Constants.ClientConfiguration.DATASOURCE_NAME);
            System.out.println("datasourceName: "+datasourceName+"\n");
            if (properties != null) {
                System.out.println();
                for (Object propKey : properties.getKeys()) {
                    if (propKey.toString().toLowerCase(Locale.ENGLISH).matches(Constants.CONNECT_TIMEOUT)) {
                        System.out.println("propKey: "+propKey+"\n");
                        poolProperties = ValueCreator.createMapValue();
                        System.out.println("poolProperties: "+poolProperties);
                        poolProperties.put(Constants.POOL_CONNECTION_TIMEOUT,
                                                   properties.getStringValue((BString) propKey));
                        System.out.println("poolProperties: "+poolProperties);
                    }
                }
            }
        }
        connectionParameters.put(org.ballerinalang.sql.Constants.SQLParamsFields.DATASOURCE_NAME, datasourceName);
        connectionParameters.put(org.ballerinalang.sql.Constants.SQLParamsFields.OPTIONS, options);
        BMap connectionPool = clientConfig.getMapValue(Constants.ClientConfiguration.CONNECTION_POOL_OPTIONS);
        connectionParameters.put(org.ballerinalang.sql.Constants.SQLParamsFields.CONNECTION_POOL, connectionPool);
        connectionParameters.put(org.ballerinalang.sql.Constants.SQLParamsFields.CONNECTION_POOL_OPTIONS,
                        poolProperties);
        
        return ClientProcessor.createSqlClient(client, connectionParameters, globalPool);
    }

    // Unable to perform a complete validation since URL differs based on the database.
    private static boolean isJdbcUrlValid(String jdbcUrl) {
        return !jdbcUrl.isEmpty() && jdbcUrl.trim().startsWith("jdbc:");
    }

    public static Object close(BObject client) {
        return ClientProcessor.close(client);
    }
}
