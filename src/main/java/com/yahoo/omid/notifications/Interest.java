/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package com.yahoo.omid.notifications;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This class represents the interest of a component in a particular column of a
 * particular column family of a table
 * 
 */
public class Interest {

    private static final Log logger = LogFactory.getLog(Interest.class);

    private String table;
    private String columnFamily;
    private String column;

    /**
     * Constructs an interest object from a string with the following format
     * tableName:columnFamily:column
     * 
     * @param interest The particular string codifying the interest
     * @return the Interest object built
     * @throws IllegalArgumentException Thrown when the interest parameter does not follow the right format
     */
    public static Interest fromString(String interest) throws IllegalArgumentException {
        String delims = "[:]";
        String[] tokens = interest.split(delims);
        if (tokens.length != 3) { // TODO Add more checks maybe or do it with a better regex???
            logger.error("Error parsing interest. Tokens length: " + tokens.length + " Table: " + tokens[0] + " CF: "
                    + tokens[1] + " Col: " + tokens[2]);
            throw new IllegalArgumentException(
                    "Cannot parse interest. Format should be \"tableName:columnFamily:column\"");
        }
        return new Interest(tokens[0], tokens[1], tokens[2]);
    }

    public Interest(String table, String columnFamily, String column) {
        this.table = table;
        this.columnFamily = columnFamily;
        this.column = column;
    }

    /**
     * @return the table
     */
    public String getTable() {
        return table;
    }

    /**
     * @return the table in HBase specific format
     */
    public byte[] getTableAsHBaseByteArray() {
        return Bytes.toBytes(table);
    }

    /**
     * @return the columnFamily
     */
    public String getColumnFamily() {
        return columnFamily;
    }

    /**
     * @return the columnFamily in HBase specific format
     */
    public byte[] getColumnFamilyAsHBaseByteArray() {
        return Bytes.toBytes(columnFamily);
    }

    /**
     * @return the column
     */
    public String getColumn() {
        return column;
    }

    /**
     * @return the column in HBase specific format
     */
    public byte[] getColumnAsHBaseByteArray() {
        return Bytes.toBytes(column);
    }
    
    /**
     * Used when an interest is registered in a particular column
     * @return the Zk node representation
     */
    public String toZkNodeRepresentation() {
        StringBuilder sb = new StringBuilder(table);
        sb.append(":");
        sb.append(columnFamily);
        sb.append(":");
        sb.append(column);
        return sb.toString();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Interest [table=" + table + ", columnFamily=" + columnFamily + ", column=" + column + "]";
    }
    
    
}
