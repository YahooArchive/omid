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

import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

/**
 * Test the interest class
 * 
 */
public class TestInterest {

    private static final Log LOG = LogFactory.getLog(TestInterest.class);

    @Test
    public void testFromStringReturnsAValidInterestObjectWhenPassingAStringInTheCorrectFormat() throws Exception {
        Interest interest = Interest.fromString("<table:columnFamily:column>");

        assertTrue("Unexpected value for table name: " + interest.getTable(), "table".equals(interest.getTable()));
        assertTrue("Unexpected value for column family name: " + interest.getColumnFamily(),
                "columnFamily".equals(interest.getColumnFamily()));
        assertTrue("Unexpected value for column name: " + interest.getColumn(), "column".equals(interest.getColumn()));
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testFromStringThrowsIllegalArgumentExceptionWhenPassingAStringWithAWrongFormat() throws Exception {
        Interest.fromString("<>table:>columnFamily:<column<>");
    }

}
