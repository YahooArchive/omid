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

package com.yahoo.omid.tso.persistence;

import com.yahoo.omid.tso.TSOState;

public class LoggerProtocol extends TSOState{
    
    /*
     * Protocol flags. Used to identify fields of the logger records.
     */
    public final static byte COMMIT = (byte) -2;
    public final static byte ABORT = (byte) -3;
    public final static byte FULLABORT = (byte) -4;
    
    
    /**
     * Logger protocol constructor. Currently it only constructs the
     * super class, TSOState.
     * 
     * @param logger
     * @param largestDeletedTimestamp
     */
    LoggerProtocol(StateLogger logger, long largestDeletedTimestamp){
        super(logger, largestDeletedTimestamp);
    }
    
    void execute(int op, long value){
     switch(op){
     case COMMIT:
         processCommit(value);
         break;
     case ABORT:
         processAbort(value);
         break;
     case FULLABORT:
         processFullAbort(value);
     }
        
    }
    
    /**
     * Returns a TSOState object based on this object.
     * 
     * @return
     */
    TSOState getState(){
        return ((TSOState) this);
    }

}
