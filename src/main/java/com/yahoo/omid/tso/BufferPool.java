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

package com.yahoo.omid.tso;

import java.io.ByteArrayOutputStream;
import java.util.ArrayDeque;
import java.util.Deque;

public class BufferPool {

    private static Deque<ByteArrayOutputStream> pool = new ArrayDeque<ByteArrayOutputStream>();
    
    public static synchronized ByteArrayOutputStream getBuffer() {
        if (pool.isEmpty()) {
            return new ByteArrayOutputStream(1500);
        }
        return pool.pollLast();
    }
    

    public static synchronized void pushBuffer(ByteArrayOutputStream buffer) {
        pool.add(buffer);
    }
}
