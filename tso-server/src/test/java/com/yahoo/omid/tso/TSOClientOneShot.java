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

import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.omid.proto.TSOProto;
import com.yahoo.omid.proto.TSOProto.Response;

/**
 * Communication endpoint for TSO clients.
 */
public class TSOClientOneShot {

    private static final Logger LOG = LoggerFactory.getLogger(TSOClientOneShot.class);

    private final String host;
    private final int port;


    public TSOClientOneShot(String host, int port) {

        this.host = host;
        this.port = port;

    }

    public TSOProto.Response makeRequest(TSOProto.Request request)
            throws InterruptedException, ExecutionException {
        TSOClientRaw raw = new TSOClientRaw(host, port);

        // do handshake
        TSOProto.HandshakeRequest.Builder handshake = TSOProto.HandshakeRequest.newBuilder();
        handshake.setClientCapabilities(
                TSOProto.Capabilities.newBuilder()
                .setShortSuffixes(true).build());
        raw.write(TSOProto.Request.newBuilder()
                  .setHandshakeRequest(handshake.build()).build());
        Response response = raw.getResponse().get();
        assert(response.getHandshakeResponse().getClientCompatible());

        raw.write(request);
        response = raw.getResponse().get();

        raw.close();
        return response;
    }

}
