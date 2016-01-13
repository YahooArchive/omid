package com.yahoo.omid.tsoclient;

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
        handshake.setClientCapabilities(TSOProto.Capabilities.newBuilder().build());
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
