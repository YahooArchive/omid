package com.yahoo.omid.tso;

import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;

import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;

import com.yahoo.omid.proto.TSOProto;

public class TSOPipelineFactory implements ChannelPipelineFactory {
    private final ChannelHandler handler;

    public TSOPipelineFactory(ChannelHandler handler) {
        this.handler = handler;
    }

    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();
        // Max packet length is 10MB. Transactions with so many cells
        // that the packet is rejected will receive a ServiceUnavailableException.
        // 10MB is enough for 2 million cells in a transaction though.
        pipeline.addLast("lengthbaseddecoder",
                         new LengthFieldBasedFrameDecoder(10*1024*1024, 0, 4, 0, 4));
        pipeline.addLast("lengthprepender", new LengthFieldPrepender(4));

        pipeline.addLast("protobufdecoder",
                         new ProtobufDecoder(TSOProto.Request.getDefaultInstance()));
        pipeline.addLast("protobufencoder", new ProtobufEncoder());

        pipeline.addLast("handler", handler);
        return pipeline;
    }

}
