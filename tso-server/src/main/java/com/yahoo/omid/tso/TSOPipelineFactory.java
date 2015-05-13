/**
 * Copyright 2011-2015 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
