package org.phicluster.nettyserver;

import static org.jboss.netty.channel.Channels.*;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.http.*;

public class ClusterHttpServerPipelineFactory implements ChannelPipelineFactory {

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = pipeline();

        pipeline.addLast("decoder", new HttpRequestDecoder());
        pipeline.addLast("encoder", new HttpResponseEncoder());
        pipeline.addLast("aggregator", new HttpChunkAggregator(1048576));
        pipeline.addLast("deflater", new HttpContentCompressor());
        pipeline.addLast("handler", new ClusterHttpServerHandler());

        return pipeline;
    }
}
