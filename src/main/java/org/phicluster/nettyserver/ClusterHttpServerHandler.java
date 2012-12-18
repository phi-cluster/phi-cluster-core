package org.phicluster.nettyserver;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.util.CharsetUtil;
import org.json.simple.JSONObject;
import org.phicluster.core.DistTaskPool;
import org.phicluster.core.task.TaskData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.is100ContinueExpected;
import static org.jboss.netty.handler.codec.http.HttpHeaders.isKeepAlive;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class ClusterHttpServerHandler extends SimpleChannelUpstreamHandler {
    protected static final Logger logger = LoggerFactory.getLogger(ClusterHttpServerHandler.class);

    private HttpRequest request;
    private final StringBuilder paramBuffer = new StringBuilder();
    private JSONObject jsonResponse;

    private DistTaskPool distTaskPool = DistTaskPool.defaultInstance();

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        HttpRequest request = this.request = (HttpRequest) e.getMessage();
        if (is100ContinueExpected(request)) {
            send100Continue(e);
        }

        ChannelBuffer content = request.getContent();
        if (content.readable()) {
            String jsonTaskData = content.toString(CharsetUtil.UTF_8);
            if (distTaskPool != null) {
                TaskData taskData = distTaskPool.offer(jsonTaskData);
                logger.info ("task created: {}", taskData);
                // TODO: needs more beautiful way to define JSON responses
                jsonResponse = new JSONObject();
                jsonResponse.put("responseCode", "0");
                jsonResponse.put("responseString", "Task successfully created");
            }
            else {
                jsonResponse = new JSONObject();
                jsonResponse.put("responseCode", "1");
                jsonResponse.put("responseString", "Distributed task pool is not active");
            }
        }

        // for future usage
        paramBuffer.setLength(0);

        QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.getUri());
        Map<String, List<String>> params = queryStringDecoder.getParameters();
        if (!params.isEmpty()) {
            for (Map.Entry<String, List<String>> p : params.entrySet()) {
                String key = p.getKey();
                List<String> vals = p.getValue();
                for (String val : vals) {
                    paramBuffer.append("PARAM: ").append(key).append(" = ").append(val).append("\r\n");
                }
            }
            paramBuffer.append("\r\n");
        }

        writeResponse(e);
    }

    private void writeResponse(MessageEvent e) {
        boolean keepAlive = isKeepAlive(request);

        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);
        response.setContent(ChannelBuffers.copiedBuffer(jsonResponse.toJSONString(), CharsetUtil.UTF_8));
        response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");

        if (keepAlive) {
            response.setHeader(CONTENT_LENGTH, response.getContent().readableBytes());
            response.setHeader(CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
        }

        ChannelFuture future = e.getChannel().write(response);
        if (!keepAlive) {
            future.addListener(ChannelFutureListener.CLOSE);
        }
    }

    private static void send100Continue(MessageEvent e) {
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, CONTINUE);
        e.getChannel().write(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        logger.warn("exception occurred: {}", e.getCause());
        e.getChannel().close();
    }
}
