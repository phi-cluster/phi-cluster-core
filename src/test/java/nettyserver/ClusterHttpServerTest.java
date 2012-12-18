package nettyserver;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.util.CharsetUtil;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.*;
import org.phicluster.nettyserver.ClusterHttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class ClusterHttpServerTest extends SimpleChannelUpstreamHandler {
    protected static final Logger logger = LoggerFactory.getLogger(ClusterHttpServerTest.class);

    private static final String HOSTNAME = "localhost";
    private static final int PORT = 8080;

    private static ClusterHttpServer httpServer;

    private ClientBootstrap bootstrap;
    private ChannelFuture channel;
    private JSONObject jsonObject;
    private ChannelBuffer content;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        httpServer = new ClusterHttpServer(8080);
        httpServer.run();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        httpServer.stop();
    }

    @Before
    public void setUp() throws Exception {
        // http client
        ChannelFactory factory = new NioClientSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());

        bootstrap = new ClientBootstrap(factory);

        logger.info("client bootstrap created...");

        bootstrap.getPipeline().addLast("codec", new HttpClientCodec());
        bootstrap.getPipeline().addLast("aggregator", new HttpChunkAggregator(1048576));
        bootstrap.getPipeline().addLast("handler", this);
        channel = bootstrap.connect(new InetSocketAddress(HOSTNAME, PORT));
        channel.awaitUninterruptibly();

        logger.info("connected to server...");
    }

    @After
    public void tearDown() throws Exception {
        logger.info("tearing down...");
        channel.addListener(ChannelFutureListener.CLOSE);
    }

    @Test
    public void testPerformPost() {
        HttpRequest request = new DefaultHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.POST, "http://" + HOSTNAME + "/post_path");

        jsonObject = new JSONObject();
        jsonObject.put("testkey2", "testvalue2");
        content = ChannelBuffers.copiedBuffer(jsonObject.toJSONString(), CharsetUtil.UTF_8);

        setHeadersAndContent(request);

        logger.info("sending request, and waiting close...");

        channel.getChannel().write(request).awaitUninterruptibly();
        channel.getChannel().getCloseFuture().awaitUninterruptibly();

        Assert.assertTrue(channel.isDone());
        Assert.assertTrue(channel.isSuccess());
    }

//    @Test
//    public void testPerformGet() {
//        HttpRequest request = new DefaultHttpRequest(
//                HttpVersion.HTTP_1_1, HttpMethod.GET, "http://" + HOSTNAME);
//
//        jsonObject = new JSONObject();
//        jsonObject.put("testkey3", "testvalue3");
//        content = ChannelBuffers.copiedBuffer(jsonObject.toJSONString(), CharsetUtil.UTF_8);
//        setHeadersAndContent(request);
//
//        logger.info("sending request, and waiting close...");
//
//        channel.getChannel().write(request).awaitUninterruptibly();
//        channel.getChannel().getCloseFuture().awaitUninterruptibly();
//
//        Assert.assertTrue(channel.isDone());
//    }

    private void setHeadersAndContent(HttpRequest request) {
        request.setHeader(HttpHeaders.Names.HOST, HOSTNAME);
        request.setHeader(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
        request.setHeader(HttpHeaders.Names.CONTENT_TYPE, "application/json");
        request.setHeader(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(content.readableBytes()));
        request.setContent(content);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        logger.info("message received...");

        HttpResponse response = (HttpResponse) e.getMessage();

        logger.info("STATUS: {}", response.getStatus());
        logger.info("VERSION: {}", response.getProtocolVersion());

        if (!response.getHeaderNames().isEmpty()) {
            for (String name : response.getHeaderNames()) {
                for (String value : response.getHeaders(name)) {
                    logger.info("HEADER: {} = {}", name, value);
                }
            }
        }
        ChannelBuffer content = response.getContent();
        if (content.readable()) {
            // trim is important, it removes \r\n chars
            String contentString = content.toString(CharsetUtil.UTF_8).trim();
            logger.info("CONTENT: {}", contentString);
            JSONParser parser = new JSONParser();
            JSONObject parsedJson = (JSONObject) parser.parse(contentString);
            // check if it returns success code (0)
            Assert.assertEquals("0", parsedJson.get("responseCode"));
        }

        channel.getChannel().disconnect();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        e.getCause().printStackTrace();
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        logger.info("channel connected: {}", e);
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        logger.info("channel disconnected: {}", e);
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        logger.info("channel closed: {}", e);
    }
}
