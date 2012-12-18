package org.phicluster.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.gearman.Gearman;
import org.gearman.GearmanClient;
import org.gearman.GearmanJobEvent;
import org.gearman.GearmanJobEventCallback;
import org.gearman.GearmanJobEventType;
import org.gearman.GearmanJoin;
import org.gearman.GearmanServer;

import org.phicluster.core.DistTaskPool;
import org.phicluster.core.GearmanDistTaskExecutor;
import org.phicluster.core.ZKInit;
import org.phicluster.core.task.request.RequestCode;
import org.phicluster.core.util.ByteUtil;


public class TestRequestorClient implements GearmanJobEventCallback<Integer> {

    public static final int GEARMAN_DEFAULT_SERVER_PORT = 4730;

    private static boolean unitTestMode = true;
    
    public static void main(String[] args) {
        // args[0]: number of requests to be submitted
        // args[1]: german nettyserver -> "<IP:PORT>,..."
        if (args.length != 2) {
            System.out.println("must have two arguments: <number of request> <IP:PORT,IP:PORT,...>");
        }
        int numberOfRequests = Integer.parseInt(args[0]);
        StringTokenizer tokens = new StringTokenizer(args[1], ",");
        Map<String, Integer> gearmanServerHosts = new HashMap<>();
        while (tokens.hasMoreTokens()) {
            String s = tokens.nextToken();
            String[] parts = s.split(":");
            if (parts.length == 1) {
                gearmanServerHosts.put(parts[0], GEARMAN_DEFAULT_SERVER_PORT);
            } else {
                gearmanServerHosts.put(parts[0], Integer.parseInt(parts[1]));
            }
        }
        
        if (gearmanServerHosts.isEmpty()) {
            throw new RuntimeException("no gearman servers provided");
        }

        if (unitTestMode) {
            try {
                prepareUnitTestMode(gearmanServerHosts);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        
        Gearman gearman = Gearman.createGearman();
        GearmanClient gearmanClient = gearman.createGearmanClient();
        Map<String, GearmanServer> gearmanServers = new HashMap<>();
        
        for (String host : gearmanServerHosts.keySet()) {
            Integer port = gearmanServerHosts.get(host);
            GearmanServer gserver = gearman.createGearmanServer(host, port); 
            gearmanServers.put(host, gserver);
            gearmanClient.addServer(gserver);
        }
        
        String functionName = RequestCode.SIMULATE_REQUEST.gearmanFunctionName();
        TestRequestorClient callback = new TestRequestorClient();
        List<GearmanJoin<Integer>> submittedJobs = new ArrayList<>();
        for (int i = 0; i < numberOfRequests; i++) {
            String jsonString = "{\"request\":{\"stages\":3,\"sleep\":5000}}";
            GearmanJoin<Integer> join = gearmanClient.submitJob(functionName, 
                                                                jsonString.getBytes(), 
                                                                new Integer(i), 
                                                                callback);
            submittedJobs.add(join);
        }
        
        boolean allCompleted = false;
        while(!allCompleted) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            allCompleted = true;
            for (GearmanJoin<Integer> join : submittedJobs) {
                if (!join.isEOF()) {
                    allCompleted = false;
                    break;
                }
            }
        }
        
        System.out.println("\n all submitted gearman jobs completed: " + submittedJobs.size());
        System.out.println("done.");
        try {
            System.in.read(); // wait for any key
            tearDownUnitTestMode();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    private static ZKInit zkInit;
    private static MockKernel kernel;
    private static MockKernel kernel_backup;

    private static void prepareUnitTestMode(Map<String, Integer> gearmanServers) throws Exception {
        zkInit = CoreSetup.initZookeeper(MockKernel.ZOOKEEPER_FOR_UNIT_TEST, true);

        // creating kernels for three nodes
        System.out.println("creating kernel...");
        kernel = CoreSetup.createMockKernel(1);// new MockKernel(1);
        kernel_backup = CoreSetup.createMockKernel(2); // for task replication
        kernel.initDistTaskSystem();
        kernel.mockAccrualFailureDetector.numberOfNodes = 2;
        
        DistTaskPool dtp = DistTaskPool.defaultInstance();
        GearmanDistTaskExecutor gearmanDistTask = new GearmanDistTaskExecutor(dtp, gearmanServers);
    }
    
    private static void tearDownUnitTestMode() throws Exception {
        System.out.println("tearing down unit test....");
        
        // clean kernel data in zk
        kernel.tearDown();
        kernel_backup.tearDown();
        
        // clean cluster data in zk
        CoreSetup.tearDownZKInit(zkInit);
        System.out.println("done.\n");
    }    

    @Override
    public void onEvent(Integer requestNo, GearmanJobEvent event) {
        byte[] eventData = event.getData();
        long taskId = -1;
        if (event.getEventType() == GearmanJobEventType.GEARMAN_JOB_SUCCESS && eventData.length >= 8) {
            taskId = ByteUtil.readLong(event.getData(), 0);
        }
        String s = String.format("[gearman job callback] request #: %d, task-id: %d, " +
        		"gearman job event: %s", requestNo, taskId, event.getEventType());
        System.out.println(s);
    }

}
