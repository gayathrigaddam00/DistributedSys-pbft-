package com.example.pbft;

import com.example.pbft.utils.CryptoUtils;
import com.google.protobuf.ByteString;
import com.pbft.pbft.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.security.KeyPair;
import java.security.PrivateKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class BankingClient {

    private static final int REQUIRED_REPLIES_WRITE = 3;
    private static final int REQUIRED_REPLIES_READ = 5;

    private final List<ManagedChannel> channels;
    private final List<PbftNodeGrpc.PbftNodeStub> asyncStubs;
    private final List<PbftNodeGrpc.PbftNodeBlockingStub> blockingStubs;

    public BankingClient(int[] ports) {
        channels = new ArrayList<>();
        asyncStubs = new ArrayList<>();
        blockingStubs = new ArrayList<>();

        for (int port : ports) {
            ManagedChannel channel = ManagedChannelBuilder
                .forAddress("localhost", port)
                .usePlaintext()
                .build();
            channels.add(channel);
            asyncStubs.add(PbftNodeGrpc.newStub(channel));
            blockingStubs.add(PbftNodeGrpc.newBlockingStub(channel));
        }
        System.out.println("Client connected to " + ports.length + " nodes.");
    }

    
    public void runReadOnlyRequestTest(String senderId) throws Exception {
    System.out.println("--- Read-Only Request: Balance of " + senderId + " ---");
    
    KeyPair clientKeyPair = CryptoUtils.generateKeyPair(senderId);
    PrivateKey clientPrivateKey = clientKeyPair.getPrivate();

    Request.Builder requestBuilder = Request.newBuilder()
            .setClientId(senderId)
            .setTimestamp(System.currentTimeMillis())
            .setOperation("BALANCE")
            .setSender(senderId)
            .setReceiver("")
            .setAmount(0);

    byte[] signature = CryptoUtils.sign(clientPrivateKey, requestBuilder.buildPartial().toByteArray());
    Request signedRequest = requestBuilder.setSignature(ByteString.copyFrom(signature)).build();

    int maxRetries = 2;
    int retryCount = 0;
    
    while (retryCount <= maxRetries) {
        if (retryCount > 0) {
            System.out.println("Retry attempt " + retryCount + " after timeout");
            Thread.sleep(2000);
        }
        
        final CountDownLatch finishLatch = new CountDownLatch(REQUIRED_REPLIES_READ);
        final Map<String, Integer> replyCounts = new ConcurrentHashMap<>();
        
        StreamObserver<Reply> sharedObserver = new StreamObserver<>() {
            @Override
            public synchronized void onNext(Reply reply) {
                System.out.println("Received reply from " + reply.getNodeId() + ": " + reply.getResult());
                String result = reply.getResult();
                if (result.startsWith("BALANCE")) {
                    replyCounts.merge(result, 1, Integer::sum);
                    if (replyCounts.get(result) >= REQUIRED_REPLIES_READ) {
                        while(finishLatch.getCount() > 0) {
                            finishLatch.countDown();
                        }
                    }
                }
            }
            
            @Override 
            public void onError(Throwable t) {
                System.err.println("Error: " + t.getMessage());
            }
            
            @Override 
            public void onCompleted() {}
        };
        
        System.out.println("Multicasting read-only request to all nodes...");
        for (PbftNodeGrpc.PbftNodeStub stub : asyncStubs) {
            stub.clientRequest(signedRequest, sharedObserver);
        }
        
        System.out.println("Waiting for 2f+1 = " + REQUIRED_REPLIES_READ + " replies...");
        if (finishLatch.await(10, TimeUnit.SECONDS)) {
            String finalResult = replyCounts.entrySet().stream()
                .filter(entry -> entry.getValue() >= REQUIRED_REPLIES_READ)
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(null);

            if (finalResult != null) {
                System.out.println("SUCCESS: Received " + replyCounts.get(finalResult) + " matching replies.");
                System.out.println("   Final Result: " + finalResult);
                System.out.println();
                return;
            }
        }
        
        System.err.println("TIMEOUT or insufficient matching replies on attempt " + (retryCount + 1));
        if (retryCount == maxRetries) {
            System.out.println("Retrying as regular read-write request (sending to leader)...");
            runReadOnlyAsWriteRequest(senderId);
            return;
        }
        
        retryCount++;
    }
    
    System.err.println("FAILURE: All retry attempts exhausted.");
    System.out.println();
}

private void runReadOnlyAsWriteRequest(String senderId) throws Exception {
    System.out.println("--- Fallback: Balance of " + senderId + " (as write request) ---");
    
    KeyPair clientKeyPair = CryptoUtils.generateKeyPair(senderId);
    PrivateKey clientPrivateKey = clientKeyPair.getPrivate();

    Request.Builder requestBuilder = Request.newBuilder()
            .setClientId(senderId)
            .setTimestamp(System.currentTimeMillis())
            .setOperation("BALANCE")
            .setSender(senderId)
            .setReceiver("")
            .setAmount(0);

    byte[] signature = CryptoUtils.sign(clientPrivateKey, requestBuilder.buildPartial().toByteArray());
    Request signedRequest = requestBuilder.setSignature(ByteString.copyFrom(signature)).build();

    final CountDownLatch finishLatch = new CountDownLatch(REQUIRED_REPLIES_WRITE);
    final Map<String, Integer> replyCounts = new ConcurrentHashMap<>();
    
    StreamObserver<Reply> sharedObserver = new StreamObserver<>() {
        @Override
        public synchronized void onNext(Reply reply) {
            System.out.println("Received reply from " + reply.getNodeId() + ": " + reply.getResult());
            String result = reply.getResult();
            if (result.startsWith("BALANCE")) {
                replyCounts.merge(result, 1, Integer::sum);
                if (replyCounts.get(result) >= REQUIRED_REPLIES_WRITE) {
                    while(finishLatch.getCount() > 0) {
                        finishLatch.countDown();
                    }
                }
            }
        }
        
        @Override 
        public void onError(Throwable t) {
            System.err.println("Error: " + t.getMessage());
        }
        
        @Override 
        public void onCompleted() {}
    };
    
    System.out.println("Sending request to all nodes (as write request)...");
    for (PbftNodeGrpc.PbftNodeStub stub : asyncStubs) {
        stub.clientRequest(signedRequest, sharedObserver);
    }
    
    if (finishLatch.await(15, TimeUnit.SECONDS)) {
        String finalResult = replyCounts.entrySet().stream()
            .filter(entry -> entry.getValue() >= REQUIRED_REPLIES_WRITE)
            .map(Map.Entry::getKey)
            .findFirst()
            .orElse(null);

        if (finalResult != null) {
            System.out.println("SUCCESS: Received " + replyCounts.get(finalResult) + " matching replies.");
            System.out.println("   Final Result: " + finalResult);
            System.out.println();
            return;
        }
    }
    
    System.err.println("FAILURE: Could not get response even as write request.");
    System.out.println();
}

public void setEquivocationTargets(int nodeIndex, List<String> targets) {
        try {
            SetEquivocationTargetsRequest request = SetEquivocationTargetsRequest.newBuilder()
                    .addAllTargetNodeIds(targets)
                    .build();
            blockingStubs.get(nodeIndex).setEquivocationTargets(request);
        } catch (Exception e) {
            System.err.println("Error setting equivocation targets for node " + (nodeIndex + 1) + ": " + e.getMessage());
        }
    }

    public void runWriteRequestTest(String senderId, String receiverId, int amount) throws Exception {
        System.out.println("--- Transaction: " + senderId + " -> " + receiverId + " (" + amount + ") ---");
        
        KeyPair clientKeyPair = CryptoUtils.generateKeyPair(senderId);
        PrivateKey clientPrivateKey = clientKeyPair.getPrivate();

        Request.Builder requestBuilder = Request.newBuilder()
                .setClientId(senderId)
                .setTimestamp(System.currentTimeMillis())
                .setOperation("TRANSFER")
                .setSender(senderId)
                .setReceiver(receiverId)
                .setAmount(amount);

        byte[] signature = CryptoUtils.sign(clientPrivateKey, requestBuilder.buildPartial().toByteArray());
        Request signedRequest = requestBuilder.setSignature(ByteString.copyFrom(signature)).build();

        int maxRetries = 2;
        int retryCount = 0;
        
        while (retryCount <= maxRetries) {
            if (retryCount > 0) {
                System.out.println("Retry attempt " + retryCount + " after timeout (leader may have changed)");
                Thread.sleep(2000);
            }
            
            final CountDownLatch finishLatch = new CountDownLatch(REQUIRED_REPLIES_WRITE);
            final Map<String, Integer> replyCounts = new ConcurrentHashMap<>();
            
            StreamObserver<Reply> sharedObserver = new StreamObserver<>() {
                @Override
                public synchronized void onNext(Reply reply) {
                    System.out.println("Received reply from " + reply.getNodeId() + ": " + reply.getResult());
                    String result = reply.getResult();
                    if (result.startsWith("SUCCESS") || result.startsWith("FAILURE")) {
                        replyCounts.merge(result, 1, Integer::sum);
                        if (replyCounts.get(result) >= REQUIRED_REPLIES_WRITE) {
                            while(finishLatch.getCount() > 0) {
                                finishLatch.countDown();
                            }
                        }
                    }
                }
                
                @Override 
                public void onError(Throwable t) {
                    System.err.println("Error: " + t.getMessage());
                }
                
                @Override 
                public void onCompleted() {}
            };
            
            System.out.println("Sending request to all nodes...");
            for (PbftNodeGrpc.PbftNodeStub stub : asyncStubs) {
                stub.clientRequest(signedRequest, sharedObserver);
            }
            
            System.out.println("Waiting for f+1 = " + REQUIRED_REPLIES_WRITE + " replies...");
            if (finishLatch.await(15, TimeUnit.SECONDS)) {
                String finalResult = replyCounts.entrySet().stream()
                    .filter(entry -> entry.getValue() >= REQUIRED_REPLIES_WRITE)
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElse(null);

                if (finalResult != null) {
                    System.out.println("SUCCESS: Received " + replyCounts.get(finalResult) + " matching replies.");
                    System.out.println("   Final Result: " + finalResult);
                    return;
                }
            }
            
            System.err.println("TIMEOUT on attempt " + (retryCount + 1));
            retryCount++;
        }
        
        System.err.println("FAILURE: All retry attempts exhausted.");
        System.out.println();
    }

    public void setNodeStatus(int nodeIndex, boolean isLive) {
        try {
            NodeStatus request = NodeStatus.newBuilder().setIsLive(isLive).build();
            blockingStubs.get(nodeIndex).setNodeStatus(request);
        } catch (Exception e) {
            System.err.println("Error setting status for node " + (nodeIndex + 1) + ": " + e.getMessage());
        }
    }

    public void setAttackMode(int nodeIndex, AttackMode mode) {
        try {
            SetAttackModeRequest request = SetAttackModeRequest.newBuilder().setMode(mode).build();
            blockingStubs.get(nodeIndex).setAttackMode(request);
        } catch (Exception e) {
            System.err.println("Error setting attack mode for node " + (nodeIndex + 1) + ": " + e.getMessage());
        }
    }
    public void setDarkTargets(int nodeIndex, List<String> targets) {
        try {
            SetDarkTargetsRequest request = SetDarkTargetsRequest.newBuilder()
                    .addAllTargetNodeIds(targets)
                    .build();
            blockingStubs.get(nodeIndex).setDarkTargets(request);
        } catch (Exception e) {
            System.err.println("Error setting dark targets for node " + (nodeIndex + 1) + ": " + e.getMessage());
        }
    }
    
    public void flushAllNodes() {
        System.out.println("Broadcasting FLUSH command to all nodes...");
        FlushRequest request = FlushRequest.newBuilder().build();
        for (PbftNodeGrpc.PbftNodeBlockingStub stub : blockingStubs) {
            try {
                stub.flushState(request);
            } catch (Exception e) {
                System.err.println("Error flushing node: " + e.getMessage());
            }
        }
        System.out.println("Flush command sent.");
    }

    public void printAllDBs() {
        System.out.println("\n--- Printing All Databases ---");
        PrintRequest request = PrintRequest.newBuilder().build();
        for (int i = 0; i < blockingStubs.size(); i++) {
            try {
                PrintDBResponse response = blockingStubs.get(i).printDB(request);
                System.out.println("Node n" + (i + 1) + ":");
                if (response.getDbCount() == 0) {
                    System.out.println("  [Empty]");
                } else {
                    response.getDbMap().entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .forEach(entry -> System.out.println("  " + entry.getKey() + ": " + entry.getValue()));
                }
            } catch (Exception e) {
                System.out.println("Node n" + (i + 1) + ": [OFFLINE - " + e.getMessage() + "]");
            }
        }
        System.out.println("--------------------------------\n");
    }

    public void printAllStatuses(long seq) {
        System.out.println("\n--- Printing Status for Seq #" + seq + " ---");
        PrintStatusRequest request = PrintStatusRequest.newBuilder().setSequenceNumber(seq).build();
        for (int i = 0; i < blockingStubs.size(); i++) {
            try {
                PrintStatusResponse response = blockingStubs.get(i).printStatus(request);
                System.out.println("Node n" + (i + 1) + ": " + response.getStatus());
            } catch (Exception e) {
                System.out.println("Node n" + (i + 1) + ": [OFFLINE - " + e.getMessage() + "]");
            }
        }
        System.out.println("------------------------------------\n");
    }
    
    public void printAllViews() {
        System.out.println("\n--- Printing View Status ---");
        PrintRequest request = PrintRequest.newBuilder().build();
        for (int i = 0; i < blockingStubs.size(); i++) {
            try {
                PrintViewResponse response = blockingStubs.get(i).printView(request);
                System.out.println("Node n" + (i + 1) + ": " + response.getPlaceholder());
            } catch (Exception e) {
                System.out.println("Node n" + (i + 1) + ": [OFFLINE - " + e.getMessage() + "]");
            }
        }
        System.out.println("----------------------------\n");
    }
public void printLog(int nodeIndex) {
    System.out.println("\n--- Message Log for Node n" + (nodeIndex + 1) + " ---");
    try {
        PrintRequest request = PrintRequest.newBuilder().build();
        PrintLogResponse response = blockingStubs.get(nodeIndex).printLog(request);
        
        if (response.getLogEntriesCount() == 0) {
            System.out.println("  [Empty log]");
        } else {
            for (String entry : response.getLogEntriesList()) {
                System.out.println(entry);
            }
        }
    } catch (Exception e) {
        System.out.println("  [OFFLINE - " + e.getMessage() + "]");
    }
    System.out.println("---" + "-".repeat(40) + "---\n");
}

public void printAllLogs() {
    for (int i = 0; i < blockingStubs.size(); i++) {
        printLog(i);
    }
}
    public void shutdown() {
        System.out.println("Shutting down client connections...");
        for (ManagedChannel channel : channels) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                channel.shutdownNow();
            }
        }
    }
}