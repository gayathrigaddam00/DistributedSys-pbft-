package com.example.pbft.config;

import com.example.pbft.utils.CryptoUtils;
import com.pbft.pbft.proto.PbftNodeGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Configuration
public class PbftConfig {

    @Value("${pbft.nodeId}")
    private String nodeId;

    private final Map<String, String> peerAddresses;
    private final Map<String, PublicKey> nodePublicKeys = new HashMap<>();
    private final Map<String, PrivateKey> nodePrivateKeys = new HashMap<>();
    private final Map<String, PublicKey> clientPublicKeys = new HashMap<>();

    public PbftConfig() {
        peerAddresses = new HashMap<>();
        this.peerAddresses.put("n1", "localhost:50051");
        this.peerAddresses.put("n2", "localhost:50052");
        this.peerAddresses.put("n3", "localhost:50053");
        this.peerAddresses.put("n4", "localhost:50054");
        this.peerAddresses.put("n5", "localhost:50055");
        this.peerAddresses.put("n6", "localhost:50056");
        this.peerAddresses.put("n7", "localhost:50057");

        try {
            for (String nodeId : peerAddresses.keySet()) {
                KeyPair keyPair = CryptoUtils.generateKeyPair(nodeId);
                nodePublicKeys.put(nodeId, keyPair.getPublic());
                nodePrivateKeys.put(nodeId, keyPair.getPrivate());
            }
            for (char c = 'A'; c <= 'J'; c++) {
                String clientId = String.valueOf(c);
                KeyPair keyPair = CryptoUtils.generateKeyPair(clientId);
                clientPublicKeys.put(clientId, keyPair.getPublic());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate key pairs", e);
        }
    }

    @Bean
    public String pbftNodeId() {
        return nodeId;
    }

    @Bean
    public Map<String, PbftNodeGrpc.PbftNodeStub> peerStubsPbft() {
        return peerAddresses.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> {
                            String[] parts = e.getValue().split(":");
                            ManagedChannel channel = ManagedChannelBuilder
                                    .forAddress(parts[0], Integer.parseInt(parts[1]))
                                    .usePlaintext()
                                    .build();
                            return PbftNodeGrpc.newStub(channel);
                        }
                ));
    }

  
    @Bean
    public Map<String, PbftNodeGrpc.PbftNodeBlockingStub> peerBlockingStubs() {
        return peerAddresses.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> {
                            String[] parts = e.getValue().split(":");
                            ManagedChannel channel = ManagedChannelBuilder
                                    .forAddress(parts[0], Integer.parseInt(parts[1]))
                                    .usePlaintext()
                                    .build();
                            return PbftNodeGrpc.newBlockingStub(channel);
                        }
                ));
    }

    public Map<String, String> getPeerAddresses() {
        return peerAddresses;
    }

    public PublicKey getNodePublicKey(String nodeId) {
        return nodePublicKeys.get(nodeId);
    }
    
    public PublicKey getClientPublicKey(String clientId) {
        return clientPublicKeys.get(clientId);
    }

    public PrivateKey getNodePrivateKey(String nodeId) {
        return nodePrivateKeys.get(nodeId);
    }
    
    public Set<String> getPeerIds() {
        return peerAddresses.keySet();
    }
}