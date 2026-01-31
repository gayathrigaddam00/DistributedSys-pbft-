package com.example.pbft.transport;

import com.example.pbft.core.PbftCore;
import com.example.pbft.core.PbftCore.BroadcastAction;
import com.example.pbft.core.PbftCore.DirectMessageAction;
import com.example.pbft.core.PbftCore.ReplyAction;
import com.example.pbft.core.PbftCore.MultipleActions;
import com.google.protobuf.AbstractMessage;
import com.pbft.pbft.proto.*;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@GrpcService
public class PbftNodeGrpcService extends PbftNodeGrpc.PbftNodeImplBase {

    private final PbftCore pbftCore;
    private final Map<String, PbftNodeGrpc.PbftNodeStub> asyncStubs;
    private final Map<String, PbftNodeGrpc.PbftNodeBlockingStub> blockingStubs;
    private final Map<String, StreamObserver<Reply>> clientResponseStreams = new ConcurrentHashMap<>();
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);

    @Autowired
    public PbftNodeGrpcService(
            PbftCore pbftCore, 
            Map<String, PbftNodeGrpc.PbftNodeStub> peerStubsPbft,
            Map<String, PbftNodeGrpc.PbftNodeBlockingStub> peerBlockingStubs) {
        this.pbftCore = pbftCore;
        this.asyncStubs = peerStubsPbft;
        this.blockingStubs = peerBlockingStubs;
        
        this.pbftCore.setMessageBroadcaster(msg -> {
            executorService.submit(() -> broadcast(msg));
        });
        
        this.pbftCore.setDirectMessageSender(action -> {
            executorService.submit(() -> sendDirect(action));
        });
    }
    
    private void execReq(Object action) {
        if (action == null) return;
        
        if (action instanceof BroadcastAction) {
            executorService.submit(() -> broadcast(((BroadcastAction) action).message()));
        } else if (action instanceof DirectMessageAction) {
            DirectMessageAction directAction = (DirectMessageAction) action;
            executorService.submit(() -> sendDirect(directAction));
        } else if (action instanceof ReplyAction) {
            sendReplyToClient(((ReplyAction) action).reply());
        } else if (action instanceof MultipleActions) {
            MultipleActions multipleActions = (MultipleActions) action;
            for (Object subAction : multipleActions.actions()) {
                execReq(subAction);
            }
        }
    }

@Override
public void clientRequest(Request request, StreamObserver<Reply> responseObserver) {
    System.out.println("DEBUG Node " + pbftCore.getNodeId() + ": clientRequest received from " + request.getClientId());
    
    clientResponseStreams.put(request.getClientId(), responseObserver);
    
    if ("BALANCE".equals(request.getOperation())) {
        System.out.println("Node " + pbftCore.getNodeId() + " processing read-only BALANCE request from " + request.getClientId());
        Reply reply = pbftCore.readRequest(request);
        if (reply != null) {
            try {
                responseObserver.onNext(reply);
                System.out.println("Node " + pbftCore.getNodeId() + " sent read-only reply to " + request.getClientId());
            } catch (Exception e) {
                System.err.println("Error sending read-only reply: " + e.getMessage());
            }
        }
        return;
    }
    if (pbftCore.isLeader()) {
        System.out.println("Node " + pbftCore.getNodeId() + " (leader) received write request from client " + request.getClientId());
        execReq(pbftCore.clientRequest(request));
    } else {
        System.out.println("Node " + pbftCore.getNodeId() + " (backup) received write request from client " + request.getClientId());
        execReq(pbftCore.clientRequest(request));
    }
}

    @Override
    public void prePrepare(PrePrepareMsg request, StreamObserver<Ack> responseObserver) {
        execReq(pbftCore.prePrepare(request));
        ack(responseObserver);
    }

    @Override
    public void prepare(PrepareMsg request, StreamObserver<Ack> responseObserver) {
        execReq(pbftCore.prepare(request));
        ack(responseObserver);
    }

    @Override
    public void prepareCertificate(PrepareCertificateMsg request, StreamObserver<Ack> responseObserver) {
        execReq(pbftCore.prepareCertificate(request));
        ack(responseObserver);
    }

    @Override
    public void commit(CommitMsg request, StreamObserver<Ack> responseObserver) {
        execReq(pbftCore.commit(request));
        ack(responseObserver);
    }

    @Override
    public void commitCertificate(CommitCertificateMsg request, StreamObserver<Ack> responseObserver) {
        execReq(pbftCore.commitCertificate(request));
        ack(responseObserver);
    }

    @Override
    public void viewChange(ViewChangeMsg request, StreamObserver<Ack> responseObserver) {
        execReq(pbftCore.viewChange(request));
        ack(responseObserver);
    }

    @Override
    public void newView(NewViewMsg request, StreamObserver<Ack> responseObserver) {
        execReq(pbftCore.triggerNewView(request));
        ack(responseObserver);
    }

    @Override
    public void setNodeStatus(NodeStatus request, StreamObserver<Ack> responseObserver) {
        pbftCore.setNodeStatus(request.getIsLive());
        ack(responseObserver);
    }

    @Override
    public void setAttackMode(SetAttackModeRequest request, StreamObserver<Ack> responseObserver) {
        pbftCore.setAttackMode(request.getMode());
        ack(responseObserver);
    }
    
    @Override
    public void setDarkTargets(SetDarkTargetsRequest request, StreamObserver<Ack> responseObserver) {
        pbftCore.setDarkTargets(request.getTargetNodeIdsList());
        ack(responseObserver);
    }
    

    @Override
    public void flushState(FlushRequest request, StreamObserver<Ack> responseObserver) {
        pbftCore.flushState();
        ack(responseObserver);
    }

    @Override
    public void printDB(PrintRequest request, StreamObserver<PrintDBResponse> responseObserver) {
        PrintDBResponse response = PrintDBResponse.newBuilder()
                .putAllDb(pbftCore.getDB())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void printStatus(PrintStatusRequest request, StreamObserver<PrintStatusResponse> responseObserver) {
        PrintStatusResponse response = PrintStatusResponse.newBuilder()
                .setStatus(pbftCore.getStatus(request.getSequenceNumber()))
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void printView(PrintRequest request, StreamObserver<PrintViewResponse> responseObserver) {
        List<String> history = pbftCore.getViewChangeHistory();
        String result = history.isEmpty() ? "No view changes yet." : String.join("\n", history);
        PrintViewResponse response = PrintViewResponse.newBuilder()
                .setPlaceholder(result)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private void broadcast(AbstractMessage msg) {
        System.out.println("Node " + pbftCore.getNodeId() + " broadcasting " + msg.getClass().getSimpleName());
        
        if (msg instanceof PrepareCertificateMsg || msg instanceof CommitCertificateMsg || 
            msg instanceof ViewChangeMsg || msg instanceof NewViewMsg) {
            blockingStubs.forEach((peerId, stub) -> {
                if (pbftCore.isTargetInDark(peerId)) {
                    System.out.println("ATTACK: Node " + pbftCore.getNodeId() + " (IN_DARK) skipping broadcast to " + peerId);
                    return; 
                }
                try {
                    if (msg instanceof PrepareCertificateMsg) {
                        stub.prepareCertificate((PrepareCertificateMsg) msg);
                    } else if (msg instanceof CommitCertificateMsg) {
                        stub.commitCertificate((CommitCertificateMsg) msg);
                    } else if (msg instanceof ViewChangeMsg) {
                        stub.viewChange((ViewChangeMsg) msg);
                    } else if (msg instanceof NewViewMsg) {
                        stub.newView((NewViewMsg) msg);
                    }
                    System.out.println("Node " + pbftCore.getNodeId() + " successfully broadcast " + msg.getClass().getSimpleName() + " to " + peerId);
                } catch (Exception e) {
                    System.err.println("Error broadcasting to " + peerId + ": " + e.getMessage());
                }
            });
        } else {
            asyncStubs.forEach((peerId, stub) -> {
                if (pbftCore.isTargetInDark(peerId)) {
                    System.out.println("ATTACK: Node " + pbftCore.getNodeId() + " (IN_DARK) skipping broadcast to " + peerId);
                    return; 
                }
                try {
                    sendMessageAsync(stub, msg);
                } catch (Exception e) {
                    System.err.println("Error broadcasting to " + peerId + ": " + e.getMessage());
                }
            });
        }
    }
    
    private void sendDirect(DirectMessageAction action) {
        String targetId = action.targetNodeId();
        AbstractMessage msg = action.message();
        if (pbftCore.isTargetInDark(targetId)) {
            System.out.println("ATTACK: Node " + pbftCore.getNodeId() + " (IN_DARK) dropping message to " + targetId);
            return;
        }
        
        System.out.println("Node " + pbftCore.getNodeId() + " sending " + msg.getClass().getSimpleName() + " to " + targetId);
        
        if (targetId.equals(pbftCore.getNodeId())) {
            System.out.println("Node " + pbftCore.getNodeId() + " handling " + msg.getClass().getSimpleName() + " locally (self-message)");
            handleMessageLocally(msg);
            return;
        }
        
        PbftNodeGrpc.PbftNodeBlockingStub blockingStub = blockingStubs.get(targetId);
        if (blockingStub != null) {
            try {
                sendMessageBlocking(blockingStub, msg);
                System.out.println("Node " + pbftCore.getNodeId() + " successfully sent " + msg.getClass().getSimpleName() + " to " + targetId);
            } catch (Exception e) {
                System.err.println("Error sending to " + targetId + ": " + e.getMessage());
            }
        } else {
            System.err.println("No blocking stub found for " + targetId);
        }
    }
    
    private void handleMessageLocally(AbstractMessage msg) {
        if (msg instanceof PrepareMsg) {
            execReq(pbftCore.prepare((PrepareMsg) msg));
        } else if (msg instanceof CommitMsg) {
            execReq(pbftCore.commit((CommitMsg) msg));
        }
    }
    
    private void sendMessageAsync(PbftNodeGrpc.PbftNodeStub stub, AbstractMessage msg) {
        StreamObserver<Ack> observer = new StreamObserver<Ack>() {
            @Override public void onNext(Ack ack) {}
            @Override public void onError(Throwable t) {}
            @Override public void onCompleted() {}
        };
        
        if (msg instanceof PrePrepareMsg) {
            stub.prePrepare((PrePrepareMsg) msg, observer);
        }
    }
    
    private void sendMessageBlocking(PbftNodeGrpc.PbftNodeBlockingStub stub, AbstractMessage msg) {
        if (msg instanceof PrepareMsg) {
            stub.prepare((PrepareMsg) msg);
        } else if (msg instanceof CommitMsg) {
            stub.commit((CommitMsg) msg);
        } else if (msg instanceof PrePrepareMsg) { 
            stub.prePrepare((PrePrepareMsg) msg);
        }
    }
    @Override
public void printLog(PrintRequest request, StreamObserver<PrintLogResponse> responseObserver) {
    PrintLogResponse response = PrintLogResponse.newBuilder()
            .addAllLogEntries(pbftCore.getMessageLog())
            .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
}

    private void sendReplyToClient(Reply reply) {
    if (reply == null) return;

    System.out.println("DEBUG Node " + pbftCore.getNodeId() + ": sendReplyToClient called for client " + reply.getClientId());

    StreamObserver<Reply> observer = clientResponseStreams.get(reply.getClientId());

    if (observer != null) {
        try {
            observer.onNext(reply);
        } catch (Exception e) {
            
            System.err.println("DEBUG Node " + pbftCore.getNodeId() + ": onNext FAILED for observer: " + e.getMessage());
            clientResponseStreams.remove(reply.getClientId());
        }
    }
}

@Override
    public void setEquivocationTargets(SetEquivocationTargetsRequest request, StreamObserver<Ack> responseObserver) {
        pbftCore.setEquivocationTargets(request.getTargetNodeIdsList());
        ack(responseObserver);
    }
    private void ack(StreamObserver<Ack> obs) {
        try {
            obs.onNext(Ack.newBuilder().setSuccess(true).build());
            obs.onCompleted();
        } catch (Exception e) {
            
        }
    }
}