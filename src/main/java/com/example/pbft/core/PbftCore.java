package com.example.pbft.core;

import com.example.pbft.config.PbftConfig;
import com.example.pbft.utils.MessageVisualizer;
import com.example.pbft.utils.CryptoUtils;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import com.pbft.pbft.proto.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.security.MessageDigest;
import java.security.PublicKey;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Service
public class PbftCore {

    public record BroadcastAction(AbstractMessage message) {}
    public record DirectMessageAction(String targetNodeId, AbstractMessage message) {}
    public record ReplyAction(Reply reply) {}
    public record MultipleActions(List<Object> actions) {}

    private final Map<ByteString, Long> recentRequests = new ConcurrentHashMap<>();
    private final String nodeId;
    private final PbftConfig config;
    private final int n;
    private final int f;
    private final int quorum;
    
    private final AtomicInteger currentView = new AtomicInteger(1);
    private final AtomicLong sequenceNumber = new AtomicLong(0);
    private final AtomicLong lastExecutedSeq = new AtomicLong(0);


    
    private Map<Long, PrePrepareMsg> prePrepareLog = new ConcurrentHashMap<>();
    private Map<Long, Map<String, PrepareMsg>> prepareLog = new ConcurrentHashMap<>();
    private Map<Long, PrepareCertificateMsg> prepareCertLog = new ConcurrentHashMap<>();
    private Map<Long, Map<String, CommitMsg>> commitLog = new ConcurrentHashMap<>();
    private Map<Long, CommitCertificateMsg> commitCertLog = new ConcurrentHashMap<>();
    
    private enum RequestState { PP, P, C, E }
    private Map<Long, RequestState> requestStatus = new ConcurrentHashMap<>();
    private Map<String, Integer> db = new ConcurrentHashMap<>();

    private final AtomicBoolean isLive = new AtomicBoolean(true);
    private AttackMode currentAttackMode = AttackMode.NONE;
    private Set<String> darkAttackTargets = ConcurrentHashMap.newKeySet();
    private Set<String> equivocationTargets = ConcurrentHashMap.newKeySet();

    private final Map<ByteString, Long> clientRequestWaitStartTime = new ConcurrentHashMap<>();
    private final Map<Long, Long> prepareWaitStartTime = new ConcurrentHashMap<>();
    private final Map<Long, Long> commitWaitStartTime = new ConcurrentHashMap<>();
    
    private final Map<Integer, Long> newViewWaitStartTime = new ConcurrentHashMap<>(); 
    private static final long TIMEOUT_MS = 10000;
    private final ScheduledExecutorService timeoutChecker = Executors.newSingleThreadScheduledExecutor();
    private volatile boolean timeoutCheckingEnabled = false;

    private final Map<Integer, Map<String, ViewChangeMsg>> viewChangeMessages = new ConcurrentHashMap<>();
    private final AtomicBoolean viewChangeInProgress = new AtomicBoolean(false);
    private final List<NewViewMsg> newViewHistory = new CopyOnWriteArrayList<>();

    private final List<String> messageLog = new CopyOnWriteArrayList<>();

    private Consumer<AbstractMessage> messageBroadcaster;
    private Consumer<DirectMessageAction> directMessageSender;

    @Autowired
    public PbftCore(@Qualifier("pbftNodeId") String pbftNodeId, PbftConfig config) {
        this.nodeId = pbftNodeId;
        this.config = config;
        this.n = config.getPeerIds().size();
        this.f = (this.n - 1) / 3;
        this.quorum = n - f;
        initializedb();
        startTimeoutChecker();
        System.out.println("Node " + nodeId + " initialized: n=" + n + ", f=" + f + ", quorum=" + quorum);
    }

    private void initializedb() {
        db.clear();
        for (char c = 'A'; c <= 'J'; c++) {
            db.put(String.valueOf(c), 10);
        }
    }

    public String getNodeId() { return nodeId; }
    
    public String getLeaderId() {
        int leaderIndex = (currentView.get() - 1) % n;
        return "n" + (leaderIndex + 1);
    }
    
    public boolean isLeader() { return nodeId.equals(getLeaderId()); }
    public boolean isCollector() { return isLeader(); }

    public void setMessageBroadcaster(Consumer<AbstractMessage> broadcaster) {
        this.messageBroadcaster = broadcaster;
    }

    public void setDirectMessageSender(Consumer<DirectMessageAction> sender) {
        this.directMessageSender = sender;
    }

    public void setNodeStatus(boolean isLive) {
        this.isLive.set(isLive);
        System.out.println("Node " + nodeId + " status set to: " + (isLive ? "LIVE" : "DOWN"));
    }
    
    public void setAttackMode(AttackMode mode) {
        this.currentAttackMode = mode;
        System.out.println("Node " + nodeId + " attack mode set to: " + mode.name());
    }

    public void setDarkTargets(List<String> targets) {
        darkAttackTargets.clear();
        darkAttackTargets.addAll(targets);
        System.out.println("ATTACK: Node " + nodeId + " will keep nodes " + targets + " in the dark.");
    }

    public void setEquivocationTargets(List<String> targets) {
        equivocationTargets.clear();
        equivocationTargets.addAll(targets);
        System.out.println("ATTACK: Node " + nodeId + " will equivocate to nodes: " + targets);
    }

    public boolean isTargetInDark(String targetId) {
        return !darkAttackTargets.isEmpty() && darkAttackTargets.contains(targetId);
    }

    public synchronized void flushState() {
        System.out.println("Node " + nodeId + " FLUSHING State of all nodes");
        timeoutCheckingEnabled = false;
        prePrepareLog.clear();
        prepareLog.clear();
        prepareCertLog.clear();
        commitLog.clear();
        commitCertLog.clear();
        requestStatus.clear();
        recentRequests.clear(); 
        clientRequestWaitStartTime.clear();
        prepareWaitStartTime.clear();
        commitWaitStartTime.clear();
        equivocationTargets.clear();
        newViewWaitStartTime.clear(); 
        viewChangeMessages.clear();
        newViewHistory.clear();
        viewChangeInProgress.set(false);
        messageLog.clear();
        currentView.set(1);
        sequenceNumber.set(0);
        lastExecutedSeq.set(0);
        initializedb();
        setNodeStatus(true);
        currentAttackMode = AttackMode.NONE;
        darkAttackTargets.clear();
        timeoutCheckingEnabled = true;
        System.out.println("Node " + nodeId + " done with flusing");
    }

    public Map<String, Integer> getDB() {
        return new HashMap<>(db);
    }

    public String getStatus(long seq) {
        RequestState state = requestStatus.get(seq);
        if (state == null) {
            return "X";
        }
        return state.name();
    }

    public List<String> getViewChangeHistory() {
        return newViewHistory.stream()
            .map(nv -> "View " + nv.getView() + " from " + nv.getNodeId())
            .collect(Collectors.toList());
    }

    public List<String> getMessageLog() {
        return new ArrayList<>(messageLog);
    }

    private String truncateBytes(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return "";
        StringBuilder hex = new StringBuilder();
        for (int i = 0; i < Math.min(bytes.length, 4); i++) {
            hex.append(String.format("%02x", bytes[i]));
        }
        return hex.toString();
    }

    private void logReceived(String messageType, String details) {
        String logEntry = String.format("[RECV] %s: %s", messageType, details);
        messageLog.add(logEntry);
    }

    private void logSent(String messageType, String details) {
        String logEntry = String.format("[SENT] %s: %s", messageType, details);
        messageLog.add(logEntry);
    }

    private String formatRequest(Request req) {
        return String.format("clientId: \"%s\" ts: %d op: \"%s\" from: \"%s\" to: \"%s\" amt: %d sig: %s",
            req.getClientId(), req.getTimestamp(), req.getOperation(),
            req.getSender(), req.getReceiver(), req.getAmount(),
            truncateBytes(req.getSignature().toByteArray()));
    }

    private String formatPrePrepare(PrePrepareMsg msg) {
        return String.format("view: %d seq: %d digest: %s nodeId: \"%s\" sig: %s request: { %s }",
            msg.getView(), msg.getSequenceNumber(),
            truncateBytes(msg.getDigest().toByteArray()),
            msg.getNodeId(),
            truncateBytes(msg.getSignature().toByteArray()),
            formatRequest(msg.getRequest()));
    }

    private String formatPrepare(PrepareMsg msg) {
        return String.format("view: %d seq: %d digest: %s nodeId: \"%s\" sig: %s",
            msg.getView(), msg.getSequenceNumber(),
            truncateBytes(msg.getDigest().toByteArray()),
            msg.getNodeId(),
            truncateBytes(msg.getSignature().toByteArray()));
    }

    private String formatPrepareCertificate(PrepareCertificateMsg msg) {
        return String.format("view: %d seq: %d digest: %s collectorId: \"%s\" preparesCount: %d sig: %s",
            msg.getView(), msg.getSequenceNumber(),
            truncateBytes(msg.getDigest().toByteArray()),
            msg.getCollectorId(),
            msg.getPreparesCount(),
            truncateBytes(msg.getSignature().toByteArray()));
    }

    private String formatCommit(CommitMsg msg) {
        return String.format("view: %d seq: %d digest: %s nodeId: \"%s\" sig: %s",
            msg.getView(), msg.getSequenceNumber(),
            truncateBytes(msg.getDigest().toByteArray()),
            msg.getNodeId(),
            truncateBytes(msg.getSignature().toByteArray()));
    }

    private String formatCommitCertificate(CommitCertificateMsg msg) {
        return String.format("view: %d seq: %d digest: %s collectorId: \"%s\" commitsCount: %d sig: %s",
            msg.getView(), msg.getSequenceNumber(),
            truncateBytes(msg.getDigest().toByteArray()),
            msg.getCollectorId(),
            msg.getCommitsCount(),
            truncateBytes(msg.getSignature().toByteArray()));
    }

    private String formatViewChange(ViewChangeMsg msg) {
        return String.format("newView: %d nodeId: \"%s\" preparedCertsCount: %d sig: %s",
            msg.getNewView(), msg.getNodeId(),
            msg.getPreparedCertsCount(),
            truncateBytes(msg.getSignature().toByteArray()));
    }

    private String formatNewView(NewViewMsg msg) {
        return String.format("view: %d nodeId: \"%s\" viewChangesCount: %d prePreparesCount: %d sig: %s",
            msg.getView(), msg.getNodeId(),
            msg.getViewChangesCount(), msg.getPrePreparesCount(),
            truncateBytes(msg.getSignature().toByteArray()));
    }

    private String formatReply(Reply reply) {
        return String.format("view: %d ts: %d clientId: \"%s\" nodeId: \"%s\" result: \"%s\" sig: %s",
            reply.getView(), reply.getTimestamp(),
            reply.getClientId(), reply.getNodeId(),
            reply.getResult(),
            truncateBytes(reply.getSignature().toByteArray()));
    }

    private void startTimeoutChecker() {
        timeoutCheckingEnabled = true;
        timeoutChecker.scheduleAtFixedRate(() -> {
            if (!timeoutCheckingEnabled || !isLive.get()) return;
            checkForTimeouts();
        }, 1000, 1000, TimeUnit.MILLISECONDS);
        System.out.println("Node " + nodeId + " started timeout checker");
    }

    private void checkForTimeouts() {
        long now = System.currentTimeMillis();
        for (Map.Entry<Integer, Long> entry : newViewWaitStartTime.entrySet()) {
            int waitingForView = entry.getKey();
            long startTime = entry.getValue();
            if (now - startTime > TIMEOUT_MS) {
                System.out.println("Node " + nodeId + " TIMEOUT waiting for NewView for view " + waitingForView);
                newViewWaitStartTime.remove(waitingForView);
                if (currentView.get() < waitingForView) {
                    System.out.println("Node " + nodeId + " initiating another view change due to NewView timeout");
                    viewChangeInProgress.set(false);
                    initiateViewChangeToView(waitingForView + 1);
                    return;
                }
            }
        }
        if (viewChangeInProgress.get()) {
            return;
        }
        for (Map.Entry<ByteString, Long> entry : clientRequestWaitStartTime.entrySet()) {
            ByteString requestSig = entry.getKey();
            long startTime = entry.getValue();
            if (now - startTime > TIMEOUT_MS) {
                System.out.println("Node " + nodeId + " TIMEOUT waiting for PrePrepare from leader");
                clientRequestWaitStartTime.remove(requestSig);
                initiateViewChange();
                return;
            }
        }

        for (Map.Entry<Long, Long> entry : prepareWaitStartTime.entrySet()) {
            long seq = entry.getKey();
            long startTime = entry.getValue();
            
            if (now - startTime > TIMEOUT_MS) {
                if (!prepareCertLog.containsKey(seq) && requestStatus.get(seq) == RequestState.PP) {
                    System.out.println("Node " + nodeId + " TIMEOUT waiting for PrepareCertificate for seq #" + seq);
                    prepareWaitStartTime.remove(seq);
                    initiateViewChange();
                    return;
                }
            }
        }
        
        for (Map.Entry<Long, Long> entry : commitWaitStartTime.entrySet()) {
            long seq = entry.getKey();
            long startTime = entry.getValue();
            
            if (now - startTime > TIMEOUT_MS) {
                if (!commitCertLog.containsKey(seq) && requestStatus.get(seq) == RequestState.P) {
                    System.out.println("Node " + nodeId + " TIMEOUT waiting for CommitCertificate for seq #" + seq);
                    commitWaitStartTime.remove(seq);
                    initiateViewChange();
                    return;
                }
            }
        }
        
    
        if (!viewChangeInProgress.get()) {
            long lastExecuted = lastExecutedSeq.get();
            long currentSeq = sequenceNumber.get(); 
            
            for (long seq = lastExecuted + 1; seq <= currentSeq; seq++) {
                RequestState state = requestStatus.get(seq);
                
                if (state == RequestState.C) {
                    System.out.println("Node " + nodeId + 
                                     " detected execution gap: seq #" + seq + 
                                     " is COMMITTED but cannot execute (last executed: " + lastExecuted + ")");
                    
                    Long commitTime = commitWaitStartTime.get(seq);
                    if (commitTime == null) {
                        commitWaitStartTime.put(seq, now);
                    } else if (now - commitTime > TIMEOUT_MS) {
                        System.out.println("Node " + nodeId + 
                                         " TIMEOUT: Execution gap timeout for seq #" + seq);
                        commitWaitStartTime.remove(seq);
                        initiateViewChange();
                        return;
                    }
                }
            }
        }
    }

    private synchronized void initiateViewChange() {
        int targetView = currentView.get() + 1;
        initiateViewChangeToView(targetView);
    }

    private synchronized void initiateViewChangeToView(int targetView) {
        if (viewChangeInProgress.get()) {
            System.out.println("Node " + nodeId + " view change already in progress, skipping");
            return;
        }
        
        viewChangeInProgress.set(true);
        
        System.out.println("\n" + "=".repeat(30));
        System.out.println("Node " + nodeId + " initiating VIEW CHANGE to view " + targetView);
        System.out.println("=".repeat(30));
        
        try {
            List<PreparedCertificate> preparedCerts = new ArrayList<>();
            for (Map.Entry<Long, PrepareCertificateMsg> entry : prepareCertLog.entrySet()) {
                PreparedCertificate cert = PreparedCertificate.newBuilder()
                    .setSequenceNumber(entry.getKey())
                    .setDigest(entry.getValue().getDigest())
                    .setView(entry.getValue().getView())
                    .build();
                preparedCerts.add(cert);
                System.out.println("  Including prepared certificate for seq #" + entry.getKey());
            }
            
            ViewChangeMsg.Builder vcBuilder = ViewChangeMsg.newBuilder()
                .setNewView(targetView)
                .setNodeId(this.nodeId)
                .addAllPreparedCerts(preparedCerts);
            
            byte[] signature = CryptoUtils.sign(config.getNodePrivateKey(this.nodeId), vcBuilder.buildPartial().toByteArray());
            
            if (currentAttackMode == AttackMode.INVALID_SIGNATURE) {
                System.out.println("ATTACK: Node " + nodeId + " corrupting ViewChange signature");
                signature = invalidSign(signature);
            }
            
            ViewChangeMsg signedVC = vcBuilder.setSignature(ByteString.copyFrom(signature)).build();
            
            System.out.println("Node " + nodeId + " created ViewChange message for view " + targetView);
            
            viewChangeMessages.computeIfAbsent(targetView, k -> new ConcurrentHashMap<>()).put(this.nodeId, signedVC);
            
            logSent("ViewChangeMsg", formatViewChange(signedVC));
            viewChange(signedVC); 
            
            if (messageBroadcaster != null) {
                System.out.println("Node " + nodeId + " broadcasting ViewChange to all peers");
                messageBroadcaster.accept(signedVC);
            } else {
                System.err.println("ERROR: Node " + nodeId + " has no message broadcaster configured!");
            }
            
        } catch (Exception e) {
            e.printStackTrace();
            viewChangeInProgress.set(false);
        }
    }

    public synchronized Object viewChange(ViewChangeMsg msg) {
        if (!isLive.get()) return null;
        
        logReceived("ViewChangeMsg", formatViewChange(msg));
        
        System.out.println("\n" + "=".repeat(60));
        System.out.println("NODE " + nodeId + " RECEIVED VIEW-CHANGE from " + msg.getNodeId());
        System.out.println("=".repeat(60));
        
        int newView = msg.getNewView();
        
        try {
            PublicKey senderPublicKey = config.getNodePublicKey(msg.getNodeId());
            ViewChangeMsg msgToVerify = msg.toBuilder().clearSignature().build();
            if (!CryptoUtils.verify(senderPublicKey, msgToVerify.toByteArray(), msg.getSignature().toByteArray())) {
                System.out.println("Node " + nodeId + " rejected ViewChange with invalid signature");
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        
        viewChangeMessages.computeIfAbsent(newView, k -> new ConcurrentHashMap<>()).put(msg.getNodeId(), msg);
        int vcCount = viewChangeMessages.get(newView).size();
        
        System.out.println("Node " + nodeId + " received ViewChange from " + msg.getNodeId() + 
                         " for view " + newView + " (total: " + vcCount + "/" + (2 * f + 1) + ")");
        
        if (vcCount >= (2 * f + 1)) {
            String newLeaderId = "n" + (((newView - 1) % n) + 1);
            
            System.out.println("Node " + nodeId + " collected 2f+1 ViewChange messages for view " + newView);
            System.out.println("New leader should be: " + newLeaderId);
            
            if (this.nodeId.equals(newLeaderId)) {
                return newViewReq(newView);
            } else {
                newViewWaitStartTime.putIfAbsent(newView, System.currentTimeMillis());
                System.out.println("Node " + nodeId + " waiting for NewView from " + newLeaderId);
            }
        }
        
        return null;
    }

    private Object newViewReq(int newView) {
        System.out.println("\n" + "=".repeat(30));
        System.out.println(" Node " + nodeId + " is NEW LEADER for view " + newView);
        System.out.println("=".repeat(30));
        
        if (currentAttackMode == AttackMode.CRASH) {
            System.out.println("ATTACK: Node " + nodeId + " (New Leader) performing CRASH. Refusing to send NewView message.");
            return null;
        }
        
        try {
            Collection<ViewChangeMsg> viewChanges = viewChangeMessages.get(newView).values();
            
            long maxSeq = sequenceNumber.get();
            Map<Long, PreparedCertificate> highestPreparedBySeq = new HashMap<>();
            
            for (ViewChangeMsg vc : viewChanges) {
                for (PreparedCertificate cert : vc.getPreparedCertsList()) {
                    long seq = cert.getSequenceNumber();
                    maxSeq = Math.max(maxSeq, seq);
                    
                    PreparedCertificate existing = highestPreparedBySeq.get(seq);
                    if (existing == null || cert.getView() > existing.getView()) {
                        highestPreparedBySeq.put(seq, cert);
                    }
                }
            }
            
            sequenceNumber.set(maxSeq);
            System.out.println("New leader " + nodeId + " updated sequence number to " + maxSeq);
            
            List<PrePrepareMsg> prePrepares = new ArrayList<>();
            
            for (long seq = 1; seq <= maxSeq; seq++) {
                PreparedCertificate cert = highestPreparedBySeq.get(seq);
                PrePrepareMsg prePrepare;
                Request requestToLog = null; 

                if (cert != null) {
                    PrePrepareMsg originalPrePrepare = prePrepareLog.get(seq);
                    
                    if (originalPrePrepare != null) {
                        PrePrepareMsg.Builder ppBuilder = PrePrepareMsg.newBuilder()
                            .setView(newView)
                            .setSequenceNumber(seq)
                            .setDigest(cert.getDigest())
                            .setRequest(originalPrePrepare.getRequest())
                            .setNodeId(this.nodeId);
                        
                        byte[] ppSignature = CryptoUtils.sign(config.getNodePrivateKey(this.nodeId), ppBuilder.buildPartial().toByteArray());
                        prePrepare = ppBuilder.setSignature(ByteString.copyFrom(ppSignature)).build();
                        
                        requestToLog = originalPrePrepare.getRequest();
                        System.out.println("   Including seq #" + seq + " with prepared request");
                    } else {
                        prePrepare = noOP(seq, newView);
                        System.out.println("   Including seq #" + seq + " as NO-OP (cert but no PrePrepare)");
                        requestToLog = prePrepare.getRequest();
                    }
                } else {
                    PrePrepareMsg originalPrePrepare = prePrepareLog.get(seq);
                    if (originalPrePrepare != null) {
                        PrePrepareMsg.Builder ppBuilder = PrePrepareMsg.newBuilder()
                            .setView(newView)
                            .setSequenceNumber(seq)
                            .setDigest(originalPrePrepare.getDigest()) 
                            .setRequest(originalPrePrepare.getRequest())
                            .setNodeId(this.nodeId);
                        
                        byte[] ppSignature = CryptoUtils.sign(config.getNodePrivateKey(this.nodeId), ppBuilder.buildPartial().toByteArray());
                        prePrepare = ppBuilder.setSignature(ByteString.copyFrom(ppSignature)).build();
                        
                        requestToLog = originalPrePrepare.getRequest();
                        System.out.println("   Including seq #" + seq + " from local PrePrepare log");
                    } else {
                        prePrepare = noOP(seq, newView);
                        System.out.println("   Including seq #" + seq + " as NO-OP (gap fill)");
                        requestToLog = prePrepare.getRequest();
                    }
                }
                
                prePrepares.add(prePrepare);
                if (requestToLog != null && requestToLog.getSignature() != ByteString.EMPTY) {
                    recentRequests.put(requestToLog.getSignature(), seq);
                }
            }
            
            NewViewMsg.Builder nvBuilder = NewViewMsg.newBuilder()
                .setView(newView)
                .addAllViewChanges(viewChanges)
                .addAllPrePrepares(prePrepares)
                .setNodeId(this.nodeId);
            
            byte[] signature = CryptoUtils.sign(config.getNodePrivateKey(this.nodeId), nvBuilder.buildPartial().toByteArray());
            
            if (currentAttackMode == AttackMode.INVALID_SIGNATURE) {
                System.out.println("ATTACK: Node " + nodeId + " (New Leader) corrupting NewView signature");
                signature = invalidSign(signature);
            }
            
            NewViewMsg signedNV = nvBuilder.setSignature(ByteString.copyFrom(signature)).build();
            
            System.out.println("New leader " + nodeId + " created NewView message with " + prePrepares.size() + " pre-prepares");
            
            logSent("NewViewMsg", formatNewView(signedNV));
            timeAttack();
            triggerNewView(signedNV); 
            return new BroadcastAction(signedNV);
            
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private PrePrepareMsg noOP(long seq, int view) throws Exception {
        Request noOpRequest = Request.newBuilder()
            .setClientId("SYSTEM")
            .setTimestamp(System.currentTimeMillis())
            .setOperation("NO-OP")
            .setSender("")
            .setReceiver("")
            .setAmount(0)
            .setSignature(ByteString.EMPTY)
            .build();
        
        byte[] digest = createDigest(noOpRequest);
        
        PrePrepareMsg.Builder ppBuilder = PrePrepareMsg.newBuilder()
            .setView(view)
            .setSequenceNumber(seq)
            .setDigest(ByteString.copyFrom(digest))
            .setRequest(noOpRequest)
            .setNodeId(this.nodeId);
        
        byte[] signature = CryptoUtils.sign(
            config.getNodePrivateKey(this.nodeId), 
            ppBuilder.buildPartial().toByteArray()
        );
        
        return ppBuilder.setSignature(ByteString.copyFrom(signature)).build();
    }

    public synchronized Object triggerNewView(NewViewMsg msg) {
        if (!isLive.get()) return null;
        
        logReceived("NewViewMsg", formatNewView(msg));
        
        System.out.println("\n" + "=".repeat(60));
        System.out.println("NODE " + nodeId + " RECEIVED NEW-VIEW for view " + msg.getView());
        System.out.println("=".repeat(60));
        
        int newView = msg.getView();
        String expectedLeader = "n" + (((newView - 1) % n) + 1);
        
        if (!msg.getNodeId().equals(expectedLeader)) {
            System.out.println("Node " + nodeId + " rejected NewView from " + msg.getNodeId() + 
                             ", expected from " + expectedLeader);
            return null;
        }
        
        try {
            PublicKey leaderPublicKey = config.getNodePublicKey(msg.getNodeId());
            NewViewMsg msgToVerify = msg.toBuilder().clearSignature().build();
            if (!CryptoUtils.verify(leaderPublicKey, msgToVerify.toByteArray(), msg.getSignature().toByteArray())) {
                System.out.println("Node " + nodeId + " rejected NewView with invalid signature");
                return null;
            }
            
            for (ViewChangeMsg vcMsg : msg.getViewChangesList()) {
                PublicKey senderPublicKey = config.getNodePublicKey(vcMsg.getNodeId());
                ViewChangeMsg vcToVerify = vcMsg.toBuilder().clearSignature().build();
                
                if (!CryptoUtils.verify(senderPublicKey, vcToVerify.toByteArray(), vcMsg.getSignature().toByteArray())) {
                    System.out.println("Node " + nodeId + " rejected NewView: contains invalid ViewChangeMsg from " + vcMsg.getNodeId());
                    return null;
                }
                
                if (vcMsg.getNewView() != msg.getView()) {
                     System.out.println("Node " + nodeId + " rejected NewView: contains ViewChangeMsg for wrong view " + vcMsg.getNewView());
                     return null;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        
        if (msg.getViewChangesCount() < (2 * f + 1)) {
            System.out.println("Node " + nodeId + " rejected NewView with insufficient view-changes");
            return null;
        }
        
        currentView.set(newView);
        viewChangeInProgress.set(false);
        
        newViewWaitStartTime.remove(newView);
        System.out.println("Node " + nodeId + " stopped waiting for NewView (received)");
        
        boolean alreadyRecorded = newViewHistory.stream()
            .anyMatch(nv -> nv.getView() == newView);
        
        if (!alreadyRecorded) {
            newViewHistory.add(msg);
            System.out.println("Node " + nodeId + " accepted NEW VIEW " + newView);
            System.out.println("   New leader: " + expectedLeader);
        } else {
            System.out.println("Node " + nodeId + " already processed NewView for view " + newView);
            return null;
        }
        
        clientRequestWaitStartTime.clear();
        prepareWaitStartTime.clear();
        commitWaitStartTime.clear();
        recentRequests.clear();
        
        System.out.println("Node " + nodeId + " cleared all timeout trackers for new view");
        
        List<Object> actions = new ArrayList<>();
        
        for (PrePrepareMsg prePrepare : msg.getPrePreparesList()) {
            long seq = prePrepare.getSequenceNumber();
            System.out.println("   Processing pre-prepare for seq #" + seq + " in new view");
            
            prePrepareLog.put(seq, prePrepare);
            
            
            Request requestToLog = prePrepare.getRequest();
            if (requestToLog != null && requestToLog.getSignature() != ByteString.EMPTY) {
                recentRequests.put(requestToLog.getSignature(), seq);
            }
            
         
            requestStatus.put(seq, RequestState.PP);
            prepareWaitStartTime.put(seq, System.currentTimeMillis());
            
            if (!isLeader()) {
                try {
                    PrepareMsg.Builder prepareBuilder = PrepareMsg.newBuilder()
                        .setView(newView)
                        .setSequenceNumber(seq)
                        .setDigest(prePrepare.getDigest())
                        .setNodeId(this.nodeId);
                    
                    byte[] signature = CryptoUtils.sign(config.getNodePrivateKey(this.nodeId), 
                                                        prepareBuilder.buildPartial().toByteArray());
                    
                    if (currentAttackMode == AttackMode.INVALID_SIGNATURE) {
                        System.out.println("ATTACK: Node " + nodeId + " corrupting Prepare signature in NewView processing");
                        signature = invalidSign(signature);
                    }
                    
                    PrepareMsg signedPrepare = prepareBuilder.setSignature(ByteString.copyFrom(signature)).build();
                    
                    System.out.println("   Node " + nodeId + " will send prepare for seq #" + seq + " to new leader " + expectedLeader);
                    
                    logSent("PrepareMsg", formatPrepare(signedPrepare));
                    actions.add(new DirectMessageAction(expectedLeader, signedPrepare));
                    
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        
        System.out.println("Node " + nodeId + " completed processing NewView for view " + newView);
        
        if (!actions.isEmpty()) {
            return new MultipleActions(actions);
        }
        
        return null;
    }

    public synchronized Object clientRequest(Request request) {
        if (!isLive.get()) return null;
        
        logReceived("Request", formatRequest(request));
        ByteString requestSignature = request.getSignature(); 

        
        if (recentRequests.containsKey(requestSignature)) {
            long existingSeq = recentRequests.get(requestSignature);
            
            if (existingSeq > 0 && requestStatus.get(existingSeq) == RequestState.E) {
                System.out.println("Node " + nodeId + " re-sending reply for executed duplicate request (seq #" + existingSeq + ")");
              
                return new ReplyAction(processReadOnlyRequest(request));
            }
            
            System.out.println("Node " + nodeId + " ignoring duplicate request (seq #" + existingSeq + " in progress)");
            return null;
        }

      
        if (isLeader() && !equivocationTargets.isEmpty()) {
            System.out.println("ATTACK: Node " + nodeId + " (Leader) performing EQUIVOCATION.");
            return equivocationAttack(request);
        }
        
       
       if (!isLeader()) {
            recentRequests.put(requestSignature, 0L); 
            
            if (!clientRequestWaitStartTime.containsKey(requestSignature)) {
                clientRequestWaitStartTime.put(requestSignature, System.currentTimeMillis());
                System.out.println("Node " + nodeId + " (backup) tracking client request from " + 
                                  request.getClientId() + ", expecting PrePrepare from leader");
            }
            return null;
        }

      
        if (currentAttackMode == AttackMode.CRASH && isLeader()) {
            System.out.println("ATTACK: Node " + nodeId + " (Leader) performing CRASH. Ignoring client request.");
            return null;
        }

        System.out.println("\n" + "=".repeat(60));
        System.out.println("LEADER RECEIVED CLIENT REQUEST");
        System.out.println("=".repeat(60));
        MessageVisualizer.printRequest(request);

        

        try {
            PublicKey clientPublicKey = config.getClientPublicKey(request.getClientId());
            Request requestToVerify = request.toBuilder().clearSignature().build();
            if (!CryptoUtils.verify(clientPublicKey, requestToVerify.toByteArray(), request.getSignature().toByteArray())) {
                System.out.println("Leader " + nodeId + " rejected invalid client signature");
                return null;
            }
        } catch (Exception e) { 
            e.printStackTrace(); 
            return null; 
        }

        long seq = sequenceNumber.incrementAndGet();
        recentRequests.put(requestSignature, seq); 
        byte[] digest = createDigest(request);

        try {
            PrePrepareMsg.Builder prePrepareBuilder = PrePrepareMsg.newBuilder()
                .setView(currentView.get())
                .setSequenceNumber(seq)
                .setDigest(ByteString.copyFrom(digest))
                .setRequest(request)
                .setNodeId(this.nodeId);
            
            byte[] signature = CryptoUtils.sign(config.getNodePrivateKey(this.nodeId), prePrepareBuilder.buildPartial().toByteArray());
            
            if (currentAttackMode == AttackMode.INVALID_SIGNATURE) {
                System.out.println("ATTACK: Node " + nodeId + " (Leader) corrupting PrePrepare signature");
                signature = invalidSign(signature);
            }
            
            PrePrepareMsg signedMsg = prePrepareBuilder.setSignature(ByteString.copyFrom(signature)).build();
            
            System.out.println("\n" + "=".repeat(60));
            System.out.println("LEADER BROADCASTING PRE-PREPARE");
            System.out.println("=".repeat(60));
            MessageVisualizer.printPrePrepare(signedMsg);
            
            prePrepareLog.put(seq, signedMsg);
            requestStatus.put(seq, RequestState.PP);
            System.out.println("Leader " + nodeId + " starting consensus for seq #" + seq);
            
            logSent("PrePrepareMsg", formatPrePrepare(signedMsg));
            timeAttack();
            return new BroadcastAction(signedMsg);
        } catch (Exception e) { 
            e.printStackTrace(); 
            return null; 
        }
    }

    private synchronized Object equivocationAttack(Request request) {
        try {
            
            long seq_n = sequenceNumber.incrementAndGet();
            long seq_n_plus_1 = sequenceNumber.incrementAndGet();
            System.out.println("ATTACK: Equivocating request with seq #" + seq_n + " and #" + seq_n_plus_1);

            byte[] digest = createDigest(request);
            ByteString digestBS = ByteString.copyFrom(digest);

            
            PrePrepareMsg.Builder prePrepareBuilder_n = PrePrepareMsg.newBuilder()
                .setView(currentView.get())
                .setSequenceNumber(seq_n)
                .setDigest(digestBS)
                .setRequest(request)
                .setNodeId(this.nodeId);
            byte[] signature_n = CryptoUtils.sign(config.getNodePrivateKey(this.nodeId), prePrepareBuilder_n.buildPartial().toByteArray());
            PrePrepareMsg msg_n = prePrepareBuilder_n.setSignature(ByteString.copyFrom(signature_n)).build();

            PrePrepareMsg.Builder prePrepareBuilder_n_plus_1 = PrePrepareMsg.newBuilder()
                .setView(currentView.get())
                .setSequenceNumber(seq_n_plus_1)
                .setDigest(digestBS)
                .setRequest(request)
                .setNodeId(this.nodeId);
            byte[] signature_n_plus_1 = CryptoUtils.sign(config.getNodePrivateKey(this.nodeId), prePrepareBuilder_n_plus_1.buildPartial().toByteArray());
            PrePrepareMsg msg_n_plus_1 = prePrepareBuilder_n_plus_1.setSignature(ByteString.copyFrom(signature_n_plus_1)).build();

            
            prePrepareLog.put(seq_n, msg_n);
            requestStatus.put(seq_n, RequestState.PP);
            logSent("PrePrepareMsg (Equivocated)", formatPrePrepare(msg_n));

            prePrepareLog.put(seq_n_plus_1, msg_n_plus_1);
            requestStatus.put(seq_n_plus_1, RequestState.PP);
            logSent("PrePrepareMsg (Equivocated)", formatPrePrepare(msg_n_plus_1));

           
            Set<String> subset1_targets = new HashSet<>(equivocationTargets);
            Set<String> subset2_targets = config.getPeerIds().stream()
                .filter(id -> !id.equals(this.nodeId) && !subset1_targets.contains(id))
                .collect(Collectors.toSet());

            List<Object> actions = new ArrayList<>();

            System.out.println("ATTACK: Sending seq #" + seq_n + " to " + subset1_targets);
            for (String targetNodeId : subset1_targets) {
                actions.add(new DirectMessageAction(targetNodeId, msg_n));
            }

            
            System.out.println("ATTACK: Sending seq #" + seq_n_plus_1 + " to " + subset2_targets);
            for (String targetNodeId : subset2_targets) {
                actions.add(new DirectMessageAction(targetNodeId, msg_n_plus_1));
            }

            
            System.out.println("ATTACK: Leader " + nodeId + " processing seq #" + seq_n_plus_1 + " for itself.");
            
            Object selfAction = prePrepare(msg_n_plus_1);
            if (selfAction != null) {
                actions.add(selfAction);
            }

            return new MultipleActions(actions);

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public synchronized Object prePrepare(PrePrepareMsg msg) {
        if (!isLive.get()) return null;
        
        logReceived("PrePrepareMsg", formatPrePrepare(msg));
        
        System.out.println("\n" + "=".repeat(60));
        System.out.println("NODE " + nodeId + " RECEIVED PRE-PREPARE");
        System.out.println("=".repeat(60));
        MessageVisualizer.printPrePrepare(msg);
        
        try {
            PublicKey leaderPublicKey = config.getNodePublicKey(msg.getNodeId());
            PrePrepareMsg msgToVerify = msg.toBuilder().clearSignature().build();
            if (!CryptoUtils.verify(leaderPublicKey, msgToVerify.toByteArray(), msg.getSignature().toByteArray())) {
                System.out.println("Node " + nodeId + " rejected PrePrepare with invalid signature");
                return null;
            }
        } catch (Exception e) { 
            e.printStackTrace(); 
            return null; 
        }

        long seq = msg.getSequenceNumber();
        if (msg.getView() < currentView.get()) {
            System.out.println("Node " + nodeId + " ignoring PrePrepare from old view " + msg.getView() + " (current view: " + currentView.get() + ")");
            return null;
        }
        
        if (prePrepareLog.containsKey(seq) && !isLeader()) {
            PrePrepareMsg existing = prePrepareLog.get(seq);
            if (existing.getView() >= msg.getView()) {
                System.out.println("Node " + nodeId + " ignoring duplicate PrePrepare for seq #" + seq + 
                                 " from view " + msg.getView() + " (already have from view " + existing.getView() + ")");
                return null;
            }
            System.out.println("Node " + nodeId + " replacing PrePrepare for seq #" + seq + 
                             " from view " + existing.getView() + " with new one from view " + msg.getView());
        }
        
        prePrepareLog.put(seq, msg);

        sequenceNumber.updateAndGet(current -> Math.max(current, seq));

        if (msg.getView() == currentView.get() && msg.getNodeId().equals(getLeaderId())) {
            if (!clientRequestWaitStartTime.isEmpty()) {
                System.out.println("Node " + nodeId + " received valid PrePrepare from leader, clearing ALL " + 
                                 clientRequestWaitStartTime.size() + " pending PrePrepare timers.");
                clientRequestWaitStartTime.clear();
            }
        } else {
            ByteString requestSig = msg.getRequest().getSignature();
            if (clientRequestWaitStartTime.remove(requestSig) != null) {
                System.out.println("Node " + nodeId + " received expected PrePrepare, stopped waiting for client request");
            }
        }
        prepareWaitStartTime.put(seq, System.currentTimeMillis());
        System.out.println("Node " + nodeId + " started waiting for PrepareCertificate for seq #" + seq);

        requestStatus.put(seq, RequestState.PP);
        
        System.out.println("Node " + nodeId + " validated PrePrepare for seq #" + seq);
        
        if (currentAttackMode == AttackMode.CRASH && !isLeader()) {
            System.out.println("ATTACK: Node " + nodeId + " (Backup) performing CRASH. Not updating status or sending PrepareMsg.");
            return null;
        }
        
        try {
            PrepareMsg.Builder prepareBuilder = PrepareMsg.newBuilder()
                .setView(msg.getView())
                .setSequenceNumber(seq)
                .setDigest(msg.getDigest())
                .setNodeId(this.nodeId);
            
            byte[] signature = CryptoUtils.sign(config.getNodePrivateKey(this.nodeId), prepareBuilder.buildPartial().toByteArray());
            
            if (currentAttackMode == AttackMode.INVALID_SIGNATURE) {
                System.out.println("ATTACK: Node " + nodeId + " (Backup) corrupting Prepare signature");
                signature = invalidSign(signature);
            }
            
            PrepareMsg signedMsg = prepareBuilder.setSignature(ByteString.copyFrom(signature)).build();

            System.out.println("\n" + "=".repeat(60));
            System.out.println("NODE " + nodeId + " SENDING PREPARE TO COLLECTOR");
            System.out.println("=".repeat(60));
            MessageVisualizer.printPrepare(signedMsg);
            
            String collectorId = getLeaderId();
            System.out.println("Node " + nodeId + " sending Prepare to collector " + collectorId);
            
            logSent("PrepareMsg", formatPrepare(signedMsg));
            return new DirectMessageAction(collectorId, signedMsg);
        } catch (Exception e) { 
            e.printStackTrace(); 
            return null; 
        }
    }

    public synchronized Object prepare(PrepareMsg msg) {
        if (!isLive.get()) return null;
        
        logReceived("PrepareMsg", formatPrepare(msg));
        
        System.out.println("\n" + "=".repeat(60));
        System.out.println("NODE " + nodeId + " RECEIVED PREPARE FROM " + msg.getNodeId());
        System.out.println("=".repeat(60));
        MessageVisualizer.printPrepare(msg);
        
        long seq = msg.getSequenceNumber();
        
        if (!prePrepareLog.containsKey(seq)) {
            System.out.println("Node " + nodeId + " received Prepare for seq #" + seq + " but no PrePrepare yet");
            return null;
        }
        
        try {
            PublicKey senderPublicKey = config.getNodePublicKey(msg.getNodeId());
            PrepareMsg msgToVerify = msg.toBuilder().clearSignature().build();
            if (!CryptoUtils.verify(senderPublicKey, msgToVerify.toByteArray(), msg.getSignature().toByteArray())) {
                System.out.println("Node " + nodeId + " rejected Prepare with invalid signature from " + msg.getNodeId());
                return null;
            }
        } catch (Exception e) { 
            e.printStackTrace(); 
            return null; 
        }

        prepareLog.computeIfAbsent(seq, k -> new ConcurrentHashMap<>()).put(msg.getNodeId(), msg);
        int prepareCount = prepareLog.get(seq).size();
        System.out.println("Node " + nodeId + " received Prepare from " + msg.getNodeId() + " for seq #" + seq + " (total: " + prepareCount + "/" + quorum + ")");

        PrepareCertificateMsg existingCert = prepareCertLog.get(seq);
        if (isCollector() && prepareCount >= quorum && (existingCert == null || existingCert.getView() < currentView.get())) {
            if (currentAttackMode == AttackMode.CRASH && isLeader()) {
                System.out.println("ATTACK: Node " + nodeId + " (Leader) performing CRASH. Refusing to send PrepareCertificateMsg.");
                return null;
            }
            
            System.out.println("Collector " + nodeId + " has quorum of Prepares for seq #" + seq + ", creating certificate");
            try {
                PrePrepareMsg prePrepare = prePrepareLog.get(seq);
                PrepareCertificateMsg.Builder certBuilder = PrepareCertificateMsg.newBuilder()
                    .setView(currentView.get())
                    .setSequenceNumber(seq)
                    .setDigest(prePrepare.getDigest())
                    .addAllPrepares(prepareLog.get(seq).values())
                    .setCollectorId(this.nodeId);
                
                byte[] signature = CryptoUtils.sign(config.getNodePrivateKey(this.nodeId), certBuilder.buildPartial().toByteArray());
                
                if (currentAttackMode == AttackMode.INVALID_SIGNATURE) {
                    System.out.println("ATTACK: Node " + nodeId + " (Collector) corrupting PrepareCertificate signature");
                    signature = invalidSign(signature);
                }
                
                PrepareCertificateMsg signedCert = certBuilder.setSignature(ByteString.copyFrom(signature)).build();

                System.out.println("\n" + "=".repeat(60));
                System.out.println("COLLECTOR BROADCASTING PREPARE CERTIFICATE");
                System.out.println("=".repeat(60));
                MessageVisualizer.printPrepareCertificate(signedCert);
                
                prepareCertLog.put(seq, signedCert);
                
                logSent("PrepareCertificateMsg", formatPrepareCertificate(signedCert));
                timeAttack();
                return new BroadcastAction(signedCert);
            } catch (Exception e) { 
                e.printStackTrace(); 
            }
        }
        return null;
    }

    public synchronized Object prepareCertificate(PrepareCertificateMsg msg) {
        if (!isLive.get()) return null;
        
        logReceived("PrepareCertificateMsg", formatPrepareCertificate(msg));
        
        System.out.println("\n" + "=".repeat(66));
        System.out.println("NODE " + nodeId + " RECEIVED PREPARE CERTIFICATE");
        System.out.println("=".repeat(66));
        MessageVisualizer.printPrepareCertificate(msg);
        
        long seq = msg.getSequenceNumber();

           if (!prePrepareLog.containsKey(seq)) {
            System.out.println("Node " + nodeId + " rejected PrepareCertificate for seq #" + seq + 
                             " - no PrePrepare exists");
            return null;
        }
        
        PrePrepareMsg prePrepare = prePrepareLog.get(seq);
        if (!prePrepare.getDigest().equals(msg.getDigest())) {
            System.out.println("Node " + nodeId + " rejected PrepareCertificate for seq #" + seq + 
                             " - digest mismatch");
            return null;
        }
        
        RequestState currentState = requestStatus.get(seq);
        if (currentState != RequestState.PP) {
            if (msg.getView() >= currentView.get() && (currentState == RequestState.P || currentState == RequestState.C || currentState == RequestState.E)) {
                System.out.println("Node " + nodeId + " (state " + currentState + ") accepting PrepareCertificate for seq #" + seq + 
                                 " to help leader " + msg.getCollectorId() + " catch up.");
            } else {
                System.out.println("Node " + nodeId + " rejected PrepareCertificate for seq #" + seq + 
                                 " - not in PP state (current: " + currentState + ", view: " + msg.getView() + ")");
                return null;
            }
        }
        
        try {
            PublicKey collectorPublicKey = config.getNodePublicKey(msg.getCollectorId());
            PrepareCertificateMsg msgToVerify = msg.toBuilder().clearSignature().build();
            if (!CryptoUtils.verify(collectorPublicKey, msgToVerify.toByteArray(), msg.getSignature().toByteArray())) {
                System.out.println("Node " + nodeId + " rejected PrepareCertificate with invalid signature");
                return null;
            }
            
            if (msg.getPreparesCount() < quorum) {
                System.out.println("Node " + nodeId + " rejected PrepareCertificate with insufficient prepares");
                return null;
            }
        } catch (Exception e) { 
            e.printStackTrace(); 
            return null; 
        }

        prepareCertLog.put(seq, msg);
        prepareWaitStartTime.remove(seq);
        System.out.println("Node " + nodeId + " received PrepareCertificate, stopped waiting");
        
        if (currentAttackMode == AttackMode.CRASH) {
            System.out.println("ATTACK: Node " + nodeId + " (" + (isLeader() ? "Leader" : "Backup") + ") performing CRASH. Not updating status to PREPARED or sending CommitMsg.");
            return null;
        }
        
        requestStatus.put(seq, RequestState.P);
        commitWaitStartTime.put(seq, System.currentTimeMillis());
        System.out.println("Node " + nodeId + " started waiting for CommitCertificate for seq #" + seq);
        System.out.println("Node " + nodeId + " is now PREPARED for seq #" + seq);
        
        try {
            CommitMsg.Builder commitBuilder = CommitMsg.newBuilder()
                .setView(currentView.get())
                .setSequenceNumber(seq)
                .setDigest(msg.getDigest())
                .setNodeId(this.nodeId);
            
            byte[] signature = CryptoUtils.sign(config.getNodePrivateKey(this.nodeId), commitBuilder.buildPartial().toByteArray());
            
            if (currentAttackMode == AttackMode.INVALID_SIGNATURE) {
                System.out.println("ATTACK: Node " + nodeId + " corrupting Commit signature");
                signature = invalidSign(signature);
            }
            
            CommitMsg signedMsg = commitBuilder.setSignature(ByteString.copyFrom(signature)).build();

            System.out.println("\n" + "=".repeat(60));
            System.out.println("NODE " + nodeId + " SENDING COMMIT TO COLLECTOR");
            System.out.println("=".repeat(60));
            MessageVisualizer.printCommit(signedMsg);
            
            String collectorId = getLeaderId();
            System.out.println("Node " + nodeId + " sending Commit to collector " + collectorId);
            
            logSent("CommitMsg", formatCommit(signedMsg));
            return new DirectMessageAction(collectorId, signedMsg);
        } catch (Exception e) { 
            e.printStackTrace(); 
        }
        return null;
    }

    public synchronized Object commit(CommitMsg msg) {
        if (!isLive.get()) return null;
        
        logReceived("CommitMsg", formatCommit(msg));
        
        System.out.println("\n" + "=".repeat(60));
        System.out.println("NODE " + nodeId + " RECEIVED COMMIT FROM " + msg.getNodeId());
        System.out.println("=".repeat(60));
        MessageVisualizer.printCommit(msg);
        
        long seq = msg.getSequenceNumber();
        
        if (requestStatus.get(seq) != RequestState.P && !isCollector()) {
            System.out.println("Node " + nodeId + " received Commit for seq #" + seq + " but not in PREPARED state");
            return null;
        }
        
        try {
            PublicKey senderPublicKey = config.getNodePublicKey(msg.getNodeId());
            CommitMsg msgToVerify = msg.toBuilder().clearSignature().build();
            if (!CryptoUtils.verify(senderPublicKey, msgToVerify.toByteArray(), msg.getSignature().toByteArray())) {
                System.out.println("Node " + nodeId + " rejected Commit with invalid signature from " + msg.getNodeId());
                return null;
            }
        } catch (Exception e) { 
            e.printStackTrace(); 
            return null; 
        }

        commitLog.computeIfAbsent(seq, k -> new ConcurrentHashMap<>()).put(msg.getNodeId(), msg);
        int commitCount = commitLog.get(seq).size();
        System.out.println("Node " + nodeId + " received Commit from " + msg.getNodeId() + " for seq #" + seq + " (total: " + commitCount + "/" + quorum + ")");

        CommitCertificateMsg existingCert = commitCertLog.get(seq);
        if (isCollector() && commitCount >= quorum && (existingCert == null || existingCert.getView() < currentView.get())) {
            if (currentAttackMode == AttackMode.CRASH && isLeader()) {
                System.out.println("ATTACK: Node " + nodeId + " (Leader) performing CRASH. Refusing to send CommitCertificateMsg.");
                return null;
            }
            
            System.out.println("Collector " + nodeId + " has quorum of Commits for seq #" + seq + ", creating certificate");
            try {
                PrepareCertificateMsg prepareCert = prepareCertLog.get(seq);
                CommitCertificateMsg.Builder certBuilder = CommitCertificateMsg.newBuilder()
                    .setView(currentView.get())
                    .setSequenceNumber(seq)
                    .setDigest(prepareCert.getDigest())
                    .addAllCommits(commitLog.get(seq).values())
                    .setCollectorId(this.nodeId);
                
                byte[] signature = CryptoUtils.sign(config.getNodePrivateKey(this.nodeId), certBuilder.buildPartial().toByteArray());
                
                if (currentAttackMode == AttackMode.INVALID_SIGNATURE) {
                    System.out.println("ATTACK: Node " + nodeId + " (Collector) corrupting CommitCertificate signature");
                    signature = invalidSign(signature);
                }
                
                CommitCertificateMsg signedCert = certBuilder.setSignature(ByteString.copyFrom(signature)).build();
                
                System.out.println("\n" + "=".repeat(60));
                System.out.println("COLLECTOR BROADCASTING COMMIT CERTIFICATE");
                System.out.println("=".repeat(60));
                MessageVisualizer.printCommitCertificate(signedCert);
                
                commitCertLog.put(seq, signedCert);
                
                logSent("CommitCertificateMsg", formatCommitCertificate(signedCert));
                timeAttack();
                return new BroadcastAction(signedCert);
            } catch (Exception e) { 
                e.printStackTrace(); 
            }
        }
        return null;
    }

    public synchronized Object commitCertificate(CommitCertificateMsg msg) {
        if (!isLive.get()) return null;
        
        logReceived("CommitCertificateMsg", formatCommitCertificate(msg));
        
        if (currentAttackMode == AttackMode.CRASH) {
            System.out.println("ATTACK: Node " + nodeId + " performing CRASH. Ignoring CommitCertificate.");
            return null;
        }
        
        long seq = msg.getSequenceNumber();
         if (!prepareCertLog.containsKey(seq)) {
            System.out.println("Node " + nodeId + " rejected CommitCertificate for seq #" + seq + 
                             " - no PrepareCertificate exists");
            return null;
        }
        
        PrepareCertificateMsg prepareCert = prepareCertLog.get(seq);
        if (!prepareCert.getDigest().equals(msg.getDigest())) {
            System.out.println("Node " + nodeId + " rejected CommitCertificate for seq #" + seq + 
                             " - digest mismatch");
            return null;
        }
        
        if (requestStatus.get(seq) != RequestState.P) {
            System.out.println("Node " + nodeId + " rejected CommitCertificate for seq #" + seq + 
                             " - not in P state");
            return null;
        }
        
        try {
            PublicKey collectorPublicKey = config.getNodePublicKey(msg.getCollectorId());
            CommitCertificateMsg msgToVerify = msg.toBuilder().clearSignature().build();
            if (!CryptoUtils.verify(collectorPublicKey, msgToVerify.toByteArray(), msg.getSignature().toByteArray())) {
                System.out.println("Node " + nodeId + " rejected CommitCertificate with invalid signature");
                return null;
            }
            
            if (msg.getCommitsCount() < quorum) {
                System.out.println("Node " + nodeId + " rejected CommitCertificate with insufficient commits");
                return null;
            }
        } catch (Exception e) { 
            e.printStackTrace(); 
            return null; 
        }

        commitCertLog.put(seq, msg);
        commitWaitStartTime.remove(seq);
        System.out.println("Node " + nodeId + " received CommitCertificate, stopped waiting");
        
        requestStatus.put(seq, RequestState.C);
        
        commitWaitStartTime.put(seq, System.currentTimeMillis());
        
        System.out.println("Node " + nodeId + " is now COMMITTED for seq #" + seq);
        
        return execTransactions();
    }
    
    public synchronized Reply readRequest(Request request) {
        if (!isLive.get()) return null;
        
        logReceived("Request(ReadOnly)", formatRequest(request));
        
        if (currentAttackMode == AttackMode.CRASH) {
            System.out.println("ATTACK: Node " + nodeId + " performing CRASH. Refusing to reply to read-only request.");
            return null;
        }
        
        try {
            PublicKey clientPublicKey = config.getClientPublicKey(request.getClientId());
            Request requestToVerify = request.toBuilder().clearSignature().build();
            if (!CryptoUtils.verify(clientPublicKey, requestToVerify.toByteArray(), request.getSignature().toByteArray())) {
                return null;
            }
        } catch (Exception e) { 
            e.printStackTrace(); 
            return null; 
        }

        System.out.println("Node " + nodeId + " EXECUTED read-only request for client " + request.getClientId());
        
      
        return processReadOnlyRequest(request);
    }

    private synchronized Object execTransactions() {
        List<Object> actions = new ArrayList<>();
        while (true) {
            long nextSeq = lastExecutedSeq.get() + 1;
            RequestState state = requestStatus.get(nextSeq);

            if (state == RequestState.C) {
                
                PrePrepareMsg ppMsg = prePrepareLog.get(nextSeq);
                if (ppMsg == null) {
                    System.err.println("CRITICAL ERROR: State is C for seq #" + nextSeq + " but no PrePrepare log exists!");
                    break; 
                }

                requestStatus.put(nextSeq, RequestState.E);
                lastExecutedSeq.set(nextSeq);
                
                commitWaitStartTime.remove(nextSeq); 
                
                System.out.println("Node " + nodeId + " EXECUTED write transaction for seq #" + nextSeq);

             
                Reply reply = executeWriteRequest(ppMsg.getRequest());
                if (reply != null) {
                    if (!"SYSTEM".equals(reply.getClientId())) {
                         actions.add(new ReplyAction(reply));
                    } else {
                         System.out.println("Node " + nodeId + " executed NO-OP for seq #" + nextSeq + ", no reply sent to client.");
                    }
                }
                

            } else if (state == RequestState.E) {
                
                System.out.println("Node " + nodeId + " (execution) catching up, already executed seq #" + nextSeq);
                lastExecutedSeq.set(nextSeq);
            } else {
                
                break; 
            }
        }
        
        if (actions.isEmpty()) {
            return null;
        }
        
        return (actions.size() == 1) ? actions.get(0) : new MultipleActions(actions);
    }


    private Reply executeWriteRequest(Request request) {
        String result;
        
        if ("NO-OP".equals(request.getOperation())) {
            result = "NO-OP executed";
            System.out.println("Node " + nodeId + " executed NO-OP (no state change)");
        } else if ("TRANSFER".equals(request.getOperation())) {
            int senderBalance = db.getOrDefault(request.getSender(), 0);
            if (senderBalance >= request.getAmount()) {
                db.compute(request.getSender(), (k, v) -> v - request.getAmount());
                db.compute(request.getReceiver(), (k, v) -> (v == null ? 0 : v) + request.getAmount());
                result = "SUCCESS: Transferred " + request.getAmount();
            } else {
                result = "FAILURE: Insufficient funds";
            }
        } else if ("BALANCE".equals(request.getOperation())) {
            result = "BALANCE: " + db.getOrDefault(request.getSender(), 0);
        } else {
            result = "ERROR: Unknown operation";
        }

        try {
            Reply.Builder replyBuilder = Reply.newBuilder()
                .setView(currentView.get())
                .setTimestamp(request.getTimestamp())
                .setClientId(request.getClientId())
                .setNodeId(this.nodeId)
                .setResult(result);
            
            byte[] signature = CryptoUtils.sign(config.getNodePrivateKey(this.nodeId), replyBuilder.buildPartial().toByteArray());
            
            if (currentAttackMode == AttackMode.INVALID_SIGNATURE) {
                System.out.println("ATTACK: Node " + nodeId + " corrupting Reply signature");
                signature = invalidSign(signature);
            }
            
            Reply reply = replyBuilder.setSignature(ByteString.copyFrom(signature)).build();
            
            logSent("Reply", formatReply(reply));
            return reply;
        } catch (Exception e) { 
            e.printStackTrace(); 
            return null; 
        }
    }

    private Reply processReadOnlyRequest(Request request) {
        String result;
        
        if ("NO-OP".equals(request.getOperation())) {
            result = "NO-OP executed";
        } else if ("TRANSFER".equals(request.getOperation())) {
            int senderBalance = db.getOrDefault(request.getSender(), 0);
            if (senderBalance >= request.getAmount()) {
                result = "SUCCESS: Transferred " + request.getAmount();
            } else {
                result = "FAILURE: Insufficient funds";
            }
        } else if ("BALANCE".equals(request.getOperation())) {
            result = "BALANCE: " + db.getOrDefault(request.getSender(), 0);
        } else {
            result = "ERROR: Unknown operation";
        }

        
        try {
            Reply.Builder replyBuilder = Reply.newBuilder()
                .setView(currentView.get())
                .setTimestamp(request.getTimestamp())
                .setClientId(request.getClientId())
                .setNodeId(this.nodeId)
                .setResult(result);
            
            byte[] signature = CryptoUtils.sign(config.getNodePrivateKey(this.nodeId), replyBuilder.buildPartial().toByteArray());
            
            if (currentAttackMode == AttackMode.INVALID_SIGNATURE) {
                System.out.println("ATTACK: Node " + nodeId + " sending invalid Reply signature");
                signature = invalidSign(signature);
            }
            
            Reply reply = replyBuilder.setSignature(ByteString.copyFrom(signature)).build();
            logSent("Reply(ReadOnly)", formatReply(reply));
            return reply;
        } catch (Exception e) { 
            e.printStackTrace(); 
            return null; 
        }
    }

    private void timeAttack() {
        if (isLeader() && currentAttackMode == AttackMode.TIMING) {
            try {
                long delayMs = 2000; 
                System.out.println("ATTACK: Node " + nodeId + " (Leader) performing TIMING attack, delaying message by " + delayMs + "ms");
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    } 
    
    private byte[] createDigest(Request request) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(request.toByteArray());
        } catch (Exception e) { 
            throw new RuntimeException(e); 
        }
    }
    
    private byte[] invalidSign(byte[] validSignature) {
        if (validSignature == null) {
            return null;
        }
        return new byte[validSignature.length];
    }
}