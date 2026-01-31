package com.example.pbft.utils;

import com.pbft.pbft.proto.*;
import java.util.Base64;


public class MessageVisualizer {
    
    private static final int SIGNATURE_PREVIEW_LEN = 16;
    private static final int DIGEST_PREVIEW_LEN = 16;
    
    public static void printRequest(Request request) {
        System.out.println("┌─ CLIENT REQUEST ─────────────────────────────────────┐");
        System.out.println("│ Client ID:    " + pad(request.getClientId(), 38) + "│");
        System.out.println("│ Timestamp:    " + pad(String.valueOf(request.getTimestamp()), 38) + "│");
        System.out.println("│ Operation:    " + pad(request.getOperation(), 38) + "│");
        System.out.println("│ Sender:       " + pad(request.getSender(), 38) + "│");
        System.out.println("│ Receiver:     " + pad(request.getReceiver(), 38) + "│");
        System.out.println("│ Amount:       " + pad(String.valueOf(request.getAmount()), 38) + "│");
        System.out.println("│ Signature:    " + truncateBytes(request.getSignature().toByteArray(), 30) + "│");
        System.out.println("└──────────────────────────────────────────────────────┘");
    }
    
    public static void printPrePrepare(PrePrepareMsg msg) {
        System.out.println("┌─ PRE-PREPARE MESSAGE ────────────────────────────────┐");
        System.out.println("│ View:         " + pad(String.valueOf(msg.getView()), 38) + "│");
        System.out.println("│ Sequence #:   " + pad(String.valueOf(msg.getSequenceNumber()), 38) + "│");
        System.out.println("│ Digest:       " + truncateBytes(msg.getDigest().toByteArray(), 30) + "│");
        System.out.println("│ Node ID:      " + pad(msg.getNodeId(), 38) + "│");
        System.out.println("│ Signature:    " + truncateBytes(msg.getSignature().toByteArray(), 30) + "│");
        System.out.println("│ Request:");
        System.out.println("│   - Client:   " + pad(msg.getRequest().getClientId(), 36) + "│");
        System.out.println("│   - Op:       " + pad(msg.getRequest().getOperation(), 36) + "│");
        System.out.println("│   - Details:  " + pad(msg.getRequest().getSender() + " -> " + 
                          msg.getRequest().getReceiver() + " (" + msg.getRequest().getAmount() + ")", 36) + "│");
        System.out.println("└──────────────────────────────────────────────────────┘");
    }
    
    public static void printPrepare(PrepareMsg msg) {
        System.out.println("┌─ PREPARE MESSAGE ────────────────────────────────────┐");
        System.out.println("│ View:         " + pad(String.valueOf(msg.getView()), 38) + "│");
        System.out.println("│ Sequence #:   " + pad(String.valueOf(msg.getSequenceNumber()), 38) + "│");
        System.out.println("│ Digest:       " + truncateBytes(msg.getDigest().toByteArray(), 30) + "│");
        System.out.println("│ Node ID:      " + pad(msg.getNodeId(), 38) + "│");
        System.out.println("│ Signature:    " + truncateBytes(msg.getSignature().toByteArray(), 30) + "│");
        System.out.println("└──────────────────────────────────────────────────────┘");
    }
    
    public static void printPrepareCertificate(PrepareCertificateMsg msg) {
        System.out.println("┌─ PREPARE CERTIFICATE ────────────────────────────────┐");
        System.out.println("│ View:         " + pad(String.valueOf(msg.getView()), 38) + "│");
        System.out.println("│ Sequence #:   " + pad(String.valueOf(msg.getSequenceNumber()), 38) + "│");
        System.out.println("│ Digest:       " + truncateBytes(msg.getDigest().toByteArray(), 30) + "│");
        System.out.println("│ Collector ID: " + pad(msg.getCollectorId(), 38) + "│");
        System.out.println("│ Signature:    " + truncateBytes(msg.getSignature().toByteArray(), 30) + "│");
        System.out.println("│ Prepares:     " + pad(String.valueOf(msg.getPreparesCount()) + " prepare messages", 38) + "│");
        
        for (int i = 0; i < msg.getPreparesCount(); i++) {
            PrepareMsg prepare = msg.getPrepares(i);
            System.out.println("│   [" + i + "] Node:   " + pad(prepare.getNodeId(), 36) + "│");
            System.out.println("│       Sig:     " + truncateBytes(prepare.getSignature().toByteArray(), 28) + "│");
        }
        System.out.println("└──────────────────────────────────────────────────────┘");
    }
    
    public static void printCommit(CommitMsg msg) {
        System.out.println("┌─ COMMIT MESSAGE ─────────────────────────────────────┐");
        System.out.println("│ View:         " + pad(String.valueOf(msg.getView()), 38) + "│");
        System.out.println("│ Sequence #:   " + pad(String.valueOf(msg.getSequenceNumber()), 38) + "│");
        System.out.println("│ Digest:       " + truncateBytes(msg.getDigest().toByteArray(), 30) + "│");
        System.out.println("│ Node ID:      " + pad(msg.getNodeId(), 38) + "│");
        System.out.println("│ Signature:    " + truncateBytes(msg.getSignature().toByteArray(), 30) + "│");
        System.out.println("└──────────────────────────────────────────────────────┘");
    }
    
    public static void printCommitCertificate(CommitCertificateMsg msg) {
        System.out.println("┌─ COMMIT CERTIFICATE ─────────────────────────────────┐");
        System.out.println("│ View:         " + pad(String.valueOf(msg.getView()), 38) + "│");
        System.out.println("│ Sequence #:   " + pad(String.valueOf(msg.getSequenceNumber()), 38) + "│");
        System.out.println("│ Digest:       " + truncateBytes(msg.getDigest().toByteArray(), 30) + "│");
        System.out.println("│ Collector ID: " + pad(msg.getCollectorId(), 38) + "│");
        System.out.println("│ Signature:    " + truncateBytes(msg.getSignature().toByteArray(), 30) + "│");
        System.out.println("│ Commits:      " + pad(String.valueOf(msg.getCommitsCount()) + " commit messages", 38) + "│");
        
        for (int i = 0; i < msg.getCommitsCount(); i++) {
            CommitMsg commit = msg.getCommits(i);
            System.out.println("│   [" + i + "] Node:   " + pad(commit.getNodeId(), 36) + "│");
            System.out.println("│       Sig:     " + truncateBytes(commit.getSignature().toByteArray(), 28) + "│");
        }
        System.out.println("└──────────────────────────────────────────────────────┘");
    }
    
    public static void printReply(Reply reply) {
        System.out.println("┌─ REPLY ──────────────────────────────────────────────┐");
        System.out.println("│ View:         " + pad(String.valueOf(reply.getView()), 38) + "│");
        System.out.println("│ Timestamp:    " + pad(String.valueOf(reply.getTimestamp()), 38) + "│");
        System.out.println("│ Client ID:    " + pad(reply.getClientId(), 38) + "│");
        System.out.println("│ Node ID:      " + pad(reply.getNodeId(), 38) + "│");
        System.out.println("│ Result:       " + pad(reply.getResult(), 38) + "│");
        System.out.println("│ Signature:    " + truncateBytes(reply.getSignature().toByteArray(), 30) + "│");
        System.out.println("└──────────────────────────────────────────────────────┘");
    }
    
    private static String truncateBytes(byte[] bytes, int maxChars) {
        if (bytes == null || bytes.length == 0) return pad("(empty)", maxChars);
        
        String hex = bytesToHex(bytes);
        if (hex.length() > maxChars) {
            int show = (maxChars - 3) / 2;
            return pad(hex.substring(0, show) + "..." + hex.substring(hex.length() - show), maxChars);
        }
        return pad(hex, maxChars);
    }
    
    private static String bytesToHex(byte[] bytes) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < Math.min(bytes.length, 8); i++) {
            result.append(String.format("%02x", bytes[i]));
        }
        if (bytes.length > 8) {
            result.append("...");
        }
        return result.toString();
    }
    
    private static String pad(String str, int length) {
        if (str == null) str = "";
        if (str.length() >= length) return str.substring(0, length);
        return str + " ".repeat(length - str.length());
    }
}