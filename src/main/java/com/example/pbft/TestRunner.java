package com.example.pbft;

import com.pbft.pbft.proto.AttackMode;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import com.pbft.pbft.proto.*;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Command(name = "test-runner", mixinStandardHelpOptions = true,
        description = "Runs test cases for the PBFT banking application.")
public class TestRunner implements Callable<Integer> {

    private static final Pattern TXN_PATTERN = Pattern.compile("\\((\\w+)(?:,\\s*(\\w+),\\s*(\\d+))?\\)");

    private static class Transaction {
        final String sender;
        final String receiver;
        final int amount;
        final boolean isReadOnly;

        Transaction(String sender, String receiver, int amount) {
            this.sender = sender;
            this.receiver = receiver;
            this.amount = amount;
            this.isReadOnly = false;
        }

        Transaction(String sender) {
            this.sender = sender;
            this.receiver = "";
            this.amount = 0;
            this.isReadOnly = true;
        }
    }

    private static class TestSet {
        final int setNumber;
        final List<Transaction> transactions = new ArrayList<>();
        final Set<String> liveNodes = new HashSet<>();
        final Set<String> byzantineNodes = new HashSet<>();
        final List<String> attackTypes = new ArrayList<>();

        TestSet(int setNumber, String liveNodesStr, String byzantineNodesStr, String attackTypesStr) {
            this.setNumber = setNumber;
            String[] live = liveNodesStr.replaceAll("[\\[\\] ]", "").split(",");
            if (live.length > 0 && !live[0].isEmpty()) {
                this.liveNodes.addAll(Arrays.asList(live));
            }

            String[] byzantine = byzantineNodesStr.replaceAll("[\\[\\] ]", "").split(",");
            if (byzantine.length > 0 && !byzantine[0].isEmpty()) {
                this.byzantineNodes.addAll(Arrays.asList(byzantine));
            }

            String[] attacks = attackTypesStr.replaceAll("[\\[\\] ]", "").split(";");
             if (attacks.length > 0 && !attacks[0].isEmpty()) {
                this.attackTypes.addAll(Arrays.asList(attacks));
            }
        }

        void addTransaction(String txnStr) {
            Matcher m = TXN_PATTERN.matcher(txnStr);
            if (m.find()) {
                if (m.group(2) != null) {
                    transactions.add(new Transaction(m.group(1), m.group(2), Integer.parseInt(m.group(3))));
                } else {
                    transactions.add(new Transaction(m.group(1)));
                }
            }
        }
    }

    @Option(names = {"-f", "--file"}, description = "Path to the test case CSV file.", required = true)
    private String file;

    private List<TestSet> testSets;
    private int currentSetIndex = 0;
    private BankingClient client;
    private final int[] allPorts = {50051, 50052, 50053, 50054, 50055, 50056, 50057};
    private final String[] allNodeIds = {"n1", "n2", "n3", "n4", "n5", "n6", "n7"};

    public static void main(String[] args) {
        int exitCode = new CommandLine(new TestRunner()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        System.out.println("Waiting 5 seconds for all nodes to start...");
        Thread.sleep(5000);

        this.client = new BankingClient(allPorts);
        loadTestCases();
        runCommandLineInterface();
        client.shutdown();
        return 0;
    }

   private void runCommandLineInterface() {
    Scanner scanner = new Scanner(System.in);
    System.out.println("PBFT Test Runner started. Type 'help' for a list of commands.");

    while (true) {
        System.out.print("> ");
        String input = scanner.nextLine().trim();
        if (input.isEmpty()) continue;
        String[] parts = input.split("\\s+");
        String command = parts[0];

        switch (command.toLowerCase()) {
            case "next":
                processNextSet();
                break;
            case "printdb":
                client.printAllDBs();
                break;
            case "printstatus":
                handlePrintStatus(parts);
                break;
            case "printview":
                client.printAllViews();
                break;
            case "printlog":
                handlePrintLog(parts);
                break;
            case "help":
                printHelp();
                break;
            case "exit":
                scanner.close();
                return;
            default:
                System.out.println("Unknown command: '" + command + "'. Type 'help' for a list of commands.");
                break;
        }
    }
}

private void handlePrintLog(String[] parts) {
    if (parts.length < 2) {
        System.out.println("Usage: printlog <node_id>   (e.g., printlog n4)");
        System.out.println("   or: printlog all         (prints all nodes)");
        return;
    }
    
    if (parts[1].equalsIgnoreCase("all")) {
        client.printAllLogs();
    } else {
        String nodeId = parts[1];
        if (!nodeId.matches("n[1-7]")) {
            System.err.println("Invalid node ID: " + nodeId + ". Must be n1, n2, n3, n4, n5, n6, or n7.");
            return;
        }
        
        int nodeNum = Integer.parseInt(nodeId.substring(1));
        client.printLog(nodeNum - 1);
    }
}

    private void handlePrintStatus(String[] parts) {
        if (parts.length < 2) {
            System.out.println("Usage: printstatus <sequence_number>");
            return;
        }
        try {
            long seq = Long.parseLong(parts[1]);
            client.printAllStatuses(seq);
        } catch (NumberFormatException e) {
            System.err.println("Invalid sequence number: " + parts[1]);
        }
    }

    private void loadTestCases() throws IOException, CsvException {
        this.testSets = new ArrayList<>();
        try (CSVReader reader = new CSVReader(new FileReader(file))) {
            List<String[]> allRows = reader.readAll();
            if (allRows.size() <= 1) {
                 System.out.println("No test cases found in " + file);
                 return;
            }

            TestSet currentSet = null;
            for (int i = 1; i < allRows.size(); i++) {
                String[] row = allRows.get(i);
                if (row == null || row.length == 0 || row[0].trim().isEmpty() && row.length <= 1) continue;

                String setNumStr = row[0].trim();
                String txnStr = (row.length > 1) ? row[1].trim() : "";
                String liveNodesStr = (row.length > 2) ? row[2].trim() : "";
                String byzantineNodesStr = (row.length > 3) ? row[3].trim() : "";
                String attackTypesStr = (row.length > 4) ? row[4].trim() : "";

                if (!setNumStr.isEmpty()) {
                    int setNumber = Integer.parseInt(setNumStr);
                    currentSet = new TestSet(setNumber, liveNodesStr, byzantineNodesStr, attackTypesStr);
                    testSets.add(currentSet);
                     if (liveNodesStr.isEmpty() && currentSet != null) {
                        System.err.println("Warning: Live node list empty for set " + setNumber + " on its first line. Ensure CSV format is correct.");
                    }
                }

                if (currentSet != null && !txnStr.isEmpty()) {
                    currentSet.addTransaction(txnStr);
                } else if (currentSet != null && setNumStr.isEmpty() && !liveNodesStr.isEmpty()) {
                     System.err.println("Warning: Redefining live nodes for set " + currentSet.setNumber + " on a subsequent row. Check CSV format.");
                     currentSet.liveNodes.clear();
                     String[] live = liveNodesStr.replaceAll("[\\[\\] ]", "").split(",");
                     if (live.length > 0 && !live[0].isEmpty()) {
                         currentSet.liveNodes.addAll(Arrays.asList(live));
                     }
                }
            }
        }
         System.out.println("Loaded " + testSets.size() + " test sets from " + file);
    }

    private List<String> parseDarkTargets(String attackStr) {
        List<String> targets = new ArrayList<>();
        Pattern p = Pattern.compile("\\((.*?)\\)");
        Matcher m = p.matcher(attackStr);
        if (m.find()) {
            String inner = m.group(1);
            if (!inner.isEmpty()) {
                for (String target : inner.split(",")) {
                    targets.add(target.trim());
                }
            }
        }
        return targets;
    }

private void processNextSet() {
    if (currentSetIndex >= testSets.size()) {
        System.out.println("All test sets have been processed. Type 'exit' to quit.");
        return;
    }

    TestSet currentSet = testSets.get(currentSetIndex);
    System.out.println("\n=======================================================");
    System.out.println("Processing test set " + currentSet.setNumber);
    System.out.println("Live Nodes: " + currentSet.liveNodes);
    System.out.println("Byzantine Nodes: " + currentSet.byzantineNodes);
    System.out.println("Attacks: " + currentSet.attackTypes);
    System.out.println("=======================================================");

    client.flushAllNodes();

    System.out.println("Waiting for nodes to flush...");
    try { Thread.sleep(1000); } catch (InterruptedException e) { Thread.currentThread().interrupt();}

    System.out.println("Configuring node statuses...");
    for (int i = 0; i < allNodeIds.length; i++) {
        boolean isLive = currentSet.liveNodes.contains(allNodeIds[i]);
        client.setNodeStatus(i, isLive);
    }

    System.out.println("Configuring byzantine attacks...");
    for (int i = 0; i < allNodeIds.length; i++) {
        String nodeId = allNodeIds[i];
        if (currentSet.byzantineNodes.contains(nodeId)) {
            AttackMode modeToSet = AttackMode.NONE;
            String attackDesc = "NONE";

            
            if (currentSet.attackTypes.stream().anyMatch(s -> s.trim().equalsIgnoreCase("crash"))) {
                modeToSet = AttackMode.CRASH;
                attackDesc = "CRASH";
            } else if (currentSet.attackTypes.stream().anyMatch(s -> s.trim().equalsIgnoreCase("time"))) {
                modeToSet = AttackMode.TIMING;
                attackDesc = "TIMING";
            } else if (currentSet.attackTypes.stream().anyMatch(s -> s.trim().equalsIgnoreCase("sign"))) {
                modeToSet = AttackMode.INVALID_SIGNATURE;
                attackDesc = "INVALID_SIGNATURE";
            }
            
         
            client.setDarkTargets(i, new ArrayList<>());
            client.setEquivocationTargets(i, new ArrayList<>());

          
            for (String attackStr : currentSet.attackTypes) {
                attackStr = attackStr.trim();
                if (attackStr.startsWith("dark(")) {
                    List<String> targets = parseDarkTargets(attackStr);
                    System.out.println("    -> Node " + nodeId + " will ALSO not send to: " + targets);
                    client.setDarkTargets(i, targets);
                }
                
                if (attackStr.startsWith("equivocation(")) {
                
                    List<String> targets = parseDarkTargets(attackStr); 
                    System.out.println("    -> Node " + nodeId + " will ALSO equivocate to: " + targets);
                    client.setEquivocationTargets(i, targets);
                  
                    if (modeToSet == AttackMode.NONE) {
                        modeToSet = AttackMode.EQUIVOCATION;
                        attackDesc = "EQUIVOCATION";
                    }
                }
            }
            
            System.out.println("  Setting node " + nodeId + " to " + attackDesc + " mode.");
            client.setAttackMode(i, modeToSet);

        } else {
         
            client.setAttackMode(i, AttackMode.NONE);
            client.setDarkTargets(i, new ArrayList<>());
            client.setEquivocationTargets(i, new ArrayList<>());
        }
    }

    System.out.println("Submitting " + currentSet.transactions.size() + " transactions...");
    for (Transaction txn : currentSet.transactions) {
        if (txn.isReadOnly) {
            try {
                System.out.println("Processing read-only request for " + txn.sender);
                client.runReadOnlyRequestTest(txn.sender);
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.err.println("Read-only request interrupted.");
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                System.err.println("Error processing read-only request for " + txn.sender + ": " + e.getMessage());
            }
        } else {
            try {
                client.runWriteRequestTest(txn.sender, txn.receiver, txn.amount);
                Thread.sleep(1500);
            } catch (InterruptedException e) {
                System.err.println("Transaction submission interrupted.");
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                System.err.println("Error submitting transaction (" + txn.sender + "->" + txn.receiver + "): " + e.getMessage());
            }
        }
    }

    currentSetIndex++;

    System.out.println("\n=======================================================");
    System.out.println("Test set " + currentSet.setNumber + " complete.");
    System.out.println("You can now use 'printdb', 'printstatus <seq>', etc. or 'next' for the next set.");
    System.out.println("=======================================================");
}

    private void printHelp() {
    System.out.println("Available commands:");
    System.out.println("  next              - Process the next set of transactions from the test file.");
    System.out.println("  printdb           - Print the current state of all bank account balances on live nodes.");
    System.out.println("  printstatus <seq> - Print the consensus status for a sequence number on live nodes.");
    System.out.println("  printview         - Print view change information from nodes.");
    System.out.println("  printlog <node>   - Print message log for node (e.g., printlog n4).");
    System.out.println("  printlog all      - Print message logs for all nodes.");
    System.out.println("  help              - Show this help message.");
    System.out.println("  exit              - Exit the test runner.");
}
}