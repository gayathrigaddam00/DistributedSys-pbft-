
# Distributed Banking System (Linear PBFT)

A Byzantine Fault-Tolerant (BFT) distributed banking application built with **Java Spring Boot** and **gRPC**. This system implements the **Linear PBFT** consensus protocol to ensure data consistency and integrity even in the presence of malicious (Byzantine) nodes.

The system is designed to tolerate up to **$f$** Byzantine faults in a network of **$3f+1$** nodes. It includes a sophisticated failure simulation engine to test resilience against various attacks like Equivocation, Timing delays, and Invalid Signatures.

[![Spec](https://img.shields.io/badge/Project-Specification-red?logo=adobeacrobatreader&logoColor=white)](CSE535_F25_Project2.pdf)

---

## 🚀 Key Features

* **Linear PBFT Consensus:** Implements an optimized variant of PBFT where communication complexity is reduced from $O(n^2)$ to $O(n)$ during the Prepare and Commit phases using a **Collector** pattern.
* **View Change Protocol:** Automatically detects leader failures (via timeouts) and executes a View Change to elect a new leader, ensuring liveness.
* **Byzantine Fault Tolerance:** Robust against up to $f=2$ malicious nodes in a 7-node cluster.
* **Attack Simulation:** Built-in capability to simulate malicious behaviors (Crash, Equivocation, Timing, etc.) to verify system resilience.
* **Cryptography:** Uses **RSA-2048** digital signatures to authenticate all messages and ensure non-repudiation.
* **Interactive Testing:** Includes a CLI Test Runner to execute batch test scenarios defined in CSV files.

---

## 🛠️ System Architecture

The system consists of **7 Replica Nodes** ($n=3f+1$, where $f=2$) and **10 Clients**.

### 1. Linear PBFT Protocol Flow
Unlike traditional PBFT (all-to-all), this project uses a **Linear** approach:
1.  **Pre-Prepare:** Leader broadcasts the request.
2.  **Prepare (Linear):** Replicas send `PREPARE` messages to a designated **Collector** (usually the leader). The Collector aggregates them into a `PrepareCertificate` and broadcasts it back.
3.  **Commit (Linear):** Replicas send `COMMIT` messages to the Collector. The Collector aggregates them into a `CommitCertificate` and broadcasts it.
4.  **Execute:** Once a replica has the Commit Certificate, it executes the request and replies to the client.

### 2. Supported Request Types
* **Transfer (Read-Write):** Requires full consensus (Ordering -> Execution).
* **Balance (Read-Only):** Processed immediately by replicas; client waits for $f+1$ matching replies.

---

## 🧠 System Functionality & Design

### 1. Linear PBFT Optimization (The "Collector" Pattern)
Standard PBFT has $O(n^2)$ communication complexity because every node talks to every other node during the Prepare and Commit phases. This project implements **Linear PBFT**, which reduces complexity to $O(n)$ using a **Collector** approach.

* **Standard PBFT:** Everyone broadcasts to everyone.
* **Linear PBFT (Our Design):**
    * **Collector Role:** The Leader (or a specific node) acts as a "Collector."
    * **Aggregation:** Instead of broadcasting votes, replicas send signed `Prepare` or `Commit` messages *only* to the Collector.
    * **Certificates:** The Collector waits for a quorum ($2f+1$), aggregates the signatures into a **Certificate** (`PrepareCertificate` or `CommitCertificate`), and broadcasts this single certificate back to all nodes.

### 2. Consensus Flow (Normal Operation)
The system ensures total ordering of client requests through the following phases:

1.  **Request:** Client sends a signed transaction `(Sender, Receiver, Amount)` to the Leader.
2.  **Pre-Prepare:** Leader assigns a sequence number (`seqID`) and broadcasts a `PrePrepareMsg` to all backups.
3.  **Prepare (Linear):**
    * Backups validate the request and send a `PrepareMsg` to the Collector (Leader).
    * Collector waits for $2f+1$ messages, creates a `PrepareCertificateMsg`, and broadcasts it.
4.  **Commit (Linear):**
    * Upon receiving the certificate, nodes enter the **Prepared** state and send a `CommitMsg` to the Collector.
    * Collector waits for $2f+1$ messages, creates a `CommitCertificateMsg`, and broadcasts it.
5.  **Execute:** Once a node receives the Commit Certificate, it enters the **Committed** state, executes the transaction, updates the local database, and replies to the client.

### 3. Fault Tolerance & View Change
The system guarantees liveness even if the Leader is malicious or crashes.

* **Timeout Detection:** Every node maintains a `timeoutChecker`. If a request takes too long to process (e.g., > 10s), the node suspects the leader is faulty.
* **View Change:**
    * The node stops processing requests and broadcasts a `ViewChangeMsg` containing its current state (prepared certificates).
    * Once a node collects $2f+1$ View Change messages, it calculates the new view's leader ID (`viewID % n`).
* **New View:** The new leader aggregates the information into a `NewViewMsg`, calculates the correct starting sequence number to prevent gaps, and rebroadcasts pending requests.

### 4. Malicious Attack Simulation
The `PbftCore` includes a dedicated attack engine that intercepts messages based on the active profile:

* **Equivocation:** The malicious leader creates **two different** `PrePrepare` messages for the same `seqID` and sends them to different subsets of nodes to diverge the network state.
* **Timing Attack:** The leader intentionally sleeps (delays) before broadcasting `PrePrepare` to slow down the network without triggering a timeout.
* **Crash / In-Dark:** The node logic explicitly checks `darkAttackTargets`. If a target is in the list, the node simply drops the outgoing message, effectively partitioning that peer.

---

## ⚔️ Implemented Byzantine Attacks

The system can simulate the following malicious behaviors, controllable via the test CSV files:

| Attack Type | Description |
| :--- | :--- |
| **Invalid Signature** | Malicious nodes sign messages with invalid keys, testing the verification logic of honest nodes. |
| **Crash** | Malicious nodes stop communicating (no PrePrepare, Prepare, or Commit messages) but remain "alive" in the network sense. |
| **In-Dark** | Malicious nodes selectively ignore specific honest nodes, attempting to partition them from the quorum. |
| **Timing** | A malicious leader intentionally delays `PrePrepare` messages to degrade performance (but avoid timeout). |
| **Equivocation** | A malicious leader sends **different** sequence numbers/requests to different subsets of nodes to cause divergence. |

---

## 💻 Tech Stack

* **Language:** Java 21
* **Framework:** Spring Boot 3.3.3
* **Communication:** gRPC & Protobuf 3.25.3
* **Cryptography:** `java.security` (RSA KeyPairs, SHA-256 signatures)
* **Build Tool:** Maven

---

## 🚀 Getting Started

### Prerequisites
* Java 21+
* Maven

### Installation
1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/gayathrigaddam00/DistributedSys-pbft.git](https://github.com/gayathrigaddam00/DistributedSys-pbft.git)
    cd DistributedSys-pbft
    ```

2.  **Build the project:**
    ```bash
    ./mvnw clean install
    ```

---

## 🏃‍♂️ Usage

To run the full 7-node cluster, you need to start each node in a separate terminal.

### 1. Start the Replica Nodes (n1 to n7)
**Terminal 1:**
```bash
java -jar target/grpc-demo-0.0.1-SNAPSHOT.jar --spring.profiles.active=n1

```

**Terminal 2:**

```bash
java -jar target/grpc-demo-0.0.1-SNAPSHOT.jar --spring.profiles.active=n2

```

*(Repeat for n3, n4, n5, n6, n7)*

### 2. Run the Test Runner

The `TestRunner` parses the CSV file, configures the nodes (setting up attacks/failures), and submits transactions.

```bash
java -cp target/grpc-demo-0.0.1-SNAPSHOT.jar:target/lib/* com.example.pbft.TestRunner -f tests/CSE535-F25-Project-2-Testcases.csv

```

---

## 🧪 Test Scenarios

The `tests/` directory contains CSV files defining transaction sets and attack scenarios.

### Test File Format

```csv
Set Number, Transactions, Live Nodes, Byzantine Nodes, Attack Type
1, "(A,B,1)", "[n1,n2,n3,n4,n5,n6,n7]", "[]", "[]"
2, "(A,B,1)", "[n1,n2,n3,n5,n6,n7]", "[n1]", "[time; dark(n2)]"

```

* **Set 1:** Normal operation. All nodes live.
* **Set 2:** Leader `n1` is Byzantine. It performs a **Timing Attack** and keeps `n2` **In-Dark**. Node `n4` is crashed (not in Live list).

### Included Test Suites

1. **`CSE535-F25-Project-2-Demo-Tests.csv`**: Quick validation of basic flows and simple attacks.
2. **`CSE535-F25-Project-2-Testcases.csv`**: Comprehensive suite covering Equivocation, View Changes, and complex failure modes.

---

## 🔗 References

* **Practical Byzantine Fault Tolerance** - Castro & Liskov
* **CSE 535: Distributed Systems** - Stony Brook University

```

```
