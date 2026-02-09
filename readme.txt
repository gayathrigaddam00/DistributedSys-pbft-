# Distributed Banking System (Linear PBFT)

A Byzantine Fault-Tolerant (BFT) distributed banking application built with **Java Spring Boot** and **gRPC**. This system implements the **Linear PBFT** consensus protocol to ensure data consistency and integrity even in the presence of malicious (Byzantine) nodes.

The system is designed to tolerate up to **$f$** Byzantine faults in a network of **$3f+1$** nodes. It includes a sophisticated failure simulation engine to test resilience against various attacks like Equivocation, Timing delays, and Invalid Signatures.

[![Spec](https://img.shields.io/badge/Project-Specification-red?logo=adobeacrobatreader&logoColor=white)](CSE535_F25_Project2.pdf)

---

## 🚀 Key Features

* **Linear PBFT Consensus:** Implements an optimized variant of PBFT where communication complexity is reduced from $O(n^2)$ to $O(n)$ during the Prepare and Commit phases using a **Collector** pattern.
* **View Change Protocol:** Automatically detects leader failures (via timeouts) and executes a View Change to elect a new leader, ensuring liveness.
* **Byzantine Fault Tolerance:** robust against up to $f=2$ malicious nodes in a 7-node cluster.
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
