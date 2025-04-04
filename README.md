# IN2011 Computer Networks Coursework 2024/2025

## Project: CRN Node Communication Implementation

This project implements a node communication protocol over UDP sockets as part of the IN2011 Computer Networks coursework. The node can communicate using a specified protocol, store key-value pairs, relay messages, and perform CAS (Compare And Swap) operations.

---

## Features

- Node initialization and port binding
- Neighbor bootstrapping for Azure lab test
- UDP communication protocol: `R`, `S`, `E`, `V`, `W`, `X`, `C`, `D`, `G`, `H`
- Local and network-based read/write
- CAS operation (Compare and Swap)
- Relay stack support
- Wireshark-compatible for protocol analysis

---

## Files Included

- `Node.java` – The main node implementation
- `AzureLabTest.java` – The Azure lab test runner
- `LocalTest.java` – Local test runner for two or more nodes
- `HashID.java` – Hashing helper (if needed by implementation)
- `README.md` – Project documentation
- `Final_Test.pcapng` – Wireshark recording of node communication

---

## Usage

### Compiling the Code
```bash
javac *.java
```

### Running Azure Lab Test (on virtual lab environment)
```bash
java AzureLabTest
```

### Running Local Test
```bash
java LocalTest
```

---

## Wireshark Recording

The `Final_Test.pcapng` file contains a capture of message traffic between nodes using UDP.

### Wireshark Display Filter
Use this to filter all necessary packets:
```wireshark
udp && (udp contains "R" || udp contains "S" || udp contains "E" || udp contains "V" || udp contains "W" || udp contains "X" || udp contains "C" || udp contains "D" || udp contains "G" || udp contains "H")
```

---

## Notes

- Run AzureLabTest **only on Azure virtual lab**.
- Make sure your IP and email are correctly set in the test file.
- The LocalTest should be used to validate full protocol support in a local setup.
- Screenshots from Wireshark are optional but help demonstrate functionality.

---

## Author
**Zidan Ahmed**  
zidan.ahmed.2@city.ac.uk

---