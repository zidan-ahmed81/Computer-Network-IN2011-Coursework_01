import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class AzureLabTest {

    // Helper method: generate a random two-character transaction ID (avoiding spaces)
    private static String generateTransactionID() {
        Random r = new Random();
        char c1 = (char)(33 + r.nextInt(94)); // ASCII 33 to 126
        char c2 = (char)(33 + r.nextInt(94));
        return "" + c1 + c2;
    }

    // Helper method: send a UDP message and wait for a response
    private static String sendUDPMessage(String message, String destIP, int destPort) throws Exception {
        DatagramSocket clientSocket = new DatagramSocket();
        clientSocket.setSoTimeout(10000); // 10-second timeout
        InetAddress IPAddress = InetAddress.getByName(destIP);
        byte[] sendData = message.getBytes(StandardCharsets.UTF_8);
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, destPort);
        clientSocket.send(sendPacket);

        byte[] receiveData = new byte[4096];
        DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
        try {
            clientSocket.receive(receivePacket);
        } catch (SocketTimeoutException e) {
            System.out.println("No response received within timeout period for message: " + message);
            clientSocket.close();
            return "";
        }
        String response = new String(receivePacket.getData(), 0, receivePacket.getLength(), StandardCharsets.UTF_8);
        clientSocket.close();
        return response;
    }

    public static void main(String[] args) throws Exception {
        // === Configuration ===
        // Your VM's IP address – replace with your actual VM IP (from ip a)
        String myVmIp = "10.216.35.23";
        // Your email address – used for your primary node's name.
        String emailAddress = "student@example.com";
        // External Azure bootstrap nodes – use multiple for better bootstrapping.
        String[] externalBootstrapNodes = {
                "10.200.51.19:20114",
                "10.200.51.18:20111",
                "10.200.51.19:20116"
        };

        // === Create multiple local Node instances ===
        int nodeCount = 3;
        int basePort = 20110;
        Node[] nodes = new Node[nodeCount];

        // Node0: primary node (using your email as the name)
        nodes[0] = new Node();
        nodes[0].setNodeName("N:" + emailAddress);
        nodes[0].openPort(basePort);
        nodes[0].write("N:" + emailAddress, myVmIp + ":" + basePort);

        // Node1 and Node2: additional local nodes.
        for (int i = 1; i < nodeCount; i++) {
            nodes[i] = new Node();
            nodes[i].setNodeName("N:Test" + i);
            nodes[i].openPort(basePort + i);
            nodes[i].write("N:Test" + i, myVmIp + ":" + (basePort + i));
        }

        // === Bootstrap: Let local nodes know about each other ===
        nodes[0].write("N:Test1", myVmIp + ":" + (basePort + 1));
        nodes[0].write("N:Test2", myVmIp + ":" + (basePort + 2));
        nodes[1].write("N:" + emailAddress, myVmIp + ":" + basePort);
        nodes[2].write("N:" + emailAddress, myVmIp + ":" + basePort);

        // === Bootstrap: Add external Azure nodes to Node0's store (using multiple nodes) ===
        for (int i = 0; i < externalBootstrapNodes.length; i++) {
            String nodeKey = "N:Azure" + (i+1);
            nodes[0].write(nodeKey, externalBootstrapNodes[i]);
        }

        System.out.println("Local nodes started and bootstrapped on ports "
                + basePort + " to " + (basePort + nodeCount - 1));
        Thread.sleep(3000); // Allow nodes to settle

        // === Local Tests (among local nodes) ===

        // 1. Name Request: Send a UDP Name Request ("G") to Node1.
        String txnID = generateTransactionID();
        String nameRequest = txnID + " G ";
        System.out.println("Local Test: Sending Name Request to Node1 (" + myVmIp + ":" + (basePort+1) + "): " + nameRequest);
        String nameResponse = sendUDPMessage(nameRequest, myVmIp, basePort + 1);
        System.out.println("Local Test: Received Name Response from Node1: " + nameResponse);

        // 2. Nearest Request: Send a UDP Nearest Request ("N") to Node2 with a dummy hash.
        txnID = generateTransactionID();
        String nearestRequest = txnID + " N dummyhash";
        System.out.println("Local Test: Sending Nearest Request to Node2 (" + myVmIp + ":" + (basePort+2) + "): " + nearestRequest);
        String nearestResponse = sendUDPMessage(nearestRequest, myVmIp, basePort + 2);
        System.out.println("Local Test: Received Nearest Response from Node2: " + nearestResponse);

        // 3. Key Existence: Send a UDP Key Existence Request ("E") to Node0 for a built-in jabberwocky key.
        txnID = generateTransactionID();
        String existenceRequest = txnID + " E D:jabberwocky0";
        System.out.println("Local Test: Sending Key Existence Request to Node0 (" + myVmIp + ":" + basePort + "): " + existenceRequest);
        String existenceResponse = sendUDPMessage(existenceRequest, myVmIp, basePort);
        System.out.println("Local Test: Received Existence Response from Node0: " + existenceResponse);

        // 4. Read Request: Send a UDP Read Request ("R") for a jabberwocky verse from Node0.
        txnID = generateTransactionID();
        String readRequest = txnID + " R D:jabberwocky0";
        System.out.println("Local Test: Sending Read Request to Node0 (" + myVmIp + ":" + basePort + "): " + readRequest);
        String readResponse = sendUDPMessage(readRequest, myVmIp, basePort);
        System.out.println("Local Test: Received Read Response from Node0: " + readResponse);

        // 5. Write Request: Write a marker to Node0 and then read it back.
        txnID = generateTransactionID();
        String markerKey = "D:MarkerTest";
        String markerValue = "MultiNodeWorks!";
        String writeRequest = txnID + " W " + markerKey + " " + markerValue;
        System.out.println("Local Test: Sending Write Request to Node0 (" + myVmIp + ":" + basePort + "): " + writeRequest);
        String writeResponse = sendUDPMessage(writeRequest, myVmIp, basePort);
        System.out.println("Local Test: Received Write Response from Node0: " + writeResponse);

        txnID = generateTransactionID();
        String readMarker = txnID + " R " + markerKey;
        System.out.println("Local Test: Sending Read Request for marker to Node0 (" + myVmIp + ":" + basePort + "): " + readMarker);
        String readMarkerResponse = sendUDPMessage(readMarker, myVmIp, basePort);
        System.out.println("Local Test: Received Read Response for marker from Node0: " + readMarkerResponse);

        // 6. CAS Request: First test with an incorrect current value, then with the correct one.
        txnID = generateTransactionID();
        String casRequestWrong = txnID + " C " + markerKey + " WrongValue NewMultiNode";
        System.out.println("Local Test: Sending CAS Request (wrong value) to Node0 (" + myVmIp + ":" + basePort + "): " + casRequestWrong);
        String casResponseWrong = sendUDPMessage(casRequestWrong, myVmIp, basePort);
        System.out.println("Local Test: Received CAS Response (should fail) from Node0: " + casResponseWrong);

        txnID = generateTransactionID();
        String casRequestCorrect = txnID + " C " + markerKey + " " + markerValue + " NewMultiNode";
        System.out.println("Local Test: Sending CAS Request (correct value) to Node0 (" + myVmIp + ":" + basePort + "): " + casRequestCorrect);
        String casResponseCorrect = sendUDPMessage(casRequestCorrect, myVmIp, basePort);
        System.out.println("Local Test: Received CAS Response (should succeed) from Node0: " + casResponseCorrect);

        txnID = generateTransactionID();
        String readAfterCAS = txnID + " R " + markerKey;
        System.out.println("Local Test: Sending Read Request after CAS to Node0 (" + myVmIp + ":" + basePort + "): " + readAfterCAS);
        String readAfterCASResponse = sendUDPMessage(readAfterCAS, myVmIp, basePort);
        System.out.println("Local Test: Received Read Response after CAS from Node0: " + readAfterCASResponse);

        // 7. Relay Request: Send a relay request to Node0 to forward a message to Node1.
        txnID = generateTransactionID();
        String relayRequest = txnID + " V N:Test1 RelayMsgTest";
        System.out.println("Local Test: Sending Relay Request to Node0 (" + myVmIp + ":" + basePort + "): " + relayRequest);
        String relayResponse = sendUDPMessage(relayRequest, myVmIp, basePort);
        System.out.println("Local Test: Received Relay Response (if any) from Node0: " + relayResponse);

        // 8. Direct API Test: Push and pop relay on Node0.
        System.out.println("Local Test: Directly testing pushRelay/popRelay on Node0...");
        nodes[0].pushRelay("N:DirectRelay");
        nodes[0].popRelay();

        // 9. Announce all local node addresses.
        System.out.println("Local Test: Announcing node addresses...");
        for (int i = 0; i < nodeCount; i++) {
            String key = (i == 0) ? "N:" + emailAddress : "N:Test" + i;
            String value = myVmIp + ":" + (basePort + i);
            nodes[i].write(key, value);
        }

        // === External Azure Node Tests ===
        System.out.println("=== External Azure Node Tests ===");

        // We'll test with one of the external bootstrap nodes. You can try them all.
        String externalNodeKey = "N:Azure1"; // Using the first external node
        // a) Name Request to external node.
        txnID = generateTransactionID();
        String azureNameRequest = txnID + " G ";
        System.out.println("Sending Name Request to External Azure node (" + externalBootstrapNodes[0] + "): " + azureNameRequest);
        String azureNameResponse = sendUDPMessage(azureNameRequest, externalBootstrapNodes[0].split(":")[0], Integer.parseInt(externalBootstrapNodes[0].split(":")[1]));
        System.out.println("Received Name Response from External Azure node: " + azureNameResponse);

        // b) Nearest Request to external node.
        txnID = generateTransactionID();
        String azureNearestRequest = txnID + " N dummyhash";
        System.out.println("Sending Nearest Request to External Azure node (" + externalBootstrapNodes[0] + "): " + azureNearestRequest);
        String azureNearestResponse = sendUDPMessage(azureNearestRequest, externalBootstrapNodes[0].split(":")[0], Integer.parseInt(externalBootstrapNodes[0].split(":")[1]));
        System.out.println("Received Nearest Response from External Azure node: " + azureNearestResponse);

        // c) Read Request for a jabberwocky verse from external node.
        txnID = generateTransactionID();
        String azureReadRequest = txnID + " R D:jabberwocky0";
        System.out.println("Sending Read Request to External Azure node (" + externalBootstrapNodes[0] + "): " + azureReadRequest);
        String azureReadResponse = sendUDPMessage(azureReadRequest, externalBootstrapNodes[0].split(":")[0], Integer.parseInt(externalBootstrapNodes[0].split(":")[1]));
        System.out.println("Received Read Response from External Azure node: " + azureReadResponse);

        // d) Write Request to external node.
        txnID = generateTransactionID();
        String azureMarkerKey = "D:AzureMarker";
        String azureMarkerValue = "WorksWithAzure!";
        String azureWriteRequest = txnID + " W " + azureMarkerKey + " " + azureMarkerValue;
        System.out.println("Sending Write Request to External Azure node (" + externalBootstrapNodes[0] + "): " + azureWriteRequest);
        String azureWriteResponse = sendUDPMessage(azureWriteRequest, externalBootstrapNodes[0].split(":")[0], Integer.parseInt(externalBootstrapNodes[0].split(":")[1]));
        System.out.println("Received Write Response from External Azure node: " + azureWriteResponse);

        // e) Read back the marker from external node.
        txnID = generateTransactionID();
        String azureReadMarker = txnID + " R " + azureMarkerKey;
        System.out.println("Sending Read Request for marker to External Azure node (" + externalBootstrapNodes[0] + "): " + azureReadMarker);
        String azureReadMarkerResponse = sendUDPMessage(azureReadMarker, externalBootstrapNodes[0].split(":")[0], Integer.parseInt(externalBootstrapNodes[0].split(":")[1]));
        System.out.println("Received Read Response for marker from External Azure node: " + azureReadMarkerResponse);

        System.out.println("Multi-node and External Azure node tests complete.");
        System.out.println("Leave the application running to capture all packets with Wireshark.");

        // Keep the application running indefinitely.
        while (true) {
            Thread.sleep(1000);
        }
    }
}
