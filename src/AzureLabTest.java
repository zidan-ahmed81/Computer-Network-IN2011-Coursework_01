import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class AzureLabTest {
    // Helper method: generate a random 2-byte transaction ID (avoiding spaces)
    private static String generateTransactionID() {
        Random r = new Random();
        char c1 = (char)(33 + r.nextInt(94)); // ASCII 33 to 126
        char c2 = (char)(33 + r.nextInt(94));
        return "" + c1 + c2;
    }

    // Helper method: send a UDP message and wait for a response
    private static String sendUDPMessage(String message, int port) throws Exception {
        DatagramSocket clientSocket = new DatagramSocket();
        clientSocket.setSoTimeout(5000); // 5-second timeout
        InetAddress IPAddress = InetAddress.getLocalHost();
        byte[] sendData = message.getBytes(StandardCharsets.UTF_8);
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, port);
        clientSocket.send(sendPacket);

        byte[] receiveData = new byte[4096];
        DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
        clientSocket.receive(receivePacket);
        String response = new String(receivePacket.getData(), 0, receivePacket.getLength(), StandardCharsets.UTF_8);
        clientSocket.close();
        return response;
    }

    public static void main(String[] args) throws Exception {
        // --- Adjust these addresses as needed ---
        // Your VM's IP address (use your actual VM IP)
        String myVmIp = "10.216.35.23";
        // Your email address: this will be used for your main node's name.
        String emailAddress = "zidan.ahmed.2@city.ac.uk";
        // CRN bootstrap node: choose one of the lab nodes (10.200.51.18 or 10.200.51.19 with port 20110-20116)
        String bootstrapIp = "10.200.51.19";
        int bootstrapPort = 20114;

        // For multi-node testing on the lab network, we create three nodes.
        int nodeCount = 7;
        int basePort = 20110; // Local ports for our test nodes.
        Node[] nodes = new Node[nodeCount];

        // Create and initialize nodes.
        // The first node uses your email as the node name.
        nodes[0] = new Node();
        nodes[0].setNodeName("N:" + emailAddress);
        nodes[0].openPort(basePort);
        nodes[0].write("N:" + emailAddress, myVmIp + ":" + basePort);

        // The remaining nodes use default test names.
        for (int i = 1; i < nodeCount; i++) {
            nodes[i] = new Node();
            nodes[i].setNodeName("N:Test" + i);
            nodes[i].openPort(basePort + i);
            nodes[i].write("N:Test" + i, myVmIp + ":" + (basePort + i));
        }

        // Bootstrap: Let the nodes know about each other.
        nodes[0].write("N:Test1", myVmIp + ":" + (basePort + 1));
        nodes[0].write("N:Test2", myVmIp + ":" + (basePort + 2));
        nodes[1].write("N:" + emailAddress, myVmIp + ":" + basePort);
        nodes[2].write("N:" + emailAddress, myVmIp + ":" + basePort);

        // Additionally, bootstrap one node with an official CRN node from Azure.
        nodes[0].write("N:AzureBootstrap", bootstrapIp + ":" + bootstrapPort);

        // Give the nodes a moment to fully start.
        System.out.println("Nodes started and bootstrapped on ports " + basePort + " to " + (basePort + nodeCount - 1));
        Thread.sleep(3000);

        // --- Now send UDP test messages across nodes ---

        // 1. Test Name Request (G) to Node1.
        String txnID = generateTransactionID();
        String nameRequest = txnID + " G ";
        System.out.println("Sending Name Request to Node1 (" + myVmIp + ":" + (basePort+1) + "): " + nameRequest);
        String nameResponse = sendUDPMessage(nameRequest, basePort + 1);
        System.out.println("Received Name Response from Node1: " + nameResponse);

        // 2. Test Nearest Request (N) to Node2 (using a dummy hash).
        txnID = generateTransactionID();
        String nearestRequest = txnID + " N dummyhash";
        System.out.println("Sending Nearest Request to Node2 (" + myVmIp + ":" + (basePort+2) + "): " + nearestRequest);
        String nearestResponse = sendUDPMessage(nearestRequest, basePort + 2);
        System.out.println("Received Nearest Response from Node2: " + nearestResponse);

        // 3. Test Key Existence Request (E) to Node0 for a built-in jabberwocky key.
        txnID = generateTransactionID();
        String existenceRequest = txnID + " E D:jabberwocky0";
        System.out.println("Sending Key Existence Request to Node0 (" + myVmIp + ":" + basePort + "): " + existenceRequest);
        String existenceResponse = sendUDPMessage(existenceRequest, basePort);
        System.out.println("Received Existence Response from Node0: " + existenceResponse);

        // 4. Test Read Request (R) for jabberwocky verse from Node0.
        txnID = generateTransactionID();
        String readRequest = txnID + " R D:jabberwocky0";
        System.out.println("Sending Read Request to Node0 (" + myVmIp + ":" + basePort + "): " + readRequest);
        String readResponse = sendUDPMessage(readRequest, basePort);
        System.out.println("Received Read Response from Node0: " + readResponse);

        // 5. Test Write Request (W) to store a marker on Node0 and then read it back.
        txnID = generateTransactionID();
        String markerKey = "D:MarkerTest";
        String markerValue = "MultiNodeWorks!";
        String writeRequest = txnID + " W " + markerKey + " " + markerValue;
        System.out.println("Sending Write Request to Node0 (" + myVmIp + ":" + basePort + "): " + writeRequest);
        String writeResponse = sendUDPMessage(writeRequest, basePort);
        System.out.println("Received Write Response from Node0: " + writeResponse);

        txnID = generateTransactionID();
        String readMarker = txnID + " R " + markerKey;
        System.out.println("Sending Read Request for marker to Node0 (" + myVmIp + ":" + basePort + "): " + readMarker);
        String readMarkerResponse = sendUDPMessage(readMarker, basePort);
        System.out.println("Received Read Response for marker from Node0: " + readMarkerResponse);

        // 6. Test Compare-And-Swap (CAS) on Node0 marker.
        // First, attempt CAS with an incorrect current value.
        txnID = generateTransactionID();
        String casRequestWrong = txnID + " C " + markerKey + " WrongValue NewMultiNode";
        System.out.println("Sending CAS Request (wrong current value) to Node0 (" + myVmIp + ":" + basePort + "): " + casRequestWrong);
        String casResponseWrong = sendUDPMessage(casRequestWrong, basePort);
        System.out.println("Received CAS Response (should fail) from Node0: " + casResponseWrong);

        // Then, attempt CAS with the correct current value.
        txnID = generateTransactionID();
        String casRequestCorrect = txnID + " C " + markerKey + " " + markerValue + " NewMultiNode";
        System.out.println("Sending CAS Request (correct current value) to Node0 (" + myVmIp + ":" + basePort + "): " + casRequestCorrect);
        String casResponseCorrect = sendUDPMessage(casRequestCorrect, basePort);
        System.out.println("Received CAS Response (should succeed) from Node0: " + casResponseCorrect);

        // Verify the CAS result.
        txnID = generateTransactionID();
        String readAfterCAS = txnID + " R " + markerKey;
        System.out.println("Sending Read Request after CAS to Node0 (" + myVmIp + ":" + basePort + "): " + readAfterCAS);
        String readAfterCASResponse = sendUDPMessage(readAfterCAS, basePort);
        System.out.println("Received Read Response after CAS from Node0: " + readAfterCASResponse);

        // 7. Test Relay Request (V).
        // Simulate a relay: send a relay request to Node0 to forward a message to Node1.
        txnID = generateTransactionID();
        // Our relay request includes target node "N:Test1" and an inner message "RelayMsgTest"
        String relayRequest = txnID + " V N:Test1 RelayMsgTest";
        System.out.println("Sending Relay Request to Node0 (" + myVmIp + ":" + basePort + "): " + relayRequest);
        String relayResponse = sendUDPMessage(relayRequest, basePort);
        System.out.println("Received Relay-related Response (if any) from Node0: " + relayResponse);

        // 8. Directly test pushRelay and popRelay via Node0's API.
        System.out.println("Directly testing pushRelay/popRelay operations on Node0...");
        nodes[0].pushRelay("N:DirectRelay");
        nodes[0].popRelay();
        System.out.println("pushRelay/popRelay operations completed without error.");

        // 9. Announce each node's address to others by writing their own mapping.
        System.out.println("Announcing node addresses...");
        for (int i = 0; i < nodeCount; i++) {
            String key = (i == 0) ? "N:" + emailAddress : "N:Test" + i;
            String value = myVmIp + ":" + (basePort + i);
            nodes[i].write(key, value);
        }

        System.out.println("Multi-node test complete. Nodes are running and inter-node UDP traffic is active.");
        System.out.println("Press Ctrl+C to exit and capture all packets with Wireshark.");

        // Keep the application running indefinitely so that Wireshark can capture all traffic.
        while (true) {
            Thread.sleep(1000);
        }
    }
}
