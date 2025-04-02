import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class AzureLabTest {

    // Helper method: generate a random two-character transaction ID (avoid spaces)
    private static String generateTransactionID() {
        Random r = new Random();
        char c1 = (char)(33 + r.nextInt(94)); // ASCII 33 to 126
        char c2 = (char)(33 + r.nextInt(94));
        return "" + c1 + c2;
    }

    // Helper method: send a UDP message and wait for a response
    // (Using explicit IP address to ensure it reaches our local node)
    private static String sendUDPMessage(String message, String destIP, int destPort) throws Exception {
        DatagramSocket clientSocket = new DatagramSocket();
        // Set timeout (e.g., 10 seconds)
        clientSocket.setSoTimeout(30000);
        InetAddress IPAddress = InetAddress.getByName(destIP);
        byte[] sendData = message.getBytes(StandardCharsets.UTF_8);
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, IPAddress, destPort);
        clientSocket.send(sendPacket);

        byte[] receiveData = new byte[4096];
        DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
        try {
            clientSocket.receive(receivePacket);
        } catch (SocketTimeoutException e) {
            System.out.println("No response received within timeout period.");
            clientSocket.close();
            return "";  // Or return a default value, or you could retry here
        }

        String response = new String(receivePacket.getData(), 0, receivePacket.getLength(), StandardCharsets.UTF_8);
        clientSocket.close();
        return response;
    }

    public static void main(String[] args) throws Exception {
        // === Configuration ===
        // Your VM's IP address – change this to your actual VM IP (from ip a)
        String myVmIp = "10.216.35.23";
        // Your email address – this will be used as the node name for your primary node.
        String emailAddress = "zidan.ahmed.2@city.ac.uk";
        // CRN bootstrap node on the Azure lab – typically one of these:
        String azureBootstrapIP = "10.200.51.19";
        int azureBootstrapPort = 20114;

        // === Create multiple local Node instances ===
        // We'll create three nodes on different ports.
        int nodeCount = 3;
        int basePort = 20110;
        Node[] nodes = new Node[nodeCount];

        // Node0 will be our primary node, named using your email.
        nodes[0] = new Node();
        nodes[0].setNodeName("N:" + emailAddress);
        nodes[0].openPort(basePort);
        // Advertise its own address (using your VM IP)
        nodes[0].write("N:" + emailAddress, myVmIp + ":" + basePort);

        // Create additional test nodes.
        for (int i = 1; i < nodeCount; i++) {
            nodes[i] = new Node();
            nodes[i].setNodeName("N:Test" + i);
            nodes[i].openPort(basePort + i);
            nodes[i].write("N:Test" + i, myVmIp + ":" + (basePort + i));
        }

        // === Bootstrap: Let nodes know about each other ===
        // Let Node0 know about Node1 and Node2.
        nodes[0].write("N:Test1", myVmIp + ":" + (basePort + 1));
        nodes[0].write("N:Test2", myVmIp + ":" + (basePort + 2));
        // Let Node1 and Node2 learn about Node0.
        nodes[1].write("N:" + emailAddress, myVmIp + ":" + basePort);
        nodes[2].write("N:" + emailAddress, myVmIp + ":" + basePort);
        // Also bootstrap Node0 with an Azure lab node.
        nodes[0].write("N:AzureBootstrap", azureBootstrapIP + ":" + azureBootstrapPort);

        System.out.println("Local nodes started and bootstrapped on ports " + basePort + " to " + (basePort + nodeCount - 1));
        Thread.sleep(3000); // Allow nodes to settle

        // === Active UDP Messaging Tests ===

        // 1. Test Name Request: Send a UDP Name Request ("G") to Node1.
        String txnID = generateTransactionID();
        String nameRequest = txnID + " G ";
        System.out.println("Sending Name Request to Node1 (" + myVmIp + ":" + (basePort+1) + "): " + nameRequest);
        String nameResponse = sendUDPMessage(nameRequest, myVmIp, basePort + 1);
        System.out.println("Received Name Response from Node1: " + nameResponse);

        // 2. Test Nearest Request: Send a UDP Nearest Request ("N") to Node2 with a dummy hash.
        txnID = generateTransactionID();
        String nearestRequest = txnID + " N dummyhash";
        System.out.println("Sending Nearest Request to Node2 (" + myVmIp + ":" + (basePort+2) + "): " + nearestRequest);
        String nearestResponse = sendUDPMessage(nearestRequest, myVmIp, basePort + 2);
        System.out.println("Received Nearest Response from Node2: " + nearestResponse);

        // 3. Test Key Existence: Send UDP Key Existence Request ("E") to Node0 for a built-in jabberwocky key.
        txnID = generateTransactionID();
        String existenceRequest = txnID + " E D:jabberwocky0";
        System.out.println("Sending Key Existence Request to Node0 (" + myVmIp + ":" + basePort + "): " + existenceRequest);
        String existenceResponse = sendUDPMessage(existenceRequest, myVmIp, basePort);
        System.out.println("Received Existence Response from Node0: " + existenceResponse);

        // 4. Test Read Request: Send UDP Read Request ("R") for jabberwocky verse from Node0.
        txnID = generateTransactionID();
        String readRequest = txnID + " R D:jabberwocky0";
        System.out.println("Sending Read Request to Node0 (" + myVmIp + ":" + basePort + "): " + readRequest);
        String readResponse = sendUDPMessage(readRequest, myVmIp, basePort);
        System.out.println("Received Read Response from Node0: " + readResponse);

        // 5. Test Write Request: Send UDP Write Request ("W") to store a marker on Node0.
        txnID = generateTransactionID();
        String markerKey = "D:MarkerTest";
        String markerValue = "MultiNodeWorks!";
        String writeRequest = txnID + " W " + markerKey + " " + markerValue;
        System.out.println("Sending Write Request to Node0 (" + myVmIp + ":" + basePort + "): " + writeRequest);
        String writeResponse = sendUDPMessage(writeRequest, myVmIp, basePort);
        System.out.println("Received Write Response from Node0: " + writeResponse);

        // Verify by sending a Read Request for the marker.
        txnID = generateTransactionID();
        String readMarker = txnID + " R " + markerKey;
        System.out.println("Sending Read Request for marker to Node0 (" + myVmIp + ":" + basePort + "): " + readMarker);
        String readMarkerResponse = sendUDPMessage(readMarker, myVmIp, basePort);
        System.out.println("Received Read Response for marker from Node0: " + readMarkerResponse);

        // 6. Test Compare-And-Swap (CAS) Request:
        // First, attempt CAS with an incorrect current value.
        txnID = generateTransactionID();
        String casRequestWrong = txnID + " C " + markerKey + " WrongValue NewMultiNode";
        System.out.println("Sending CAS Request (wrong current value) to Node0 (" + myVmIp + ":" + basePort + "): " + casRequestWrong);
        String casResponseWrong = sendUDPMessage(casRequestWrong, myVmIp, basePort);
        System.out.println("Received CAS Response (should fail) from Node0: " + casResponseWrong);

        // Then, attempt CAS with the correct current value.
        txnID = generateTransactionID();
        String casRequestCorrect = txnID + " C " + markerKey + " " + markerValue + " NewMultiNode";
        System.out.println("Sending CAS Request (correct current value) to Node0 (" + myVmIp + ":" + basePort + "): " + casRequestCorrect);
        String casResponseCorrect = sendUDPMessage(casRequestCorrect, myVmIp, basePort);
        System.out.println("Received CAS Response (should succeed) from Node0: " + casResponseCorrect);

        // Verify CAS result by reading back the marker.
        txnID = generateTransactionID();
        String readAfterCAS = txnID + " R " + markerKey;
        System.out.println("Sending Read Request after CAS to Node0 (" + myVmIp + ":" + basePort + "): " + readAfterCAS);
        String readAfterCASResponse = sendUDPMessage(readAfterCAS, myVmIp, basePort);
        System.out.println("Received Read Response after CAS from Node0: " + readAfterCASResponse);

        // 7. Test Relay Request: Simulate relaying by sending a relay message to Node0
        // to forward an inner message to Node1.
        txnID = generateTransactionID();
        // Our relay request: target "N:Test1" and inner message "RelayMsgTest"
        String relayRequest = txnID + " V N:Test1 RelayMsgTest";
        System.out.println("Sending Relay Request to Node0 (" + myVmIp + ":" + basePort + "): " + relayRequest);
        String relayResponse = sendUDPMessage(relayRequest, myVmIp, basePort);
        System.out.println("Received Relay Response (if any) from Node0: " + relayResponse);

        // 8. Direct API test: pushRelay and popRelay on Node0.
        System.out.println("Directly testing pushRelay/popRelay on Node0...");
        nodes[0].pushRelay("N:DirectRelay");
        nodes[0].popRelay();

        // 9. Announce each node's address to others (simulate inter-node mapping update).
        System.out.println("Announcing node addresses...");
        for (int i = 0; i < nodeCount; i++) {
            String key = (i == 0) ? "N:" + emailAddress : "N:Test" + i;
            String value = myVmIp + ":" + (basePort + i);
            nodes[i].write(key, value);
        }

        System.out.println("Multi-node test complete. Local nodes are running and inter-node UDP traffic is active.");
        System.out.println("Leave this running and capture all packets with Wireshark.");

        // Keep the application running indefinitely so that Wireshark can capture ongoing traffic.
        while (true) {
            Thread.sleep(1000);
        }
    }
}
