import java.net.*;
import java.nio.charset.StandardCharsets;



public class ExtraCRNTest {
    public static void main(String[] args) {
        try {
            // Create and configure a Node instance
            Node node = new Node();
            node.setNodeName("N:ExtraTest");
            int testPort = 20300;
            node.openPort(testPort);

            // Start the node's UDP handler on a separate thread
            Thread nodeThread = new Thread(() -> {
                try {
                    node.handleIncomingMessages(0);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            nodeThread.start();

            // Give the node a moment to start listening
            Thread.sleep(500);

            // Create a UDP client socket for sending messages
            DatagramSocket clientSocket = new DatagramSocket();
            InetAddress localhost = InetAddress.getLocalHost();
            byte[] buffer = new byte[2048];

            // ----------------------------
            // Test 1: Name Request (G message)
            // ----------------------------
            String txName = "TX";
            String nameRequest = txName + " G";
            sendUDP(clientSocket, nameRequest, localhost, testPort);
            String responseName = receiveUDP(clientSocket, buffer);
            System.out.println("Name Request Response: " + responseName);
            // Expected: "TX H N:ExtraTest" (or similar)

            // ----------------------------
            // Test 2: Nearest Request (N message)
            // ----------------------------
            // For nearest, we need a hashID payload. Compute the hash for a test data key.
            String testKey = "D:NearestTest";
            byte[] hash = HashID.computeHashID(testKey);
            StringBuilder hex = new StringBuilder();
            for (byte b : hash) {
                hex.append(String.format("%02x", b));
            }
            String txNearest = "TN";
            String nearestRequest = txNearest + " N " + hex.toString();
            sendUDP(clientSocket, nearestRequest, localhost, testPort);
            String responseNearest = receiveUDP(clientSocket, buffer);
            System.out.println("Nearest Request Response: " + responseNearest);
            // Expected: "TN O ..." with one to three address key/value pairs

            // ----------------------------
            // Test 3: Write Request (W message) then Key Existence (E message)
            // ----------------------------
            String keyExist = "D:ExistTest";
            String valueExist = "ExistValue";
            String txWrite = "TW";
            String writeRequest = txWrite + " W " + keyExist + " " + valueExist;
            sendUDP(clientSocket, writeRequest, localhost, testPort);
            String responseWrite = receiveUDP(clientSocket, buffer);
            System.out.println("Write Request Response: " + responseWrite);
            // Expected: Response with code 'A' (for new write) or 'R' (for replacement)

            // Now test key existence:
            String txExist = "TE";
            String existRequest = txExist + " E " + keyExist;
            sendUDP(clientSocket, existRequest, localhost, testPort);
            String responseExist = receiveUDP(clientSocket, buffer);
            System.out.println("Key Existence Response: " + responseExist);
            // Expected: "TE F Y" if the key exists, or "TE F N" or "TE F ?" based on conditions

            // ----------------------------
            // Test 4: Read Request (R message)
            // ----------------------------
            String txRead = "TR";
            String readRequest = txRead + " R " + keyExist;
            sendUDP(clientSocket, readRequest, localhost, testPort);
            String responseRead = receiveUDP(clientSocket, buffer);
            System.out.println("Read Request Response: " + responseRead);
            // Expected: "TR S Y ExistValue" if the key exists

            // ----------------------------
            // Test 5: Compare And Swap Request (C message)
            // ----------------------------
            String txCAS = "TC";
            String casRequest = txCAS + " C " + keyExist + " " + valueExist + " " + "NewExistValue";
            sendUDP(clientSocket, casRequest, localhost, testPort);
            String responseCAS = receiveUDP(clientSocket, buffer);
            System.out.println("CAS Request Response: " + responseCAS);
            // Expected: "TC D R" if the CAS succeeded, or an appropriate failure code

            // ----------------------------
            // Test 6: Relay Message (V message)
            // ----------------------------
            // For a relay, the format is: transactionID, "V", a node name, and an inner message.
            // Here we simulate relaying a name request. We relay to our own node for testing.
            String txRelay = "TRV";
            String targetNode = "N:ExtraTest"; // relay to self for this test
            String innerMessage = "RR G"; // inner name request with transaction ID "RR"
            String relayMessage = txRelay + " V " + targetNode + " " + innerMessage;
            sendUDP(clientSocket, relayMessage, localhost, testPort);
            String responseRelay = receiveUDP(clientSocket, buffer);
            System.out.println("Relay Message Response: " + responseRelay);
            // Expected: A response with transaction ID "TRV" that carries the inner message's reply

            clientSocket.close();
            System.out.println("ExtraCRNTest completed successfully.");

        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    // Helper method to send a UDP message
    private static void sendUDP(DatagramSocket socket, String message, InetAddress address, int port) throws Exception {
        byte[] data = message.getBytes(StandardCharsets.UTF_8);
        DatagramPacket packet = new DatagramPacket(data, data.length, address, port);
        socket.send(packet);
        System.out.println("Sent: " + message);
    }

    // Helper method to receive a UDP message with a timeout
    private static String receiveUDP(DatagramSocket socket, byte[] buffer) throws Exception {
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        socket.setSoTimeout(2000); // 2-second timeout
        socket.receive(packet);
        return new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
    }
}
