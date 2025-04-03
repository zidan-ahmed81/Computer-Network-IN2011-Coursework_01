import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

interface NodeInterface {
    public void setNodeName(String nodeName) throws Exception;
    public void openPort(int portNumber) throws Exception;
    public void handleIncomingMessages(int delay) throws Exception;
    public boolean isActive(String nodeName) throws Exception;
    public void pushRelay(String nodeName) throws Exception;
    public void popRelay() throws Exception;
    public boolean exists(String key) throws Exception;
    public String read(String key) throws Exception;
    public boolean write(String key, String value) throws Exception;
    public boolean CAS(String key, String currentValue, String newValue) throws Exception;
}

public class Node implements NodeInterface {

    // The node's own name.
    private String nodeName;

    // The UDP socket used for sending/receiving messages.
    private DatagramSocket socket;

    // A simple local store for key/value pairs.
    private Map<String, String> localStore = new ConcurrentHashMap<>();

    // A list representing your known nodes (routing table).
// In a real implementation, you would choose the closest nodes based on hash distance.
    private List<InetSocketAddress> knownNodes = Collections.synchronizedList(new ArrayList<>());
    private ArrayDeque<String> relayStack;
    private HashMap<Object, Object> dataStore;


    @Override
    public void setNodeName(String nodeName) throws Exception {
        this.nodeName = nodeName;
        System.out.println("Node name set to: " + nodeName);
    }

    @Override
    public void openPort(int portNumber) throws Exception {
        socket = new DatagramSocket(portNumber);
        socket.setSoTimeout(1000); // Check every second when using a delay
        System.out.println("Opened port: " + portNumber);
    }

    @Override
    public void handleIncomingMessages(int delay) throws Exception {
        System.out.println("Listening for incoming messages...");
        long endTime = System.currentTimeMillis() + delay;
        while (System.currentTimeMillis() < endTime) {
            try {
                byte[] buffer = new byte[1024];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                processPacket(packet);
            } catch (SocketTimeoutException e) {
                // Continue waiting until endTime is reached.
            }
        }
        System.out.println("Timeout reached, exiting handleIncomingMessages()");
    }

    // Helper method to process incoming packets
    private void processPacket(DatagramPacket packet) {
        String message = new String(packet.getData(), 0, packet.getLength());
        System.out.println("Received packet from "
                + packet.getAddress() + ":" + packet.getPort()
                + " -> " + message);
    }

    @Override
    public boolean isActive(String nodeName) throws Exception {
        // Minimal implementation always returns true.
        return true;
    }

    @Override
    public void pushRelay(String nodeName) throws Exception {
        relayStack.push(nodeName);
    }

    @Override
    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) {
            relayStack.pop();
        }
    }

    @Override
    public boolean exists(String key) throws Exception {
        return dataStore.containsKey(key);
    }

    @Override
    public String read(String key) throws Exception {
        // 1. First, check if the key exists locally.
        if (localStore.containsKey(key)) {
            return localStore.get(key);
        }

        // 2. Generate a transaction ID for this request.
        // A simple example: generate two random letters (ensuring they're not spaces).
        String txID = generateTxID();

        // 3. Construct the read request message.
        // According to the CRN protocol, a read request is: "<txID> R <key>"
        String requestMessage = txID + " R " + key;
        byte[] requestBytes = requestMessage.getBytes(StandardCharsets.UTF_8);

        // 4. Determine the nearest nodes to the key.
        // In a real implementation, you would use your routing table to get the three closest nodes.
        // Here we simply return all known nodes.
        List<InetSocketAddress> targets = getNearestNodes(key);

        // 5. Send the read request to each target node.
        for (InetSocketAddress target : targets) {
            DatagramPacket packet = new DatagramPacket(requestBytes, requestBytes.length,
                    target.getAddress(), target.getPort());
            socket.send(packet);
        }

        // 6. Wait for a response with a timeout.
        byte[] buffer = new byte[1024];
        DatagramPacket responsePacket = new DatagramPacket(buffer, buffer.length);
        long startTime = System.currentTimeMillis();
        // We wait up to 5 seconds for a response (you might want to resend if none is received).
        while (System.currentTimeMillis() - startTime < 5000) {
            try {
                socket.setSoTimeout(5000);
                socket.receive(responsePacket);

                // 7. Process the response message.
                String response = new String(responsePacket.getData(), 0, responsePacket.getLength(),
                        StandardCharsets.UTF_8);
                // Verify that the transaction ID matches.
                if (response.startsWith(txID)) {
                    // The expected response format is: "<txID> S <responseChar> <value>"
                    String[] parts = response.split(" ", 4);
                    if (parts.length >= 3) {
                        String responseChar = parts[2];
                        if ("Y".equals(responseChar) && parts.length == 4) {
                            // Condition A true: the remote node has the key.
                            return parts[3];
                        } else {
                            // Either the key wasn’t found, or the response isn’t what we expected.
                            // You could try additional nodes or return null.
                            return null;
                        }
                    }
                }
            } catch (SocketTimeoutException e) {
                // Timeout reached: break out of the loop.
                break;
            }
        }
        // If no valid response is received, return null.
        return null;
    }

    // Helper method to generate a simple transaction ID.
    private String generateTxID() {
        Random random = new Random();
        char c1 = (char) ('A' + random.nextInt(26));
        char c2 = (char) ('A' + random.nextInt(26));
        return "" + c1 + c2;
    }

    // Helper method to get the nearest nodes to the key.
// This is a simplified version. In practice, use your routing table and the hash distance metric.
    private List<InetSocketAddress> getNearestNodes(String key) {
        return knownNodes;
    }


    @Override
    public boolean write(String key, String value) throws Exception {
        // For this minimal version, simply put the key/value pair in our store.
        dataStore.put(key, value);
        return true;
    }

    @Override
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        // Use the atomic replace method from ConcurrentHashMap.
        return dataStore.replace(key, currentValue, newValue);
    }

    // Additional method for graceful shutdown
    public void shutdown() {
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }
}
