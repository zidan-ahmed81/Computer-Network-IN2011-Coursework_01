import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Stack;
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

    private String nodeName;
    private DatagramSocket socket;
    private List<InetSocketAddress> neighbors;
    private Map<String, String> dataStore = new ConcurrentHashMap<>();
    private Stack<String> relayStack = new Stack<>();

    private String generateTransactionId() {
        Random rand = new Random();
        // Generate two characters that are not spaces.
        char c1 = (char)(33 + rand.nextInt(94)); // ASCII 33 (!) to 126 (~)
        char c2 = (char)(33 + rand.nextInt(94));
        return "" + c1 + c2;
    }

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
        // First, check if we have the key locally.
        if (dataStore.containsKey(key)) {
            return dataStore.get(key);
        }

        // Otherwise, try each neighbor until we get a positive response.
        // You might want to limit the number of neighbors you try.
        for (InetSocketAddress neighbor : neighbors) {
            String value = sendReadRequest(neighbor, key);
            if (value != null) {
                // Optionally, you could store the returned value locally.
                dataStore.put(key, value);
                return value;
            }
        }
        // If no neighbor returned a valid value, return null.
        return null;
    }

    // Sends a read request to a neighbor and waits for its response.
    private String sendReadRequest(InetSocketAddress neighbor, String key) throws Exception {
        // Create a transaction ID.
        String transId = generateTransactionId();
        // Build the read request message as per the CRN protocol.
        // Format: [transId] " R " [key] " "
        String message = transId + " R " + key + " ";
        byte[] sendData = message.getBytes(StandardCharsets.UTF_8);

        // Send the message via UDP.
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, neighbor.getAddress(), neighbor.getPort());
        socket.send(sendPacket);

        // Set a timeout (e.g. 3 seconds) and wait for the response.
        byte[] recvBuffer = new byte[1024];
        DatagramPacket recvPacket = new DatagramPacket(recvBuffer, recvBuffer.length);
        socket.setSoTimeout(3000); // 3 seconds timeout

        try {
            socket.receive(recvPacket);
        } catch (Exception e) {
            // If timeout or other error, return null.
            return null;
        }

        // Convert the received packet to a String.
        String response = new String(recvPacket.getData(), 0, recvPacket.getLength(), StandardCharsets.UTF_8);
        // Response format should be: [transId] " S " [responseChar] " " [responseString]
        // Check that the transaction id matches.
        if (!response.startsWith(transId)) {
            return null;  // Transaction ID mismatch.
        }

        // Split the response on spaces.
        String[] parts = response.split(" ", 4);
        if (parts.length < 3) {
            return null;  // Malformed response.
        }
        // parts[0] is the transaction id, parts[1] should be "S", parts[2] is responseChar,
        // and parts[3] (if present) is the value.
        String responseChar = parts[2];
        if ("Y".equals(responseChar)) {
            // A "Y" indicates the key was found and parts[3] is the stored value.
            if (parts.length >= 4) {
                return parts[3];
            }
        }
        // If responseChar is 'N' (not found) or '?' (not responsible), then we treat it as not found.
        return null;
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
