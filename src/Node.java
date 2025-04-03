import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
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
    private int port;
    private DatagramSocket socket;
    private final int MAX_PACKET_SIZE = 512; // A safe upper bound for UDP message size
    // Use a thread-safe map for storing key/value pairs.
    private Map<String, String> dataStore = new ConcurrentHashMap<>();
    // Stack for relay nodes.
    private Stack<String> relayStack = new Stack<>();

    @Override
    public void setNodeName(String nodeName) throws Exception {
        this.nodeName = nodeName;
        // If the socket is already open, add our own address key/value pair.
        if (socket != null) {
            String ip = InetAddress.getLocalHost().getHostAddress();
            dataStore.put(nodeName, ip + ":" + port);
        }
    }

    @Override
    public void openPort(int portNumber) throws Exception {
        this.port = portNumber;
        socket = new DatagramSocket(portNumber);
    }

    @Override
    public void handleIncomingMessages(int delay) throws Exception {
        long endTime = System.currentTimeMillis() + delay;

        while (delay == 0 || System.currentTimeMillis() < endTime) {
            try {
                socket.setSoTimeout(Math.max(1, delay == 0 ? 0 : (int)(endTime - System.currentTimeMillis())));
                byte[] buffer = new byte[MAX_PACKET_SIZE];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);

                String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                processMessage(message, packet.getAddress(), packet.getPort());
            } catch (java.net.SocketTimeoutException e) {
                // Timeout reached, no incoming packets
                return;
            }
        }
    }


    private void processMessage(String message, InetAddress senderAddress, int senderPort) throws Exception {
        String[] parts = message.split(" ", 3); // Expecting at least: ID1 ID2 TYPE...
        if (parts.length < 3) return;

        String transactionID = parts[0] + parts[1];
        char type = parts[2].charAt(0);

        switch (type) {
            case 'G': // Name Request
                sendNameResponse(transactionID, senderAddress, senderPort);
                break;
            // You'll add other types like 'E', 'R', 'W', etc. here
            default:
                // Optionally print or log unrecognized types
                break;
        }
    }

    private void sendNameResponse(String transactionID, InetAddress address, int port) throws Exception {
        String response = transactionID + " H " + nodeName + " ";
        byte[] data = response.getBytes(StandardCharsets.UTF_8);
        DatagramPacket packet = new DatagramPacket(data, data.length, address, port);
        socket.send(packet);
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
        return dataStore.get(key);
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
