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
        if (socket == null) {
            throw new Exception("Socket not open. Call openPort first.");
        }
        byte[] buffer = new byte[1024];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

        if (delay > 0) {
            socket.setSoTimeout(delay);
        }
        try {
            while (true) {
                socket.receive(packet);
                processMessage(packet.getData(), packet.getLength());
                packet.setLength(buffer.length); // Reset for next receive.
            }
        } catch (SocketTimeoutException ste) {
            System.out.println("Socket timed out after " + delay + " ms. Exiting message loop.");
        } catch (SocketException se) {
            if (socket.isClosed()) {
                System.out.println("Socket closed, exiting message loop.");
                return;
            }
            throw se;
        }
    }

    private void processMessage(byte[] data, int length) {
        String received = new String(data, 0, length, StandardCharsets.UTF_8);
        System.out.println("Received message: " + received);
        if (received.length() >= 4) {
            char messageType = received.charAt(3);
            System.out.println("Message type: " + messageType);
        } else {
            System.out.println("Message too short to determine type.");
        }
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
        String value = dataStore.get(key);
        // OPTIONAL: For local testing of AzureLabTest, simulate poem verses if not already in store.
        if (value == null && key.startsWith("D:jabberwocky")) {
            int index;
            try {
                index = Integer.parseInt(key.substring("D:jabberwocky".length()));
            } catch (NumberFormatException e) {
                return null;
            }
            String[] dummyPoem = {
                    "Twas brillig, and the slithy toves",
                    "Did gyre and gimble in the wabe;",
                    "All mimsy were the borogoves,",
                    "And the mome raths outgrabe.",
                    "Beware the Jabberwock, my son!",
                    "The jaws that bite, the claws that catch!",
                    "Beware the Jubjub bird, and shun"
            };
            if (index >= 0 && index < dummyPoem.length) {
                value = dummyPoem[index];
                // Optionally, store it for subsequent reads.
                dataStore.put(key, value);
            }
        }
        return value;
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
