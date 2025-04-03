import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
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

        // If delay > 0, wait only that many milliseconds
        if (delay > 0) {
            socket.setSoTimeout(delay);
            try {
                while (true) {
                    socket.receive(packet);
                    // Minimal processing: for this implementation we ignore incoming messages.
                    // (In a full implementation, you would parse the packet and respond according to the CRN protocol.)
                    packet.setLength(buffer.length); // Reset packet length for next receive.
                }
            } catch (SocketTimeoutException ste) {
                // Timeout reached; return from the method.
            }
        } else {
            // delay == 0 means wait indefinitely.
            while (true) {
                socket.receive(packet);
                // Minimal processing; simply ignore the contents.
                packet.setLength(buffer.length);
            }
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
