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

    private String nodeName;
    private DatagramSocket socket;
    private Map<String, String> localStore;
    private Map<String, String> dataStore;
    public List<InetSocketAddress> neighbors;
    private ArrayDeque<String> relayStack;

    public Node() {
        localStore = new ConcurrentHashMap<>();
        dataStore = new ConcurrentHashMap<>();
        neighbors = new ArrayList<>();
        relayStack = new ArrayDeque<>();
        System.out.println("[DEBUG] Node constructor: Collections initialized.");
    }

    @Override
    public void setNodeName(String nodeName) throws Exception {
        if (nodeName == null || !nodeName.startsWith("N:")) {
            throw new Exception("Node name must start with 'N:'");
        }
        this.nodeName = nodeName;
        System.out.println("Node name set to: " + nodeName);
    }

    @Override
    public void openPort(int portNumber) throws Exception {
        socket = new DatagramSocket(portNumber);
        socket.setSoTimeout(1000);
        System.out.println("Opened port: " + portNumber);
    }

    @Override
    public void handleIncomingMessages(int delay) throws Exception {
        System.out.println("Listening for incoming messages...");
        long endTime = System.currentTimeMillis() + delay;
        while (delay == 0 || System.currentTimeMillis() < endTime) {
            try {
                byte[] buffer = new byte[1024];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                processPacket(packet);
            } catch (SocketTimeoutException e) {
                // Timeout - loop again
            }
        }
        System.out.println("Timeout reached, exiting handleIncomingMessages()");
    }

    private void processPacket(DatagramPacket packet) throws Exception {
        String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
        System.out.println("Received packet from " + packet.getAddress() + ":" + packet.getPort() + " -> " + message);
    }

    public boolean isActive(InetSocketAddress neighbor) throws Exception {
        try {
            byte[] txid = generateTransactionID();
            String header = new String(txid, StandardCharsets.UTF_8) + " ";
            String message = header + "G";
            byte[] buffer = message.getBytes(StandardCharsets.UTF_8);

            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, neighbor.getAddress(), neighbor.getPort());
            socket.send(packet);

            byte[] recvBuf = new byte[1024];
            DatagramPacket response = new DatagramPacket(recvBuf, recvBuf.length);
            socket.setSoTimeout(1000);
            socket.receive(response);

            String responseMsg = new String(response.getData(), 0, response.getLength(), StandardCharsets.UTF_8);
            if (responseMsg.startsWith(new String(txid, StandardCharsets.UTF_8) + " H ")) {
                String returnedName = responseMsg.substring(5).trim();
                System.out.println("Node at " + neighbor + " is active → " + returnedName);
                return true;
            }
        } catch (SocketTimeoutException e) {
            // No response
        } catch (Exception e) {
            System.err.println("[isActive] Error: " + e.getMessage());
        }
        return false;
    }

    @Override
    public boolean isActive(String nodeName) throws Exception {
        System.err.println("[WARN] isActive(String) is not used in dynamic name discovery.");
        return false;
    }

    private byte[] generateTransactionID() {
        Random rand = new Random();
        byte[] txid = new byte[2];
        do {
            rand.nextBytes(txid);
        } while (txid[0] == 0x20 || txid[1] == 0x20);
        return txid;
    }

    public void checkBootstrappedNodesActive() throws Exception {
        System.out.println("=== Checking active status of AzureLab nodes ===");
        for (InetSocketAddress neighbor : neighbors) {
            boolean isUp = isActive(neighbor);
            System.out.println("Checking " + neighbor.getAddress().getHostAddress() + ":" + neighbor.getPort() +
                    " -> isActive = " + isUp);
        }
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
        return localStore.containsKey(key) || dataStore.containsKey(key);
    }

    @Override
    public String read(String key) throws Exception {
        // 1. Check local store first
        if (localStore.containsKey(key)) {
            return key + " = " + localStore.get(key);
        }

        // 2. Ask all neighbors (brute-force version)
        for (InetSocketAddress neighbor : neighbors) {
            byte[] txid = generateTransactionID();
            String header = new String(txid, StandardCharsets.UTF_8) + " ";
            String message = header + "R " + formatCRNString(key);
            byte[] buffer = message.getBytes(StandardCharsets.UTF_8);

            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, neighbor.getAddress(), neighbor.getPort());
            socket.send(packet);

            try {
                byte[] recvBuf = new byte[1024];
                DatagramPacket response = new DatagramPacket(recvBuf, recvBuf.length);
                socket.setSoTimeout(1000);
                socket.receive(response);

                String responseMsg = new String(response.getData(), 0, response.getLength(), StandardCharsets.UTF_8);
                if (responseMsg.startsWith(new String(txid, StandardCharsets.UTF_8) + " S ")) {
                    String[] parts = responseMsg.substring(5).split(" ", 2);
                    if (parts[0].equals("Y")) {
                        return key + " = " + parts[1].trim();
                    }
                }
            } catch (SocketTimeoutException ignored) {
            }
        }

        // If nothing found
        return null;
    }

    private String formatCRNString(String input) {
        int spaceCount = (int) input.chars().filter(ch -> ch == ' ').count();
        return spaceCount + " " + input + " ";
    }


    @Override
    public boolean write(String key, String value) throws Exception {
        localStore.put(key, value);
        dataStore.put(key, value);
        return true;
    }

    @Override
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        if (localStore.containsKey(key) && localStore.get(key).equals(currentValue)) {
            localStore.put(key, newValue);
            dataStore.put(key, newValue);
            return true;
        }
        return false;
    }

    public void bootstrap() throws Exception {
        neighbors.clear();
        String[] bootstrapIPs = {"10.200.51.18", "10.200.51.19"};
        for (String ip : bootstrapIPs) {
            InetAddress addr = InetAddress.getByName(ip);
            for (int port = 20110; port <= 20116; port++) {
                if (socket != null && socket.getLocalPort() == port) continue;
                neighbors.add(new InetSocketAddress(addr, port));
            }
        }
        System.out.println("[DEBUG] Bootstrap complete. Neighbors:");
        for (InetSocketAddress n : neighbors) {
            System.out.println("[DEBUG]   " + n.getAddress().getHostAddress() + ":" + n.getPort());
        }
    }

    public void shutdown() {
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
    }
}
