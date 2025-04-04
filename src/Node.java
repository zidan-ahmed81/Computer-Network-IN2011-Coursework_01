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

    public boolean checkNodeIsActive(InetSocketAddress neighbor) throws Exception {
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
            System.err.println("[checkNodeIsActive] Error: " + e.getMessage());
        }
        return false;
    }

    @Override
    public boolean isActive(String nodeName) throws Exception {
        for (InetSocketAddress neighbor : neighbors) {
            String foundName = queryNodeName(neighbor);
            if (foundName != null && foundName.equals(nodeName)) {
                return true;
            }
        }
        return false;
    }

    private String queryNodeName(InetSocketAddress neighbor) throws Exception {
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
                return responseMsg.substring(5).trim();
            }
        } catch (Exception ignored) {}
        return null;
    }

    private byte[] generateTransactionID() {
        Random rand = new Random();
        byte[] txid = new byte[2];
        do {
            rand.nextBytes(txid);
        } while (txid[0] == 0x20 || txid[1] == 0x20);
        return txid;
    }

    private String formatCRNString(String input) {
        int spaceCount = (int) input.chars().filter(ch -> ch == ' ').count();
        return spaceCount + " " + input + " ";
    }

    public void checkBootstrappedNodesActive() throws Exception {
        System.out.println("=== Checking active status of AzureLab nodes ===");
        for (InetSocketAddress neighbor : neighbors) {
            boolean isUp = checkNodeIsActive(neighbor);
            System.out.println("Checking " + neighbor.getAddress().getHostAddress() + ":" + neighbor.getPort() +
                    " -> isActive = " + isUp);
        }
    }

    @Override
    public void pushRelay(String nodeName) throws Exception {
        if (nodeName == null || !nodeName.startsWith("N:")) {
            throw new Exception("Relay node name must start with 'N:'");
        }
        relayStack.push(nodeName);
        System.out.println("[DEBUG] Pushed to relay stack: " + nodeName);
    }

    @Override
    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) {
            String removed = relayStack.pop();
            System.out.println("[DEBUG] Popped from relay stack: " + removed);
        } else {
            System.out.println("[DEBUG] Relay stack is already empty.");
        }
    }

    @Override
    public boolean exists(String key) throws Exception {
        // Step 1: Check local store
        if (localStore.containsKey(key) || dataStore.containsKey(key)) {
            System.out.println("[exists] Found in local store");
            return true;
        }

        // Step 2: Network check via E → V
        for (InetSocketAddress neighbor : neighbors) {
            byte[] txid = generateTransactionID();
            String header = new String(txid, StandardCharsets.UTF_8) + " ";
            String message = header + "E " + formatCRNString(key);
            byte[] buffer = message.getBytes(StandardCharsets.UTF_8);

            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, neighbor.getAddress(), neighbor.getPort());
            socket.send(packet);

            try {
                byte[] recvBuf = new byte[1024];
                DatagramPacket response = new DatagramPacket(recvBuf, recvBuf.length);
                socket.setSoTimeout(1000);
                socket.receive(response);

                String responseMsg = new String(response.getData(), 0, response.getLength(), StandardCharsets.UTF_8);
                if (responseMsg.startsWith(new String(txid, StandardCharsets.UTF_8) + " V Y")) {
                    System.out.println("[exists] Found via E → V from " + neighbor);
                    return true;
                }
            } catch (SocketTimeoutException ignored) {}
        }

        // Step 3: Optional fallback to read
        String value = read(key);
        if (value != null) {
            System.out.println("[exists] Found using read() fallback");
            return true;
        }

        // Not found
        System.out.println("[exists] Key not found");
        return false;
    }


    @Override
    public String read(String key) throws Exception {
        if (localStore.containsKey(key)) {
            return localStore.get(key);
        }
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
            } catch (SocketTimeoutException ignored) {}
        }
        return null;
    }

    @Override
    public boolean write(String key, String value) throws Exception {
        localStore.put(key, value);
        dataStore.put(key, value);
        boolean acknowledged = false;

        for (InetSocketAddress neighbor : neighbors) {
            byte[] txid = generateTransactionID();
            String header = new String(txid, StandardCharsets.UTF_8) + " ";
            String message = header + "W " + formatCRNString(key) + formatCRNString(value);
            byte[] buffer = message.getBytes(StandardCharsets.UTF_8);

            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, neighbor.getAddress(), neighbor.getPort());
            socket.send(packet);

            try {
                byte[] recvBuf = new byte[1024];
                DatagramPacket response = new DatagramPacket(recvBuf, recvBuf.length);
                socket.setSoTimeout(1000);
                socket.receive(response);

                String responseMsg = new String(response.getData(), 0, response.getLength(), StandardCharsets.UTF_8);
                if (responseMsg.startsWith(new String(txid, StandardCharsets.UTF_8) + " X Y")) {
                    acknowledged = true;
                }
            } catch (SocketTimeoutException ignored) {}
        }

        return true;
    }


    @Override
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        // 1. Store locally as a backup (optional)
        if (localStore.containsKey(key) && localStore.get(key).equals(currentValue)) {
            localStore.put(key, newValue);
            dataStore.put(key, newValue);
        }

        // 2. Send to neighbors via CRN protocol
        for (InetSocketAddress neighbor : neighbors) {
            byte[] txid = generateTransactionID();
            String header = new String(txid, StandardCharsets.UTF_8) + " ";
            String message = header + "C " + formatCRNString(key) + formatCRNString(currentValue) + formatCRNString(newValue);
            byte[] buffer = message.getBytes(StandardCharsets.UTF_8);

            DatagramPacket packet = new DatagramPacket(buffer, buffer.length, neighbor.getAddress(), neighbor.getPort());
            socket.send(packet);

            try {
                byte[] recvBuf = new byte[1024];
                DatagramPacket response = new DatagramPacket(recvBuf, recvBuf.length);
                socket.setSoTimeout(1000);
                socket.receive(response);

                String responseMsg = new String(response.getData(), 0, response.getLength(), StandardCharsets.UTF_8);
                if (responseMsg.startsWith(new String(txid, StandardCharsets.UTF_8) + " D Y")) {
                    System.out.println("[CAS] Success from neighbor " + neighbor);
                    return true;
                }
            } catch (SocketTimeoutException ignored) {}
        }

        System.out.println("[CAS] Failed on all neighbors");
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