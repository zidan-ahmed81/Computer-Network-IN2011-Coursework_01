import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;

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
    // Local key/value store for data keys (keys starting with "D:")
    private ConcurrentMap<String, String> dataStore = new ConcurrentHashMap<>();
    // A stack to maintain relay nodes (if non-empty, all outgoing requests will be relayed)
    private Deque<String> relayStack = new ConcurrentLinkedDeque<>();
    // Store for address key/value pairs (keys starting with "N:")
    private ConcurrentMap<String, String> addressStore = new ConcurrentHashMap<>();
    // Mapping to record which node wrote a given key.
    private ConcurrentMap<String, String> writerMapping = new ConcurrentHashMap<>();

    private String nodeName;
    private byte[] nodeHash;
    DatagramSocket socket;

    @Override
    public void setNodeName(String nodeName) throws Exception {
        this.nodeName = nodeName;
        this.nodeHash = HashID.computeHashID(nodeName);
        System.out.println("Node name set to " + nodeName);
    }

    @Override
    public void openPort(int portNumber) throws Exception {
        socket = new DatagramSocket(portNumber);
        // Find a non-loopback IPv4 address.
        InetAddress selectedAddress = null;
        for (Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces(); nets.hasMoreElements();) {
            NetworkInterface netint = nets.nextElement();
            if (!netint.isUp() || netint.isLoopback()) continue;
            for (Enumeration<InetAddress> addrs = netint.getInetAddresses(); addrs.hasMoreElements();) {
                InetAddress addr = addrs.nextElement();
                if (!addr.isLoopbackAddress() && addr instanceof Inet4Address) {
                    selectedAddress = addr;
                    break;
                }
            }
            if (selectedAddress != null) break;
        }
        if (selectedAddress == null) {
            throw new Exception("No valid non-loopback network interface found.");
        }
        String localAddress = selectedAddress.getHostAddress() + ":" + portNumber;
        if (nodeName != null) {
            addressStore.put(nodeName, localAddress);
        }
        System.out.println("Opened UDP port " + portNumber + " at " + localAddress);
    }

    @Override
    public void handleIncomingMessages(int delay) throws Exception {
        byte[] buffer = new byte[1024];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        socket.setSoTimeout(delay > 0 ? delay : 0);
        try {
            while (true) {
                socket.receive(packet);
                String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                System.out.println("Received: " + message);
                processMessage(message, packet.getAddress(), packet.getPort());
            }
        } catch (SocketTimeoutException ste) {
            // Timeout reached – exit the method.
        }
    }

    private void processMessage(String message, InetAddress senderAddress, int senderPort) {
        String[] parts = message.split(" ", 3);
        if (parts.length < 2) {
            System.out.println("Invalid message format");
            return;
        }
        String transactionId = parts[0];
        String command = parts[1];
        String payload = (parts.length >= 3) ? parts[2] : "";

        switch (command) {
            case "G":
                sendNameResponse(transactionId, senderAddress, senderPort);
                break;
            case "H":
                String remoteNodeName = payload.trim();
                String remoteAddress = senderAddress.getHostAddress() + ":" + senderPort;
                if (!addressStore.containsKey(remoteNodeName)) {
                    addressStore.put(remoteNodeName, remoteAddress);
                    System.out.println("Passive mapping: Discovered node " + remoteNodeName + " at " + remoteAddress);
                }
                break;
            case "R":
                sendReadResponse(transactionId, payload, senderAddress, senderPort);
                break;
            case "W":
                String[] kv = payload.split(" ", 2);
                if (kv.length == 2) {
                    boolean writeResult = false;
                    try {
                        writeResult = write(kv[0], kv[1]);
                    } catch (Exception e) {
                        System.err.println("Error in write: " + e.getMessage());
                    }
                    sendWriteResponse(transactionId, writeResult, senderAddress, senderPort);
                }
                break;
            case "O": {
                System.out.println("Received Nearest response: " + payload);
                String[] tokens = payload.split(" ");
                for (int i = 0; i < tokens.length;) {
                    if (i + 3 < tokens.length && "0".equals(tokens[i])) {
                        String remoteNodeNameNearest = tokens[i + 1];
                        String remoteAddrStr = tokens[i + 3];
                        if (!addressStore.containsKey(remoteNodeNameNearest)) {
                            addressStore.put(remoteNodeNameNearest, remoteAddrStr);
                            System.out.println("Active mapping: Discovered node " + remoteNodeNameNearest + " at " + remoteAddrStr);
                        }
                        i += 4;
                    } else {
                        i++;
                    }
                }
                break;
            }
            case "C":
                String[] partsCAS = payload.split(" ", 3);
                if (partsCAS.length == 3) {
                    boolean casResult = false;
                    try {
                        casResult = CAS(partsCAS[0], partsCAS[1], partsCAS[2]);
                    } catch (Exception e) {
                        System.err.println("Error in CAS: " + e.getMessage());
                    }
                    sendCASResponse(transactionId, casResult, senderAddress, senderPort);
                }
                break;
            case "N":
                processNearestRequest(transactionId, payload, senderAddress, senderPort);
                break;
            case "E":
                processKeyExistenceRequest(transactionId, payload, senderAddress, senderPort);
                break;
            case "F":
                System.out.println("Received Key existence response: " + payload);
                break;
            case "V":
                processRelayMessage(transactionId, payload, senderAddress, senderPort);
                break;
            case "I":
                System.out.println("Received Information message: " + payload);
                break;
            case "S":
                System.out.println("Received an unexpected S response: " + payload);
                break;
            case "X":
                System.out.println("Received Write response (X): " + payload);
                break;
            case "D":
                System.out.println("Received CAS response (D): " + payload);
                break;
            default:
                System.out.println("Unknown command: " + command);
        }
    }

    private void sendNameResponse(String transactionId, InetAddress address, int port) {
        String response = transactionId + " H " + nodeName;
        sendResponse(response, address, port);
    }

    private void sendReadResponse(String transactionId, String key, InetAddress address, int port) {
        String value = localRead(key);
        String response = (value != null) ? (transactionId + " S Y " + value) : (transactionId + " S N");
        sendResponse(response, address, port);
    }

    private void sendWriteResponse(String transactionId, boolean success, InetAddress address, int port) {
        String response = transactionId + " X " + (success ? "A" : "X");
        sendResponse(response, address, port);
    }

    private void sendCASResponse(String transactionId, boolean success, InetAddress address, int port) {
        String response = transactionId + " D " + (success ? "R" : "N");
        sendResponse(response, address, port);
    }

    private void processNearestRequest(String transactionId, String hexHash, InetAddress address, int port) {
        try {
            byte[] requestHash = hexStringToByteArray(hexHash);
            List<AddressDistance> distances = new ArrayList<>();
            for (Map.Entry<String, String> entry : addressStore.entrySet()) {
                String key = entry.getKey();
                byte[] keyHash = HashID.computeHashID(key);
                int distance = computeDistance(requestHash, keyHash);
                distances.add(new AddressDistance(key, entry.getValue(), distance));
            }
            distances.sort(Comparator.comparingInt(a -> a.distance));
            StringBuilder responseBuilder = new StringBuilder(transactionId + " O");
            int count = 0;
            for (AddressDistance ad : distances) {
                if (count >= 3) break;
                responseBuilder.append(" ").append(formatAddressPair(ad.nodeName, ad.address));
                count++;
            }
            sendResponse(responseBuilder.toString(), address, port);
            System.out.println("Sent nearest response: " + responseBuilder.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void processKeyExistenceRequest(String transactionId, String key, InetAddress address, int port) {
        try {
            boolean existsLocal = dataStore.containsKey(key);
            char responseChar = existsLocal ? 'Y' : 'N';
            String response = transactionId + " F " + responseChar;
            sendResponse(response, address, port);
            System.out.println("Sent key existence response: " + response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void processRelayMessage(String transactionId, String payload, InetAddress address, int port) {
        try {
            String[] tokens = payload.split(" ", 2);
            if (tokens.length < 2) {
                System.out.println("Invalid relay message format");
                return;
            }
            String targetNode = tokens[0];
            String innerMessage = tokens[1];
            if (targetNode.equals(nodeName)) {
                if (innerMessage.contains(" G")) {
                    String relayResponse = transactionId + " H " + nodeName;
                    sendResponse(relayResponse, address, port);
                    System.out.println("Processed relay message (name request): " + relayResponse);
                } else {
                    String relayResponse = transactionId + " I Unimplemented relay inner command";
                    sendResponse(relayResponse, address, port);
                    System.out.println("Processed relay message (generic): " + relayResponse);
                }
            } else {
                String targetAddressStr = addressStore.get(targetNode);
                if (targetAddressStr != null) {
                    String[] parts = targetAddressStr.split(":");
                    InetAddress targetAddr = InetAddress.getByName(parts[0]);
                    int targetPort = Integer.parseInt(parts[1]);
                    System.out.println("Forwarding relay message to " + targetNode);
                    String response = forwardRelay(innerMessage, targetAddr, targetPort);
                    String relayResponse = transactionId + " " + response;
                    sendResponse(relayResponse, address, port);
                } else {
                    String relayResponse = transactionId + " I Relay target address not found";
                    sendResponse(relayResponse, address, port);
                    System.out.println("Relay forwarding failed: target " + targetNode + " not found");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder hex = new StringBuilder();
        for (byte b : bytes) {
            String hexByte = Integer.toHexString(0xff & b);
            if (hexByte.length() == 1) {
                hex.append('0');
            }
            hex.append(hexByte);
        }
        return hex.toString();
    }

    public void performActiveMapping() throws Exception {
        String hexHash = bytesToHex(nodeHash);
        String tid = generateTransactionID();
        String request = tid + " N " + hexHash;
        for (Map.Entry<String, String> entry : addressStore.entrySet()) {
            if (entry.getKey().equals(nodeName)) continue;
            String addrStr = entry.getValue();
            String[] parts = addrStr.split(":");
            InetAddress addr = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);
            sendRequest(request, addr, port);
            System.out.println("Sent Nearest request: " + request + " to " + addrStr);
        }
    }

    public void startPeriodicActiveMapping(long periodMillis) {
        Thread mappingThread = new Thread(() -> {
            while (true) {
                try {
                    performActiveMapping();
                    Thread.sleep(periodMillis);
                } catch (InterruptedException ie) {
                    System.err.println("Active mapping thread interrupted.");
                    break;
                } catch (Exception e) {
                    System.err.println("Error during periodic active mapping: " + e.getMessage());
                }
            }
        });
        mappingThread.setDaemon(true);
        mappingThread.start();
        System.out.println("Started periodic active mapping every " + periodMillis + " ms");
    }

    private String forwardRelay(String message, InetAddress targetAddr, int targetPort) {
        try {
            String tid = generateTransactionID();
            String request = tid + " " + message;
            sendDirect(request, targetAddr, targetPort);
            long startTime = System.currentTimeMillis();
            int timeout = 5000;
            byte[] buffer = new byte[1024];
            DatagramPacket responsePacket = new DatagramPacket(buffer, buffer.length);
            while (System.currentTimeMillis() - startTime < timeout) {
                try {
                    int remaining = timeout - (int)(System.currentTimeMillis() - startTime);
                    socket.setSoTimeout(remaining);
                    socket.receive(responsePacket);
                    String response = new String(responsePacket.getData(), 0, responsePacket.getLength(), StandardCharsets.UTF_8);
                    String[] tokens = response.split(" ", 2);
                    if (tokens.length >= 2 && tokens[0].equals(tid)) {
                        return tokens[1];
                    }
                } catch (SocketTimeoutException ste) {
                    break;
                }
            }
            return "I Relay response timeout";
        } catch (Exception e) {
            return "I Relay error: " + e.getMessage();
        }
    }

    @Override
    public boolean isActive(String nodeName) throws Exception {
        return this.nodeName != null && this.nodeName.equals(nodeName);
    }

    @Override
    public void pushRelay(String nodeName) throws Exception {
        relayStack.push(nodeName);
        System.out.println("Pushed relay: " + nodeName);
    }

    @Override
    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) {
            String removed = relayStack.pop();
            System.out.println("Popped relay: " + removed);
        }
    }

    @Override
    public boolean exists(String key) throws Exception {
        return dataStore.containsKey(key);
    }

    private String generateTransactionID() {
        Random random = new Random();
        int r = random.nextInt(0x10000);
        return String.format("%04x", r);
    }

    @Override
    public String read(String key) throws Exception {
        System.out.println("Initiating read for key: " + key);
        if (dataStore.containsKey(key)) {
            System.out.println("Key found in local cache.");
            return dataStore.get(key);
        }
        String tid = generateTransactionID();
        String message = tid + " R " + key + " ";
        System.out.println("Sending R request: " + message);
        System.out.println("Current addressStore: " + addressStore);
        List<InetSocketAddress> targets = new ArrayList<>();
        // Check writerMapping for a specific writer.
        String writer = writerMapping.get(key);
        if (writer != null && addressStore.containsKey(writer)) {
            String addrStr = addressStore.get(writer);
            String[] parts = addrStr.split(":");
            InetAddress addr = InetAddress.getByName(parts[0]);
            int port = Integer.parseInt(parts[1]);
            targets.add(new InetSocketAddress(addr, port));
            System.out.println("Targeting writer node: " + writer);
        } else {
            for (Map.Entry<String, String> entry : addressStore.entrySet()) {
                if (entry.getKey().equals(this.nodeName)) continue;
                String addrStr = entry.getValue();
                String[] parts = addrStr.split(":");
                InetAddress addr = InetAddress.getByName(parts[0]);
                int port = Integer.parseInt(parts[1]);
                targets.add(new InetSocketAddress(addr, port));
            }
        }
        int attempts = 0;
        while (attempts < 3) {
            for (InetSocketAddress target : targets) {
                sendRequest(message, target.getAddress(), target.getPort());
            }
            long startTime = System.currentTimeMillis();
            int timeout = 5000;
            byte[] buffer = new byte[1024];
            DatagramPacket responsePacket = new DatagramPacket(buffer, buffer.length);
            while (System.currentTimeMillis() - startTime < timeout) {
                try {
                    int remaining = timeout - (int)(System.currentTimeMillis() - startTime);
                    socket.setSoTimeout(remaining);
                    socket.receive(responsePacket);
                    String response = new String(responsePacket.getData(), 0, responsePacket.getLength(), StandardCharsets.UTF_8);
                    System.out.println("Received response: " + response);
                    String[] tokens = response.split(" ", 4);
                    if (tokens.length >= 4 && tokens[0].equals(tid) && tokens[1].equals("S") && tokens[2].equals("Y")) {
                        dataStore.put(key, tokens[3].trim());
                        return tokens[3].trim();
                    }
                } catch (SocketTimeoutException ste) {
                    break;
                }
            }
            attempts++;
        }
        return null;
    }

    // With the new read method checking writerMapping, targetedRead becomes a convenience.
    public String targetedRead(String key) throws Exception {
        return read(key);
    }

    private String localRead(String key) {
        return dataStore.get(key);
    }

    @Override
    public boolean write(String key, String value) throws Exception {
        dataStore.put(key, value);
        System.out.println("Written key: " + key + " with value: " + value);
        return true;
    }

    public boolean write(String key, String value, boolean broadcast) throws Exception {
        dataStore.put(key, value);
        System.out.println("Written key: " + key + " with value: " + value);
        // Record writer for the key.
        writerMapping.put(key, nodeName);
        if (broadcast) {
            String tid = generateTransactionID();
            String message = tid + " W " + key + " " + value;
            for (Map.Entry<String, String> entry : addressStore.entrySet()) {
                if (entry.getKey().equals(this.nodeName)) continue;
                String addrStr = entry.getValue();
                String[] parts = addrStr.split(":");
                InetAddress addr = InetAddress.getByName(parts[0]);
                int port = Integer.parseInt(parts[1]);
                sendRequest(message, addr, port);
                System.out.println("Broadcast write sent to " + addrStr + ": " + message);
            }
        }
        return true;
    }

    public boolean broadcastWrite(String key, String value) throws Exception {
        return write(key, value, true);
    }

    @Override
    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        boolean success = dataStore.replace(key, currentValue, newValue);
        System.out.println("CAS on key: " + key + " from '" + currentValue + "' to '" + newValue + "' " + (success ? "succeeded" : "failed"));
        return success;
    }

    private void sendResponse(String response, InetAddress address, int port) {
        byte[] respBytes = response.getBytes(StandardCharsets.UTF_8);
        DatagramPacket respPacket = new DatagramPacket(respBytes, respBytes.length, address, port);
        try {
            socket.send(respPacket);
            System.out.println("Sent response: " + response);
        } catch (Exception e) {
            System.err.println("Error sending response: " + e.getMessage());
        }
    }

    private void sendRequest(String message, InetAddress address, int port) throws Exception {
        if (!relayStack.isEmpty()) {
            String relayTarget = relayStack.peek();
            String tid = message.substring(0, 4);
            message = tid + " V " + relayTarget + " " + message.substring(5);
        }
        sendDirect(message, address, port);
        System.out.println("Sent packet: " + message + " to " + address.getHostAddress() + ":" + port);
    }

    private void sendDirect(String message, InetAddress address, int port) throws Exception {
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        DatagramPacket packet = new DatagramPacket(bytes, bytes.length, address, port);
        socket.send(packet);
        System.out.println("Sent packet: " + message + " to " + address.getHostAddress() + ":" + port);
    }

    private int computeDistance(byte[] hash1, byte[] hash2) {
        int matchingBits = 0;
        for (int i = 0; i < Math.min(hash1.length, hash2.length); i++) {
            int xor = hash1[i] ^ hash2[i];
            for (int j = 7; j >= 0; j--) {
                if ((xor & (1 << j)) == 0) {
                    matchingBits++;
                } else {
                    return 256 - matchingBits;
                }
            }
        }
        return 256 - matchingBits;
    }

    private byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }

    private String formatAddressPair(String nodeName, String address) {
        return "0 " + nodeName + " 0 " + address;
    }

    private static class AddressDistance {
        String nodeName;
        String address;
        int distance;

        AddressDistance(String nodeName, String address, int distance) {
            this.nodeName = nodeName;
            this.address = address;
            this.distance = distance;
        }
    }
}
