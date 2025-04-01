import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

class AzureLabTest {

    // DummyResponder simulates a remote node that holds poem verses.
    static class DummyResponder implements Runnable {
        private int port;
        private Map<String, String> poemVerses;

        public DummyResponder(int port) {
            this.port = port;
            poemVerses = new HashMap<>();
            // Populate dummy poem verses.
            poemVerses.put("D:jabberwocky0", "’Twas brillig, and the slithy toves");
            poemVerses.put("D:jabberwocky1", "Did gyre and gimble in the wabe;");
            poemVerses.put("D:jabberwocky2", "All mimsy were the borogoves,");
            poemVerses.put("D:jabberwocky3", "And the mome raths outgrabe.");
            poemVerses.put("D:jabberwocky4", "Beware the Jabberwock, my son!");
            poemVerses.put("D:jabberwocky5", "The jaws that bite, the claws that catch!");
            poemVerses.put("D:jabberwocky6", "Beware the Jubjub bird, and shun");
        }

        @Override
        public void run() {
            try (DatagramSocket ds = new DatagramSocket(port)) {
                byte[] buffer = new byte[1024];
                System.out.println("DummyResponder listening on port " + port);
                while (true) {
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                    ds.receive(packet);
                    String message = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                    // Expected message format: "<TID> R <key> "
                    String[] parts = message.split(" ", 3);
                    if (parts.length < 3) continue;
                    String tid = parts[0];
                    String command = parts[1];
                    String key = parts[2].trim();
                    if ("R".equals(command) && poemVerses.containsKey(key)) {
                        String verse = poemVerses.get(key);
                        // Response: "<TID> S Y <verse>"
                        String response = tid + " S Y " + verse;
                        byte[] respBytes = response.getBytes(StandardCharsets.UTF_8);
                        DatagramPacket respPacket = new DatagramPacket(respBytes, respBytes.length,
                                packet.getAddress(), packet.getPort());
                        ds.send(respPacket);
                        System.out.println("DummyResponder sent: " + response);
                    }
                }
            } catch (Exception e) {
                System.err.println("DummyResponder exception: " + e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        // Replace with your actual email and lab IP address if needed.
        String emailAddress = "zidan.ahmed.2@city.ac.uk";
        // Choose either 10.200.51.18 or 10.200.51.19.
        String ipAddress = "10.200.51.19";

        try {
            // Start DummyResponder on port 30110 for local testing.
            int dummyPort = 30110;
            Thread dummyThread = new Thread(new DummyResponder(dummyPort));
            dummyThread.setDaemon(true); // Ensure it exits when the main thread ends.
            dummyThread.start();

            // Create and initialize a Node instance.
            Node node = new Node();
            String nodeName = "N:" + emailAddress;
            node.setNodeName(nodeName);
            int port = 20114; // Use port 20114 as per your updated requirement.
            node.openPort(port);

            // Add the dummy node to the node's addressStore using reflection.
            // This ensures that R requests are sent to the dummy responder.
            java.lang.reflect.Field field = Node.class.getDeclaredField("addressStore");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            Map<String, String> addressStore = (Map<String, String>) field.get(node);
            addressStore.put("N:dummy", "127.0.0.1:" + dummyPort);
            System.out.println("Added dummy node address: N:dummy -> 127.0.0.1:" + dummyPort);

            // Wait for bootstrapping (simulate waiting for other nodes).
            System.out.println("Waiting for bootstrapping...");
            node.handleIncomingMessages(5000);

            // Manually trigger a read for "D:jabberwocky0" to test R/S flow.
            System.out.println("Manually triggering a read for key D:jabberwocky0");
            String manualResult = node.read("D:jabberwocky0");
            System.out.println("Manual read result: " + manualResult);

            // Test reading the full poem (verses 0 to 6).
            System.out.println("Getting the full poem...");
            for (int i = 0; i < 7; i++) {
                String key = "D:jabberwocky" + i;
                String value = node.read(key);
                if (value == null) {
                    System.err.println("Can't find poem verse " + i);
                    System.exit(2);
                } else {
                    System.out.println("Verse " + i + ": " + value);
                }
            }

            // Test writing a marker to verify write functionality.
            System.out.println("Writing a marker so it's clear my code works");
            {
                String key = "D:" + emailAddress;
                String value = "It works!";
                boolean success = node.write(key, value);
                System.out.println("Write marker success: " + success);
                System.out.println("Marker read-back: " + node.read(key));
            }

            // Announce your node's address so that other nodes can contact you.
            System.out.println("Letting other nodes know where we are");
            node.write(nodeName, ipAddress + ":" + port);

            // Handle incoming connections indefinitely.
            System.out.println("Handling incoming connections (press Ctrl+C to exit)...");
            node.handleIncomingMessages(0);

        } catch (Exception e) {
            System.err.println("Exception during AzureLabTest");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
