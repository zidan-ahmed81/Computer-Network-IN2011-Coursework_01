import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;

public class AzureLabTest {
    public static void main(String[] args) {
        // Replace with your actual email and the VM's reachable IP address.
        String emailAddress = "zidan.ahmed.2@city.ac.uk";
        String ipAddress = "10.200.51.19";  // Ensure this is the correct VM IP.

        try {
            // Create and initialize the Node.
            Node node = new Node();
            String nodeName = "N:" + emailAddress;
            node.setNodeName(nodeName);
            int port = 20110; // Use a port in the allowed range (e.g., 20110 to 20130).
            node.openPort(port);

            // Allow time for bootstrapping (to receive addresses from other nodes).
            System.out.println("Waiting for bootstrapping...");
            node.handleIncomingMessages(5000);

            // Test reading poem verses from the network.
            System.out.println("Attempting to read poem verses:");
            for (int i = 0; i < 7; i++) {
                String key = "D:jabberwocky" + i;
                String verse = node.read(key);
                if (verse == null) {
                    System.err.println("Poem verse " + i + " not found.");
                } else {
                    System.out.println("Verse " + i + ": " + verse);
                }
            }

            // Test writing a marker key/value pair.
            String markerKey = "D:" + emailAddress;
            String markerValue = "It works!";
            boolean writeSuccess = node.write(markerKey, markerValue);
            System.out.println("Write marker result: " + writeSuccess);
            System.out.println("Marker read-back: " + node.read(markerKey));

            // Announce your node's address so that other nodes can contact you.
            String myAddress = ipAddress + ":" + port;
            System.out.println("Announcing node address: " + myAddress);
            node.write(nodeName, myAddress);

            // Begin handling incoming messages indefinitely.
            System.out.println("Handling incoming connections indefinitely (press Ctrl+C to exit)...");
            node.handleIncomingMessages(0);
        } catch (Exception e) {
            System.err.println("Exception during AzureLabTest: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
