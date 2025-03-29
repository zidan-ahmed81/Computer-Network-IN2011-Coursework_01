// IN2011 Computer Networks
// Coursework 2024/2025
//
// This is a test program to show how Node.java can be used.
// It creates a single instance of Node.java.
// A bootstrapping stage gives the nodes the addresses of a few of the nodes on the Azure virtual lab
// Then it performs some basic tests on the network.
//
// Running this test is not enough to check that all of the features of your
// implementation work.  You will need to do your own testing as well.
//
// You will need to run this on the virtual lab computers.  If you run it on your own computer
// it will not be able to access the nodes on the virtual lab.
//
// You can use this to record the wireshark evidence of things working.
// But please be aware it does not test all of the features so you will need to modify it or
// write your own tests to show everything works.

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;

class AzureLabTest {
    public static void main (String [] args) {
        String emailAddress = "your.name@city.ac.uk"; // Replace with your actual email
        String ipAddress = "10.216.34.203"; // Replace with your Azure VM's IP

        try {
            Node node = new Node();
            String nodeName = "N:" + emailAddress;
            node.setNodeName(nodeName);

            int port = 20110;
            node.openPort(port);

            System.out.println("Waiting for another node to get in contact...");
            node.handleIncomingMessages(12 * 1000); // Wait for traffic

            // ========== TEST 1: Read (R) + Response (S) ==========
            System.out.println("🔍 Reading a known poem key (triggers R/S)");
            String value = node.read("D:jabberwocky0");
            System.out.println("Read result: " + value);
            Thread.sleep(1000);

            // ========== TEST 2: Write then Exists (E) + Response (F) ==========
            System.out.println("📦 Writing a key for existence test");
            node.write("D:test-exists", "tempvalue");
            Thread.sleep(1000);
            System.out.println("🔎 Checking if 'D:test-exists' exists (triggers E/F)");
            boolean exists = node.exists("D:test-exists");
            System.out.println("Exists result: " + exists);
            Thread.sleep(1000);

            // ========== TEST 3: Write then CAS (C) + Response (D) ==========
            System.out.println("⚙️ Writing key for CAS test");
            node.write("D:test-cas", "first");
            Thread.sleep(1000);
            System.out.println("🔁 Attempting CAS to update value (triggers C/D)");
            boolean updated = node.CAS("D:test-cas", "first", "second");
            System.out.println("CAS result: " + updated);
            Thread.sleep(1000);

            // ========== TEST 4: Relay (V) ==========
            System.out.println("🌐 Setting up relay and sending read (triggers V)");
            node.pushRelay("N:white"); // Use a real node name you've seen in Wireshark
            String relayed = node.read("D:jabberwocky2");
            System.out.println("Relayed read result: " + relayed);
            node.popRelay();
            Thread.sleep(1000);

            // ========== Original Code (Optional) ==========
            System.out.println("Getting the rest of the poem...");
            for (int i = 1; i < 7; ++i) {
                String key = "D:jabberwocky" + i;
                String verse = node.read(key);
                if (verse != null) {
                    System.out.println(verse);
                } else {
                    System.err.println("Missing verse: " + i);
                }
            }

            System.out.println("Writing a final marker...");
            String markerKey = "D:" + emailAddress;
            node.write(markerKey, "It works!");
            System.out.println("Marker read back: " + node.read(markerKey));

            System.out.println("Publishing address to the network...");
            node.write(nodeName, ipAddress + ":" + port);

            System.out.println("📡 Handling incoming messages...");
            node.handleIncomingMessages(0); // Keep running

        } catch (Exception e) {
            System.err.println("❌ Exception during AzureLabTest");
            e.printStackTrace(System.err);
        }
    }
}
