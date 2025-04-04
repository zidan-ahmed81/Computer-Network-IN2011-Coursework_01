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
import java.util.Date;
import java.util.Random;

class AzureLabTest {
    public static void main(String[] args) {
        String emailAddress = "zidan.ahmed.2@city.ac.uk";
        String ipAddress = "10.216.34.203";

        try {
            Node node = new Node();
            String nodeName = "N:" + emailAddress;
            node.setNodeName(nodeName);

            int port = 20110;
            node.openPort(port);
            node.bootstrap();
            node.checkBootstrappedNodesActive();

            System.out.println("Testing relay stack operations...");
            node.pushRelay("N:blue");
            node.pushRelay("N:yellow");
            node.pushRelay("N:orange");
            node.popRelay();
            node.popRelay();
            node.popRelay();
            node.popRelay();

            System.out.println("Waiting for another node to get in contact");
            node.handleIncomingMessages(15 * 1000);

            System.out.println("Getting the poem...");
            for (int i = 0; i < 7; ++i) {
                String key = "D:jabberwocky" + i;
                String value = node.read(key);
                if (value == null) {
                    System.err.println("Can't find poem verse " + i);
                } else {
                    System.out.println(value);
                }
            }

            System.out.println("Writing a marker so it's clear my code works");
            {
                String key = "D:" + emailAddress;
                String value = "It works!";
                boolean success = node.write(key, value);
                System.out.println(node.read(key));
            }

            System.out.println("Checking if D:jabberwocky0 exists...");
            boolean exists = node.exists("D:jabberwocky0");
            System.out.println("Result from exists(): " + exists);

            System.out.println("Testing network CAS operation...");
            String key = "D:casNet";
            node.write(key, "alpha");

            boolean ok = node.CAS(key, "alpha", "beta");
            System.out.println("CAS(alpha → beta): " + ok);

            String result = node.read(key);
            System.out.println("Result: " + result);

        } catch (Exception e) {
            System.err.println("Exception during AzureLabTest");
            e.printStackTrace(System.err);
        }
    }
}
