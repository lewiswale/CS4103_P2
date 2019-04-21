package client;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import server.Node;

import java.io.*;
import java.net.Socket;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Scanner;

public class Client {
    private static ArrayList<Node> nodes = new ArrayList<>();
    private static final String HOST_FILE = "servers.csv";
    private static String loggerFileName;
    private static PrintWriter logger;

    private static String getTimestamp() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        return "[" + dateFormat.format(date) + "] ";
    }

    public static void main(String[] args) {
        try {
            Scanner input = new Scanner(System.in);
            System.out.println("Send or Receive? (S or R)");
            String choice = input.nextLine().toUpperCase();

            System.out.println("What is your name?");
            String name = input.nextLine();
            loggerFileName = "Client" + name + "Log.log";

            logger = new PrintWriter(new FileWriter(loggerFileName), true);
            logger.println(getTimestamp() + "New client created.");

            logger.println(getTimestamp() + "Getting all registered servers.");
            FileReader fr = new FileReader(HOST_FILE);
            CSVReader csvReader = new CSVReaderBuilder(fr).withSkipLines(1).build();
            String[] nextRecord;

            while ((nextRecord = csvReader.readNext()) != null) {
                int id = Integer.parseInt(nextRecord[0]);
                String host = nextRecord[1];
                int port = Integer.parseInt(nextRecord[2]);

                nodes.add(new Node(id, host, port));
            }

            boolean serverFound = false;
            int serverID;
            String serverHost = "";
            int serverPort = 0;
            while (!serverFound) {
                System.out.println("What server would you like to connect to?");
                serverID = input.nextInt();
                for (Node node : nodes) {
                    if (node.getId() == serverID) {
                        logger.println(getTimestamp() + "Found server.");
                        serverHost = node.getHost();
                        serverPort = node.getPort();
                        serverFound = true;
                    }
                }
                if (!serverFound) {
                    logger.println(getTimestamp() + "Invalid server ID given.");
                    System.out.println("Could not find server, please try again");
                }
            }

            Socket server = new Socket(serverHost, serverPort);
            BufferedReader inFromServer = new BufferedReader(new InputStreamReader(server.getInputStream()));
            PrintWriter outToServer = new PrintWriter(new OutputStreamWriter(server.getOutputStream()), true);

            if (choice.equals("S")) {
                System.out.println("Who would you like to send a message to?");
                String recipient = input.nextLine();
                logger.println(getTimestamp() + "Read recipient.");

                System.out.println("What is the message you would like to send?");
                String post = input.nextLine();
                logger.println(getTimestamp() + "Post read.");

                logger.println(getTimestamp() + "Posting...");
                outToServer.println("POST");
                String msg = inFromServer.readLine();

                if (msg.equals("OK")) {
                    outToServer.println(name);
                    outToServer.println(recipient);
                    outToServer.println(post);
                }
            } else {
                outToServer.println("PULL");
                String msg = inFromServer.readLine();

                if (msg.equals("OK")) {
                    outToServer.println(name);
                    String incoming = inFromServer.readLine();

                    if (incoming.equals("INCOMING")) {
                        String post = inFromServer.readLine();
                        String sender = inFromServer.readLine();
                        System.out.println("Message received from " + sender + "\n\"" + post + "\"");
                    } else {
                        System.out.println("No messages for you have been found.");
                    }
                }
            }

        } catch (FileNotFoundException e) {
            logger.println(getTimestamp() + "ERROR could not find file.");
            e.printStackTrace();
        } catch (IOException e) {
            logger.println(getTimestamp() + "ERROR IO Exception.");
            e.printStackTrace();
        }

        System.out.println("What node would you like to connect to? ");

    }
}
