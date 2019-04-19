package server;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import java.io.*;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class ServerNode {
    private int id;
    private String host;
    private int port;
    private boolean isCoordinator;
    private int coordinatorId;
    private String coordinatorHost;
    private int coordinatorPort;
    private ServerSocket listener;
    private final String HOST_FILE = "servers.csv";
    private ArrayList<Node> nodes = new ArrayList<>();
    private Socket serverToTalkTo;
    private Node nextNode = null;
    private String loggerFileName;
    private PrintWriter logger;

    public ServerNode(int id, String host, int port, int coordinatorId, String coordinatorHost, int coordinatorPort) throws IOException {
        this.id = id;
        this.host = host;
        this.port = port;
        this.coordinatorId = coordinatorId;
        this.coordinatorHost = coordinatorHost;
        this.coordinatorPort = coordinatorPort;
        this.loggerFileName = "Server" + id + "Log.log";

        logger = new PrintWriter(new FileWriter(loggerFileName), true);
        logger.println(getTimestamp() + "New server created.");
    }

    private boolean isCoordinator() {return isCoordinator;}

    private String getTimestamp() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        return "[" + dateFormat.format(date) + "] ";
    }

    private void initialiseServer() throws IOException {
        listener = new ServerSocket(port);
        logger.println(getTimestamp() + "Server " + id + " now listening.");
        if (id == coordinatorId) {
            logger.println(getTimestamp() + "Server is coordinator.");
            isCoordinator = true;
        }
    }

    private void buildNodeList() {
        logger.println(getTimestamp() + "Reading host file.");
        try {
            FileReader fr = new FileReader(HOST_FILE);
            CSVReader csvReader = new CSVReaderBuilder(fr).withSkipLines(1).build();
            String[] nextRecord;

            while ((nextRecord = csvReader.readNext()) != null) {
                int id = Integer.parseInt(nextRecord[0]);
                String host = nextRecord[1];
                int port = Integer.parseInt(nextRecord[2]);

                nodes.add(new Node(id, host, port));
            }
        } catch (FileNotFoundException e) {
            logger.println(getTimestamp() + "ERROR could not find file.");
            e.printStackTrace();
        } catch (IOException e) {
            logger.println(getTimestamp() + "ERROR IO Exception.");
            e.printStackTrace();
        }
    }

    private void printNodeList() {
        logger.println(getTimestamp() + "Host file read.");
        for (Node node : nodes) {
            System.out.println(node.getId() + ", " + node.getHost() + ":" + node.getPort());
        }
    }

    private boolean checkAllServersOnline() {
        logger.println(getTimestamp() + "Checking network state.");
        for (Node node : nodes) {
            try {
                logger.println(getTimestamp() + "Checking Server " + node.getId());
                serverToTalkTo = new Socket(node.getHost(), node.getPort());
                BufferedReader inFromServer = new BufferedReader(new InputStreamReader(serverToTalkTo.getInputStream()));
                PrintWriter outToServer = new PrintWriter(new OutputStreamWriter(serverToTalkTo.getOutputStream()), true);

                outToServer.println("HELLO");

                String msg = inFromServer.readLine();

                if (msg.equals("OK")) {

                    outToServer.println("Hello from Coordinator!");

                    msg = inFromServer.readLine();
                    System.out.println(msg);
                }

                serverToTalkTo.close();
            } catch (UnknownHostException e) {
                logger.println(getTimestamp() + "ERROR unknown host.");
                e.printStackTrace();
                return false;
            } catch (ConnectException e) {
                logger.println(getTimestamp() + "ERROR Server " + node.getId() + " is not communicating.");
                System.out.println(node.getId() + " is not online.");
                return false;
            } catch (IOException e) {
                logger.println(getTimestamp() + "ERROR IO Exception.");
                e.printStackTrace();
                return false;
            }
        }

        return true;
    }

    private void setNextNode(Node nextNode) {
        logger.println(getTimestamp() + "Setting next node to " + nextNode.getId());
        this.nextNode = nextNode;
    }

    private void buildRing() {
        logger.println(getTimestamp() + "Beginning ring construction.");
        for (int i = 0; i < nodes.size(); i++) {
            Node currentNode = nodes.get(i);
            try {
                logger.println(getTimestamp() + "Updating Server " + currentNode.getId() + " next node.");
                serverToTalkTo = new Socket(currentNode.getHost(), currentNode.getPort());
                BufferedReader inFromServer = new BufferedReader(new InputStreamReader(serverToTalkTo.getInputStream()));
                PrintWriter outToServer = new PrintWriter(new OutputStreamWriter(serverToTalkTo.getOutputStream()), true);

                outToServer.println("NEXT NODE");

                String msg = inFromServer.readLine();

                if (msg.equals("OK")) {
                    Node nextNode;

                    if (i < nodes.size()-1) {
                        nextNode = nodes.get(i+1);
                    } else {
                        nextNode = nodes.get(0);
                    }

                    outToServer.println(nextNode.getId() + "," + nextNode.getHost() + "," + nextNode.getPort());
                    logger.println(getTimestamp() + "New next node sent.");
                }

                logger.println(getTimestamp() + "Closing connection.");
                serverToTalkTo.close();

            } catch (UnknownHostException e) {
                logger.println(getTimestamp() + "ERROR unknown host.");
                e.printStackTrace();
            } catch (ConnectException e) {
                logger.println(getTimestamp() + "ERROR Server " + currentNode.getId() + " is not communicating.");
                System.out.println(currentNode.getId() + " is not online.");
            } catch (IOException e) {
                logger.println(getTimestamp() + "ERROR IO Exception.");
                e.printStackTrace();
            }
        }
    }

    private void printNextNode() {
        System.out.println("Next node ID: " + nextNode.getId());
        System.out.println("Next node Host: " + nextNode.getHost());
        System.out.println("Next node Port: " + nextNode.getPort());
    }

    private void listenForConnections() throws IOException {
        while (true) {
            logger.println(getTimestamp() + "Listening for connection...");
            System.out.println("Listening...");
            Socket connected = listener.accept();
            logger.println(getTimestamp() + "Client " + connected.toString() + " connected.");
            System.out.println("Speaking to " + connected.toString());

            BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connected.getInputStream()));
            PrintWriter outToClient = new PrintWriter(new OutputStreamWriter(connected.getOutputStream()), true);

            String msg = inFromClient.readLine();
            System.out.println(msg);
            logger.println(getTimestamp() + "Received " + msg);

            if (msg.equals("HELLO")) {
                logger.println(getTimestamp() + "Acknowledging client.");
                outToClient.println("OK");

                msg = inFromClient.readLine();
                logger.println(getTimestamp() + "Received " + msg);
                System.out.println(msg);

                String msgToSend = "Hello Coordinator! From " + id;
                logger.println(getTimestamp() + "Sending " + msgToSend);
                outToClient.println(msgToSend);

            } else if (msg.equals("NEXT NODE")) {
                logger.println(getTimestamp() + "Acknowledging client.");
                outToClient.println("OK");

                msg = inFromClient.readLine();
                logger.println(getTimestamp() + "Received new next node.");
                String[] splitMessage = msg.split(",");

                int nextId = Integer.parseInt(splitMessage[0]);
                String nextHost = splitMessage[1];
                int nextPort = Integer.parseInt(splitMessage[2]);

                setNextNode(new Node(nextId, nextHost, nextPort));
                printNextNode();
            }

            logger.println(getTimestamp() + "Closing connection.");
            connected.close();
        }
    }

    public static void main(String[] args) throws IOException {
        ServerNode ss = null;
        try {
            int id = Integer.parseInt(args[0]);
            String host = args[1];
            int port = Integer.parseInt(args[2]);
            int coordinatorId = Integer.parseInt(args[3]);
            String coordinatorHost = args[4];
            int coordinatorPort = Integer.parseInt(args[5]);
            ss = new ServerNode(id, host, port, coordinatorId, coordinatorHost, coordinatorPort);
        } catch (IOException e) {
            System.out.println("Invalid details given.");
            System.exit(-1);
        }

        ss.initialiseServer();

        ServerNode finalSs = ss;
        new Thread(() -> {
            try {
                finalSs.listenForConnections();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();

        if (ss.isCoordinator) {
            ss.buildNodeList();
            ss.printNodeList();
            while (true) {
                if (ss.checkAllServersOnline()) {
                    ss.buildRing();
                    break;
                }
            }
        }
    }
}
