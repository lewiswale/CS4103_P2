package server;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import java.io.*;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

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

    public ServerNode(int id, String host, int port, int coordinatorId, String coordinatorHost, int coordinatorPort) throws IOException {
        this.id = id;
        this.host = host;
        this.port = port;
        this.coordinatorId = coordinatorId;
        this.coordinatorHost = coordinatorHost;
        this.coordinatorPort = coordinatorPort;
    }

    private boolean isCoordinator() {return isCoordinator;}

    private void initialiseServer() throws IOException {
        listener = new ServerSocket(port);
        if (id == coordinatorId) {
            isCoordinator = true;
        }
    }

    private void buildNodeList() {
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
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void printNodeList() {
        for (Node node : nodes) {
            System.out.println(node.getId() + ", " + node.getHost() + ":" + node.getPort());
        }
    }

    private boolean checkAllServersOnline() {
        for (Node node : nodes) {
            try {
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
                e.printStackTrace();
                return false;
            } catch (ConnectException e) {
                System.out.println(node.getId() + " is not online.");
                return false;
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        }

        return true;
    }

    private void setNextNode(Node nextNode) {
        this.nextNode = nextNode;
    }

    private void buildRing() {
        for (int i = 0; i < nodes.size(); i++) {
            Node currentNode = nodes.get(i);
            try {
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
                }

                serverToTalkTo.close();

            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (ConnectException e) {
                System.out.println(currentNode.getId() + " has stopped communicating.");
            } catch (IOException e) {
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
            System.out.println("Listening...");
            Socket connected = listener.accept();
            System.out.println("Speaking to " + connected.toString());

            BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connected.getInputStream()));
            PrintWriter outToClient = new PrintWriter(new OutputStreamWriter(connected.getOutputStream()), true);

            String msg = inFromClient.readLine();
            System.out.println(msg);

            if (msg.equals("HELLO")) {
                outToClient.println("OK");

                msg = inFromClient.readLine();
                System.out.println(msg);

                outToClient.println("Hello Coordinator! From " + id);
            } else if (msg.equals("NEXT NODE")) {
                outToClient.println("OK");

                msg = inFromClient.readLine();
                String[] splitMessage = msg.split(",");

                int nextId = Integer.parseInt(splitMessage[0]);
                String nextHost = splitMessage[1];
                int nextPort = Integer.parseInt(splitMessage[2]);

                setNextNode(new Node(nextId, nextHost, nextPort));
                printNextNode();
            }

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
