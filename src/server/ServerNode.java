package server;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;

import java.io.*;
import java.net.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.TimeUnit;

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
    private boolean hasToken = false;
    private ArrayList<Post> postsToMake = new ArrayList<>();
    private ArrayList<Post> pullsToMake = new ArrayList<>();
    private static ArrayList<Post> posts = new ArrayList<>();

    /**
     * ServerNode constructor
     * @param id ID to be assigned to server
     * @param host host address of server
     * @param port local port assigned to server
     * @param coordinatorId ID of coordinator node
     * @param coordinatorHost Host address of coordinator node
     * @param coordinatorPort Port of coordinator node
     * @throws IOException
     */
    public ServerNode(int id, String host, int port, int coordinatorId, String coordinatorHost, int coordinatorPort) throws IOException {
        this.id = id;
        this.host = host;
        this.port = port;
        this.coordinatorId = coordinatorId;
        this.coordinatorHost = coordinatorHost;
        this.coordinatorPort = coordinatorPort;
        this.loggerFileName = "Server" + id + "Log.log";

        logger = new PrintWriter(new FileWriter(loggerFileName), true);
        logger.println(getTimestamp() + "New server created with ID " + id);
    }

    /**
     * Method to get current timestamp for log entry
     * @return Formatted string containing timestamp
     */
    private String getTimestamp() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
        Date date = new Date();
        return "[" + dateFormat.format(date) + "] ";
    }

    /**
     * Method to initialise ServerSocket used for listening.
     * @throws IOException
     */
    private void initialiseServer() throws IOException {
        listener = new ServerSocket(port);
        logger.println(getTimestamp() + "Server " + id + " now listening.");
        if (id == coordinatorId) {
            logger.println(getTimestamp() + "Server is coordinator.");
            isCoordinator = true;
        }
    }

    /**
     * Reads and parses csv file containing server details.
     */
    private void buildNodeList() {
        logger.println(getTimestamp() + "Reading host file.");
        nodes = new ArrayList<>();
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

    /**
     * Prints parsed csv file
     */
    private void printNodeList() {
        logger.println(getTimestamp() + "Host file read.");
        for (Node node : nodes) {
            System.out.println(node.getId() + ", " + node.getHost() + ":" + node.getPort());
        }
    }

    /**
     * Attempts to establish contact with each node. If a node is unreachable, ring cannot be constructed, so stops.
     * @return true if all nodes are online, false if inactive node found.
     */
    private boolean checkAllServersOnline() {
        logger.println(getTimestamp() + "Checking network state.");
        for (Node node : nodes) {
            try {
                logger.println(getTimestamp() + "Checking Server " + node.getId());
                serverToTalkTo = new Socket(node.getHost(), node.getPort());
                BufferedReader inFromServer = new BufferedReader(new InputStreamReader(serverToTalkTo.getInputStream()));
                PrintWriter outToServer = new PrintWriter(new OutputStreamWriter(serverToTalkTo.getOutputStream()), true);

                String msgToSend = "HELLO";
                logger.println(getTimestamp() + "Sending to " + node.getId() + ": " + msgToSend);
                outToServer.println(msgToSend);

                String msg = inFromServer.readLine();
                logger.println(getTimestamp() + "Received from " + node.getId() + ": " + msg);

                if (msg.equals("OK")) {
                    logger.println(getTimestamp() + "Acknowledgement received.");
                    msgToSend = "Hello from Coordinator!";
                    logger.println(getTimestamp() + "Sending to " + node.getId() + ": " + msgToSend);
                    outToServer.println(msgToSend);

                    msg = inFromServer.readLine();
                    logger.println(getTimestamp() + "Received from " + node.getId() + ": " + msg);
                    System.out.println(msg);
                }

                logger.println(getTimestamp() + "Closing connection with " + node.getId());
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
        logger.println(getTimestamp() + "All nodes online!");
        return true;
    }

    /**
     * Set the node current ServerNode can directly communicate with.
     * @param nextNode node to be set as nextNode
     */
    private void setNextNode(Node nextNode) {
        logger.println(getTimestamp() + "Setting next node for " + id + " to " + nextNode.getId());
        this.nextNode = nextNode;
    }

    /**
     * Checks whether next node ID is greater than current coordinator ID. If so, triggers election.
     */
    private void checkForElection() {
        if (nextNode.getId() > coordinatorId) {
            logger.println(getTimestamp() + "Next node ID greater than coordinator ID");
            logger.println(getTimestamp() + "TRIGGERING ELECTION");
            sendElection("");
        }
    }

    /**
     * Sends election messages to next node.
     * @param currentIds String containing all ID's currently read.
     */
    private void sendElection(String currentIds) {
        try {
            logger.println(getTimestamp() + "Connecting to next node, ID = " + nextNode.getId() + ".");
            serverToTalkTo = new Socket(nextNode.getHost(), nextNode.getPort());
            BufferedReader inFromServer = new BufferedReader(new InputStreamReader(serverToTalkTo.getInputStream()));
            PrintWriter outToServer = new PrintWriter(new OutputStreamWriter(serverToTalkTo.getOutputStream()), true);

            String msgToSend = "ELECTION";
            logger.println(getTimestamp() + "Sending to " + nextNode.getId() + ": " + msgToSend);
            outToServer.println(msgToSend);

            String msg = inFromServer.readLine();
            logger.println(getTimestamp() + "Received from " + nextNode.getId() + ": " + msg);

            if (msg.equals("OK")) {
                msgToSend = currentIds + id + ",";
                logger.println(getTimestamp() + "Sending to " + nextNode.getId() + ": " + msgToSend);
                outToServer.println(msgToSend);
            }
            logger.println(getTimestamp() + "Closing connection with " + nextNode.getId());
            serverToTalkTo.close();

        } catch (UnknownHostException e) {
            logger.println(getTimestamp() + "ERROR unknown host.");
            e.printStackTrace();
        } catch (ConnectException e) {
            logger.println(getTimestamp() + "ERROR Server " + nextNode.getId() + " is not communicating.");
            System.out.println(nextNode.getId() + " is not online.");
        } catch (IOException e) {
            logger.println(getTimestamp() + "ERROR IO Exception.");
            e.printStackTrace();
        }
    }

    /**
     * Once election ring has completed, all collected ID's are inspected.
     * @param ids String of node ID's
     * @return Highest node ID found within the network
     */
    private int findNewCoordinatorID(String[] ids) {
        logger.println(getTimestamp() + "Finding server with highest ID.");
        int highestID = -1;

        for (String id : ids) {
            int numId = Integer.parseInt(id);
            if (numId > highestID)
                highestID = numId;
        }
        logger.println(getTimestamp() + "New coordinator shall be " + highestID);
        return highestID;
    }

    /**
     * Sends update coordinator to next node.
     * @param startID node ID of node where ring starts
     * @param newCoordinator ID of node that is to be the new coordinator.
     */
    private void updateCoordinatorID(int startID, int newCoordinator) {
        try {
            logger.println(getTimestamp() + "Connecting to next node.");
            serverToTalkTo = new Socket(nextNode.getHost(), nextNode.getPort());
            BufferedReader inFromServer = new BufferedReader(new InputStreamReader(serverToTalkTo.getInputStream()));
            PrintWriter outToServer = new PrintWriter(new OutputStreamWriter(serverToTalkTo.getOutputStream()), true);

            String msgToSend = "UPDATE COORDINATOR ID";
            logger.println(getTimestamp() + "Sending to " + nextNode.getId() + ": " + msgToSend);
            outToServer.println(msgToSend);

            String msg = inFromServer.readLine();
            logger.println(getTimestamp() + "Received from " + nextNode.getId() + ": " + msg);

            if (msg.equals("OK")) {
                msgToSend = startID + "," + newCoordinator;
                logger.println(getTimestamp() + "Sending to " + nextNode.getId() + ": " + msgToSend);
                outToServer.println(msgToSend);
            }

            logger.println(getTimestamp() + "Closing connection with " + nextNode.getId());
            serverToTalkTo.close();

        } catch (UnknownHostException e) {
            logger.println(getTimestamp() + "ERROR unknown host.");
            e.printStackTrace();
        } catch (ConnectException e) {
            logger.println(getTimestamp() + "ERROR Server " + nextNode.getId() + " is not communicating.");
            System.out.println(nextNode.getId() + " is not online.");
        } catch (IOException e) {
            logger.println(getTimestamp() + "ERROR IO Exception.");
            e.printStackTrace();
        }
    }

    /**
     * Retrieve respective endpoint(host and port) for new coordinator
     * @param newCoordinator ID of new coordinator
     */
    private void updateCoordinatorEndpoint(int newCoordinator) {
        logger.println(getTimestamp() + "Reading host file.");
        buildNodeList();
        logger.println(getTimestamp() + "Finding new coordinator endpoint.");
        for (Node node : nodes) {
            if (node.getId() == newCoordinator) {
                logger.println(getTimestamp() + "Updating coordinator info.");
                coordinatorId = node.getId();
                coordinatorHost = node.getHost();
                coordinatorPort = node.getPort();
                if (isCoordinator) {
                    isCoordinator = false;
                    logger.println(getTimestamp() + "I AM NO LONGER COORDINATOR");
                }
                logger.println(getTimestamp() + "NEW COORDINATOR IS " + coordinatorId);
                break;
            }
        }
    }

    /**
     * Once all nodes have an updated coordinator, new coordinator is informed.
     */
    private void electionCompleted() {
        try {
            serverToTalkTo = new Socket(coordinatorHost, coordinatorPort);
            BufferedReader inFromServer = new BufferedReader(new InputStreamReader(serverToTalkTo.getInputStream()));
            PrintWriter outToServer = new PrintWriter(new OutputStreamWriter(serverToTalkTo.getOutputStream()), true);

            String msgToSend = "ELECTION COMPLETE";
            logger.println(getTimestamp() + "Sending to " + coordinatorId + ": " + msgToSend);
            outToServer.println(msgToSend);

            String msg = inFromServer.readLine();
            logger.println(getTimestamp() + "Received from " + coordinatorId + ": " + msg);
            logger.println(msg);

            if (msg.equals("OK")) {
                logger.println(getTimestamp() + "Election completion acknowledged.");
            }

            logger.println(getTimestamp() + "Closing connection with " + coordinatorId);
            serverToTalkTo.close();

        } catch (UnknownHostException e) {
            logger.println(getTimestamp() + "ERROR unknown host.");
            e.printStackTrace();
        } catch (ConnectException e) {
            logger.println(getTimestamp() + "ERROR Server " + coordinatorId + " is not communicating.");
            System.out.println(coordinatorId + " is not online.");
        } catch (IOException e) {
            logger.println(getTimestamp() + "ERROR IO Exception.");
            e.printStackTrace();
        }
    }

    /**
     * Method used to pass the token from one node to the next.
     */
    private void passToken() {
        try {
            serverToTalkTo = new Socket(nextNode.getHost(), nextNode.getPort());
            BufferedReader inFromServer = new BufferedReader(new InputStreamReader(serverToTalkTo.getInputStream()));
            PrintWriter outToServer = new PrintWriter(new OutputStreamWriter(serverToTalkTo.getOutputStream()), true);

            String msgToSend = "TOKEN";
            logger.println(getTimestamp() + "Sending to " + nextNode.getId() + ": " + msgToSend);
            outToServer.println(msgToSend);

            String msg = inFromServer.readLine();
            logger.println(getTimestamp() + "Received from " + nextNode.getId() + ": " + msg);

            if (msg.equals("OK"))
                logger.println(getTimestamp() + "Token successfully passed.");

            logger.println(getTimestamp() + "Closing connection with " + nextNode.getId());
            serverToTalkTo.close();

        } catch (UnknownHostException e) {
            logger.println(getTimestamp() + "ERROR unknown host.");
            e.printStackTrace();
        } catch (ConnectException e) {
            logger.println(getTimestamp() + "ERROR Server " + nextNode.getId() + " is not communicating.");
            System.out.println(nextNode.getId() + " is not online.");
        } catch (IOException e) {
            logger.println(getTimestamp() + "ERROR IO Exception.");
            e.printStackTrace();
        }
    }

    /**
     * Method used to build ring structure within the network.
     */
    private void buildRing() {
        logger.println(getTimestamp() + "Beginning ring construction.");
        for (int i = 0; i < nodes.size(); i++) {
            Node currentNode = nodes.get(i);
            try {
                logger.println(getTimestamp() + "Updating Server " + currentNode.getId() + " next node.");
                serverToTalkTo = new Socket(currentNode.getHost(), currentNode.getPort());
                BufferedReader inFromServer = new BufferedReader(new InputStreamReader(serverToTalkTo.getInputStream()));
                PrintWriter outToServer = new PrintWriter(new OutputStreamWriter(serverToTalkTo.getOutputStream()), true);

                String msgToSend = "NEXT NODE";
                logger.println(getTimestamp() + "Sending to " + currentNode.getId() + ": " + msgToSend);
                outToServer.println(msgToSend);

                String msg = inFromServer.readLine();
                logger.println(getTimestamp() + "Received from " + currentNode.getId() + ": " + msg);

                if (msg.equals("OK")) {
                    Node nextNode;

                    if (i < nodes.size()-1) {
                        nextNode = nodes.get(i+1);
                    } else {
                        nextNode = nodes.get(0);
                    }

                    msgToSend = nextNode.getId() + "," + nextNode.getHost() + "," + nextNode.getPort();
                    logger.println(getTimestamp() + "Sending to " + currentNode.getId() + ": " + msgToSend);
                    outToServer.println(msgToSend);
                    logger.println(getTimestamp() + "New next node sent to " + currentNode.getId());
                }

                logger.println(getTimestamp() + "Closing connection with " + currentNode.getId());
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

        for (Node node : nodes) {
            try {
                serverToTalkTo = new Socket(node.getHost(), node.getPort());
                PrintWriter outToServer = new PrintWriter(new OutputStreamWriter(serverToTalkTo.getOutputStream()), true);

                String msgToSend = "COMPLETE";
                logger.println(getTimestamp() + "Sending to " + node.getId() + ": " + msgToSend);
                outToServer.println(msgToSend);

                logger.println(getTimestamp() + "Closing connection with " + node.getId());
                serverToTalkTo.close();

            } catch (UnknownHostException e) {
                logger.println(getTimestamp() + "ERROR unknown host.");
                e.printStackTrace();
            } catch (ConnectException e) {
                logger.println(getTimestamp() + "ERROR Server " + node.getId() + " is not communicating.");
                System.out.println(node.getId() + " is not online.");
            } catch (IOException e) {
                logger.println(getTimestamp() + "ERROR IO Exception.");
                e.printStackTrace();
            }
        }
    }

    /**
     * Print next node
     */
    private void printNextNode() {
        System.out.println("Next node ID: " + nextNode.getId());
        System.out.println("Next node Host: " + nextNode.getHost());
        System.out.println("Next node Port: " + nextNode.getPort());
    }

    /**
     * Creates a new post and adds it to the queue
     * @param sender sender of post
     * @param recipient desired recipient of post
     * @param post message within the post
     */
    private void addPostToQueue(String sender, String recipient, String post) {
        postsToMake.add(new Post(sender, recipient, post));
    }

    /**
     * Adds post to shared resource
     */
    private void postMessage() {
        posts.add(postsToMake.get(0));
        postsToMake.remove(0);
    }

    /**
     * Retrieves post from shared resource.
     * @param recipient recipient of post to be retrieved
     * @return post for recipient
     */
    private Post getPost(String recipient) {
        Post toReturn = null;

        for (Post post : posts) {
            if (post.getRecipient().equals(recipient)) {
                toReturn = post;
                break;
            }
        }

        posts.remove(toReturn);

        return toReturn;
    }

    /**
     * Main server listening loop.
     * @throws IOException
     * @throws InterruptedException
     */
    private void listenForConnections() throws IOException, InterruptedException {
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
            logger.println(getTimestamp() + "Received from client: " + msg);
            String msgToSend = "OK";

            if (msg.equals("HELLO")) {
                logger.println(getTimestamp() + "Acknowledging client.");
                logger.println(getTimestamp() + "Sending to client: " + msgToSend);
                outToClient.println(msgToSend);

                msg = inFromClient.readLine();
                logger.println(getTimestamp() + "Received from client: " + msg);
                System.out.println(msg);

                msgToSend = "Hello Coordinator! From " + id;
                logger.println(getTimestamp() + "Sending to client: " + msgToSend);
                outToClient.println(msgToSend);
                logger.println(getTimestamp() + "Closing connection with client.");
                connected.close();

            } else if (msg.equals("NEXT NODE")) {
                logger.println(getTimestamp() + "Acknowledging client.");
                logger.println(getTimestamp() + "Sending to client: " + msgToSend);
                outToClient.println(msgToSend);

                msg = inFromClient.readLine();
                logger.println(getTimestamp() + "Received from client: " + msg);
                String[] splitMessage = msg.split(",");

                logger.println(getTimestamp() + "Parsing new next node");
                int nextId = Integer.parseInt(splitMessage[0]);
                String nextHost = splitMessage[1];
                int nextPort = Integer.parseInt(splitMessage[2]);

                setNextNode(new Node(nextId, nextHost, nextPort));
                printNextNode();
                logger.println(getTimestamp() + "Waiting for ring completion...");
                logger.println(getTimestamp() + "Closing connection with client.");
                connected.close();
            } else if (msg.equals("COMPLETE")) {
                logger.println(getTimestamp() + "Completion confirmed.");
//                TimeUnit.SECONDS.sleep(1);
                checkForElection();
                logger.println(getTimestamp() + "Closing connection with client.");
                connected.close();

            } else if (msg.equals("ELECTION")) {
                logger.println(getTimestamp() + "Acknowledging client.");
                logger.println(getTimestamp() + "Sending to client: " + msgToSend);
                outToClient.println(msgToSend);

                msg = inFromClient.readLine();
                logger.println(getTimestamp() + "Received from client: " + msg);
                logger.println(getTimestamp() + "Closing connection with client.");
                connected.close();

                logger.println(getTimestamp() + "Reading server IDs gathered so far.");
                String[] ids = msg.split(",");

                if (Integer.parseInt(ids[0]) == id) {
                    logger.println(getTimestamp() + "Ring fully explored.");
                    int newCoordinatorID = findNewCoordinatorID(ids);
                    if (coordinatorId != newCoordinatorID) {
                        logger.println(getTimestamp() + "Starting coordinator update propagation.");
                        updateCoordinatorID(id, newCoordinatorID);
                    } else {
                        logger.println(getTimestamp() + "Coordinator already been updated.");
                        logger.println(getTimestamp() + "No need for update cycle.");
                    }
                } else {
                    sendElection(msg);
                }
            } else if (msg.equals("UPDATE COORDINATOR ID")) {
                logger.println(getTimestamp() + "Acknowledging client.");
                logger.println(getTimestamp() + "Sending to client: " + msgToSend);
                outToClient.println(msgToSend);

                msg = inFromClient.readLine();
                logger.println(getTimestamp() + "Received from client: " + msg);

                logger.println(getTimestamp() + "Parsing message.");
                String[] splitMsg = msg.split(",");
                int startID = Integer.parseInt(splitMsg[0]);

                int newCoordinatorID = Integer.parseInt(splitMsg[1]);

                if (coordinatorId != newCoordinatorID) {
                    if (newCoordinatorID != id) {
                        updateCoordinatorEndpoint(newCoordinatorID);
                    } else {
                        coordinatorId = id;
                        coordinatorHost = host;
                        coordinatorPort = port;
                        isCoordinator = true;
                        logger.println(getTimestamp() + "I AM NOW COORDINATOR");
                    }
                } else {
                    logger.println(getTimestamp() + "New coordinator already set.");
                    logger.println(getTimestamp() + "Redundant election cancelled.");
                    continue;
                }

                if (startID != id) {
                    updateCoordinatorID(startID, newCoordinatorID);
                } else {
                    electionCompleted();
                }

                logger.println(getTimestamp() + "Closing connection with client.");
                connected.close();

            } else if (msg.equals("ELECTION COMPLETE")) {
                logger.println(getTimestamp() + "Election has been completed.");
                logger.println(getTimestamp() + "Acknowledging client.");
                logger.println(getTimestamp() + "Sending to client: " + msgToSend);
                outToClient.println(msgToSend);
                logger.println(getTimestamp() + "Closing connection with client.");
                connected.close();
                passToken();
            } else if (msg.equals("TOKEN")) {
                System.out.println("token received");
                logger.println(getTimestamp() + "RECEIVED TOKEN");
                logger.println(getTimestamp() + "Acknowledging client.");
                logger.println(getTimestamp() + "Sending to client: " + msgToSend);
                outToClient.println(msgToSend);
                logger.println(getTimestamp() + "Closing connection with client.");
                connected.close();

                passToken();
            } else if (msg.equals("POST")) {
                logger.println(getTimestamp() + "Acknowledging client.");
                logger.println(getTimestamp() + "Sending to client: " + msgToSend);
                outToClient.println(msgToSend);

                String sender = inFromClient.readLine();
                logger.println(getTimestamp() + "Received from client: " + msg);

                String recipient = inFromClient.readLine();
                logger.println(getTimestamp() + "Received from client: " + msg);

                String post = inFromClient.readLine();
                logger.println(getTimestamp() + "Received from client: " + msg);

                logger.println(getTimestamp() + "Closing connection with client.");
                connected.close();
                addPostToQueue(sender, recipient, post);

            } else if (msg.equals("PULL")) {
                logger.println(getTimestamp() + "Acknowledging client.");
                logger.println(getTimestamp() + "Sending to client: " + msgToSend);
                outToClient.println(msgToSend);

                String recipient = inFromClient.readLine();
                logger.println(getTimestamp() + "Received from client: " + msg);

                Post post = getPost(recipient);

                if (post != null) {
                    msgToSend = "INCOMING";
                    logger.println(getTimestamp() + "Sending to client: " + msgToSend);
                    outToClient.println(msgToSend);

                    msgToSend = post.getPost();
                    logger.println(getTimestamp() + "Sending to client: " + msgToSend);
                    outToClient.println(msgToSend);

                    msgToSend = post.getSender();
                    logger.println(getTimestamp() + "Sending to client: " + msgToSend);
                    outToClient.println(msgToSend);
                } else {
                    msgToSend = "NO MESSAGES";
                    logger.println(getTimestamp() + "Sending to client: " + msgToSend);
                    outToClient.println(msgToSend);
                }

                logger.println(getTimestamp() + "Closing connection with client.");
                connected.close();
            }
        }
    }

    /**
     * Main method
     * @param args array of arguments, contain server ID, host and port, as well as coordinator ID, host and port.
     * @throws IOException
     */
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

        //thread for server to listen on
        new Thread(() -> {
            try {
                finalSs.listenForConnections();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        //kick starts ring construction
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
