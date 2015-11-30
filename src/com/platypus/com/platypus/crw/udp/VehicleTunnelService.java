package com.platypus.com.platypus.crw.udp;

import com.platypus.crw.udp.UdpConstants;
import com.platypus.crw.udp.UdpServer;
import com.platypus.crw.udp.UdpServer.Request;

import java.io.IOException;
import java.net.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Standalone service for supporting vehicle registration for UDP communication.
 *
 * Should be run standalone on a publicly visible server.
 *
 * @author Prasanna Velagapudi <psigen@gmail.com>
 */
public class VehicleTunnelService {
    private static final Logger logger = Logger.getLogger(VehicleTunnelService.class.getName());

    public static final int DEFAULT_UDP_PORT = 6077;
    public static final int IPTOS_LOWDELAY = 0x10;

    protected final UdpServer _udpServer;
    protected final Timer _registrationTimer = new Timer();
    protected final Map<SocketAddress, Client> _clients = new LinkedHashMap<>();

    protected static class Client {
        int ttl;
        String name;
        AtomicReference<SocketAddress> vehicleAddr = new AtomicReference<>();
        AtomicReference<SocketAddress> controllerAddr = new AtomicReference<>();

        InetSocketAddress tunnelToVehicleAddr;
        DatagramSocket tunnelToVehicle;

        InetSocketAddress tunnelToControllerAddr;
        DatagramSocket tunnelToController;
    }

    public VehicleTunnelService() {
        this(DEFAULT_UDP_PORT);
    }

    public VehicleTunnelService(int udpPort) {
        _udpServer = new UdpServer(udpPort);
        _udpServer.setHandler(_handler);
        _udpServer.start();

        _registrationTimer.scheduleAtFixedRate(_registrationTask, 0, UdpConstants.REGISTRATION_RATE_MS);
    }

    public void shutdown() {
        _udpServer.stop();
    }

    private final class TunnelHandler implements Runnable {
        private final DatagramSocket _source;
        private final DatagramSocket _tunnel;
        private final AtomicReference<SocketAddress> _destination;

        public TunnelHandler(DatagramSocket source, DatagramSocket tunnel,
                             AtomicReference<SocketAddress> destination) {
            _source = source;
            _tunnel = tunnel;
            _destination = destination;
        }

        @Override
        public void run() {
            final byte[] buffer = new byte[UdpConstants.MAX_PACKET_SIZE];
            final DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

            try {
                _source.receive(packet);
                logger.info("Forwarding [" + packet.getLength() + "]: " +
                        packet.getSocketAddress() + " -> " +
                        _source.getLocalPort() + " : " +
                        _tunnel.getLocalPort() + " -> " +
                        _destination.get());
                packet.setSocketAddress(_destination.get());
                _tunnel.send(packet);

            } catch (IOException e) {
                logger.severe("Error on tunnel '" + _destination.get() + "': " + e);
            }
        }
    }

    private final UdpServer.RequestHandler _handler = new UdpServer.RequestHandler() {

        @Override
        public void received(Request req) {
            try {
                final String command = req.stream.readUTF();
                switch (UdpConstants.COMMAND.fromStr(command)) {
                    case CMD_REGISTER:

                        synchronized(_clients) {
                            // Look for client in table
                            Client c = _clients.get(req.source);

                            // If not found, create a new entry
                            if (c == null) {
                                c = new Client();

                                try {
                                    // Create a tunnel from the controller to the vehicle.
                                    c.tunnelToVehicle = new DatagramSocket();
                                    c.tunnelToVehicle.setSendBufferSize(UdpConstants.MAX_PACKET_SIZE);
                                    c.tunnelToVehicle.setTrafficClass(IPTOS_LOWDELAY);
                                    c.tunnelToVehicleAddr = new InetSocketAddress(
                                            InetAddress.getLocalHost(), c.tunnelToVehicle.getLocalPort());

                                    // Create a tunnel from the vehicle to the controller.
                                    c.tunnelToController = new DatagramSocket();
                                    c.tunnelToController.setSendBufferSize(UdpConstants.MAX_PACKET_SIZE);
                                    c.tunnelToController.setTrafficClass(IPTOS_LOWDELAY);
                                    c.tunnelToControllerAddr = new InetSocketAddress(
                                            InetAddress.getLocalHost(), c.tunnelToController.getLocalPort());

                                    // Start threads that forward packets across the tunnel.
                                    new Thread(new TunnelHandler(c.tunnelToVehicle, c.tunnelToController,
                                                                 c.vehicleAddr)).start();
                                    new Thread(new TunnelHandler(c.tunnelToController, c.tunnelToVehicle,
                                                                 c.controllerAddr)).start();
                                } catch (SocketException e) {
                                    logger.severe("Unable to open UDP sockets.");
                                    throw new RuntimeException("Unable to open UDP sockets.", e);
                                }


                                _clients.put(req.source, c);
                            }

                            // Update the registration properties for this client.
                            c.vehicleAddr.set(req.source);
                            c.name = req.stream.readUTF();
                            c.ttl = UdpConstants.REGISTRATION_TIMEOUT_COUNT;
                        }
                        break;
                    case CMD_CONNECT:

                        // Unpack address to which to connect.
                        String hostname = req.stream.readUTF();
                        int port = req.stream.readInt();
                        InetSocketAddress addr = new InetSocketAddress(hostname, port);

                        // Find a client that matches the incoming connection request.
                        Client c = null;
                        synchronized(_clients) {
                            for (Map.Entry<SocketAddress, Client> e : _clients.entrySet()) {
                                if (e.getValue().tunnelToVehicleAddr.equals(addr)) {
                                    c = e.getValue();
                                    break;
                                }
                            }
                        }

                        // Ignore requests for non-tunneled clients.
                        if (c == null) {
                            logger.warning("Connection requested for invalid client: " + addr);
                            return;
                        }

                        logger.info("Connection request for: " +
                                req.source + " -> " +
                                c.tunnelToVehicleAddr.getPort() + " : " +
                                c.tunnelToControllerAddr.getPort() + " -> " +
                                c.vehicleAddr.get());

                        // Forward this connection request to the server in question.
                        synchronized(c) {
                            c.controllerAddr.set(req.source);
                            UdpServer.Response respCon = new UdpServer.Response(req.ticket, c.vehicleAddr.get());
                            respCon.stream.writeUTF(command);
                            respCon.stream.writeUTF(c.tunnelToControllerAddr.getAddress().getHostAddress());
                            respCon.stream.writeInt(c.tunnelToControllerAddr.getPort());
                            _udpServer.respond(respCon);
                        }
                        break;
                    case CMD_LIST:

                        // Create a response to the same client
                        UdpServer.Response respList = new UdpServer.Response(req);
                        respList.stream.writeUTF(command);

                        // List all of the clients
                        synchronized(_clients) {
                            respList.stream.writeInt(_clients.size());
                            for (Map.Entry<SocketAddress, Client> e : _clients.entrySet()) {
                                respList.stream.writeUTF(e.getValue().name);
                                respList.stream.writeUTF(e.getValue().tunnelToVehicleAddr.getAddress().getHostAddress());
                                respList.stream.writeInt(e.getValue().tunnelToVehicleAddr.getPort());
                            }
                        }
                        _udpServer.respond(respList);
                        break;
                    case CMD_REGISTER_POSE_LISTENER:
                    case CMD_SEND_POSE:
                    case CMD_SET_POSE:
                    case CMD_GET_POSE:
                    case CMD_REGISTER_IMAGE_LISTENER:
                    case CMD_SEND_IMAGE:
                    case CMD_CAPTURE_IMAGE:
                    case CMD_REGISTER_CAMERA_LISTENER:
                    case CMD_SEND_CAMERA:
                    case CMD_START_CAMERA:
                    case CMD_STOP_CAMERA:
                    case CMD_GET_CAMERA_STATUS:
                    case CMD_REGISTER_SENSOR_LISTENER:
                    case CMD_SEND_SENSOR:
                    case CMD_SET_SENSOR_TYPE:
                    case CMD_GET_SENSOR_TYPE:
                    case CMD_GET_NUM_SENSORS:
                    case CMD_REGISTER_VELOCITY_LISTENER:
                    case CMD_SEND_VELOCITY:
                    case CMD_SET_VELOCITY:
                    case CMD_GET_VELOCITY:
                    case CMD_REGISTER_WAYPOINT_LISTENER:
                    case CMD_SEND_WAYPOINT:
                    case CMD_START_WAYPOINTS:
                    case CMD_STOP_WAYPOINTS:
                    case CMD_GET_WAYPOINTS:
                    case CMD_GET_WAYPOINT_STATUS:
                    case CMD_IS_CONNECTED:
                    case CMD_IS_AUTONOMOUS:
                    case CMD_SET_AUTONOMOUS:
                    case CMD_SET_GAINS:
                    case CMD_GET_GAINS:
                    case UNKNOWN:
                        break;
                    default:
                        logger.log(Level.WARNING, "Ignoring unknown command: {0}", command);
                }
            } catch (IOException e) {
                logger.log(Level.WARNING, "Failed to parse request: {0}", req.ticket);
            }
        }

        @Override
        public void timeout(long ticket, SocketAddress destination) {
            logger.log(Level.SEVERE, "Unexpected timeout: {0}", ticket);
        }
    };

    // Removes outdated registrations from client list
    protected TimerTask _registrationTask = new TimerTask() {
        @Override
        public void run() {
            synchronized(_clients) {
                for (Iterator<Map.Entry<SocketAddress, Client>> it = _clients.entrySet().iterator(); it.hasNext();) {
                    Map.Entry<SocketAddress, Client> client = it.next();
                    if (client.getValue().ttl == 0) {
                        client.getValue().tunnelToVehicle.close();
                        client.getValue().tunnelToController.close();
                        it.remove();
                    } else {
                        client.getValue().ttl--;
                    }
                }
            }
        }
    };

    /**
     * Returns a map containing the current set of clients.
     *
     * @return a map from the socket address of clients to their text names
     */
    public Map<SocketAddress, String> getClients() {
        HashMap<SocketAddress, String> map = new LinkedHashMap<>();

        synchronized(_clients) {
            for (Client client : _clients.values()) {
                map.put(client.vehicleAddr.get(), client.name);
            }
        }

        return map;
    }

    /**
     * Simple startup script that runs the VehicleTunnelService using the
     * specified UDP port if available and prints a list of connected clients.
     *
     * @param args takes a UDP port number as the first argument.
     */
    public static void main(String args[]) throws UnknownHostException {
        final int port = (args.length > 0) ? Integer.parseInt(args[0]) : DEFAULT_UDP_PORT;
        final VehicleTunnelService service = new VehicleTunnelService();
        System.out.println("Started vehicle tunnel: " + InetAddress.getLocalHost() + ":" + port);

        // Periodically print the registered clients
        Timer printer = new Timer();
        printer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                Collection<Map.Entry<SocketAddress, String>> clients = service.getClients().entrySet();
                if (clients.size() > 0) {
                    System.out.println("CLIENT LIST:");
                    for (Map.Entry<SocketAddress, String> e : clients) {
                        System.out.println("\t" + e.getValue() + " : " + e.getKey());
                    }
                } else {
                    System.out.println("NO CLIENTS.");
                }
            }
        }, 0, 1000);
    }
}