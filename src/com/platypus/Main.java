package com.platypus;

import com.platypus.crw.AsyncVehicleServer;
import com.platypus.crw.FunctionObserver;
import com.platypus.crw.SimpleBoatSimulator;
import com.platypus.crw.udp.UdpVehicleServer;
import com.platypus.crw.udp.UdpVehicleService;
import com.platypus.crw.udp.VehicleRegistryService;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        // Register with the default "registry.senseplatypus.com" server.
        InetSocketAddress registryAddress = new InetSocketAddress(
                "localhost", VehicleRegistryService.DEFAULT_UDP_PORT);

        // Create a *fake* vehicle and connect it to the registry.
        SimpleBoatSimulator sim = new SimpleBoatSimulator();
        final UdpVehicleService service = new UdpVehicleService(sim);
        service.addRegistry(registryAddress);
        System.out.println("SERVICE:" + service.getSocketAddress());

        // Create a simple proxy (as would be used in a GUI) and connect it to the registry.
        final UdpVehicleServer server = new UdpVehicleServer();
        server.setRegistryService(registryAddress);

        // Print the dummy vehicle's currently set registries.
        System.out.println("Vehicle registered at:");
        for (InetSocketAddress address : service.listRegistries()) {
            System.out.println("\t" + address);
        }

        // Loop forever printing connections on the dummy GUI.
        while (true) {
            Thread.sleep(1000);
            server.getVehicleServices(new FunctionObserver<Map<SocketAddress, String>>() {
                @Override
                public void completed(final Map<SocketAddress, String> socketAddressStringMap) {
                    System.out.println("Found:");
                    for (Map.Entry<SocketAddress, String> entry: socketAddressStringMap.entrySet()) {
                        System.out.println("\t" + entry);

                        server.setVehicleService(entry.getKey());
                        try { Thread.sleep(1000); } catch (InterruptedException e) {}
                        server.isConnected(new FunctionObserver<Boolean>() {
                            @Override
                            public void completed(Boolean aBoolean) {
                                System.out.println("\t\tconnected:" + aBoolean);
                            }

                            @Override
                            public void failed(FunctionError functionError) {
                                System.out.println("\t\tfailed:" + functionError);
                            }
                        });
                    }
                }

                @Override
                public void failed(final FunctionError functionError) {
                    System.err.println("Failed to connect to registry.");
                }
            });
        }
    }
}
