package net.johnewart.gearman.net;

public class ConnectionFactory {

    final String host;
    final int port;

    public ConnectionFactory(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public Connection build() {
        return new Connection(host, port);
    }
}
