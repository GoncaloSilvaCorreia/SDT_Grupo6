package lib2p;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class Libp2pNode {

    private String nodeId;
    private String peerId;
    private List<String> peers;
    private Map<String, List<MessageListener>> subscribers;
    private boolean isRunning;

    public interface MessageListener {
        void onMessageReceived(String message, String senderId);
    }

    /**
     * Construtor do nó libp2p
     */
    public Libp2pNode(String peerId) {
        this.nodeId = UUID.randomUUID().toString().substring(0, 8);
        this.peerId = peerId;
        this.peers = new CopyOnWriteArrayList<>();
        this.subscribers = new HashMap<>();
        this.isRunning = false;
    }

    /**
     * Iniciar nó
     */
    public void start() {
        this.isRunning = true;
        System.out.println("Nó libp2p iniciado");
        System.out.println("   ID do Nó: " + nodeId);
        System.out.println("   Peer ID: " + peerId);
        System.out.println("   Porta: " + LibP2pConfig.LISTEN_PORT);
    }

    /**
     * Conectar a outro peer
     */
    public void connectToPeer(String peerId) {
        if (!peers.contains(peerId)) {
            peers.add(peerId);
            System.out.println("Conectado ao peer: " + peerId);
        }
    }

    /**
     * Subscrever a um tópico
     */
    public void subscribe(String topic, MessageListener listener) {
        subscribers.computeIfAbsent(topic, k -> new CopyOnWriteArrayList<>())
                .add(listener);
        System.out.println("Subscrito ao tópico: " + topic);
    }

    /**
     * Obter lista de peers conectados
     */
    public List<String> getPeers() {
        return new ArrayList<>(peers);
    }

    /**
     * Parar nó
     */
    public void stop() {
        this.isRunning = false;
        this.peers.clear();
        System.out.println("Nó libp2p parado");
    }

    /**
     * Getters
     */
    public String getPeerId() {
        return peerId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public boolean isRunning() {
        return isRunning;
    }
}