package lib2p;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Libp2pPeer {

    private static String peerId;
    private static Libp2pNode peerNode;
    private static int peerPort;
    private static String localIp;

    // Committed vector and its version
    private static final List<String> documentCidVector = Collections.synchronizedList(new ArrayList<>());
    private static final AtomicInteger documentVectorVersion = new AtomicInteger(0);

    // Pending (tentative) vectors and embeddings: version -> vector; version -> (cid->embedding)
    private static final Map<Integer, List<String>> pendingVectors = new ConcurrentHashMap<>();
    private static final Map<Integer, Map<String, String>> pendingEmbeddings = new ConcurrentHashMap<>();

    // Armazenamento de embeddings por CID (após commit)
    private static final Map<String, String> documentEmbeddings = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Uso: java lib2p.Libp2pPeer <peerId> [port]");
            System.out.println("Exemplo: java lib2p.Libp2pPeer peer1 8091");
            return;
        }

        peerId = args[0];

        if (args.length >= 2) {
            try {
                peerPort = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                peerPort = 8091;
            }
        } else {
            Integer n = extractTrailingNumber(peerId);
            if (n != null) peerPort = 8090 + n; // peer1 -> 8091
            else peerPort = 8091;
        }

        System.out.println("Iniciando Peer com libp2p...");

        peerNode = new Libp2pNode(peerId);
        peerNode.start();

        // Connect logically to leader
        peerNode.connectToPeer("leader");

        // get local IP
        localIp = getLocalIpAddress();
        if (localIp == null) localIp = "127.0.0.1";

        // register with leader
        registerPeerWithLeader(peerId, localIp, peerPort);

        // Create HTTP server for peer endpoints
        HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", peerPort), 0);

        // Endpoint to receive tentative updates from leader
        server.createContext("/api/messages/receive", new ReceiveMessageHandler());

        // Endpoint for leader commit
        server.createContext("/api/peers/commit", new CommitHandler());

        server.setExecutor(Executors.newFixedThreadPool(4));
        server.start();

        System.out.println("Peer " + peerId + " pronto!");
        System.out.println("À escuta de mensagens do líder (ip: " + localIp + ", port: " + peerPort + ")\n");

        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            System.out.println("Encerrando peer " + peerId + "...");
            peerNode.stop();
        }
    }

    private static Integer extractTrailingNumber(String id) {
        String num = id.replaceAll("^.*?(\\d+)$", "$1");
        if (num.equals(id)) {
            return null;
        }
        try {
            return Integer.parseInt(num);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Registar o peer no líder (envia "peerId:ip:port" no body)
     */
    private static void registerPeerWithLeader(String peerId, String ip, int port) {
        try {
            String leaderUrl = "http://" + LibP2pConfig.LEADER_HOST + ":" +
                    LibP2pConfig.LEADER_HTTP_PORT + "/api/peers/connect";

            URL url = new URL(leaderUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "text/plain; charset=UTF-8");
            conn.setDoOutput(true);
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);

            String payload = peerId + ":" + ip + ":" + port;

            try (OutputStream os = conn.getOutputStream()) {
                os.write(payload.getBytes(StandardCharsets.UTF_8));
                os.flush();
            }

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                System.out.println("Registado no lider com sucesso!");
            } else {
                System.err.println("Erro ao registar no líder (código: " + responseCode + ")");
                try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getErrorStream() != null ? conn.getErrorStream() : conn.getInputStream(), StandardCharsets.UTF_8))) {
                    StringBuilder response = new StringBuilder();
                    String responseLine;
                    while ((responseLine = br.readLine()) != null) {
                        response.append(responseLine.trim());
                    }
                    System.err.println("Resposta do lider: " + response.toString());
                }
            }
            conn.disconnect();

        } catch (Exception e) {
            System.err.println("Erro ao registar no lider: " + e.getMessage());
        }
    }

    /**
     * Handler para receber mensagens do líder (tentative updates)
     * Expected format: "version;cid" or "version;cid;embedding..."
     */
    static class ReceiveMessageHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCors(exchange);
            exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=UTF-8");

            if ("POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                try {
                    String message = readRequestBody(exchange);

                    if (message != null && !message.isEmpty()) {
                        System.out.println("[" + peerId.toUpperCase() + "] Mensagem recebida:");
                        System.out.println("   " + message + "\n");

                        // Process the tentative update (store pending + send confirmation)
                        handleDocumentUpdateTentative(message);
                    }

                    String response = "Mensagem recebida";
                    sendResponse(exchange, 200, response);

                } catch (Exception e) {
                    System.err.println("Erro ao processar mensagem: " + e.getMessage());
                    e.printStackTrace();
                    sendResponse(exchange, 500, "Erro: " + e.getMessage());
                }
            } else if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "");
            } else {
                sendResponse(exchange, 405, "Metodo nao permitido");
            }
        }

        private void handleDocumentUpdateTentative(String message) {
            try {
                String[] parts = message.split(";");
                if (parts.length < 2) {
                    System.err.println("Formato da mensagem de atualização inválido: " + message);
                    return;
                }

                int receivedVersion;
                try {
                    receivedVersion = Integer.parseInt(parts[0]);
                } catch (NumberFormatException nfe) {
                    System.err.println("Versão inválida na mensagem: " + parts[0]);
                    return;
                }

                String cid = parts[1];

                String embedding = null;
                if (parts.length > 2) {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 2; i < parts.length; i++) {
                        if (i > 2) sb.append(";");
                        sb.append(parts[i]);
                    }
                    embedding = sb.toString().trim();
                }

                System.out.println("Tentativa de atualização recebida: Versão=" + receivedVersion + ", CID=" + cid + (embedding != null ? ", embedding recebido" : ", sem embedding"));

                // Check version continuity: expected = current + 1
                int expected = documentVectorVersion.get() + 1;
                if (receivedVersion != expected) {
                    System.err.println("Conflito de versão: recebido " + receivedVersion + " esperado " + expected + ". Iniciando resolução (não implementado).");
                    // In case of conflict, do not proceed (future resolution path)
                    return;
                }

                // Build tentative vector (copy current + new cid if not present)
                List<String> tentative;
                synchronized (documentCidVector) {
                    tentative = new ArrayList<>(documentCidVector);
                    if (!tentative.contains(cid)) {
                        tentative.add(cid);
                    }
                }
                pendingVectors.put(receivedVersion, tentative);

                // Store pending embedding
                pendingEmbeddings.putIfAbsent(receivedVersion, new ConcurrentHashMap<>());
                if (embedding != null && !embedding.isEmpty()) {
                    pendingEmbeddings.get(receivedVersion).put(cid, embedding);
                }

                // Compute hash of tentative vector and send confirmation to leader
                String hash = computeVectorHash(tentative);
                System.out.println("Hash do vetor tentativo (versao " + receivedVersion + "): " + hash);
                sendConfirmationToLeader(peerId, receivedVersion, hash);

            } catch (Exception e) {
                System.err.println("Falha ao processar a atualização do documento: " + e.getMessage());
            }
        }
    }

    /** Compute a SHA-256 hex of the joined CIDs (comma-separated) */
    private static String computeVectorHash(List<String> vector) throws Exception {
        String joined = String.join(",", vector);
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(joined.getBytes(StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) sb.append('0');
            sb.append(hex);
        }
        return sb.toString();
    }

    /** Send confirmation to leader: body format peerId:version:hash */
    private static void sendConfirmationToLeader(String peerId, int version, String hash) {
        try {
            String leaderUrl = "http://" + LibP2pConfig.LEADER_HOST + ":" + LibP2pConfig.LEADER_HTTP_PORT + "/api/peers/confirm";
            URL url = new URL(leaderUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "text/plain; charset=UTF-8");
            conn.setDoOutput(true);
            conn.setConnectTimeout(4000);
            conn.setReadTimeout(4000);

            String payload = peerId + ":" + version + ":" + hash;
            try (OutputStream os = conn.getOutputStream()) {
                os.write(payload.getBytes(StandardCharsets.UTF_8));
                os.flush();
            }

            int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                System.err.println("Erro ao enviar confirmacao ao lider (codigo " + responseCode + ")");
                try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getErrorStream() != null ? conn.getErrorStream() : conn.getInputStream(), StandardCharsets.UTF_8))) {
                    StringBuilder response = new StringBuilder();
                    String responseLine;
                    while ((responseLine = br.readLine()) != null) response.append(responseLine.trim());
                    System.err.println("Resposta do lider: " + response.toString());
                }
            } else {
                System.out.println("Confirmacao enviada ao lider para versao " + version);
            }
            conn.disconnect();
        } catch (Exception e) {
            System.err.println("Falha ao enviar confirmacao ao lider: " + e.getMessage());
        }
    }

    /** Handler para aplicar commit enviado pelo líder.
     *  Expected body formats:
     *    1) "version;cid1,cid2,..." (leader includes full vector)
     *    2) "version" (peer will fallback to its pendingVectors if present)
     */
    static class CommitHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCors(exchange);
            exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=UTF-8");

            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "Metodo nao permitido");
                return;
            }

            String body;
            try {
                body = readRequestBody(exchange);
            } catch (Exception e) {
                sendResponse(exchange, 500, "Erro a ler corpo");
                return;
            }

            if (body == null || body.trim().isEmpty()) {
                sendResponse(exchange, 400, "Corpo vazio");
                return;
            }

            // Parse
            try {
                String[] parts = body.trim().split(";", 2);
                int version = Integer.parseInt(parts[0]);

                List<String> committedVector = null;
                if (parts.length == 2 && parts[1] != null && !parts[1].isEmpty()) {
                    String[] cids = parts[1].split(",");
                    committedVector = new ArrayList<>(Arrays.asList(cids));
                } else {
                    // fallback to pendingVectors
                    committedVector = pendingVectors.get(version);
                }

                if (committedVector == null) {
                    sendResponse(exchange, 400, "Nenhum vetor pendente para a versão " + version);
                    return;
                }

                // Apply commit: replace current vector and move pending embeddings
                synchronized (documentCidVector) {
                    documentCidVector.clear();
                    documentCidVector.addAll(committedVector);
                    documentVectorVersion.set(version);
                }

                Map<String, String> embMap = pendingEmbeddings.getOrDefault(version, Collections.emptyMap());
                for (Map.Entry<String, String> e : embMap.entrySet()) {
                    documentEmbeddings.put(e.getKey(), e.getValue());
                }

                // cleanup pending
                pendingVectors.remove(version);
                pendingEmbeddings.remove(version);

                System.out.println("Commit aplicado localmente: versão " + version + ", vector: " + documentCidVector);
                sendResponse(exchange, 200, "Commit aplicado");

            } catch (NumberFormatException nfe) {
                sendResponse(exchange, 400, "Versao invalida");
            } catch (Exception e) {
                System.err.println("Erro ao aplicar commit: " + e.getMessage());
                sendResponse(exchange, 500, "Erro interno");
            }
        }
    }

    private static String getLocalIpAddress() {
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            return socket.getLocalAddress().getHostAddress();
        } catch (Exception e) {
            try {
                return InetAddress.getLocalHost().getHostAddress();
            } catch (Exception ex) {
                return null;
            }
        }
    }

    private static void addCors(HttpExchange exchange) {
        exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        exchange.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type");
    }

    // Utility shared by all handlers
    private static String readRequestBody(HttpExchange exchange) throws IOException {
        try (InputStream is = exchange.getRequestBody();
             BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) sb.append(line);
            return sb.toString();
        }
    }

    // Utility shared by all handlers
    private static void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
        byte[] bytes = response == null ? new byte[0] : response.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            if (bytes.length > 0) os.write(bytes);
        }
    }
}