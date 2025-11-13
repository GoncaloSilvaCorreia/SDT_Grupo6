package lib2p;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Libp2pLeader - HTTP API that accepts uploads, registers peers, propagates
 * tentative document updates to peers and coordinates commit after majority confirmations.
 */
public class Libp2pLeader {

    private static final int HTTP_PORT = LibP2pConfig.LEADER_HTTP_PORT;
    private static Libp2pNode leaderNode;
    // map peerId -> "ip:port"
    private static final Map<String, String> peerAddressMap = new ConcurrentHashMap<>();
    private static final String UPLOAD_DIR = "uploads";

    // Current committed vector of CIDs and its version
    private static final List<String> currentDocumentCidVector = Collections.synchronizedList(new ArrayList<>());
    private static final AtomicInteger documentVectorVersion = new AtomicInteger(0);

    // Pending vectors (version -> vector), pending embeddings (version -> cid -> embedding)
    private static final Map<Integer, List<String>> pendingVectors = new ConcurrentHashMap<>();
    private static final Map<Integer, Map<String, String>> pendingEmbeddings = new ConcurrentHashMap<>();

    // Confirmations: version -> (peerId -> hash)
    private static final Map<Integer, Map<String, String>> confirmationsByVersion = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        System.out.println("Iniciando Lider com libp2p...\n");

        // Criar diretório de uploads se não existir
        Files.createDirectories(Paths.get(UPLOAD_DIR));

        // Criar nó do líder
        leaderNode = new Libp2pNode("leader");
        leaderNode.start();

        // Criar servidor HTTP para API
        HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", HTTP_PORT), 0);

        // Endpoint para enviar mensagens (broadcast)
        server.createContext("/api/messages/send", new SendMessageHandler());

        // Endpoint para listar peers
        server.createContext("/api/peers", new ListPeersHandler());

        // Endpoint para conectar a um peer (registo)
        server.createContext("/api/peers/connect", new ConnectPeerHandler());

        // Endpoint para upload de ficheiros
        server.createContext("/api/files/upload", new UploadHandler());

        // Endpoint para peers enviarem confirmações (peerId:version:hash)
        server.createContext("/api/peers/confirm", new ConfirmHandler());

        server.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(10));
        server.start();

        System.out.println("API do Lider iniciada na porta " + HTTP_PORT);
        System.out.println("Aceder de outro PC: http://" + LibP2pConfig.LEADER_HOST + ":" + HTTP_PORT);
        System.out.println("A espera de requisicoes...\n");
    }

    /** Handler para upload de ficheiros */
    static class UploadHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCors(exchange);
            if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 204, "");
                return;
            }
            exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=UTF-8");

            if ("POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                try {
                    // Obter nome do ficheiro do header ou query parameter
                    String filename = getFilename(exchange);
                    if (filename == null || filename.isEmpty()) {
                        filename = "ficheiro_" + System.currentTimeMillis();
                    }

                    // Ler conteúdo do ficheiro
                    String sanitizedFilename = sanitizeFilename(filename);
                    String filepath = UPLOAD_DIR + File.separator + sanitizedFilename;

                    try (InputStream is = exchange.getRequestBody()) {
                        Files.copy(is, Paths.get(filepath));
                    }

                    System.out.println("Ficheiro recebido: " + filename + " -> " + filepath);

                    // Lógica de criação de uma nova versão PENDENTE do vetor de documentos
                    processNewDocumentTentative(filepath);

                    String response = "Ficheiro " + filename + " recebido e pendente de commit";
                    sendResponse(exchange, 200, response);

                } catch (Exception e) {
                    System.err.println("Erro no upload: " + e.getMessage());
                    e.printStackTrace();
                    String error = "Erro no upload: " + e.getMessage();
                    sendResponse(exchange, 500, error);
                }
            } else {
                sendResponse(exchange, 405, "Metodo nao permitido");
            }
        }

        private String getFilename(HttpExchange exchange) {
            String filename = exchange.getRequestHeaders().getFirst("filename");
            if (filename != null && !filename.isEmpty()) {
                return filename;
            }
            String query = exchange.getRequestURI().getQuery();
            if (query != null && query.contains("filename=")) {
                return query.split("filename=")[1].split("&")[0];
            }
            return null;
        }

        private String sanitizeFilename(String filename) {
            return filename.replaceAll("[^a-zA-Z0-9._-]", "_");
        }
    }

    /**
     * Create a tentative/pending new version (current + new CID) and propagate the tentative update
     * to all registered peers. The leader does NOT replace the current vector until majority confirmation.
     */
    private static void processNewDocumentTentative(String filepath) throws Exception {
        // 1. Gerar CID a partir do conteúdo do ficheiro
        String cid = generateCid(filepath);

        // 2. Determine new version id (tentative)
        int newVersion = documentVectorVersion.get() + 1;

        // 3. Build pending vector without mutating currentDocumentCidVector
        List<String> newVector;
        synchronized (currentDocumentCidVector) {
            newVector = new ArrayList<>(currentDocumentCidVector);
            if (!newVector.contains(cid)) {
                newVector.add(cid);
            }
        }
        pendingVectors.put(newVersion, newVector);

        // 4. Generate embedding placeholder and store in pendingEmbeddings
        String embedding = "embedding_for_" + cid.substring(0, Math.min(10, cid.length()));
        Map<String, String> embMap = new ConcurrentHashMap<>();
        embMap.put(cid, embedding);
        pendingEmbeddings.put(newVersion, embMap);

        // Ensure confirmations storage initialized
        confirmationsByVersion.putIfAbsent(newVersion, new ConcurrentHashMap<>());

        // 5. Propagate tentative update to peers
        DocumentUpdate update = new DocumentUpdate(newVersion, cid, embedding);
        propagateUpdateToPeers(update);
        System.out.println("Vetor pendente criado (versao " + newVersion + ") com CID " + cid);
    }

    private static String generateCid(String filePath) throws Exception {
        byte[] fileBytes = Files.readAllBytes(Paths.get(filePath));
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(fileBytes);
        StringBuilder hexString = new StringBuilder();
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }

    private static void propagateUpdateToPeers(DocumentUpdate update) {
        // Format sent to peers: "version;cid;embedding"
        String message = update.getVersion() + ";" + update.getCid();
        if (update.getEmbedding() != null && !update.getEmbedding().isEmpty()) {
            message += ";" + update.getEmbedding();
        }
        System.out.println("A propagar atualização pendente para os peers: " + message);

        List<String> peerIds = new ArrayList<>(peerAddressMap.keySet());
        for (String peerId : peerIds) {
            sendMessageToPeer(peerId, message);
        }
    }

    /** Handler para enviar mensagens para todos os peers registados (broadcast) */
    static class SendMessageHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCors(exchange);
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "Metodo nao permitido");
                return;
            }

            String message = readRequestBody(exchange);
            if (message == null || message.trim().isEmpty()) {
                sendResponse(exchange, 400, "Mensagem vazia");
                return;
            }

            System.out.println("Lider recebeu (broadcast): " + message);

            int sent = 0;
            List<String> peerIds = new ArrayList<>(peerAddressMap.keySet());
            for (String peerId : peerIds) {
                boolean ok = sendMessageToPeer(peerId, message);
                if (ok) sent++;
            }

            String response = "Mensagem enviada para " + sent + " de " + peerAddressMap.size() + " peers";
            sendResponse(exchange, 200, response);
        }
    }

    /** Handler para listar peers e os seus endereços */
    static class ListPeersHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCors(exchange);
            exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=UTF-8");

            StringBuilder sb = new StringBuilder();
            sb.append("Peers conectados: ").append(peerAddressMap.size()).append("\n");
            for (Map.Entry<String, String> entry : peerAddressMap.entrySet()) {
                sb.append("- Peer ID: ").append(entry.getKey())
                        .append(", Endereço: ").append(entry.getValue()).append("\n");
            }

            String response = sb.toString();
            sendResponse(exchange, 200, response);
        }
    }

    /** Handler para registar um peer. Aceita o formato "peerId:ip:port" */
    static class ConnectPeerHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCors(exchange);
            exchange.getResponseHeaders().add("Content-Type", "text/plain");

            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "Metodo nao permitido");
                return;
            }

            String body = readRequestBody(exchange);
            if (body == null || body.trim().isEmpty()) {
                sendResponse(exchange, 400, "Corpo da mensagem vazio. Use o formato peerId:ip:port");
                return;
            }

            // aceitar format "peerId:ip:port"
            String[] parts = body.trim().split(":");
            if (parts.length < 3) {
                sendResponse(exchange, 400, "Formato inválido. Use peerId:ip:port");
                return;
            }

            String peerId = parts[0];
            String port = parts[parts.length - 1];
            String ip = String.join(":", Arrays.copyOfRange(parts, 1, parts.length - 1));
            String addr = ip + ":" + port;

            // armazenar
            peerAddressMap.put(peerId, addr);
            leaderNode.connectToPeer(peerId); // mantém comportamento anterior
            System.out.println("Peer " + peerId + " conectado em " + addr + "!");

            sendResponse(exchange, 200, "Peer conectado: " + peerId);
        }
    }

    /** Handler for receiving confirmations from peers: "peerId:version:hash" */
    static class ConfirmHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            addCors(exchange);
            exchange.getResponseHeaders().add("Content-Type", "text/plain; charset=UTF-8");

            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "Metodo nao permitido");
                return;
            }

            String body = readRequestBody(exchange);
            if (body == null || body.trim().isEmpty()) {
                sendResponse(exchange, 400, "Corpo vazio");
                return;
            }

            String[] parts = body.trim().split(":");
            if (parts.length < 3) {
                sendResponse(exchange, 400, "Formato inválido. Use peerId:version:hash");
                return;
            }

            String peerId = parts[0];
            int version;
            try {
                version = Integer.parseInt(parts[1]);
            } catch (NumberFormatException nfe) {
                sendResponse(exchange, 400, "Versão inválida");
                return;
            }
            String hash = parts[2];

            confirmationsByVersion.putIfAbsent(version, new ConcurrentHashMap<>());
            confirmationsByVersion.get(version).put(peerId, hash);
            System.out.println("Confirmação recebida de " + peerId + " para versão " + version + " -> " + hash);

            // check majority for this version
            try {
                checkAndCommitVersionIfMajority(version);
            } catch (Exception e) {
                System.err.println("Erro ao validar commits: " + e.getMessage());
            }

            sendResponse(exchange, 200, "Confirmacao recebida");
        }
    }

    /** Checks confirmations for a version and commits if a majority agrees on same hash. */
    private static void checkAndCommitVersionIfMajority(int version) throws Exception {
        Map<String, String> confirmations = confirmationsByVersion.get(version);
        if (confirmations == null) return;

        int registeredPeers = peerAddressMap.size();
        if (registeredPeers == 0) {
            // If no peers registered, auto-commit
            System.out.println("Nenhum peer registado — commit automático da versão " + version);
            commitVersion(version);
            return;
        }

        // Count occurrences of each hash
        Map<String, Integer> hashCounts = new HashMap<>();
        for (String hash : confirmations.values()) {
            hashCounts.merge(hash, 1, Integer::sum);
        }

        // Find top hash and count
        String topHash = null;
        int topCount = 0;
        for (Map.Entry<String, Integer> e : hashCounts.entrySet()) {
            if (e.getValue() > topCount) {
                topHash = e.getKey();
                topCount = e.getValue();
            }
        }

        int majority = (registeredPeers / 2) + 1;
        if (topHash != null && topCount >= majority) {
            System.out.println("Maioria atingida para versao " + version + " (hash " + topHash + ", count=" + topCount + "). Efetuando commit.");
            commitVersion(version);
        } else {
            System.out.println("Ainda sem maioria para versao " + version + " (topCount=" + topCount + ", needed=" + majority + ")");
        }
    }

    /** Commits a pending version: send commit to all peers and apply locally. */
    private static void commitVersion(int version) {
        List<String> vector = pendingVectors.get(version);
        if (vector == null) {
            System.err.println("Sem vetor pendente para commit na versao " + version);
            return;
        }

        // Build payload to send to peers: "version;cid1,cid2,..."
        String joined = String.join(",", vector);
        String payload = version + ";" + joined;

        // Send commit to all peers
        List<String> peerIds = new ArrayList<>(peerAddressMap.keySet());
        for (String peerId : peerIds) {
            sendCommitToPeer(peerId, payload);
        }

        // Apply locally
        synchronized (currentDocumentCidVector) {
            currentDocumentCidVector.clear();
            currentDocumentCidVector.addAll(vector);
            documentVectorVersion.set(version);
        }

        // Remove pending and confirmations
        pendingVectors.remove(version);
        pendingEmbeddings.remove(version);
        confirmationsByVersion.remove(version);

        System.out.println("Versao " + version + " committed localmente. Vector atual: " + currentDocumentCidVector);
    }

    /** Envia commit para peerId usando o addr guardado em peerAddressMap (POST /api/peers/commit). */
    private static boolean sendCommitToPeer(String peerId, String commitPayload) {
        try {
            String addr = peerAddressMap.get(peerId);
            if (addr == null || addr.trim().isEmpty()) {
                System.err.println("Sem endereco para " + peerId + " — salto envio.");
                return false;
            }
            String[] a = addr.split(":");
            if (a.length < 2) {
                System.err.println("Endereco invalido para " + peerId + ": " + addr);
                return false;
            }
            String portStr = a[a.length - 1];
            int port = Integer.parseInt(portStr);
            String peerIP = String.join(":", Arrays.copyOfRange(a, 0, a.length - 1));

            String peerUrl = "http://" + peerIP + ":" + port + "/api/peers/commit";
            URL url = new URL(peerUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "text/plain; charset=UTF-8");
            conn.setDoOutput(true);
            conn.setConnectTimeout(4000);
            conn.setReadTimeout(4000);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(commitPayload.getBytes(StandardCharsets.UTF_8));
                os.flush();
            }

            int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                System.err.println("Falha ao enviar commit para " + peerId + " (código: " + responseCode + ")");
                // read error
                try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getErrorStream() != null ? conn.getErrorStream() : conn.getInputStream(), StandardCharsets.UTF_8))) {
                    StringBuilder response = new StringBuilder();
                    String responseLine;
                    while ((responseLine = br.readLine()) != null) {
                        response.append(responseLine.trim());
                    }
                    System.err.println("Resposta do peer: " + response.toString());
                } catch (Exception ignored) {}
                conn.disconnect();
                return false;
            }

            conn.disconnect();
            return true;

        } catch (Exception e) {
            System.err.println("Erro ao enviar commit para " + peerId + ": " + e.getMessage());
            return false;
        }
    }

    /** Envia mensagem para peerId usando o addr guardado em peerAddressMap */
    private static boolean sendMessageToPeer(String peerId, String message) {
        try {
            String addr = peerAddressMap.get(peerId);
            if (addr == null || addr.trim().isEmpty()) {
                System.err.println("Sem endereco para " + peerId + " — salto envio.");
                return false;
            }
            String[] a = addr.split(":");
            if (a.length < 2) {
                System.err.println("Endereco invalido para " + peerId + ": " + addr);
                return false;
            }
            String portStr = a[a.length - 1];
            int port = Integer.parseInt(portStr);
            String peerIP = String.join(":", Arrays.copyOfRange(a, 0, a.length - 1));

            String peerUrl = "http://" + peerIP + ":" + port + "/api/messages/receive";
            URL url = new URL(peerUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "text/plain; charset=UTF-8");
            conn.setDoOutput(true);
            conn.setConnectTimeout(3000);
            conn.setReadTimeout(3000);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(message.getBytes(StandardCharsets.UTF_8));
                os.flush();
            }

            int responseCode = conn.getResponseCode();
            if (responseCode != 200) {
                System.err.println("Falha ao enviar mensagem para " + peerId + " (código: " + responseCode + ")");
                return false;
            }

            conn.disconnect();
            return true;

        } catch (Exception e) {
            System.err.println("Erro ao enviar mensagem para " + peerId + ": " + e.getMessage());
            return false;
        }
    }

    private static String readRequestBody(HttpExchange exchange) throws IOException {
        try (InputStream is = exchange.getRequestBody();
             BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) sb.append(line);
            return sb.toString();
        }
    }

    private static void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
        byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(statusCode, responseBytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(responseBytes);
        }
    }

    private static void addCors(HttpExchange exchange) {
        exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        exchange.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type, filename");
    }
}