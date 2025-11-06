package lib2p;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Libp2pLeader {

    private static final int HTTP_PORT = 9091;
    private static Libp2pNode leaderNode;
    // map peerId -> "ip:port"
    private static final Map<String, String> peerAddressMap = new ConcurrentHashMap<>();
    private static final String UPLOAD_DIR = "uploads";

    // Vetor de CIDs de documentos e sua versão
    private static final List<String> documentCidVector = new ArrayList<>();
    private static final AtomicInteger documentVectorVersion = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        System.out.println("Iniciando Lider com libp2p...\n");

        // Criar diretório de uploads se não existir
        new File(UPLOAD_DIR).mkdirs();

        // Criar nó do líder
        leaderNode = new Libp2pNode("leader");
        leaderNode.start();

        // Criar servidor HTTP para API
        HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", HTTP_PORT), 0);

        // Endpoint para enviar mensagens
        server.createContext("/api/messages/send", new SendMessageHandler());

        // Endpoint para listar peers
        server.createContext("/api/peers", new ListPeersHandler());

        // Endpoint para conectar a um peer (registo)
        server.createContext("/api/peers/connect", new ConnectPeerHandler());

        // Endpoint para upload de ficheiros
        server.createContext("/api/files/upload", new UploadHandler());

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
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().add("Content-Type", "text/plain");

            if ("POST".equals(exchange.getRequestMethod())) {
                try {
                    // Obter nome do ficheiro do header ou query parameter
                    String filename = getFilename(exchange);
                    if (filename == null || filename.isEmpty()) {
                        filename = "ficheiro_" + System.currentTimeMillis();
                    }

                    // Ler conteúdo do ficheiro
                    InputStream is = exchange.getRequestBody();
                    String sanitizedFilename = sanitizeFilename(filename);
                    String filepath = UPLOAD_DIR + File.separator + sanitizedFilename;

                    // Escrever ficheiro no disco
                    Files.copy(is, Paths.get(filepath));

                    System.out.println("Ficheiro recebido: " + filename);

                    // Lógica de atualização do vetor de documentos
                    processNewDocument(filepath);

                    String response = "Ficheiro " + filename + " enviado com sucesso e propagado para os peers";
                    sendResponse(exchange, 200, response);

                } catch (Exception e) {
                    System.err.println("Erro no upload: " + e.getMessage());
                    e.printStackTrace();
                    String error = "Erro no upload: " + e.getMessage();
                    sendResponse(exchange, 500, error);
                }
            } else {
                sendResponse(exchange, 405, "Método não permitido");
            }
        }

        private String getFilename(HttpExchange exchange) {
            // Simplificado: Apenas via header "filename" ou query param
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

    private static void processNewDocument(String filepath) throws Exception {
        // 1. Gerar CID a partir do conteúdo do ficheiro
        String cid = generateCid(filepath);

        // 2. Criar nova versão do vetor de CIDs
        int newVersion;
        synchronized (documentCidVector) {
            documentCidVector.add(cid);
            newVersion = documentCidVector.size();
            documentVectorVersion.set(newVersion);
        }

        // 3. Gerar um embedding de exemplo (substituir por uma implementação real)
        String embedding = "embedding_for_" + cid.substring(0, 10);

        // 4. Criar objeto de atualização com o embedding
        DocumentUpdate update = new DocumentUpdate(newVersion, cid, embedding);

        // 5. Propagar para os peers
        propagateUpdateToPeers(update);
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
        // Formato: "version;cid;embedding"
        String message = update.getVersion() + ";" + update.getCid();
        if (update.getEmbedding() != null && !update.getEmbedding().isEmpty()) {
            message += ";" + update.getEmbedding();
        }
        System.out.println("A propagar atualização para os peers: " + message);

        List<String> peerIds = new ArrayList<>(peerAddressMap.keySet());
        for (String peerId : peerIds) {
            sendMessageToPeer(peerId, message);
        }
    }

    /** Handler para enviar mensagens para todos os peers registados */
    static class SendMessageHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().add("Content-Type", "text/plain");

            if (!"POST".equals(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "Metodo nao permitido");
                return;
            }

            String message = readRequestBody(exchange);
            if (message == null || message.trim().isEmpty()) {
                sendResponse(exchange, 400, "Mensagem vazia");
                return;
            }

            System.out.println("Lider recebeu: " + message);

            int sent = 0;
            // copia das chaves para evitar concorrência durante iteração
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
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
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
            exchange.getResponseHeaders().add("Access-control-Allow-Origin", "*");
            exchange.getResponseHeaders().add("Content-Type", "text/plain");

            if (!"POST".equals(exchange.getRequestMethod())) {
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
            // ip pode ter ":" se IPv6; o porto é o último token
            String portStr = a[a.length - 1];
            int port = Integer.parseInt(portStr);
            // construir ip juntando os tokens exceto o ultimo
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
                os.write(message.getBytes("UTF-8"));
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
        InputStream is = exchange.getRequestBody();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            sb.append(line);
        }
        reader.close();
        return sb.toString();
    }

    private static void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
        byte[] responseBytes = response.getBytes("UTF-8");
        exchange.sendResponseHeaders(statusCode, responseBytes.length);
        OutputStream os = exchange.getResponseBody();
        os.write(responseBytes);
        os.close();
    }
}
