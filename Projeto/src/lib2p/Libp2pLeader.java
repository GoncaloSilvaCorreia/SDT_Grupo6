package lib2p;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Libp2pLeader {

    private static final int HTTP_PORT = 9091;
    private static Libp2pNode leaderNode;
    // map peerId -> "ip:port"
    private static final Map<String, String> peerAddressMap = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        System.out.println("Iniciando Lider com libp2p...\n");

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
        
        // Endpoint para adicionar documento (two-phase commit)
        server.createContext("/api/document/add", new AddDocumentHandler());

        server.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(10));
        server.start();

        System.out.println("API do Lider iniciada na porta " + HTTP_PORT);
        System.out.println("Aceder de outro PC: http://" + LibP2pConfig.LEADER_HOST + ":" + HTTP_PORT);
        System.out.println("A espera de requisicoes...\n");
    }

    /** Handler para enviar mensagens para todos os peers registados */
    static class SendMessageHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().add("Content-Type", "application/json");

            if (!"POST".equals(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "{\"error\":\"Metodo nao permitido\"}");
                return;
            }

            String message = readRequestBody(exchange);
            if (message == null || message.trim().isEmpty()) {
                sendResponse(exchange, 400, "{\"error\":\"Mensagem vazia\"}");
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

            String response = "{\"status\":\"Mensagem enviada para " + sent + " peers\", \"count\": " + peerAddressMap.size() + "}";
            sendResponse(exchange, 200, response);
        }
    }

    /** Handler para listar peers e os seus endereços */
    static class ListPeersHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().add("Content-Type", "application/json");

            // Construir JSON simples: [{ "peerId":"peer1", "addr":"ip:port" }, ...]
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            Iterator<Map.Entry<String, String>> it = peerAddressMap.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, String> e = it.next();
                sb.append("{\"peerId\":\"").append(e.getKey()).append("\",\"addr\":\"").append(e.getValue()).append("\"}");
                if (it.hasNext()) sb.append(",");
            }
            sb.append("]");

            String response = "{\"peers\": " + sb.toString() + ", \"count\": " + peerAddressMap.size() + "}";
            sendResponse(exchange, 200, response);
        }
    }

    /** Handler para registar um peer. Aceita dois formatos:
     *  - "peerId" (compatibilidade)
     *  - "peerId:ip:port" (recomendado)
     */
    static class ConnectPeerHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().add("Content-Type", "application/json");

            if (!"POST".equals(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "{\"error\":\"Metodo nao permitido\"}");
                return;
            }

            String body = readRequestBody(exchange);
            if (body == null || body.trim().isEmpty()) {
                sendResponse(exchange, 400, "{\"error\":\"Peer ID vazio\"}");
                return;
            }

            // aceitar format "peerId:ip:port" ou "peerId"
            String peerId = null;
            String addr = null;

            String[] parts = body.trim().split(":");
            if (parts.length >= 3) {
                // peerId may be parts[0], ip may be join of parts[1..n-2], port last
                peerId = parts[0];
                String port = parts[parts.length - 1];
                StringBuilder ipBuilder = new StringBuilder();
                for (int i = 1; i < parts.length - 1; i++) {
                    if (i > 1) ipBuilder.append(":");
                    ipBuilder.append(parts[i]);
                }
                String ip = ipBuilder.toString();
                addr = ip + ":" + port;
            } else {
                // only peerId provided -> store with default addr null (leader não sabe onde enviar)
                peerId = body.trim();
                addr = null;
            }

            // armazenar
            if (peerId != null) {
                if (addr != null) {
                    peerAddressMap.put(peerId, addr);
                    leaderNode.connectToPeer(peerId); // mantém comportamento anterior
                    System.out.println("Peer " + peerId + " conectado em " + addr + "!");
                } else {
                    // store with placeholder to indicate peer exists but no address provided
                    peerAddressMap.putIfAbsent(peerId, "");
                    leaderNode.connectToPeer(peerId);
                    System.out.println("Peer " + peerId + " conectado (endereço nao informado)!");
                }
            }

            sendResponse(exchange, 200, "{\"status\":\"Peer conectado: " + peerId + "\"}");
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
            StringBuilder ipBuilder = new StringBuilder();
            for (int i = 0; i < a.length - 1; i++) {
                if (i > 0) ipBuilder.append(":");
                ipBuilder.append(a[i]);
            }
            String peerIP = ipBuilder.toString();

            String peerUrl = "http://" + peerIP + ":" + port + "/api/messages/receive";
            URL url = new URL(peerUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "text/plain");
            conn.setDoOutput(true);
            conn.setConnectTimeout(3000);
            conn.setReadTimeout(3000);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(message.getBytes("UTF-8"));
                os.flush();
            }

            int responseCode = conn.getResponseCode();
            conn.disconnect();

            if (responseCode == 200) {
                return true;
            } else {
                System.err.println("Erro ao enviar para " + peerId + ": codigo " + responseCode);
                return false;
            }
        } catch (Exception e) {
            System.err.println("Erro ao enviar para " + peerId + ": " + e.getMessage());
            return false;
        }
    }

    /**
     * Handler para adicionar documento usando two-phase commit
     */
    static class AddDocumentHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().add("Content-Type", "application/json");

            if (!"POST".equals(exchange.getRequestMethod())) {
                sendResponse(exchange, 405, "{\"error\":\"Metodo nao permitido\"}");
                return;
            }

            try {
                String body = readRequestBody(exchange);
                
                // Parse simples JSON: {"cid": "...", "embeddings": {...}}
                String cid = extractJsonField(body, "cid");
                
                if (cid == null || cid.isEmpty()) {
                    sendResponse(exchange, 400, "{\"error\":\"CID vazio\"}");
                    return;
                }
                
                System.out.println("Lider: Iniciando two-phase commit para CID=" + cid);
                
                // FASE 1: PREPARE - enviar para todos os peers
                Map<String, String> peerHashes = new HashMap<>();
                int version = (int) (System.currentTimeMillis() / 1000); // versão baseada em timestamp
                
                List<String> peerIds = new ArrayList<>(peerAddressMap.keySet());
                int prepareSuccess = 0;
                
                for (String peerId : peerIds) {
                    String hash = sendPrepareToPeer(peerId, cid, version, body);
                    if (hash != null) {
                        peerHashes.put(peerId, hash);
                        prepareSuccess++;
                    }
                }
                
                System.out.println("Lider: Prepare completado - " + prepareSuccess + "/" + peerIds.size() + " peers responderam");
                
                // Verificar se temos maioria (quorum)
                int totalPeers = peerIds.size();
                int majority = (totalPeers / 2) + 1;
                
                if (prepareSuccess < majority) {
                    System.err.println("Lider: FALHA - maioria não alcançada (" + prepareSuccess + " < " + majority + ")");
                    sendResponse(exchange, 500, "{\"error\":\"Maioria nao alcancada\", \"prepared\": " + prepareSuccess + ", \"required\": " + majority + "}");
                    return;
                }
                
                // Verificar se todos os hashes são iguais
                Set<String> uniqueHashes = new HashSet<>(peerHashes.values());
                if (uniqueHashes.size() > 1) {
                    System.err.println("Lider: AVISO - hashes diferentes entre peers: " + uniqueHashes);
                    // Decidir qual hash é correto (maioria)
                }
                
                String correctHash = findMajorityHash(peerHashes);
                System.out.println("Lider: Hash da maioria: " + correctHash);
                
                // FASE 2: COMMIT - enviar commit para todos os peers
                int commitSuccess = 0;
                for (String peerId : peerIds) {
                    boolean ok = sendCommitToPeer(peerId, version);
                    if (ok) commitSuccess++;
                }
                
                System.out.println("Lider: Commit completado - " + commitSuccess + "/" + peerIds.size() + " peers confirmaram");
                
                String response = "{\"status\":\"committed\", \"cid\":\"" + cid + "\", \"version\": " + version + 
                                 ", \"prepared\": " + prepareSuccess + ", \"committed\": " + commitSuccess + 
                                 ", \"hash\": \"" + correctHash + "\"}";
                sendResponse(exchange, 200, response);
                
            } catch (Exception e) {
                System.err.println("Lider: Erro no two-phase commit: " + e.getMessage());
                e.printStackTrace();
                sendResponse(exchange, 500, "{\"error\": \"" + e.getMessage() + "\"}");
            }
        }
    }
    
    /**
     * Envia prepare para um peer e retorna o hash ou null se falhar
     */
    private static String sendPrepareToPeer(String peerId, String cid, int version, String fullBody) {
        try {
            String addr = peerAddressMap.get(peerId);
            if (addr == null || addr.trim().isEmpty()) {
                System.err.println("Sem endereco para " + peerId);
                return null;
            }
            
            String[] a = addr.split(":");
            if (a.length < 2) {
                System.err.println("Endereco invalido para " + peerId + ": " + addr);
                return null;
            }
            
            String portStr = a[a.length - 1];
            int port = Integer.parseInt(portStr);
            StringBuilder ipBuilder = new StringBuilder();
            for (int i = 0; i < a.length - 1; i++) {
                if (i > 0) ipBuilder.append(":");
                ipBuilder.append(a[i]);
            }
            String peerIP = ipBuilder.toString();
            
            String peerUrl = "http://" + peerIP + ":" + port + "/api/document/prepare";
            URL url = new URL(peerUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);
            
            // Adicionar versão ao body se não existir
            String payload = fullBody;
            if (!payload.contains("\"version\"")) {
                payload = payload.substring(0, payload.length() - 1) + ", \"version\": " + version + "}";
            }
            
            try (OutputStream os = conn.getOutputStream()) {
                os.write(payload.getBytes("UTF-8"));
                os.flush();
            }
            
            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                // Ler resposta para obter hash
                BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
                StringBuilder response = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }
                reader.close();
                
                String hash = extractJsonField(response.toString(), "hash");
                System.out.println("Prepare OK para " + peerId + ": hash=" + hash);
                return hash;
            } else {
                System.err.println("Erro prepare para " + peerId + ": codigo " + responseCode);
                return null;
            }
            
        } catch (Exception e) {
            System.err.println("Erro prepare para " + peerId + ": " + e.getMessage());
            return null;
        }
    }
    
    /**
     * Envia commit para um peer
     */
    private static boolean sendCommitToPeer(String peerId, int version) {
        try {
            String addr = peerAddressMap.get(peerId);
            if (addr == null || addr.trim().isEmpty()) {
                return false;
            }
            
            String[] a = addr.split(":");
            if (a.length < 2) {
                return false;
            }
            
            String portStr = a[a.length - 1];
            int port = Integer.parseInt(portStr);
            StringBuilder ipBuilder = new StringBuilder();
            for (int i = 0; i < a.length - 1; i++) {
                if (i > 0) ipBuilder.append(":");
                ipBuilder.append(a[i]);
            }
            String peerIP = ipBuilder.toString();
            
            String peerUrl = "http://" + peerIP + ":" + port + "/api/document/commit";
            URL url = new URL(peerUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);
            
            String payload = "{\"version\": " + version + "}";
            
            try (OutputStream os = conn.getOutputStream()) {
                os.write(payload.getBytes("UTF-8"));
                os.flush();
            }
            
            int responseCode = conn.getResponseCode();
            conn.disconnect();
            
            if (responseCode == 200) {
                System.out.println("Commit OK para " + peerId);
                return true;
            } else {
                System.err.println("Erro commit para " + peerId + ": codigo " + responseCode);
                return false;
            }
            
        } catch (Exception e) {
            System.err.println("Erro commit para " + peerId + ": " + e.getMessage());
            return false;
        }
    }
    
    /**
     * Encontra o hash que aparece na maioria dos peers
     */
    private static String findMajorityHash(Map<String, String> peerHashes) {
        Map<String, Integer> hashCount = new HashMap<>();
        for (String hash : peerHashes.values()) {
            hashCount.put(hash, hashCount.getOrDefault(hash, 0) + 1);
        }
        
        String majorityHash = null;
        int maxCount = 0;
        for (Map.Entry<String, Integer> entry : hashCount.entrySet()) {
            if (entry.getValue() > maxCount) {
                maxCount = entry.getValue();
                majorityHash = entry.getKey();
            }
        }
        return majorityHash;
    }
    
    /**
     * Extrai um campo simples de um JSON (parsing básico)
     */
    private static String extractJsonField(String json, String fieldName) {
        if (json == null) return null;
        
        // Try with quotes first (for strings)
        String quotedPattern = "\"" + fieldName + "\"\\s*:\\s*\"([^\"]+)\"";
        java.util.regex.Matcher m = java.util.regex.Pattern.compile(quotedPattern).matcher(json);
        if (m.find()) {
            return m.group(1);
        }
        
        // Try without quotes (for numbers)
        String unquotedPattern = "\"" + fieldName + "\"\\s*:\\s*([^,}\\s]+)";
        m = java.util.regex.Pattern.compile(unquotedPattern).matcher(json);
        if (m.find()) {
            return m.group(1);
        }
        
        return null;
    }

    /* utilitários */
    private static String readRequestBody(HttpExchange exchange) throws IOException {
        InputStream is = exchange.getRequestBody();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) sb.append(line);
        return sb.toString();
    }

    private static void sendResponse(HttpExchange exchange, int statusCode, String response)
            throws IOException {
        byte[] bytes = response.getBytes("UTF-8");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        OutputStream os = exchange.getResponseBody();
        os.write(bytes);
        os.close();
    }
}