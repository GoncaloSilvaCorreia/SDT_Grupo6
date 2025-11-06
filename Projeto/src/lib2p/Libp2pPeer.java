package lib2p;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class Libp2pPeer {

    private static String peerId;
    private static Libp2pNode peerNode;
    private static int peerPort;
    private static String localIp;

    // Vetor de CIDs de documentos e sua versão
    private static final List<String> documentCidVector = new ArrayList<>();
    private static final AtomicInteger documentVectorVersion = new AtomicInteger(0);

    // Armazenamento de embeddings por CID (string genérica: JSON, base64, etc.)
    private static final Map<String, String> documentEmbeddings = new ConcurrentHashMap<>();


    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Uso: java lib2p.Libp2pPeer <peerId> [port]");
            System.out.println("Exemplo: java lib2p.Libp2pPeer peer1 8091");
            return;
        }

        peerId = args[0];

        // Determinar porta: 1) argumento 2) extrair número do peerId (peer1 -> 8091) 3) default 8091
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

        // Criar nó do peer (simples)
        peerNode = new Libp2pNode(peerId);
        peerNode.start();

        // Conectar ao lider (lógica interna, apenas registo)
        peerNode.connectToPeer("leader");

        // obter IP local (tenta caminho "inteligente")
        localIp = getLocalIpAddress();
        if (localIp == null) localIp = "127.0.0.1";

        // Registar o peer no líder informando também a porta (formato: peerId:ip:port)
        registerPeerWithLeader(peerId, localIp, peerPort);

        // Criar servidor HTTP no Peer para receber mensagens
        HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", peerPort), 0);

        // Endpoint para receber mensagens do líder
        server.createContext("/api/messages/receive", new ReceiveMessageHandler());

        server.setExecutor(Executors.newFixedThreadPool(4));
        server.start();

        System.out.println("Peer " + peerId + " pronto!");
        System.out.println("À escuta de mensagens do líder (ip: " + localIp + ", port: " + peerPort + ")\n");

        // Manter aplicação a correr
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            System.out.println("Encerrando peer " + peerId + "...");
            peerNode.stop();
        }
    }

    private static Integer extractTrailingNumber(String id) {
        // extrai dígitos finais, ex: peer12 -> 12
        String num = id.replaceAll("^.*?(\\d+)$", "$1");
        if (num.equals(id)) { // não houve match
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
                os.write(payload.getBytes("UTF-8"));
                os.flush();
            }

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                System.out.println("Registado no lider com sucesso!");
            } else {
                System.err.println("Erro ao registar no líder (código: " + responseCode + ")");
                // Optional: read error stream from connection
                try (BufferedReader br = new BufferedReader(new InputStreamReader(conn.getErrorStream(), "utf-8"))) {
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
     * Handler para receber mensagens do líder
     */
    static class ReceiveMessageHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().add("Content-Type", "text/plain");

            if ("POST".equals(exchange.getRequestMethod())) {
                try {
                    String message = readRequestBody(exchange);

                    if (message != null && !message.isEmpty()) {
                        System.out.println("[" + peerId.toUpperCase() + "] Mensagem recebida:");
                        System.out.println("   " + message + "\n");

                        // Processar a atualização do documento
                        handleDocumentUpdate(message);
                    }

                    String response = "Mensagem recebida";
                    sendResponse(exchange, 200, response);

                } catch (Exception e) {
                    System.err.println("Erro ao processar mensagem: " + e.getMessage());
                    e.printStackTrace();
                    sendResponse(exchange, 500, "Erro: " + e.getMessage());
                }
            } else {
                sendResponse(exchange, 405, "Metodo nao permitido");
            }
        }

        private void handleDocumentUpdate(String message) {
            try {
                // Suporta: "versao;cid" ou "versao;cid;embedding..."
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

                // Se houver mais de 2 partes, junta o resto como string de embedding (para permitir ';' no conteúdo)
                String embedding = null;
                if (parts.length > 2) {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 2; i < parts.length; i++) {
                        if (i > 2) sb.append(";");
                        sb.append(parts[i]);
                    }
                    embedding = sb.toString().trim();
                }

                System.out.println("Atualização de documento processada: Versão=" + receivedVersion + ", CID=" + cid + (embedding != null ? ", embedding recebido" : ", sem embedding"));

                // Lógica de atualização do vetor local
                synchronized (documentCidVector) {
                    if (receivedVersion > documentVectorVersion.get()) {
                        if (!documentCidVector.contains(cid)) {
                            documentCidVector.add(cid);
                        }
                        documentVectorVersion.set(receivedVersion);
                        System.out.println("Vetor de documentos atualizado para a versão: " + receivedVersion);
                        System.out.println("Vetor atual: " + documentCidVector);
                    } else {
                        System.out.println("Atualização ignorada (versão antiga ou igual). Versão recebida: " + receivedVersion + ", Versão atual: " + documentVectorVersion.get());
                    }
                }

                // Armazenar embedding se presente (substitui o anterior)
                if (embedding != null && !embedding.isEmpty()) {
                    documentEmbeddings.put(cid, embedding);
                    System.out.println("Embedding armazenado para CID: " + cid);
                }

            } catch (Exception e) {
                System.err.println("Falha ao processar a atualização do documento: " + e.getMessage());
            }
        }

        private String readRequestBody(HttpExchange exchange) throws IOException {
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

        private void sendResponse(HttpExchange exchange, int statusCode, String response)
                throws IOException {
            byte[] bytes = response.getBytes("UTF-8");
            exchange.sendResponseHeaders(statusCode, bytes.length);
            OutputStream os = exchange.getResponseBody();
            os.write(bytes);
            os.close();
        }
    }

    /**
     * Tenta obter IP local conectando a um endereço remota (não envia dados).
     */
    private static String getLocalIpAddress() {
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            return socket.getLocalAddress().getHostAddress();
        } catch (Exception e) {
            // fallback
            try {
                return InetAddress.getLocalHost().getHostAddress();
            } catch (Exception ex) {
                return null;
            }
        }
    }
}