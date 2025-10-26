package com.ipfs.api;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FileUploadServer {

    private static final int PORT = 9090;
    private static final String UPLOAD_DIR = "uploads";

    public static void main(String[] args) throws Exception {
        // Criar diretório de uploads se não existir
        Files.createDirectories(Paths.get(UPLOAD_DIR));

        // Bindar em 0.0.0.0
        HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", PORT), 0);

        // Endpoint para upload de ficheiros
        server.createContext("/api/files/upload", new UploadHandler());

        server.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(10));
        server.start();

        System.out.println("Servidor REST iniciado na porta " + PORT);
        System.out.println("Endpoint: POST http://0.0.0.0:" + PORT + "/api/files/upload");
        System.out.println("Para aceder de outro PC: http://<IP_LOCAL>:" + PORT + "/api/files/upload");
        System.out.println("À espera de requisições...\n");
    }

    static class UploadHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            exchange.getResponseHeaders().add("Content-Type", "application/json");

            if ("POST".equals(exchange.getRequestMethod())) {
                try {
                    // Obter nome do ficheiro do header ou query parameter
                    String filename = exchange.getRequestHeaders().getFirst("filename");
                    if (filename == null || filename.isEmpty()) {
                        // Tentar obter da query string
                        String query = exchange.getRequestURI().getQuery();
                        if (query != null && query.contains("filename=")) {
                            filename = query.split("filename=")[1].split("&")[0];
                        }
                    }

                    if (filename == null || filename.isEmpty()) {
                        filename = "ficheiro_" + System.currentTimeMillis();
                    }

                    // Ler conteúdo do ficheiro
                    InputStream is = exchange.getRequestBody();
                    String filepath = UPLOAD_DIR + "/" + sanitizeFilename(filename);

                    // Escrever ficheiro no disco
                    Files.copy(is, Paths.get(filepath));

                    System.out.println("Ficheiro recebido: " + filename);
                    System.out.println("Caminho: " + filepath);
                    System.out.println("Tamanho: " + new File(filepath).length() + " bytes");

                    String response = "{\"status\": \"Ficheiro " + filename + " enviado com sucesso\", \"path\": \"" + filepath + "\"}";
                    sendResponse(exchange, 200, response);

                } catch (Exception e) {
                    System.err.println("Erro no upload: " + e.getMessage());
                    String error = "{\"error\": \"" + e.getMessage() + "\"}";
                    sendResponse(exchange, 500, error);
                }
            } else {
                sendResponse(exchange, 405, "{\"error\": \"Método não permitido\"}");
            }
        }

        private String sanitizeFilename(String filename) {
            return filename.replaceAll("[^a-zA-Z0-9._-]", "_");
        }

        private void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
            exchange.sendResponseHeaders(statusCode, response.getBytes().length);
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
    }
}