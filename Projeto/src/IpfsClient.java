package com.ipfs.api;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;

public class IpfsClient {

    private String host;
    private int port;

    public IpfsClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Adiciona um ficheiro ao IPFS e retorna o CID
     */
    public String addFile(byte[] fileData, String fileName) throws Exception {
        // Criar ficheiro temporário
        String tempFilePath = System.getProperty("java.io.tmpdir") +
                File.separator + UUID.randomUUID() + "_" + fileName;
        Files.write(Paths.get(tempFilePath), fileData);

        try {
            // Construir URL para upload ao IPFS
            String url = String.format("http://%s:%d/api/v0/add", host, port);

            // Fazer request multipart
            String boundary = "----WebKitFormBoundary" + System.nanoTime();
            HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);

            // Escrever corpo do request
            try (OutputStream os = conn.getOutputStream()) {
                writeFileToRequest(os, fileData, fileName, boundary);
            }

            // Ler resposta
            int statusCode = conn.getResponseCode();
            if (statusCode != 200) {
                throw new Exception("Erro ao adicionar ficheiro ao IPFS: " + statusCode);
            }

            // Extrair CID da resposta
            String response = readResponse(conn.getInputStream());
            String cid = extractCidFromResponse(response);

            return cid;

        } finally {
            // Limpar ficheiro temporário
            Files.deleteIfExists(Paths.get(tempFilePath));
        }
    }

    private void writeFileToRequest(OutputStream os, byte[] fileData,
                                    String fileName, String boundary) throws IOException {
        String lineEnd = "\r\n";
        String twoHyphens = "--";

        // Escrever boundary e headers
        os.write((twoHyphens + boundary + lineEnd).getBytes());
        os.write(("Content-Disposition: form-data; name=\"file\"; filename=\"" +
                fileName + "\"" + lineEnd).getBytes());
        os.write(("Content-Type: application/octet-stream" + lineEnd + lineEnd).getBytes());

        // Escrever dados do ficheiro
        os.write(fileData);

        // Escrever boundary final
        os.write((lineEnd + twoHyphens + boundary + twoHyphens + lineEnd).getBytes());
        os.flush();
    }

    private String readResponse(InputStream is) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        StringBuilder response = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            response.append(line);
        }
        return response.toString();
    }

    private String extractCidFromResponse(String response) throws Exception {
        // A resposta IPFS é JSON com campo "Hash"
        // Exemplo: {"Name":"file","Hash":"QmXxxx...","Size":"1234"}

        int hashIndex = response.indexOf("\"Hash\":");
        if (hashIndex == -1) {
            throw new Exception("CID não encontrado na resposta IPFS");
        }

        int startIndex = response.indexOf("\"", hashIndex + 7) + 1;
        int endIndex = response.indexOf("\"", startIndex);

        return response.substring(startIndex, endIndex);
    }
}