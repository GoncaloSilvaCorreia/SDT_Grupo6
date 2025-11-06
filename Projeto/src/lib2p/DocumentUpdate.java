package lib2p;

public class DocumentUpdate {
    private int version;
    private String cid;
    private String embedding;

    public DocumentUpdate(int version, String cid, String embedding) {
        this.version = version;
        this.cid = cid;
        this.embedding = embedding;
    }

    // Getters and setters
    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public String getCid() {
        return cid;
    }

    public void setCid(String cid) {
        this.cid = cid;
    }

    public String getEmbedding() {
        return embedding;
    }

    public void setEmbedding(String embedding) {
        this.embedding = embedding;
    }

    // toString() for easy printing
    @Override
    public String toString() {
        return "DocumentUpdate{" +
                "version=" + version +
                ", cid='" + cid + '\'' +
                ", embedding='" + embedding + '\'' +
                '}';
    }
}
