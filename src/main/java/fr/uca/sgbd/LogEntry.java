package fr.uca.sgbd;

/**
 * Représente une entrée dans le journal de transactions (TJT).
 */
public class LogEntry {
    public int transactionId;
    public int pageId;
    public int slot;
    public byte[] beforeImage;
    public byte[] afterImage;
    public LogType type;

    public LogEntry(int transactionId, LogType type) {
        this.transactionId = transactionId;
        this.type = type;
        this.pageId = -1;
        this.slot = -1;
    }

    public LogEntry(int transactionId, LogType type, int pageId, int slot, byte[] beforeImage, byte[] afterImage) {
        this.transactionId = transactionId;
        this.type = type;
        this.pageId = pageId;
        this.slot = slot;
        this.beforeImage = beforeImage;
        this.afterImage = afterImage;
    }

    @Override
    public String toString() {
        return String.format("LogEntry[txn=%d, type=%s, page=%d, slot=%d]", 
            transactionId, type, pageId, slot);
    }
}
