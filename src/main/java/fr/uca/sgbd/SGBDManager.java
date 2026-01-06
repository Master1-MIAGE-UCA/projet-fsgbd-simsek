package fr.uca.sgbd;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** 
 * 
 * SGBD Manager
 * 
 * Mini-SGBD – Étape 1 : stockage par pages fixes.
 *
 * Contrat:
 * - PAGE_SIZE = 4096 octets
 * - RECORD_SIZE = 100 octets (taille fixe)
 * - records par page = PAGE_SIZE / RECORD_SIZE = 40
 * - insertRecord écrit à la fin du fichier
 * - readRecord(id) lit par index (0-based)
 * - getPage(pageIndex) lit la page (0-based) entière
 */
public class SGBDManager implements AutoCloseable {
    public static final int PAGE_SIZE = 4096;
    public static final int RECORD_SIZE = 100;
    public static final int RECORDS_PER_PAGE = PAGE_SIZE / RECORD_SIZE; // 40

    private final RandomAccessFile raf;
    private final BufferManager bufferManager;

    // TP4: TIV (Tampon d'Images Avant) et Verrous
    private final Map<Integer, byte[]> tiv = new HashMap<>();
    private final Set<String> locks = new HashSet<>();

    private boolean inTransaction = false;
    // Longueur logique utilisée durant une transaction pour positionner les insertions sans écrire sur disque
    private long txnLogicalLength = -1L;

    // TP5: Journal de transactions (TJT) et Fichier de journal (FJT)
    private final List<LogEntry> tjt = new ArrayList<>();
    private final RandomAccessFile fjt;
    private int transactionIdCounter = 0;
    private int currentTransactionId = -1;
    /**
     * Lance une transaction. Si une transaction est déjà en cours, elle est commitée.
     */
    public synchronized void begin() throws IOException {
        if (inTransaction) {
            commit();
        }
        inTransaction = true;
        currentTransactionId = ++transactionIdCounter;
        txnLogicalLength = raf.length();
        
        // TP5: Ajouter entrée BEGIN dans le journal
        addLogEntry(new LogEntry(currentTransactionId, LogType.BEGIN));
    }

    /**
     * Commit la transaction : écrit les pages transactionnelles modifiées sur disque.
     */
    public synchronized void commit() throws IOException {
        // TP5: Le COMMIT ne force plus les pages sur disque
        // Il écrit seulement dans le journal
        
        // Ajouter entrée COMMIT dans le TJT
        addLogEntry(new LogEntry(currentTransactionId, LogType.COMMIT));
        
        // Forcer l'écriture du TJT dans le FJT
        flushJournalToDisk();
        
        // Nettoyer les pages transactionnelles (sans écrire sur disque)
        for (var entry : bufferManager.getBufferPool().entrySet()) {
            BufferPage page = entry.getValue();
            page.transactional = false;
        }
        
        // TP4: Nettoyage
        tiv.clear();
        locks.clear();
        
        inTransaction = false;
        currentTransactionId = -1;
        txnLogicalLength = -1L;
    }

    /**
     * Rollback la transaction : ignore les modifications des pages transactionnelles.
     */
    public synchronized void rollback() throws IOException {
        // TP4: Restauration depuis le TIV
        for (Map.Entry<Integer, byte[]> entry : tiv.entrySet()) {
            int pageId = entry.getKey();
            byte[] originalData = entry.getValue();
            try {
                BufferPage page = bufferManager.FIX(pageId);
                System.arraycopy(originalData, 0, page.data, 0, PAGE_SIZE);
                // La donnée restaurée est considérée comme "propre" (identique au disque)
                // ou son état précédent. Pour simplifier, on met dirty=false.
                page.dirty = false; 
                page.transactional = false;
                bufferManager.UNFIX(pageId);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        // TP5: Ajouter entrée ROLLBACK dans le TJT et flush
        addLogEntry(new LogEntry(currentTransactionId, LogType.ROLLBACK));
        flushJournalToDisk();
        
        // Nous utilisons le TIV au lieu de l'ancienne méthode (suppression des pages).
        // Cependant, peut-il y avoir des pages nouvellement créées pendant la transaction et absentes du TIV ?
        // Dans la logique insertRecord, nous enregistrons dans le TIV à chaque modification.
        // Même s'il s'agit d'une nouvelle page (vide), elle doit être ajoutée au TIV.
        
        tiv.clear();
        locks.clear();
        inTransaction = false;
        currentTransactionId = -1;
        txnLogicalLength = -1L;
    }

    public SGBDManager(String path) throws FileNotFoundException {
        this(new File(path));
    }

    public SGBDManager(File file) throws FileNotFoundException {
        this.raf = new RandomAccessFile(file, "rw");
        // Fichier de journal (FJT)
        String journalPath = file.getAbsolutePath() + ".log";
        this.fjt = new RandomAccessFile(journalPath, "rw");
        this.bufferManager = new BufferManager(this);
    }

    /**
     * Ecrit un enregistrement de taille RECORD_SIZE en respectant les frontières de page.
     * Si la chaîne dépasse 100 octets UTF-8, elle est tronquée; sinon rembourrée par des zéros (\0).
     * Règle: si l'espace restant dans la page courante est insuffisant, on avance au début de la page suivante.
     */
    public synchronized void insertRecord(String value) throws IOException {
        byte[] dataUtf8 = value.getBytes(StandardCharsets.UTF_8);
        byte[] fixed = new byte[RECORD_SIZE];
        int len = Math.min(dataUtf8.length, RECORD_SIZE);
        System.arraycopy(dataUtf8, 0, fixed, 0, len);
        // padding implicite avec 0 pour le reste

        long length = inTransaction ? txnLogicalLength : raf.length();
        long offsetInPage = length % PAGE_SIZE;
        if (offsetInPage + RECORD_SIZE > PAGE_SIZE) {
            length += (PAGE_SIZE - offsetInPage); // logique d'append page suivante
            offsetInPage = 0;
        }
        int pageIndex = (int) (length / PAGE_SIZE);
        int slot = (int) (offsetInPage / RECORD_SIZE);

        BufferPage page = bufferManager.FIX(pageIndex);
        try {
            // TP4: Verrouillage et copie dans le TIV
            if (inTransaction) {
                String lockKey = pageIndex + ":" + slot;
                if (!locks.contains(lockKey)) {
                    locks.add(lockKey);
                    // Sauvegarder l'état original de la page dans le TIV (si absent)
                    if (!tiv.containsKey(pageIndex)) {
                        byte[] original = new byte[PAGE_SIZE];
                        System.arraycopy(page.data, 0, original, 0, PAGE_SIZE);
                        tiv.put(pageIndex, original);
                    }
                }
                
                // TP5: Ajouter entrée INSERT dans le journal
                byte[] beforeImage = new byte[RECORD_SIZE];
                System.arraycopy(page.data, slot * RECORD_SIZE, beforeImage, 0, RECORD_SIZE);
                addLogEntry(new LogEntry(currentTransactionId, LogType.INSERT, 
                    pageIndex, slot, beforeImage, fixed));
            }

            System.arraycopy(fixed, 0, page.data, slot * RECORD_SIZE, RECORD_SIZE);
            bufferManager.USE(pageIndex);
            if (inTransaction) {
                page.transactional = true;
                // avance la longueur logique de la transaction
                long newEnd = (long) pageIndex * PAGE_SIZE + (slot + 1) * RECORD_SIZE;
                if (newEnd > txnLogicalLength) txnLogicalLength = newEnd;
            }
            // Pas d'écriture disque ici (insertion classique = buffer seulement)
        } finally {
            bufferManager.UNFIX(pageIndex);
        }
    }

    /**
     * Insère un enregistrement de façon synchrone : écrit dans le buffer et force l'écriture sur disque.
     */
    public synchronized void insertRecordSync(String value) throws IOException {
        byte[] dataUtf8 = value.getBytes(StandardCharsets.UTF_8);
        byte[] fixed = new byte[RECORD_SIZE];
        int len = Math.min(dataUtf8.length, RECORD_SIZE);
        System.arraycopy(dataUtf8, 0, fixed, 0, len);

        int pageIndex = 0;
        boolean inserted = false;
        while (!inserted) {
            BufferPage page = bufferManager.FIX(pageIndex);
            int slot = -1;
            try {
                for (int i = 0; i < RECORDS_PER_PAGE; i++) {
                    boolean empty = true;
                    for (int j = 0; j < RECORD_SIZE; j++) {
                        if (page.data[i * RECORD_SIZE + j] != 0) { empty = false; break; }
                    }
                    if (empty) { slot = i; break; }
                }
                if (slot != -1) {
                    // TP4: Verrouillage et TIV (valable aussi pour Sync)
                    if (inTransaction) {
                        String lockKey = pageIndex + ":" + slot;
                        if (!locks.contains(lockKey)) {
                            locks.add(lockKey);
                            if (!tiv.containsKey(pageIndex)) {
                                byte[] original = new byte[PAGE_SIZE];
                                System.arraycopy(page.data, 0, original, 0, PAGE_SIZE);
                                tiv.put(pageIndex, original);
                            }
                        }
                        
                        // TP5: Ajouter entrée INSERT dans le journal
                        byte[] beforeImage = new byte[RECORD_SIZE];
                        System.arraycopy(page.data, slot * RECORD_SIZE, beforeImage, 0, RECORD_SIZE);
                        addLogEntry(new LogEntry(currentTransactionId, LogType.INSERT, 
                            pageIndex, slot, beforeImage, fixed));
                    }

                    System.arraycopy(fixed, 0, page.data, slot * RECORD_SIZE, RECORD_SIZE);
                    bufferManager.USE(pageIndex);
                    if (inTransaction) {
                        page.transactional = true;
                        long newEnd = (long) pageIndex * PAGE_SIZE + (slot + 1) * RECORD_SIZE;
                        if (newEnd > txnLogicalLength) txnLogicalLength = newEnd;
                    } else {
                        // Écrit directement l'enregistrement au bon offset et force la page
                        long filePos = (long) pageIndex * PAGE_SIZE + (long) slot * RECORD_SIZE;
                        raf.seek(filePos);
                        raf.write(fixed);
                        bufferManager.FORCE(pageIndex);
                        long newEnd = (long) pageIndex * PAGE_SIZE + (slot + 1) * RECORD_SIZE;
                        // Ajuste la longueur du fichier exactement à la fin du dernier enregistrement
                        raf.setLength(newEnd);
                    }
                    inserted = true;
                } else {
                    // Page pleine, passe à la suivante
                    bufferManager.UNFIX(pageIndex);
                    pageIndex++;
                    continue;
                }
            } finally {
                bufferManager.UNFIX(pageIndex);
            }
        }
    }

    /**
     * Lit un enregistrement par son index (0-based).
     * Retourne la chaîne décodée sans les octets nuls de fin.
     */
    public synchronized String readRecord(int recordId) throws IOException {
        if (recordId < 0) throw new IndexOutOfBoundsException("recordId négatif");

        long pageIndex = recordId / RECORDS_PER_PAGE;
        long indexInPage = recordId % RECORDS_PER_PAGE;
        BufferPage page = bufferManager.FIX((int) pageIndex);
        try {
            byte[] sourceData = page.data;

            // TP4: Si en transaction et enregistrement verrouillé, lire depuis le TIV
            if (inTransaction) {
                String lockKey = (int)pageIndex + ":" + (int)indexInPage;
                if (locks.contains(lockKey) && tiv.containsKey((int)pageIndex)) {
                    sourceData = tiv.get((int)pageIndex);
                }
            }

            byte[] buf = new byte[RECORD_SIZE];
            System.arraycopy(sourceData, (int) (indexInPage * RECORD_SIZE), buf, 0, RECORD_SIZE);
            int effective = trimRightZeros(buf);
            if (effective == 0) {
                // Fallback: lit directement depuis le disque si disponible
                long filePos = pageIndex * PAGE_SIZE + indexInPage * RECORD_SIZE;
                if (filePos + RECORD_SIZE <= raf.length()) {
                    byte[] direct = new byte[RECORD_SIZE];
                    raf.seek(filePos);
                    raf.readFully(direct);
                    int eff2 = trimRightZeros(direct);
                    return new String(direct, 0, eff2, StandardCharsets.UTF_8);
                }
            }
            return new String(buf, 0, effective, StandardCharsets.UTF_8);
        } finally {
            bufferManager.UNFIX((int) pageIndex);
        }
    }

    /**
     * Retourne tous les enregistrements d'une page (0-based).
     */
    public synchronized List<String> getPage(int pageIndex) throws IOException {
        if (pageIndex < 0) throw new IndexOutOfBoundsException("pageIndex négatif");
        long pageStart = (long) pageIndex * PAGE_SIZE;
        if (pageStart >= raf.length()) return Collections.emptyList();

        int toReadRecords = RECORDS_PER_PAGE;
        long totalRecords = getRecordCount();
        long firstRecordIndex = (long) pageIndex * RECORDS_PER_PAGE;
        if (firstRecordIndex >= totalRecords) return Collections.emptyList();

        long remaining = totalRecords - firstRecordIndex;
        if (remaining < toReadRecords) toReadRecords = (int) remaining;

        List<String> pageList = new ArrayList<>(toReadRecords);
        BufferPage page = bufferManager.FIX(pageIndex);
        try {
            for (int i = 0; i < toReadRecords; i++) {
                byte[] buf = new byte[RECORD_SIZE];
                System.arraycopy(page.data, i * RECORD_SIZE, buf, 0, RECORD_SIZE);
                int effective = trimRightZeros(buf);
                pageList.add(new String(buf, 0, effective, StandardCharsets.UTF_8));
            }
        } finally {
            bufferManager.UNFIX(pageIndex);
        }
        return pageList;
    }

    /** Nombre total d'enregistrements stockés (arrondi inférieur). */
    public synchronized long getRecordCount() throws IOException {
        long bytes = raf.length();
        long fullPages = bytes / PAGE_SIZE;
        long remainder = bytes % PAGE_SIZE;
        return fullPages * RECORDS_PER_PAGE + (remainder / RECORD_SIZE);
    }

    /** Nombre total de pages allouées (arrondi inférieur). */
    public synchronized long getPageCount() throws IOException {
        long bytes = raf.length();
        return (bytes + PAGE_SIZE - 1) / PAGE_SIZE; // ceil division, pages potentiellement incomplètes
    }

    /**
     * Lit les données brutes d'une page sous forme de byte[] (pour BufferManager).
     */
    public synchronized byte[] readPageBytes(int pageIndex) throws IOException {
        if (pageIndex < 0) throw new IndexOutOfBoundsException("pageIndex négatif");

        byte[] pageData = new byte[PAGE_SIZE];
        long pageStart = (long) pageIndex * PAGE_SIZE;

        if (pageStart >= raf.length()) {
            // Si la page n'existe pas dans le fichier, retourner une page vide
            return pageData; // Déjà remplie de zéros
        }

        raf.seek(pageStart);
    raf.read(pageData);

        // Si la page n'est pas entièrement lue (fin de fichier), le reste reste à zéro
        return pageData;
    }

    /**
     * Écrit les données brutes d'une page sur disque à la position correspondante.
     */
    public synchronized void writePageBytes(int pageIndex, byte[] data) throws IOException {
        if (pageIndex < 0) throw new IndexOutOfBoundsException("pageIndex négatif");
        if (data == null || data.length != PAGE_SIZE) throw new IllegalArgumentException("Taille de page incorrecte");
        long pageStart = (long) pageIndex * PAGE_SIZE;
        raf.seek(pageStart);
        raf.write(data);
    }

    private static int trimRightZeros(byte[] buf) {
        int i = buf.length - 1;
        while (i >= 0 && buf[i] == 0) i--;
        return i + 1;
    }

    @Override
    public void close() throws IOException {
        raf.close();
        fjt.close();
    }

    /**
     * TP5: Checkpoint - Force l'écriture de toutes les pages modifiées sur disque.
     */
    public synchronized void checkpoint() throws IOException {
        // Forcer l'écriture sur disque de toutes les pages modifiées
        for (var entry : bufferManager.getBufferPool().entrySet()) {
            BufferPage page = entry.getValue();
            if (page.dirty) {
                writePageBytes(entry.getKey(), page.data);
                page.dirty = false;
            }
        }
        
        // Ajuster la longueur du fichier si nécessaire
        // (pour éviter de compter des enregistrements vides en fin de page)
        long currentLength = raf.length();
        long actualDataLength = 0;
        
        // Calculer la longueur réelle des données
        for (int pageIdx = 0; pageIdx < (currentLength + PAGE_SIZE - 1) / PAGE_SIZE; pageIdx++) {
            byte[] pageData = readPageBytes(pageIdx);
            for (int slot = RECORDS_PER_PAGE - 1; slot >= 0; slot--) {
                boolean isEmpty = true;
                for (int i = 0; i < RECORD_SIZE; i++) {
                    if (pageData[slot * RECORD_SIZE + i] != 0) {
                        isEmpty = false;
                        break;
                    }
                }
                if (!isEmpty) {
                    actualDataLength = (long) pageIdx * PAGE_SIZE + (slot + 1) * RECORD_SIZE;
                    break;
                }
            }
            if (actualDataLength > 0) break;
        }
        
        if (actualDataLength > 0 && actualDataLength < currentLength) {
            raf.setLength(actualDataLength);
        }
        
        // Ajouter entrée CHECKPOINT dans le journal
        int checkpointTxnId = inTransaction ? currentTransactionId : 0;
        addLogEntry(new LogEntry(checkpointTxnId, LogType.CHECKPOINT));
        flushJournalToDisk();
    }

    /**
     * TP5: Recover - Récupération après panne avec REDO et UNDO.
     */
    public synchronized void recover() throws IOException {
        // Lire le journal depuis le fichier
        List<LogEntry> journal = readJournalFromDisk();
        
        if (journal.isEmpty()) {
            return; // Pas de récupération nécessaire
        }
        
        // Analyser le journal pour identifier les transactions commitées
        Set<Integer> committedTransactions = journal.stream()
            .filter(e -> e.type == LogType.COMMIT)
            .map(e -> e.transactionId)
            .collect(Collectors.toSet());
        
        // Trouver le dernier checkpoint
        int lastCheckpointIndex = -1;
        for (int i = journal.size() - 1; i >= 0; i--) {
            if (journal.get(i).type == LogType.CHECKPOINT) {
                lastCheckpointIndex = i;
                break;
            }
        }
        
        // Phase REDO: rejouer les opérations des transactions commitées depuis le checkpoint
        int startIndex = lastCheckpointIndex >= 0 ? lastCheckpointIndex + 1 : 0;
        for (int i = startIndex; i < journal.size(); i++) {
            LogEntry entry = journal.get(i);
            
            if (committedTransactions.contains(entry.transactionId)) {
                if (entry.type == LogType.INSERT || entry.type == LogType.UPDATE) {
                    // REDO: réappliquer l'image après
                    if (entry.afterImage != null && entry.pageId >= 0 && entry.slot >= 0) {
                        BufferPage page = bufferManager.FIX(entry.pageId);
                        System.arraycopy(entry.afterImage, 0, page.data, 
                            entry.slot * RECORD_SIZE, RECORD_SIZE);
                        page.dirty = true;
                        bufferManager.UNFIX(entry.pageId);
                        // Forcer l'écriture sur disque
                        bufferManager.FORCE(entry.pageId);
                    }
                }
            }
        }
        
        // Phase UNDO: annuler les opérations des transactions non commitées
        Set<Integer> activeTransactions = journal.stream()
            .filter(e -> e.type == LogType.BEGIN)
            .map(e -> e.transactionId)
            .collect(Collectors.toSet());
        activeTransactions.removeAll(committedTransactions);
        
        // Parcourir le journal à l'envers pour UNDO
        for (int i = journal.size() - 1; i >= startIndex; i--) {
            LogEntry entry = journal.get(i);
            
            if (activeTransactions.contains(entry.transactionId)) {
                if (entry.type == LogType.INSERT || entry.type == LogType.UPDATE) {
                    // UNDO: restaurer l'image avant
                    if (entry.beforeImage != null && entry.pageId >= 0 && entry.slot >= 0) {
                        BufferPage page = bufferManager.FIX(entry.pageId);
                        System.arraycopy(entry.beforeImage, 0, page.data, 
                            entry.slot * RECORD_SIZE, RECORD_SIZE);
                        page.dirty = true;
                        bufferManager.UNFIX(entry.pageId);
                        // Forcer l'écriture sur disque
                        bufferManager.FORCE(entry.pageId);
                    }
                }
            }
        }
        
        // Nettoyer le buffer pool
        bufferManager.getBufferPool().clear();
    }

    /**
     * Lit le journal depuis le fichier FJT.
     */
    private List<LogEntry> readJournalFromDisk() throws IOException {
        List<LogEntry> journal = new ArrayList<>();
        fjt.seek(0);
        
        try {
            while (fjt.getFilePointer() < fjt.length()) {
                String line = fjt.readLine();
                if (line != null && !line.trim().isEmpty()) {
                    String[] parts = line.split("\\|");
                    if (parts.length >= 2) {
                        int txnId = Integer.parseInt(parts[0]);
                        LogType type = LogType.valueOf(parts[1]);
                        int pageId = parts.length > 2 ? Integer.parseInt(parts[2]) : -1;
                        int slot = parts.length > 3 ? Integer.parseInt(parts[3]) : -1;
                        
                        journal.add(new LogEntry(txnId, type, pageId, slot, null, null));
                    }
                }
            }
        } catch (Exception e) {
            // Fin du fichier ou erreur de parsing
        }
        
        return journal;
    }

    /**
     * TP5: Crash - Simule une panne en vidant tous les buffers sans écriture disque.
     */
    public synchronized void crash() {
        // Vider le buffer pool (perte de toutes les modifications en mémoire)
        bufferManager.getBufferPool().clear();
        
        // Réinitialiser l'état transactionnel
        tiv.clear();
        locks.clear();
        inTransaction = false;
        currentTransactionId = -1;
        txnLogicalLength = -1L;
    }

    /**
     * Ajoute une entrée dans le TJT (Transaction Journal in Memory).
     */
    private void addLogEntry(LogEntry entry) {
        tjt.add(entry);
    }

    /**
     * Force l'écriture du TJT dans le FJT (File Journal on Disk).
     */
    private synchronized void flushJournalToDisk() throws IOException {
        for (LogEntry entry : tjt) {
            String logLine = String.format("%d|%s|%d|%d%n", 
                entry.transactionId, entry.type, entry.pageId, entry.slot);
            fjt.writeBytes(logLine);
        }
        tjt.clear();
    }
}
