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

/**
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
public class MiniSGBD implements AutoCloseable {
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
    /**
     * Lance une transaction. Si une transaction est déjà en cours, elle est commitée.
     */
    public synchronized void begin() throws IOException {
        if (inTransaction) {
            commit();
        }
        inTransaction = true;
        txnLogicalLength = raf.length();
    }

    /**
     * Commit la transaction : écrit les pages transactionnelles modifiées sur disque.
     */
    public synchronized void commit() throws IOException {
        for (var entry : bufferManager.getBufferPool().entrySet()) {
            BufferFrame frame = entry.getValue();
            if (frame.dirty && frame.transactional) {
                writePageBytes(entry.getKey(), frame.data);
                frame.dirty = false;
            }
            frame.transactional = false;
        }
                // Finalise la longueur du fichier selon la longueur logique de la transaction
        // TP4 Correction: writePageBytes agrandit le fichier à la taille de la page (4096).
        // Mais la taille réelle des données est txnLogicalLength. Nous devons couper le fichier (trim).
        if (txnLogicalLength >= 0) {
            raf.setLength(txnLogicalLength);
        }
        
        // TP4: Temizlik
        tiv.clear();
        locks.clear();
        
        inTransaction = false;
        txnLogicalLength = -1L;
    }

    /**
     * Rollback la transaction : ignore les modifications des pages transactionnelles.
     */
    public synchronized void rollback() {
        // TP4: Restauration depuis le TIV
        for (Map.Entry<Integer, byte[]> entry : tiv.entrySet()) {
            int pageId = entry.getKey();
            byte[] originalData = entry.getValue();
            try {
                BufferFrame frame = bufferManager.FIX(pageId);
                System.arraycopy(originalData, 0, frame.data, 0, PAGE_SIZE);
                // La donnée restaurée est considérée comme "propre" (identique au disque)
                // ou son état précédent. Pour simplifier, on met dirty=false.
                frame.dirty = false; 
                frame.transactional = false;
                bufferManager.UNFIX(pageId);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        // Nous utilisons le TIV au lieu de l'ancienne méthode (suppression des pages).
        // Cependant, peut-il y avoir des pages nouvellement créées pendant la transaction et absentes du TIV ?
        // Dans la logique insertRecord, nous enregistrons dans le TIV à chaque modification.
        // Même s'il s'agit d'une nouvelle page (vide), elle doit être ajoutée au TIV.
        
        tiv.clear();
        locks.clear();
        inTransaction = false;
        txnLogicalLength = -1L;
    }

    public MiniSGBD(String path) throws FileNotFoundException {
        this(new File(path));
    }

    public MiniSGBD(File file) throws FileNotFoundException {
        this.raf = new RandomAccessFile(file, "rw");
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

        BufferFrame frame = bufferManager.FIX(pageIndex);
        try {
            // TP4: Verrouillage et copie dans le TIV
            if (inTransaction) {
                String lockKey = pageIndex + ":" + slot;
                if (!locks.contains(lockKey)) {
                    locks.add(lockKey);
                    // Sauvegarder l'état original de la page dans le TIV (si absent)
                    if (!tiv.containsKey(pageIndex)) {
                        byte[] original = new byte[PAGE_SIZE];
                        System.arraycopy(frame.data, 0, original, 0, PAGE_SIZE);
                        tiv.put(pageIndex, original);
                    }
                }
            }

            System.arraycopy(fixed, 0, frame.data, slot * RECORD_SIZE, RECORD_SIZE);
            bufferManager.USE(pageIndex);
            if (inTransaction) {
                frame.transactional = true;
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
            BufferFrame frame = bufferManager.FIX(pageIndex);
            int slot = -1;
            try {
                for (int i = 0; i < RECORDS_PER_PAGE; i++) {
                    boolean empty = true;
                    for (int j = 0; j < RECORD_SIZE; j++) {
                        if (frame.data[i * RECORD_SIZE + j] != 0) { empty = false; break; }
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
                                System.arraycopy(frame.data, 0, original, 0, PAGE_SIZE);
                                tiv.put(pageIndex, original);
                            }
                        }
                    }

                    System.arraycopy(fixed, 0, frame.data, slot * RECORD_SIZE, RECORD_SIZE);
                    bufferManager.USE(pageIndex);
                    if (inTransaction) {
                        frame.transactional = true;
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
        BufferFrame frame = bufferManager.FIX((int) pageIndex);
        try {
            byte[] sourceData = frame.data;

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

        List<String> page = new ArrayList<>(toReadRecords);
        BufferFrame frame = bufferManager.FIX(pageIndex);
        try {
            for (int i = 0; i < toReadRecords; i++) {
                byte[] buf = new byte[RECORD_SIZE];
                System.arraycopy(frame.data, i * RECORD_SIZE, buf, 0, RECORD_SIZE);
                int effective = trimRightZeros(buf);
                page.add(new String(buf, 0, effective, StandardCharsets.UTF_8));
            }
        } finally {
            bufferManager.UNFIX(pageIndex);
        }
        return page;
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
    }
}
