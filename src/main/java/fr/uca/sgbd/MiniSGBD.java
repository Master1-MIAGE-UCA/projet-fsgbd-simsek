package fr.uca.sgbd;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

    public MiniSGBD(String path) throws FileNotFoundException {
        this(new File(path));
    }

    public MiniSGBD(File file) throws FileNotFoundException {
    this.raf = new RandomAccessFile(file, "rw");
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

        long length = raf.length();
        long offsetInPage = length % PAGE_SIZE;
        if (offsetInPage + RECORD_SIZE > PAGE_SIZE) {
            // saut à la prochaine page
            long skip = PAGE_SIZE - offsetInPage;
            raf.seek(length + skip);
        } else {
            raf.seek(length);
        }
        raf.write(fixed);
    }

    /**
     * Lit un enregistrement par son index (0-based).
     * Retourne la chaîne décodée sans les octets nuls de fin.
     */
    public synchronized String readRecord(int recordId) throws IOException {
        if (recordId < 0) throw new IndexOutOfBoundsException("recordId négatif");

        long pageIndex = recordId / RECORDS_PER_PAGE;
        long indexInPage = recordId % RECORDS_PER_PAGE;
        long pos = pageIndex * PAGE_SIZE + indexInPage * RECORD_SIZE;
        if (pos + RECORD_SIZE > raf.length()) throw new IndexOutOfBoundsException("recordId hors bornes");

        byte[] buf = new byte[RECORD_SIZE];
        raf.seek(pos);
        int read = raf.read(buf);
        if (read < RECORD_SIZE) throw new EOFException("Enregistrement incomplet lu");

        int effective = trimRightZeros(buf);
        return new String(buf, 0, effective, StandardCharsets.UTF_8);
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
        byte[] buf = new byte[RECORD_SIZE];
        long pos = pageStart;
        raf.seek(pos);
        for (int i = 0; i < toReadRecords; i++) {
            int read = raf.read(buf);
            if (read < RECORD_SIZE) break; // enregistrement partiel en fin de fichier
            int effective = trimRightZeros(buf);
            page.add(new String(buf, 0, effective, StandardCharsets.UTF_8));
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
        int bytesRead = raf.read(pageData);

        // Si la page n'est pas entièrement lue (fin de fichier), le reste reste à zéro
        return pageData;
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
