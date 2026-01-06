package fr.uca.sgbd;

/**
 * Représente une page en mémoire tampon (buffer).
 * Contient les données brutes, l'état de modification (dirty) et le compteur d'utilisation.
 * 
 * 
 */
public class BufferPage {
    public byte[] data;
    public boolean dirty;
    public int usageCount;
    public boolean transactional;

    public BufferPage(byte[] data) {
        this.data = data;
        this.dirty = false;
        this.usageCount = 0;
        this.transactional = false;
    }
}
