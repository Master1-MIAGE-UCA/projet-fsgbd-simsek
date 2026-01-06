package fr.uca.sgbd;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Gère le buffer de pages en mémoire et les primitives FIX, UNFIX, USE, FORCE.
 */
public class BufferManager {
    private final Map<Integer, BufferPage> bufferPool = new HashMap<>();
    public Map<Integer, BufferPage> getBufferPool() {
        return bufferPool;
    }
    private final SGBDManager sgbdManager;

    public BufferManager(SGBDManager sgbdManager) {
        this.sgbdManager = sgbdManager;
    }

    /**
     * FIX(pageId) : charge une page en mémoire si besoin, incrémente le compteur d’utilisation.
     * Retourne le BufferPage correspondant.
     */
    public BufferPage FIX(int pageId) throws IOException {
        BufferPage page = bufferPool.get(pageId);
        if (page == null) {
            byte[] pageData = sgbdManager.readPageBytes(pageId);
            page = new BufferPage(pageData);
            bufferPool.put(pageId, page);
        }
        page.usageCount++;
        return page;
    }

    /**
     * UNFIX(pageId) : décrémente le compteur d’utilisation.
     * Si le compteur atteint zéro, la page peut être évacuée du buffer (non fait ici).
     */
    public void UNFIX(int pageId) {
        BufferPage page = bufferPool.get(pageId);
        if (page != null && page.usageCount > 0) {
            page.usageCount--;
            // Optionnel : évacuer si usageCount == 0
        }
    }

    /**
     * USE(pageId) : marque la page comme modifiée (dirty).
     */
    public void USE(int pageId) {
        BufferPage page = bufferPool.get(pageId);
        if (page != null) {
            page.dirty = true;
        }
    }

    /**
     * FORCE(pageId) : écrit la page sur disque si elle est modifiée (dirty).
     */
    public void FORCE(int pageId) throws IOException {
        BufferPage page = bufferPool.get(pageId);
        if (page != null && page.dirty) {
            sgbdManager.writePageBytes(pageId, page.data);
            page.dirty = false;
        }
    }
}
