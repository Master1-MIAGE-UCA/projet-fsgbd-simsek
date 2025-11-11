package fr.uca.sgbd;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Gère le buffer de pages en mémoire et les primitives FIX, UNFIX, USE, FORCE.
 */
public class BufferManager {
    private final Map<Integer, BufferFrame> bufferPool = new HashMap<>();
    private final MiniSGBD miniSGBD;

    public BufferManager(MiniSGBD miniSGBD) {
        this.miniSGBD = miniSGBD;
    }

    /**
     * FIX(pageId) : charge une page en mémoire si besoin, incrémente le compteur d’utilisation.
     * Retourne le BufferFrame correspondant.
     */
    public BufferFrame FIX(int pageId) throws IOException {
        BufferFrame frame = bufferPool.get(pageId);
        if (frame == null) {
            byte[] pageData = miniSGBD.readPageBytes(pageId);
            frame = new BufferFrame(pageData);
            bufferPool.put(pageId, frame);
        }
        frame.usageCount++;
        return frame;
    }

    /**
     * UNFIX(pageId) : décrémente le compteur d’utilisation.
     * Si le compteur atteint zéro, la page peut être évacuée du buffer (non fait ici).
     */
    public void UNFIX(int pageId) {
        BufferFrame frame = bufferPool.get(pageId);
        if (frame != null && frame.usageCount > 0) {
            frame.usageCount--;
            // Optionnel : évacuer si usageCount == 0
        }
    }

    /**
     * USE(pageId) : marque la page comme modifiée (dirty).
     */
    public void USE(int pageId) {
        BufferFrame frame = bufferPool.get(pageId);
        if (frame != null) {
            frame.dirty = true;
        }
    }

    /**
     * FORCE(pageId) : écrit la page sur disque si elle est modifiée (dirty).
     */
    public void FORCE(int pageId) throws IOException {
        BufferFrame frame = bufferPool.get(pageId);
        if (frame != null && frame.dirty) {
            miniSGBD.writePageBytes(pageId, frame.data);
            frame.dirty = false;
        }
    }
}
