package fr.uca.sgbd;

/**
 * Types d'entrées dans le journal de transactions.
 */
public enum LogType {
    BEGIN,
    UPDATE,
    INSERT,
    DELETE,
    COMMIT,
    ROLLBACK,
    CHECKPOINT
}
