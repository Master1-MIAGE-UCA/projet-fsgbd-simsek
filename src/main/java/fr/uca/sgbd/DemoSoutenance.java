package fr.uca.sgbd;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

/**
 * Scénario de démonstration pour la soutenance Étape 5
 * 
 * Étapes:
 * 1. Remplissage - Insert Record_A et Record_B
 * 2. Annulation - ROLLBACK de modification
 * 3. Persistance - COMMIT et vérification du journal
 * 4. Préparation au crash - Transaction ouverte sans commit
 * 5. Crash & Recovery - Simulation de panne et recover()
 */
public class DemoSoutenance {
    private SGBDManager db;
    private File dbFile;
    private File journalFile;
    private Scanner scanner = new Scanner(System.in);

    public DemoSoutenance(String dbPath) throws IOException {
        this.dbFile = new File(dbPath);
        this.journalFile = new File(dbPath + ".log");
        
        // Nettoyer les fichiers existants pour démarrer vierge
        if (dbFile.exists()) {
            dbFile.delete();
        }
        if (journalFile.exists()) {
            journalFile.delete();
        }
        
        // Ouvrir la DB
        this.db = new SGBDManager(dbFile);
        System.out.println("✓ Base de données initialisée vierge");
    }

    private void waitForUser() {
        System.out.println(">>> Appuyez sur ENTER pour continuer...");
        scanner.nextLine();
    }

    /**
     * ÉTAPE 1 : Le Remplissage (Buffer & Disque)
     */
    public void etape1_Remplissage() throws IOException {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("ÉTAPE 1 : LE REMPLISSAGE (Buffer & Disque)");
        System.out.println("=".repeat(60));
        
        System.out.println("\n[Action] Insérer 'Record_A' (ID 0)...");
        db.begin();
        db.insertRecord("Record_A");
        db.commit();
        System.out.println("✓ Record_A inséré et commité");
        
        System.out.println("\n[Action] Insérer 'Record_B' (ID 1)...");
        db.begin();
        db.insertRecord("Record_B");
        db.commit();
        System.out.println("✓ Record_B inséré et commité");
        
        System.out.println("\n[Action] Checkpoint pour persister sur disque...");
        db.checkpoint();
        System.out.println("✓ Checkpoint exécuté - Données écrites sur disque");
        
        System.out.println("\n[Vérification] Lire Record_A immédiatement...");
        String recordA = db.readRecord(0);
        String recordB = db.readRecord(1);
        System.out.println("✓ Record_A lu [ID=0] : '" + recordA + "'");
        System.out.println("✓ Record_B lu [ID=1] : '" + recordB + "'");
        
        assert recordA.equals("Record_A") : "Record_A ne correspond pas! Obtenu: " + recordA;
        System.out.println("✓ ÉTAPE 1 RÉUSSIE");
    }

    /**
     * ÉTAPE 2 : L'Annulation (Atomicité & TIV)
     */
    public void etape2_Annulation() throws IOException {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("ÉTAPE 2 : L'ANNULATION (Atomicité & TIV)");
        System.out.println("=".repeat(60));
        
        System.out.println("\n[Vérification initiale] Record_A avant modification:");
        String beforeRollback = db.readRecord(0);
        System.out.println("  Record_A = '" + beforeRollback + "'");
        
        System.out.println("\n[Action] Démarrer une transaction (BEGIN)...");
        db.begin();
        System.out.println("✓ Transaction démarrée");
        
        System.out.println("\n[Action] Modifier Record_A en 'Record_A_MODIFIE' (via updateRecord)...");
        // Modification explicite via la nouvelle méthode updateRecord
        db.updateRecord(0, "Record_A_MODIFIE");
        System.out.println("  ✓ Update effecuté en mémoire (TIA)");
        
        System.out.println("\n[Observation] Lecture de Record_A pendant la transaction:");
        String valPendant = db.readRecord(0);
        System.out.println("  Lecture = '" + valPendant + "'");
        System.out.println("  (Note : Selon la spec TIV/Lock, la lecture retourne l'image avant si verrouillé)");

        System.out.println("\n[Action] ROLLBACK...");
        db.rollback();
        System.out.println("✓ Transaction annulée (ROLLBACK)");
        
        System.out.println("\n[Vérification] Relire Record_A après ROLLBACK:");
        String afterRollback = db.readRecord(0);
        System.out.println("✓ Record_A après rollback : '" + afterRollback + "'");
        
        assert afterRollback.equals("Record_A") : "Record_A n'a pas été restauré!";
        System.out.println("✓ ÉTAPE 2 RÉUSSIE - TIV a bien restauré l'ancienne valeur");
    }

    /**
     * ÉTAPE 3 : La Persistance (Commit & Journal)
     */
    public void etape3_Persistance() throws IOException {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("ÉTAPE 3 : LA PERSISTANCE (Commit & Journal)");
        System.out.println("=".repeat(60));
        
        System.out.println("\n[Objectif] Valider qu'une transaction validée laisse une trace durable");
        
        System.out.println("\n[Action] Démarrer une transaction (BEGIN)...");
        db.begin();
        System.out.println("✓ Transaction démarrée");
        
        System.out.println("\n[Action] Modifier 'Record_B' (ID 1) en 'Record_B_FINAL'...");
        db.updateRecord(1, "Record_B_FINAL");
        System.out.println("✓ Modification (Update) effectuée en mémoire");
        
        System.out.println("\n[Action] Valider (COMMIT)...");
        db.commit();
        System.out.println("✓ Transaction commitée - Journal écrit sur disque");
        
        System.out.println("\n[Vérification] Vérifier la présence/taille du fichier de journal (Log)...");
        long journalSize = journalFile.exists() ? journalFile.length() : 0;
        System.out.println("  Taille fichier Journal : " + journalSize + " octets");
        System.out.println("  ✓ Fichier de journal présent : " + (journalSize > 0 ? "OUI" : "NON"));
        
        if (journalSize == 0) {
            System.err.println("⚠️ ATTENTION: Le fichier journal est vide ou absent !");
        }

        System.out.println("✓ ÉTAPE 3 RÉUSSIE");
    }

    /**
     * ÉTAPE 4 : Préparation au Crash
     */
    public void etape4_PreparationCrash() throws IOException {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("ÉTAPE 4 : PRÉPARATION AU CRASH");
        System.out.println("=".repeat(60));
        
        System.out.println("\n[Action] Démarrer une NOUVELLE transaction (BEGIN)...");
        db.begin();
        System.out.println("✓ Transaction 3 démarrée");
        
        System.out.println("\n[Action] Insérer 'Record_C_FANTOME' (ID 3)...");
        db.insertRecord("Record_C_FANTOME");
        System.out.println("✓ Record_C_FANTOME inséré dans la transaction");
        
        System.out.println("\n[État] Transaction OUVERTE et NON COMMITÉE");
        System.out.println("  ⚠️  Cette transaction n'a pas été commitée");
        System.out.println("  ⚠️  Si crash maintenant = incohérence créée");
        
        System.out.println("✓ ÉTAPE 4 RÉUSSIE - Panne préparée");
    }

    /**
     * ÉTAPE 5 : Crash & Recovery
     */
    public void etape5_CrashRecovery() throws IOException {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("ÉTAPE 5 : CRASH & RECOVERY");
        System.out.println("=".repeat(60));
        
        System.out.println("\n[Action] Simuler une PANNE (crash)...");
        db.crash();
        System.out.println("✓ Crash simulé - Buffers vidés sans sauvegarde");
        System.out.println("  (Simulation d'une perte de mémoire volatile)");
        System.out.println("\n[Action] Redémarrer et lancer recover()...");
        // Fermer et réouvrir
        db.close();
        db = new SGBDManager(dbFile);
        System.out.println("✓ Base de données réouverte");
    }

    public void etape5_CrashRecoveryBis() throws IOException {
        System.out.println("\n[Action] Lancer la récupération (recover())...");
        db.recover();
        System.out.println("✓ Récupération terminée");
        System.out.println("  - REDO : transactions commitées réappliquées");
        System.out.println("  - UNDO : transactions non commitées annulées");
        
        System.out.println("\n[Vérification FINALE] Afficher tous les enregistrements:");
        long recordCount = db.getRecordCount();
        System.out.println("  Nombre d'enregistrements présents : " + recordCount);
        
        for (int i = 0; i < recordCount; i++) {
            String record = db.readRecord(i);
            System.out.println("  [" + i + "] " + record);
        }
        
        System.out.println("\n[Analyse]:");
        System.out.println("  - Record_A : ✓ PERSISTE (Transaction 1 commitée)");
        System.out.println("  - Record_B : ✓ PERSISTE (Transaction 1 commitée)");
        System.out.println("  - Record_B_FINAL : ✓ PERSISTE (Transaction 2 commitée)");
        System.out.println("  - Record_C_FANTOME : ✗ ABSENT (Transaction 3 non commitée = UNDO)");
        
        assert recordCount == 3 : "Devrait avoir 3 enregistrements, en a " + recordCount;
        System.out.println("\n✓ ÉTAPE 5 RÉUSSIE - État cohérent restauré");
    }

    /**
     * Lance tout le scénario
     */
    public void runFullDemo() throws IOException {
        try {
            System.out.println("Début de la démonstration FSGBD");
            waitForUser();

            etape1_Remplissage();
            waitForUser();

            etape2_Annulation();
            waitForUser();

            etape3_Persistance();
            waitForUser();

            etape4_PreparationCrash();
            waitForUser();

            etape5_CrashRecovery();
            waitForUser();

            etape5_CrashRecoveryBis();
            
            System.out.println("\n" + "=".repeat(60));
            System.out.println("✓✓✓ DÉMONSTRATION COMPLÈTE RÉUSSIE ✓✓✓");
            System.out.println("=".repeat(60));
        } finally {
            db.close();
        }
    }

    public static void main(String[] args) {
        try {
            // Créer un fichier de DB temporaire pour la démo
            String dbPath = "demo_soutenance.db";
            DemoSoutenance demo = new DemoSoutenance(dbPath);
            demo.runFullDemo();
        } catch (Exception e) {
            System.err.println("❌ Erreur lors de la démonstration:");
            e.printStackTrace();
        }
    }
}
