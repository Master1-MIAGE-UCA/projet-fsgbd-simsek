package fr.uca.sgbd;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class App {
    public static void main(String[] args) throws IOException {
        String dbPath = "etudiants.db";
        java.nio.file.Files.deleteIfExists(java.nio.file.Path.of(dbPath));
        MiniSGBD db = new MiniSGBD(dbPath);

        // Exemple 1 : rollback
        db.begin();
        db.insertRecord("Etudiant 200");
        db.insertRecord("Etudiant 201");
        db.rollback();

        System.out.println("Rollback sonrası kayıtlar:");
        for (int i = 0; i < db.getRecordCount(); i++) {
            System.out.println("Record " + i + ": " + db.readRecord(i));
        }

        // Exemple 2 : commit
        db.begin();
        db.insertRecord("Etudiant 202");
        db.insertRecord("Etudiant 203");
        db.commit();

        System.out.println("Commit sonrası kayıtlar:");
        for (int i = 0; i < db.getRecordCount(); i++) {
            System.out.println("Record " + i + ": " + db.readRecord(i));
        }

        db.close();
    }
}
