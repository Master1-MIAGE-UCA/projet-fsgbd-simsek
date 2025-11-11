package fr.uca.sgbd;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class App {
    public static void main(String[] args) throws IOException {
        String dbPath = args.length > 0 ? args[0] : "etudiants.db";

    // Nettoyage pour la démonstration : supprime le fichier s'il existe
        Path p = Path.of(dbPath);
        Files.deleteIfExists(p);

        try (MiniSGBD db = new MiniSGBD(dbPath)) {
            // Insertion de 105 enregistrements
            for (int i = 1; i <= 105; i++) {
                db.insertRecord("Etudiant " + i);
            }

            // Lecture d'un enregistrement (id 41 -> affiche "Etudiant 42")
            System.out.println("Enregistrement 42 : " + db.readRecord(41));

            // Lecture de pages complètes
            System.out.println("Page 1 : " + db.getPage(0));
            System.out.println("Page 2 : " + db.getPage(1));
            System.out.println("Page 3 : " + db.getPage(2));

            // Informations supplémentaires
            System.out.println("Total records: " + db.getRecordCount());
            System.out.println("Records per page: " + MiniSGBD.RECORDS_PER_PAGE);
        }
    }
}
