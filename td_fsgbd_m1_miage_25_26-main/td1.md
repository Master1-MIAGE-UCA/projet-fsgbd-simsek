
# Mini-SGBD – Etape 1 : Stockage des données

##  Objectif
Cette première étape consiste à comprendre **comment un SGBD stocke ses données sur disque**.  
Plutôt que de lire/écrire ligne par ligne comme dans un fichier texte, un vrai SGBD organise ses données en **pages** de taille fixe (par exemple 4 Ko).  

- Chaque enregistrement est stocké dans une **page**.  
- Chaque page contient un nombre fixe d’enregistrements.  

## Précisions

- **Page** : unité de stockage de base (ici 4096 octets = 4 Ko).  
-  **Enregistrement** (record) : une ligne de table, de taille fixe (ici 100 octets).  
- **Organisation** :
  - 1 page = `PAGE_SIZE / RECORD_SIZE` enregistrements.  
  - Exemple : `4096 / 100 = 40` enregistrements par page.  

Si nous stockons 160 étudiants dans un fichier `etudiants.db` :

- **Page 0** : Étudiants 1 → 40  
- **Page 1** : Étudiants 41 → 80  
- **Page 2** : Étudiants 81 → 120
- **Page 3** : Étudiants 121 → 160

##  Fonctionnalités à implémenter

- **Insertion (`insertRecord`)**  
  Écrit un enregistrement à la fin du fichier.  
  Chaque enregistrement est normalisé à 100 octets (padding avec des zéros si plus court).

- **Lecture d’un enregistrement (`readRecord`)**  
  Récupère un enregistrement par son identifiant (ID).  
  Exemple : `readRecord(41)` retourne l’étudiant numéro 42.

- **Lecture d’une page (`getPage`)**  
  Récupère tous les enregistrements contenus dans une page.  
  Exemple : `getPage(0)` retourne les 40 premiers étudiants.

## Exemple d'exécution

```java
public static void main(String[] args) throws IOException {
    MiniSGBD db = new MiniSGBD("etudiants.db");

    // Insertion de 105 enregistrements
    for (int i = 1; i <= 105; i++) {
        db.insertRecord("Etudiant " + i);
    }

    // Lecture d'un enregistrement
    System.out.println("Enregistrement 42 : " + db.readRecord(41));

    // Lecture de pages entières
    System.out.println("Page 1 : " + db.getPage(0));
    System.out.println("Page 2 : " + db.getPage(1));
    System.out.println("Page 3 : " + db.getPage(2));
}
```
## Rendu
Informations à venir sur Moodle
