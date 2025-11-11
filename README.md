
# Mini-SGBD – Étape 1 : Stockage des données

Ce projet montre comment un SGBD simple stocke ses données sur disque : les données sont organisées en pages de taille fixe (4096 octets) et chaque enregistrement occupe 100 octets.

## Fonctionnement

- Une page = 4096 octets, un enregistrement = 100 octets, donc 40 enregistrements par page.
- Les enregistrements sont écrits à la fin du fichier, alignés sur les pages.
- Lecture par identifiant : `readRecord(id)` (exemple : `readRecord(41)` retourne "Etudiant 42").
- Lecture d’une page : `getPage(pageIndex)` (exemple : `getPage(0)` retourne les 40 premiers étudiants).

## Exemple d’utilisation

```java
MiniSGBD db = new MiniSGBD("etudiants.db");
for (int i = 1; i <= 105; i++) db.insertRecord("Etudiant " + i);
System.out.println("Enregistrement 42 : " + db.readRecord(41));
System.out.println("Page 1 : " + db.getPage(0));
System.out.println("Page 2 : " + db.getPage(1));
System.out.println("Page 3 : " + db.getPage(2));
```

Sortie attendue :

```
Enregistrement 42 : Etudiant 42
Page 1 : [Etudiant 1, ..., Etudiant 40]
Page 2 : [Etudiant 41, ..., Etudiant 80]
Page 3 : [Etudiant 81, ..., Etudiant 105]
```

## Compilation et exécution

Compiler :
```bash
javac src/main/java/fr/uca/sgbd/*.java
```

Exécuter :
```bash
java -cp src/main/java fr.uca.sgbd.App
```

## Paramètres principaux

- PAGE_SIZE = 4096
- RECORD_SIZE = 100
- RECORDS_PER_PAGE = 40

## Remarques

- Pas de suppression/mise à jour, uniquement insertion en fin.
- Pas de métadonnées ni de catalogue.
- Pas de buffering en mémoire : lecture/écriture directes.

Projet éducatif. Aucune licence spécifique.