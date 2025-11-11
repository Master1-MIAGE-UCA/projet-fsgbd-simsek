
# Mini-SGBD – Étape 1 & 2 : Stockage des données + Gestion du buffer

Ce projet montre comment un SGBD simple stocke ses données sur disque et gère les pages en mémoire.

Les données sont organisées en pages de taille fixe (4096 octets) et chaque enregistrement occupe 100 octets.

## Fonctionnement

### Étape 1 : Stockage par pages
- Une page = 4096 octets, un enregistrement = 100 octets, donc 40 enregistrements par page.
- Les enregistrements sont écrits à la fin du fichier, alignés sur les pages.
- Lecture par identifiant : `readRecord(id)` (exemple : `readRecord(41)` retourne "Etudiant 42").
- Lecture d'une page : `getPage(pageIndex)` (exemple : `getPage(0)` retourne les 40 premiers étudiants).

### Étape 2 : Gestion du buffer et primitives
Un SGBD charge les pages en mémoire avant de les manipuler (plus rapide que lire/écrire directement sur disque).

**Primitives implémentées :**
- **FIX(pageId)** : charge une page en mémoire (ou la récupère si elle y est déjà), incrémente le compteur d'utilisation.
- **UNFIX(pageId)** : indique que la page n'est plus utilisée, décrémente le compteur.
- **USE(pageId)** : marque la page comme modifiée (dirty).
- **FORCE(pageId)** : écrit la page sur disque si elle est modifiée.

**Nouvelles fonctionnalités :**
- `insertRecordSync(String data)` : insère un enregistrement de façon synchrone (écrit dans le buffer ET force l'écriture sur disque).
- `readRecord()` et `getPage()` utilisent maintenant le buffer via FIX/UNFIX.

## Exemple d'utilisation

```java
MiniSGBD db = new MiniSGBD("etudiants.db");

// Insertion synchrone (écrit dans buffer + disque)
for (int i = 1; i <= 105; i++) {
    db.insertRecordSync("Etudiant " + i);
}

// Lecture avec buffer
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

- Insertion classique `insertRecord()` : écrit directement sur disque (sans buffer).
- Insertion synchrone `insertRecordSync()` : utilise le buffer avec FIX/USE/FORCE.
- Lecture/écriture passent par le buffer via les primitives FIX/UNFIX.
- Le buffer est chargé en mémoire et ne s'évacue pas automatiquement (acceptable pour ce TD).
- Pas de suppression/mise à jour, uniquement insertion en fin.
- Pas de métadonnées ni de catalogue.

Projet éducatif. Aucune licence spécifique.