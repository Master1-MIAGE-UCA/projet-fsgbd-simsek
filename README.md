
# Mini-SGBD – Étapes 1, 2, 3 & 4

Ce projet implémente un mini-SGBD éducatif qui couvre quatre étapes fondamentales :
1.  **Stockage des données** : Organisation des données en pages de taille fixe sur disque.
2.  **Gestion du buffer** : Utilisation d'un buffer en mémoire pour optimiser les accès disque avec les primitives `FIX`, `UNFIX`, `USE`, `FORCE`.
3.  **Transactions simplifiées** : Ajout d'un mécanisme de transaction avec `BEGIN`, `COMMIT`, `ROLLBACK`.
4.  **TIV, TIA et Verrouillage** : Gestion de la cohérence et de l'isolation avec des images avant/après et des verrous.

## Fonctionnement

### Étape 1 : Stockage par pages
- **Page** : 4096 octets.
- **Enregistrement** : 100 octets.
- **Organisation** : 40 enregistrements par page. Les enregistrements sont ajoutés à la fin du fichier, en respectant les limites de page.

### Étape 2 : Gestion du buffer
- **`insertRecord(String data)`** : Insertion classique qui écrit uniquement dans le buffer en mémoire. Les modifications ne sont pas persistantes sans une action supplémentaire (commit ou force).
- **`insertRecordSync(String data)`** : Insertion synchrone qui écrit dans le buffer et force immédiatement l'écriture sur disque, garantissant la persistance.
- **`readRecord(id)` / `getPage(pageIndex)`** : Utilisent le buffer pour lire les données, optimisant les accès disque.

### Étape 3 : Transactions simplifiées
- **`begin()`** : Démarre une transaction. Les modifications suivantes sont marquées comme transactionnelles.
- **`commit()`** : Valide la transaction en cours. Toutes les pages modifiées et marquées comme transactionnelles sont écrites sur disque.
- **`rollback()`** : Annule la transaction. Les modifications transactionnelles dans le buffer sont ignorées et supprimées.

### Étape 4 : TIV, TIA et Verrouillage
Cette étape introduit une gestion plus fine des transactions pour assurer l'isolation et la cohérence (simulant le comportement d'un SGBD réel).

- **TIA (Tampon d'Images Après)** : Correspond au buffer classique. Il contient les nouvelles valeurs (modifiées).
- **TIV (Tampon d'Images Avant)** : Un buffer temporaire qui stocke l'état original d'une page *avant* sa modification dans une transaction.
- **Verrouillage** : Lorsqu'un enregistrement est modifié dans une transaction, il est verrouillé.
- **Lecture cohérente** :
    - Si une transaction tente de lire un enregistrement qu'elle a elle-même modifié (et verrouillé), le système retourne l'ancienne valeur depuis le **TIV** (règle d'isolation stricte pour l'exercice).
    - Sinon, la lecture se fait depuis le **TIA**.
- **Rollback amélioré** : Le rollback restaure les données depuis le TIV vers le TIA, annulant ainsi les modifications sans avoir besoin de relire le disque.

## Exemple d'utilisation

```java
// Exemple de transaction avec commit et rollback
SGBDManager db = new SGBDManager("etudiants.db");

// 1. Rollback : les modifications sont perdues
db.begin();
db.insertRecord("Etudiant 200");
db.insertRecord("Etudiant 201");
db.rollback(); 
// => "Etudiant 200" et "Etudiant 201" ne sont PAS sur le disque.

// 2. Commit : les modifications sont persistantes
db.begin();
db.insertRecord("Etudiant 202");
db.insertRecord("Etudiant 203");
db.commit();
// => "Etudiant 202" et "Etudiant 203" sont sur le disque.
```

## Compilation et exécution

Le projet utilise Maven.

1.  **Compiler et tester :**
    ```bash
    mvn clean package
    ```

2.  **Exécuter l'exemple principal (`App.java`) :**
    ```bash
    java -cp target/classes fr.uca.sgbd.App
    ```

## Paramètres principaux

- `PAGE_SIZE` = 4096
- `RECORD_SIZE` = 100
- `RECORDS_PER_PAGE` = 40
