
# Mini-SGBD – Étape 3 : Transactions simplifiées (BEGIN / COMMIT / ROLLBACK)

## Objectif
Dans cette étape, nous ajoutons un **mécanisme simplifié de transactions**.  
Une transaction est une suite d’opérations exécutées comme une seule unité logique :  
- Soit **toutes les modifications sont validées** (`COMMIT`),  
- Soit **aucune** n’est conservée (`ROLLBACK`).  

⚠️ Dans cette version simplifiée, nous n’introduisons pas encore de **journal de transaction**.  
  Les mécanismes classiques d'écriture dans la mémoire secondaires sont aussi modifiés pour rendre cette étape plus simple.

---

## Ordres à implémenter

### 1. `BEGIN`
- Lance une transaction.  
- Si une transaction est déjà en cours → elle est **implicitement commitée**.  
- Active un indicateur `inTransaction = true`.  

### 2. `COMMIT`
- Pour chaque page du buffer :  
  - Si elle est marquée comme modifiée **et** liée à la transaction → elle est écrite sur le disque.  
  - Le drapeau transactionnel de la page est remis à `false`.  
- La transaction est terminée (`inTransaction = false`).  

### 3. `ROLLBACK`
- Pour chaque page du buffer :  
  - Si elle est marquée comme liée à la transaction, on **ignore ses modifications** (elle est simplement déchargée du buffer).  
- La transaction est terminée (`inTransaction = false`).  

## Adaptation du modèle de page
Chaque page dans le buffer doit maintenant contenir :  
- Son contenu 
- Un indicateur `dirty` (déjà présent, signale si modifiée).  
- **NOUVEAU : un indicateur `transactional` (booléen)** qui dit si la modification appartient à une transaction en cours.  

##  Nouvelle API à implémenter

```java
public void begin();
public void commit() throws IOException;
public void rollback();
```

## Exemple d’utilisation

```java
MiniSGBD db = new MiniSGBD("etudiants.db");

// Exemple 1 : rollback
db.begin();
db.insertRecord("Etudiant 200");   
db.insertRecord("Etudiant 201");
db.rollback(); 						// les pages transactionnelles sont ignorées
// => Etudiant 200 et 201 ne sont PAS sur disque

// Exemple 2 : commit
db.begin();
db.insertRecord("Etudiant 202");
db.insertRecord("Etudiant 203");
db.commit(); 						// les pages transactionnelles sont forcées sur disque
// => Etudiant 202 et 203 sont bien présents

```

## Résultat attendu

-   Après un `ROLLBACK`, les modifications effectuées depuis le `BEGIN` sont perdues.
    
-   Après un `COMMIT`, les modifications sont garanties présentes sur disque.
    
-   Les pages sont marquées ou pas comme **transactionnelles** selon l’état de la transaction.

## Rendu

Ajoutez le code produit pour ce TD au dépôt que vous avez utilisé précédemment. Pensez à ajouter un tag afin que l'on puisse facilement retrouver vos productions pour les différentes étapes.
