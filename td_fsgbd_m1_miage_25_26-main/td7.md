
# Mini-SGBD – Étape 5 : Techniques de reprise sur panne : Journalisation, Checkpoints, Algorithmes UNDO / REDO

## 1. Objectif

Dans cette étape, vous allez ajouter à votre mini-SGBD les premiers mécanismes d’une **technique de reprise sur panne** comparable à celles utilisées par les SGBD réels.

Vous allez implémenter :
-   un **journal de transactions (log)**.
-   un système de **checkpoints**.
-   un algorithme simple de **récupération** permettant de restaurer un état cohérent après une panne.

Ces éléments constituent les fondations des techniques **UNDO / REDO**.

## 2. Contexte pédagogique

Dans le chapitre précédent, vous avez étudié :

-   les différents espaces mémoire (buffer, mémoire stable, mémoire volatile).
-   les stratégies d’écriture sur disque.
-   la journalisation.   
-   les points de sauvegarde (_checkpoints_).
-   les algorithmes de reprise après panne.

Ce TD vous fera implémenter une version **réduite**, mais réaliste, de ces mécanismes.

## 3. Nouveaux éléments à intégrer au Mini-SGBD

### 3.1 Journal de transactions (TJT)

Vous devez créer un **log** permettant d’enregistrer les opérations effectuées par les transactions.

Structure suggérée :

`class  LogEntry { int transactionId;
    RecordId recordId; byte[] beforeImage; byte[] afterImage;
    LogType type; // UPDATE, INSERT, DELETE, BEGIN, COMMIT, ROLLBACK, CHECKPOINT }` 

Et une structure de stockage :

`List<LogEntry> log;` 

Le journal doit être mis à jour **à chaque opération significative**, notamment :

-   `BEGIN`
-   `UPDATE` (avec images avant/après)
-   `INSERT`
-   `DELETE`
-   `COMMIT`
-   `ROLLBACK`
-   `CHECKPOINT`

### 3.2 Fichier de journal de transactions (FJT)

Vous devrez créer un fichier qui sera le pendant du journal de transactions mais sous forme de fichier.

Le contenu sera exactement le même que pour le journal de transactions.
    
### 3.3 Points de sauvegarde (checkpoints)

Vous devez implémenter une méthode :
`public  void  checkpoint()` 

Cette méthode devra :

1.  Forcer l’écriture sur disque de toutes les pages modifiées (FORCE).
2.  Ajouter dans le journal une entrée `CHECKPOINT`.

L’intérêt du checkpoint : réduire la quantité de log nécessaire lors de la récupération.

### 3.4 Algorithme de reprise (recovery)

Lorsque votre mini-SGBD démarre (simulation : appel à `recover()`), il doit être capable de :

#### 1. Lire le journal depuis le début.
#### 2. Déterminer :
-   les transactions **commitées**.
-   les transactions **non commitées** au moment de la panne.
    
#### 3. Effectuer :
### Phase REDO (refaire, pour les transactions commitées)

Rejouer toutes les opérations des transactions commitées depuis le dernier checkpoint : réappliquer les "images après" dans le TIA puis sur disque.

### Phase UNDO (annuler, pour les transactions non commitées)

Pour les transactions non commitées :  restaurer les "images avant" dans TIA et forcer un ROLLBACK

## 4. Modifications à intégrer dans le Mini-SGBD

### 4.1 Mise à jour du COMMIT

Lors d’un `COMMIT` :

1.  Écrire dans le TJT une entrée `COMMIT`.
2.  Forcer l'écriture du TJT dans le FJT, puis vider le TJT.
3.  Vider les états transactionnels en mémoire (TIV, verrous, etc.).

Le COMMIT ne dois plus forcer l'écriture sur le disque

### 4.2 Mise à jour du ROLLBACK

Lors d’un rollback explicite :

1.  Lire les "images avant" dans le TIV.    
2.  Restaurer les valeurs dans le TIA.    
3.  Écrire dans le TJT une entrée `ROLLBACK`.
4.  Forcer l'écriture du TJT dans le FJT, puis vider le TJT.    

## 5. Travail demandé

### Étape 1 — Construire la classe `LogEntry`.

Inclure 
-   identifiant de transaction
-   identifiant de l’enregistrement
-   image avant
-   images après
-   type de log
    

### Étape 2 — Modifier les opérations du SGBD.

Chaque opération doit générer une entrée dans le journal :

-   `BEGIN(transaction)`
-   `UPDATE(record)`
-   `INSERT(record)`
-   `DELETE(record)`
-   `COMMIT(transaction)`
-   `ROLLBACK(transaction)`
    

### Étape 3 — Implémenter le `checkpoint()`.

### Étape 4 — Implémenter la méthode `recover()`.

Elle devra appliquer :
1.  Analyse du journal
2.  REDO des transactions commitées
3.  UNDO des transactions non commitées
    

### Étape 5 — Expérimentations

Vous devrez simuler un crash :

1.  Lancer quelques opérations.
2.  Lancer un `crash()` qui ira vider tous les buffers
3.  Lancer la récupération avec  `recover()`.
4.  Vérifier que l’état obtenu est cohérent.


## 6. Résultat attendu

À la fin de ce TD, votre mini-SGBD doit pouvoir :

-   maintenir un journal cohérent.
-   réaliser des checkpoints.
-   rejouer les modifications commitées après un crash.
-   annuler les modifications non commitées.
-   garantir un état final cohérent.
    
