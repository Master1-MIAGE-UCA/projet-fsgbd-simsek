# Soutenance

## Scénario de démonstration 

### Informations préalables

 - **État initial :** Base de données vide.
 - Si vous avez typé les données stockées avec un type numérique ou autre, **adaptez le scénario** au type de vous avez géré
 - Votre démonstration doit être **prête avant de commencer**, le code doit déjà être **compilé**, l'IDE lancé avec **le projet déjà ouvert**.
 - Des modifications de code / des informations sur votre code seront être demandées.
 - Si vous vous êtes fait aider par une IA, merci de préciser le modèle que vous avez utilisé ainsi que la méthode (chat, plugin dans l'IDE, vibe coding...) - Les IAs étaient autorisées, vous n'avez rien à craindre.
 - **Temps de présentation** : 5 minutes (max)

### Étape 1 : Le Remplissage (Buffer & Disque)

_Objectif : Vérifier que l'insertion de base fonctionne et utilise le Buffer._

1.  **Action :** Insérer 2 enregistrements "témoins" : `Record_A` (ID 0) et `Record_B` (ID 1).
    
2.  **Vérification :** Lire immédiatement `Record_A`.
        

### Étape 2 : L'Annulation (Atomicité & TIV)

_Objectif : Prouver que l'on peut revenir en arrière grâce au TIV (Tampon d'Image Avant)._

1.  **Action :** Démarrer une transaction (`BEGIN`).
    
2.  **Action :** Modifier `Record_A` en `Record_A_MODIFIE`.
    
3.  **Action :** Demander un `ROLLBACK`.
    
4.  **Vérification :** Relire `Record_A`.
        

### Étape 3 : La Persistance (Commit & Journal)

_Objectif : Valider qu'une transaction validée laisse une trace durable._

1.  **Action :** Démarrer une transaction (`BEGIN`).
    
2.  **Action :** Modifier `Record_B` en `Record_B_FINAL`.
    
3.  **Action :** Valider (`COMMIT`).
    
4.  **Vérification :** Vérifier la présence/taille du fichier de journal (Log).
        

### Étape 4 : Préparation au Crash

_Objectif : Créer une incohérence volontaire (une donnée en cours de modification)._

1.  **Action :** Démarrer une **nouvelle** transaction (`BEGIN`).
    
2.  **Action :** Insérer un nouvel enregistrement `Record_C_FANTOME` (ID 2).
    
3.  **Action :** Ne rien faire d'autre. **Laisser la transaction ouverte.**
        

### Étape 5 : Crash & Recovery

_Objectif : Vérifier que l'algorithme de reprise nettoie correctement la base._

1.  **Action :** Simuler la panne (Arrêt brutal ou méthode `simulateCrash` qui vide les buffers sans sauvegarder).
    
2.  **Action :** Redémarrer et lancer `recover()`.
    
3.  **Vérification finale :** Demander l'affichage de tous les enregistrements.
            
