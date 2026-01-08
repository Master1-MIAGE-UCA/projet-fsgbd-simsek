# Mini-SGBD en Java – Étape 4

## TIV, TIA et mécanisme de verrouillage

## 1. Objectif

Dans cette étape, vous allez enrichir votre mini-SGBD avec deux mécanismes fondamentaux :

1.  La gestion d’une **image avant (TIV)** et d’une **image après (TIA)** des données
2.  Un mécanisme de **verrouillage simple** des enregistrements lors des mises à jour en transaction.
    

Ces ajouts permettent de simuler :
-   le fonctionnement interne du système de mise à jour d’un SGBD,
-   la gestion de la cohérence en présence de transactions,
-   la possibilité de rollback réel, sans journal disque.

## 2. Rappels conceptuels

### 2.1 TIA – Tampon d'Images Après

Le **TIA** correspond à votre buffer actuel.  
Il contient les **données modifiées** en mémoire (les nouvelles valeurs).

### 2.2 TIV – Tampon d'Images Avant

Le **TIV** est un nouveau buffer que vous devez introduire.  
Il contient les **anciennes valeurs** des données avant modification, uniquement pour les données impliquées dans une transaction.

Il sert à :
-   assurer des lectures cohérentes,    
-   restaurer les données en cas de rollback.

## 3. Nouvelles structures à mettre en place

### 3.1 Buffer TIV

Vous devez ajouter une structure pour mémoriser les images avant modification.
Exemple de structure possible en Java :

```java
Map<PageId, byte[]> TIV;
```

Où :
-   `PageId` identifie un enregistrement (par exemple : `(pageId, offset)`),
-   `byte[]` est le contenu original de l’enregistrement, vous pouvez donc utiliser le même type que celui ci au lieu d'un byte array.

Remarque importante :  
Même si une seule ligne (ou un seul enregistrement) est concernée par la modification, le tampon d’image avant doit stocker **toute la page**, afin de rester cohérent avec le fonctionnement des SGBD relationnels classiques qui travaillent au niveau bloc/page et non au niveau tuple.

### 3.2 Table de verrous

Vous devez introduire un mécanisme simple pour **verrouiller** un enregistrement lors d’une modification en transaction.
Vous pouvez vous contenter d'un indicateur que vous rajouterez directement  sur la page dans le TIA.

Notez cependant qu'en faisant ceci, vous allez verrouiller toute la page et pas uniquement l'enregistrement, une solution idéale si serait de gérer une liste des verrous de manière globale. 

Exemple :

```java
Set<RecordId> locks; 
```

Un enregistrement peut être :
-   **verrouillé** → il est en cours de modification dans une transaction,    
-   **non verrouillé** → il est accessible normalement.

## 4. Nouvelle logique des mises à jour

Lorsqu’une mise à jour est demandée sur un enregistrement dans le cadre d’une transaction :

1.  Vérifier si l’enregistrement est déjà verrouillé. 
2.  S’il ne l’est pas :
    -   Ajouter un **verrou** sur cet enregistrement.
    -   Copier sa valeur actuelle depuis le TIA vers le TIV.
4.  Modifier la donnée dans le TIA uniquement.    
5.  Marquer la page comme `dirty` et `transactionnelle` (si ce n’est pas déjà fait).
    
Vous devrez enfin empêcher toute modification sur un enregistrement déjà verrouillé par une autre transaction.   

## 5. Nouvelle politique de lecture

Lorsqu’une **lecture** est demandée sur un enregistrement :

| Situation | Source utilisée |
|--|--|
| Pas de transaction | TIA |
| Transaction en cours + pas de verrou | TIA |
| Transaction en cours + enregistrement verrouillé | TIV |

Si la donnée est en cours de modification (verrou posée sur l’enregistrement), la lecture doit retourner **l’ancienne valeur contenue dans le TIV**, et non la nouvelle valeur présente dans le TIA.

## 6. Modification du ROLLBACK

Le `ROLLBACK` doit maintenant :

Pour chaque enregistrement modifié pendant la transaction :
1.  Récupérer son ancienne valeur dans le TIV.
2.  Restaurer cette valeur dans le TIA (écrasement de la valeur modifiée).
3.  Supprimer l’entrée correspondante dans le TIV.
4.  Libérer le verrou associé à cet enregistrement.

Aucune écriture disque ne doit être effectuée lors du `ROLLBACK`.

## 7. Modification du COMMIT

Lors du `COMMIT` :

1.  Forcer sur disque toutes les pages transactionnelles modifiées (`FORCE`).
2.  Libérer tous les verrous posés par la transaction.
3.  Supprimer toutes les entrées correspondantes du TIV.
4.  Réinitialiser l’état transactionnel (indicateur de transaction en cours).

Après cela, l’état du TIA devient l’état de référence sur disque.

## Rendu

Ajoutez le code produit pour ce TD au dépôt que vous avez utilisé précédemment. Pensez à ajouter un tag afin que l'on puisse facilement retrouver vos productions pour les différentes étapes.
