
# Mini-SGBD – Étape 2 : Gestion du buffer et primitives FIX / UNFIX / USE / FORCE

## Objectif
Dans cette étape, nous introduisons la **gestion des pages en mémoire tampon (buffer)**.  
Un SGBD ne travaille pas directement sur le disque à chaque opération (ce serait trop lent).  
Au lieu de cela, il :
- charge une **page** depuis le disque en mémoire (primitives `FIX`, `USE`),
- manipule cette page,
- puis la libère (`UNFIX`),
- et éventuellement force son écriture immédiate sur disque (`FORCE`).

## Primitives à implémenter

### 1. `FIX(pageId)`
- Charge une page en mémoire si elle n’y est pas déjà.
- Retourne un objet /structure représentant cette page (par ex. un tableau d’octets).
- Incrémente un compteur indiquant qu’elle est utilisée.

### 2. `UNFIX(pageId)`
- Indique que le programme n’utilise plus cette page.
- Décrémente le compteur d'utilisation.
- Si le compteur atteint zéro, la page peut être évacuée du buffer (au besoin, pas à faire pour ce TD).

### 3. `USE(pageId)`
- Marque la page comme **modifiée** (dirty).
- Signifie qu’elle devra être réécrite sur disque à un moment donné.

### 4. `FORCE(pageId)`
- Écrit immédiatement la page sur disque si elle est marquée "dirty".
- Utilisé pour garantir que les modifications sont persistantes.
## Adaptation des fonctions existantes

Les méthodes de lecture déjà implémentées (`readRecord`, `getPage`) doivent désormais :
- **FIX** la page avant de la lire,
- Lire les données,
- Puis **UNFIX** la page après usage.

## Nouvelle méthode à implémenter : insertion synchrone

Ajoutez une méthode :

```java
public void insertRecordSync(String data) throws IOException 
```

Cette méthode viendra en complément de l'insertion classique, l'insertion classique écrira uniquement dans la mémoire tampon (buffer). L'écriture synchrone quand à elle écrira dans la mémoire tampon et répercutera ses changements dans les fichiers de données en plus et ce de manière synchrone.

## Rendu
Ajoutez le code produit pour ce TD au dépôt que vous avez utilisé précédemment. Pensez à ajouter un tag "TD1" ou "TD2" afin que l'on puisse facilement retrouver vos productions pour les différentes étapes.

