# Brief — `--chunk-size` : collision cross-chunk sur les colonnes "valeur" non-ID

## MàJ 2026-08-18 — non reproduit, investigation close côté dupehell

Reproduction complète tentée côté dupehell avec la commande exacte donnée
plus bas (`--domain aviation --size 500000000 --chunk-size 100000000
--seed 42 --difficulty hell --only-entity passenger --output-format ipc`),
sur le build actuel (post `c85959d`/`f753f45`) :

- **Sondes RNG isolées** (formule réelle `chunk_seed = seed+chunk_idx`,
  `batch_seed = chunk_seed + batch_idx*1000+1000`) : ~97,7% uniques sur
  50M tirages combinés cross-chunk — conforme au hasard pur, aucune
  corrélation entre seeds proches.
- **Run réel non-chunké** (`--size 1000000`) : 433 257 `booking_reference`
  distincts / 433 333 masters (99,98%).
- **Run réel chunké à échelle réduite** (`--size 5000000 --chunk-size
  1000000`, 5 chunks) : 2 164 441 distincts / 2 166 665 masters (99,9%).
- **Run réel à pleine échelle** (commande de repro exacte du brief) :
  510 625 010 lignes produites — **identique au chiffre cité dans ce
  brief**, donc reproduction fidèle des conditions. `booking_reference`
  distincts mesurés (`polars.n_unique()` direct sur le fichier dupehell) :
  **196 210 951**, pas 10 000 000. Ce chiffre colle à la prédiction
  théorique (~199,5M, calcul par anniversaire sur l'espace PNR 32^6 avec
  n_base=43,3M masters/chunk × 5 chunks).

**Conclusion : le bug décrit ci-dessous ne se reproduit pas** sur le
build actuel de dupehell, avec la commande de repro fournie. Rien dans
le pipeline (génération, seeding, chunking, concaténation finale) ne
produit l'effondrement de cardinalité rapporté. Hypothèses pour
expliquer l'écart avec les chiffres 10M/50M du brief :

1. Mesure faite sur un **build dupehell différent** (avant un fix depuis
   committé, ou état intermédiaire non identifié).
2. Le vrai bug est dans la **chaîne de mesure côté linkars-xtrem**
   (leur propre lecture/dérivation du fichier, pas un `n_unique()`
   direct) — à vérifier de leur côté.
3. Erreur de mesure/unité dans ce brief.

Investigation fermée côté dupehell sauf nouvel élément. Prochaine étape
si le problème persiste : fournir un dump `polars.n_unique()` fait
directement sur le fichier dupehell produit par linkars-xtrem
(exactement comme ci-dessus), pour confirmer si l'écart apparaît déjà
à la lecture brute ou seulement après la chaîne de traitement xtrem.

**Découverte annexe (opérationnelle, pas un bug) :** générer 500M avec
`--chunk-size` nécessite temporairement **~2× la taille du dataset
final** en disque — les 5 fichiers chunk (dataset+GT) et le fichier
final concaténé coexistent tant que le nettoyage du dossier temporaire
`.dupehell_chunks_*` n'a pas eu lieu. Un run à cette échelle a échoué
ici par manque d'espace disque (100% plein) juste après la
concaténation du dataset final (déjà utilisable) mais avant celle du
GT. À anticiper/documenter côté dimensionnement disque, pas un défaut
de génération.

---

Contexte : trouvé le 17/08/2026 côté GIT3 (`linkars-xtrem`) en creusant un
crash disque sur un run manuel 500M (guard "oxygen" déclenché pendant le
blocking). Root cause tranchée par comparaison directe de deux datasets
aviation de 510 625 010 lignes chacun, mêmes règles de blocking/comparaisons,
même code xtrem (`git diff` vide sur toute la période) :

- **Ancien mécanisme** (script externe `build_500m_chunked.py`, 5 shards de
  100M générés séparément puis concaténés) : `record_id`/`master_id`/
  `flight_id`/`passenger_id`/`booking_reference` **préfixés manuellement par
  shard** (`S0_`…`S4_`) avant concat, pour garantir l'unicité inter-shard.
- **Nouveau mécanisme natif** (`--chunk-size`, commit `c85959d`) :
  `ChunkOffsets` thread la contiguïté globale de `record_id`/`master_id`/
  `hn_master_id_counter` à travers les chunks — **et rien d'autre**.

## Le problème

`booking_reference` (schéma `aviation.json`, `type: "string"`, **pas de
`pool_name`**) est une valeur générée procéduralement par chunk, pas un
compteur d'ID géré par `ChunkOffsets`. Résultat mesuré sur un run identique
(`--domain aviation --size 500000000 --chunk-size 100000000 --seed 42
--difficulty hell --only-entity passenger`, 5 sous-runs seed 42..46) :

| | Dataset shard-prefixé (ancien script) | Dataset `--chunk-size` natif |
|---|---|---|
| Lignes totales | 510 625 010 | 510 625 010 (identique) |
| `booking_reference` distinctes | **50 000 000** | **10 000 000** |
| Plus gros bloc (blocking xtrem) | 32 | **88** |
| Paires générées (règle `booking_reference`) | 2 421 244 336 | **12 539 462 626** (×5,18) |

Cardinalité réelle divisée par 5 sur un dataset de même taille totale : les
5 sous-runs indépendamment seedés (`seed + chunk_idx`) génèrent des
`booking_reference` qui se recouvrent massivement entre chunks — cohérent
avec un espace de génération (pool implicite / espace de valeurs
alphanumériques) dont la taille effective est du même ordre que la taille
d'UN chunk (100M), pas du total (500M), et/ou une corrélation entre seeds
proches (42/43/44/45/46) dans le générateur pseudo-aléatoire utilisé pour ce
champ.

## Pourquoi c'est un problème réel (pas juste esthétique)

N'importe quel champ `string` sans `pool_name` (généré proceduralement,
donc potentiellement TOUTES les colonnes de ce type dans TOUS les domaines,
pas seulement `booking_reference` en aviation) est probablement exposé au
même risque dès que `--chunk-size` est utilisé avec `size` assez grand pour
que la génération par chunk retombe sur un espace de valeurs restreint
relativement au total. Conséquence côté consommateur (ex. linkars-xtrem) :
cardinalité effondrée → blocs de blocking artificiellement énormes → volume
de paires générées qui explose (ici ×5,18) → explosion disque/RAM en aval,
sans qu'aucune erreur ne remonte côté génération elle-même (le dataset est
généré avec succès, le problème n'apparaît qu'en aval).

## Reproduction

```
dupehell --domain aviation --size 500000000 --seed 42 --difficulty hell \
  --only-entity passenger --chunk-size 100000000 --output-format ipc \
  --output-dir <dir>
```

Puis compter les valeurs distinctes de `booking_reference` dans le fichier
produit (`polars.scan_ipc(...).select(pl.col("booking_reference").n_unique()).collect()`)
et comparer à `--size 500000000` sans `--chunk-size` (run non-chunké, RAM
permettant) sur la même seed de base — la cardinalité devrait être très
proche du total de lignes concernées (peu de doublons naturels à `--hell`),
pas divisée par ~5.

## Piste de fix (pas creusée côté génération elle-même, à investiguer ici)

`ChunkOffsets` (ou un mécanisme équivalent) devrait aussi couvrir les
colonnes `string` sans `pool_name` générées de façon procédurale — soit en
dérivant le générateur de valeur d'un état qui inclut l'INDEX GLOBAL
(`chunk_offset + local_index`) et pas seulement `(chunk_seed, local_index)`,
soit en réservant explicitement un sous-espace de valeurs disjoint par
chunk (façon namespace, similaire dans l'esprit au préfixage manuel de
l'ancien script mais fait nativement et de façon déterministe/reproductible
plutôt qu'en post-traitement).

À vérifier en particulier : est-ce spécifique aux champs `string` sans
`pool_name`, ou est-ce que des champs avec `pool_name` explicite (pool
partagé, taille fixe) ont aussi un risque de collision cross-chunk du même
ordre si le pool est trop petit relativement à `--chunk-size` × nombre de
chunks ?

---
Contexte complet de la session d'investigation (logs, chiffres, fausses
pistes écartées) : `GIT3/.claude/memory/session1708.md` (côté du dépôt
GIT3, pas ce repo).
