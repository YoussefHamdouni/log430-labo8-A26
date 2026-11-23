# Labo 08 – Activité 7 : test de charge avec Locust

## Mise en place

- `locustfiles/locustfile.py` : chaque utilisateur virtuel envoie `POST /orders` (utilisateur 1 à 3, produit 1 ou 2, quantité 1), avec une pause de 1 à 3 s entre deux requêtes. Une réponse est comptée comme réussie si elle a le statut `201` et contient un `order_id`.
- Un service `locust` est ajouté au `docker-compose.yml` (réseau `labo08-network`, port `8089`). Il cible directement `http://store_manager:5000`.
- Paramètres (comme au labo 4, activité 5) : **150 utilisateurs**, **spawn rate 2**, durée d'environ **[À COMPLÉTER] min**.

## Résultats

![Statistiques Locust](images/a7-locust-stats.png)
![Graphiques Locust](images/a7-locust-charts.png)

| Mesure | Valeur |
|---|---|
| Nombre de requêtes `POST /orders` | [À COMPLÉTER] |
| Échecs (%) | [À COMPLÉTER] |
| Temps de réponse médian (ms) | [À COMPLÉTER] |
| 95e centile (ms) | [À COMPLÉTER] |
| Maximum (ms) | [À COMPLÉTER] |
| Débit max (RPS) | [À COMPLÉTER] |

État de la saga en base après le test :

```sql
SELECT COUNT(*) AS commandes, SUM(is_paid) AS payees FROM orders;
SELECT COUNT(*) AS outbox, SUM(payment_id IS NULL) AS sans_paiement FROM outbox;
SELECT * FROM stocks;
```

```
[COLLER LE RÉSULTAT ICI]
```

## Observations

1. **Le `201` ne veut pas dire que la saga est finie.** `POST /orders` insère la commande, publie `OrderCreated` sur Kafka, puis répond tout de suite. Le retrait du stock et la création du paiement se font plus tard, de façon asynchrone. Locust mesure donc seulement la première étape, et le temps de réponse reste [bas / stable : À COMPLÉTER] même quand le reste de la saga prend du retard. Pour savoir si les sagas ont abouti, il faut regarder la base après le test : [N] commandes créées, dont [N] payées.
2. **Un seul consommateur.** `OrderEventConsumer` est un seul thread qui lit au plus 10 messages à la fois et les traite un par un. L'`OutboxProcessor` fait un appel HTTP *synchrone* vers Payments pour chaque commande. Sous charge, les événements s'accumulent dans le topic : [observé / pas observé : À COMPLÉTER, en s'appuyant sur les logs `docker compose logs store_manager`].
3. **Connexions MySQL.** `get_sqlalchemy_session()` crée un nouvel `engine`, et donc un nouveau pool de connexions, à chaque appel. Avec 150 utilisateurs, cela peut mener à des erreurs `Too many connections` ou à une hausse des temps de réponse : [À COMPLÉTER selon le résultat]. Il faudrait créer un seul `engine`, au niveau du module.
4. **Comparaison avec le labo 4.** [À COMPLÉTER : au labo 4, la création de commande était synchrone ; comparer la médiane et le taux d'échec.]

**Conclusion :** [À COMPLÉTER, en 2 ou 3 phrases]
