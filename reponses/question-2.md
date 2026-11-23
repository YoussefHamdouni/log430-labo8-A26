# Labo 08 – Question 2

> Sur la relation entre nos Handlers et le patron CQRS : pensez-vous qu'ils utilisent plus souvent les Commands ou les Queries ? Est-ce qu'on tient l'état des Queries à jour par rapport aux changements d'état causés par les Commands ?

## Rappel : CQRS dans le Store Manager

Depuis le labo 2, le code sépare deux modèles :
- les **Commands** (dossiers `commands/`) **modifient** l'état dans MySQL : `add_order`, `modify_order`, `delete_order`, `check_out_items_from_stock`, `check_in_items_to_stock`… ;
- les **Queries** (dossiers `queries/`) **lisent** l'état, souvent depuis le modèle de lecture dans Redis : `get_order_by_id` lit la clé `order:{id}`, les rapports lisent les commandes dans Redis, et la requête GraphQL de stock lit `stock:{id}`.

## Les handlers utilisent presque uniquement des Commands

Chaque handler représente une **transition d'état** de la saga : son rôle est de **changer** quelque chose, puis d'annoncer le résultat. Il appelle donc des Commands, jamais des Queries :

| Handler | Command appelée | Effet |
|---|---|---|
| `OrderCreatedHandler` | `check_out_items_from_stock(...)` | `UPDATE stocks SET quantity = quantity - …` |
| `StockDecreasedHandler` | insertion dans la table `outbox`, puis `OutboxProcessor` → `modify_order(...)` | Crée le paiement, enregistre `payment_link` et `is_paid` |
| `PaymentCreationFailedHandler` | `check_in_items_to_stock(...)` | Remet les articles en stock (compensation) |
| `StockDecreaseFailedHandler`, `StockIncreasedHandler` | `delete_order(...)` | Supprime la commande (compensation) |

```python
# src/orders/handlers/order_created_handler.py
from stocks.commands.write_stock import check_out_items_from_stock   # une Command
...
check_out_items_from_stock(session, event_data['order_items'])
session.commit()
```

Les handlers n'ont pas besoin de lire l'état : toutes les données nécessaires (id de la commande, articles, montant) **voyagent dans l'événement** (`event_data`). C'est logique dans une architecture event-driven : l'événement transporte l'information, et le handler agit dessus.

## Le modèle de lecture est-il tenu à jour ?

**En partie seulement.** Pour les **commandes**, oui :
- à la création, `add_order` écrit la commande dans MySQL **puis** dans Redis :
  ```python
  # src/orders/commands/write_order.py
  session.commit()
  add_order_to_redis(order_id, user_id, total_amount, items)
  ```
- à la suppression (compensation), `delete_order` retire aussi la clé Redis (`delete_order_from_redis(order_id)`) ;
- quand le paiement est créé, l'`OutboxProcessor` met à jour `is_paid` et `payment_link` dans MySQL **et** dans le hash `order:{id}` de Redis :
  ```python
  # src/payments/outbox_processor.py
  update_succeeded = modify_order(event_data["order_id"], True, order.payment_id)
  ...
  r = get_redis_conn()
  if r.exists(f"order:{event_data['order_id']}"):
      r.hset(f"order:{event_data['order_id']}", mapping={
          "is_paid": str(True),
          "payment_link": event_data["payment_link"]
      })
  ```

Pour le **stock**, non. Les handlers modifient le stock uniquement dans MySQL :

```python
# src/stocks/commands/write_stock.py
def check_out_items_from_stock(session, order_items):
    """ Decrease stock quantities in Redis """
    update_stock_mysql(session, order_items, "-")     # MySQL seulement, malgré la docstring
```

La fonction `update_stock_redis(...)` existe dans le même fichier, mais **aucun handler ne l'appelle**. Or la requête GraphQL de stock lit Redis :

```python
# src/stocks/schemas/query.py
def resolve_stock_level(self, info, product_id):
    redis_client = get_redis_conn()
    quantity = redis_client.hget(f"stock:{product_id}", "quantity")
```

Après une saga, `GET /stocks/{id}` (qui lit MySQL) donne donc la bonne quantité, mais une requête GraphQL renvoie encore l'**ancienne** valeur chargée au démarrage. Le modèle de lecture du stock dans Redis n'est resynchronisé qu'au redémarrage (`populate_redis_on_startup`), et seulement si Redis est vide.

## Conclusion

Les handlers sont essentiellement des **Commands** : ils exécutent les transitions de la saga et laissent les lectures aux endpoints de Queries. Le modèle de lecture est tenu à jour pour les **commandes** (création, paiement, suppression), mais pas pour le **stock** : les quantités dans Redis divergent de MySQL après chaque saga. C'est la cohérence à terme (*eventual consistency*) que CQRS accepte par principe, mais ici, il manque l'étape de synchronisation. On la corrigerait en appelant `update_stock_redis(order_items, "-")` après le `commit` dans `OrderCreatedHandler`, et `update_stock_redis(order_items, "+")` dans la compensation qui remet le stock.
