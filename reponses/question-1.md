# Labo 08 – Question 1

> Comment on faisait pour passer d'un état à l'autre dans la saga dans le labo 6, et comment on le fait ici ? Est-ce que le contrôle de transition est fait par la même structure dans le code ?

## Labo 6 : un chef d'orchestre qui décide de chaque transition

Au labo 6, une seule classe contrôlait toute la saga : `OrderSagaController`, dans le service **orchestrateur**. Sa méthode `run()` contient une boucle `while` et une suite de `if/elif`, une branche par état. Chaque handler fait **un appel HTTP synchrone** (via KrakenD) et **retourne** l'état suivant ; c'est le contrôleur qui le range dans `current_saga_state` et choisit l'étape d'après :

```python
# log430-labo6 : src/controllers/order_saga_controller.py
while self.current_saga_state is not OrderSagaState.END:
    if self.current_saga_state == OrderSagaState.START:
        self.current_saga_state = self.create_order_handler.run()
    elif self.current_saga_state == OrderSagaState.ORDER_CREATED:
        self.decrease_stock_handler = DecreaseStockHandler(self.create_order_handler.order_id, order_data['items'])
        self.current_saga_state = self.decrease_stock_handler.run()
    elif self.current_saga_state == OrderSagaState.STOCK_DECREASED:
        self.create_payment_handler = CreatePaymentHandler(self.create_order_handler.order_id, order_data)
        self.current_saga_state = self.create_payment_handler.run()
    ...
```

Les états possibles sont une énumération (`OrderSagaState`), l'état courant vit **en mémoire** dans le contrôleur, et toute la logique de transition est **centralisée** à un seul endroit.

## Labo 8 : des services qui réagissent aux événements

Au labo 8, il n'y a plus de contrôleur de saga ni de boucle. La transition est portée par **l'événement lui-même** : le champ `event` du message Kafka. Chaque handler fait son opération locale, puis **écrit le nom de l'événement suivant** dans `event_data` et le **publie** sur le topic `order-saga-events` :

```python
# log430-labo8 : src/orders/handlers/order_created_handler.py
def handle(self, event_data: Dict[str, Any]) -> None:
    order_event_producer = OrderEventProducer()
    session = get_sqlalchemy_session()
    try:
        check_out_items_from_stock(session, event_data['order_items'])
        session.commit()
        event_data['event'] = "StockDecreased"          # transition vers l'état suivant
    except Exception as e:
        session.rollback()
        event_data['event'] = "StockDecreaseFailed"     # transition vers la compensation
        event_data['error'] = str(e)
    finally:
        session.close()
        order_event_producer.get_instance().send(config.KAFKA_TOPIC, value=event_data)
```

Personne ne « décide » de la suite : le consommateur Kafka (`OrderEventConsumer`) lit chaque message et le confie au handler enregistré pour ce type d'événement, grâce au `HandlerRegistry` :

```python
# src/orders/queries/order_event_consumer.py
def _process_message(self, event_data: dict) -> None:
    event_type = event_data.get('event')
    handler = self.registry.get_handler(event_type)
    if handler:
        handler.handle(event_data)
```

```python
# src/store_manager.py : un handler par événement de la machine à états
registry = HandlerRegistry()
registry.register(OrderCreatedHandler())
registry.register(StockDecreasedHandler())
registry.register(StockDecreaseFailedHandler())
registry.register(StockIncreasedHandler())
registry.register(PaymentCreatedHandler())
registry.register(PaymentCreationFailedHandler())
registry.register(SagaCompletedHandler())
...
```

C'est une **saga chorégraphiée** : chaque handler connaît seulement son pas de danse (« quand je reçois `OrderCreated`, je retire le stock, puis j'annonce `StockDecreased` »), et l'enchaînement complet émerge de ces réactions successives.

## Est-ce la même structure ?

**Non.** Les deux labos utilisent des « handlers » et suivent la même machine à états, mais le contrôle des transitions n'est pas au même endroit :

| | Labo 6 (orchestration) | Labo 8 (chorégraphie) |
|---|---|---|
| Qui décide de l'étape suivante ? | Une seule classe, `OrderSagaController` | Chaque handler, en publiant l'événement suivant |
| Comment l'état est représenté | Une énumération `OrderSagaState`, gardée en mémoire | Le champ `event` du message Kafka |
| Ce que renvoie un handler | L'état suivant (`return OrderSagaState.STOCK_DECREASED`) | Rien : il publie un événement (`send(...)`) |
| Communication | Appels HTTP synchrones, via KrakenD | Événements asynchrones, via Kafka |
| Compensation | Méthode `rollback()` du handler, appelée par le contrôleur | Un handler dédié par événement d'échec (`StockDecreaseFailedHandler`, `StockIncreasedHandler`…) |

Par exemple, la compensation « supprimer la commande si le stock n'a pas pu être retiré » était un `rollback()` au labo 6 ; au labo 8, c'est un handler à part, déclenché par l'événement `StockDecreaseFailed` :

```python
# src/stocks/handlers/stock_decrease_failed_handler.py
def handle(self, event_data: Dict[str, Any]) -> None:
    try:
        delete_order(event_data['order_id'])
    except Exception as e:
        event_data['error'] = str(e)
    finally:
        event_data['event'] = "OrderCancelled"
        OrderEventProducer().get_instance().send(config.KAFKA_TOPIC, value=event_data)
```

**Conséquences.** Au labo 6, le déroulement complet se lit dans une seule méthode, mais l'orchestrateur est un point de défaillance unique, et son état est perdu s'il plante. Au labo 8, il n'y a plus de point central et les services sont découplés. En revanche, le flux est dispersé dans une dizaine de classes : pour comprendre la saga, il faut suivre les événements, par exemple dans les logs du consommateur, ou s'appuyer sur le diagramme de machine à états de l'arc42.
