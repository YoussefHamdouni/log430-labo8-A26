"""
Labo 08 – Activité 7 : test de charge de la création de commandes (Store Manager)
Lancement : docker compose up -d locust, puis ouvrir http://localhost:8089
Paramètres utilisés (comme au labo 4, activité 5) : 150 utilisateurs, spawn rate 2
"""
import random
from locust import HttpUser, task, between


class OrderCreationUser(HttpUser):
    # Pause aléatoire entre deux requêtes d'un même utilisateur virtuel
    wait_time = between(1, 3)

    @task
    def create_order(self):
        payload = {
            "user_id": random.randint(1, 3),  # utilisateurs du jeu de données initial
            "items": [
                {"product_id": random.choice([1, 2]), "quantity": 1}  # produits avec beaucoup de stock
            ],
        }
        with self.client.post("/orders", json=payload, name="POST /orders", catch_response=True) as response:
            if response.status_code == 201 and "order_id" in response.json():
                response.success()
            else:
                response.failure(f"Statut {response.status_code} : {response.text[:200]}")
