from locust import HttpUser, task, between


class StoreManagerUser(HttpUser):
    wait_time = between(1, 2)

    @task
    def create_order(self):
        self.client.post(
            "/orders",
            json={
                "user_id": 1,
                "items": [
                    {"product_id": 2, "quantity": 1},
                    {"product_id": 3, "quantity": 2}
                ]
            }
        )