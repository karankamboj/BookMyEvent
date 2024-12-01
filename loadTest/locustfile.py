from locust import HttpUser, task, between
import json
import random

class EventBookingUserBehavior(HttpUser):
    """
    Simulates user interactions with the Event Booking Application
    """
    # Wait time between tasks to simulate realistic user behavior
    wait_time = between(1, 3)

    # Sample test data for various operations
    event_categories = ['Concert', 'Sports', 'Theater', 'Conference', 'Workshop']
    sample_locations = [1, 2, 3, 4, 5]

    def on_start(self):
        """
        Setup method called when a simulated user starts
        """
        self.base_url = "http://localhost:5001"  # Adjust to your server configuration

    @task(3)  # Higher weight for search tasks
    def search_events(self):
        """
        Perform event searches with various filters
        """
        search_scenarios = [
            {
                "query": random.choice(["music", "sports", "tech", "art", "conference"]),
                "category": random.choice(self.event_categories),
                "min_tickets": random.randint(10, 50)
            },
            {
                "query": "",
                "location_id": random.choice(self.sample_locations),
                "max_tickets": random.randint(50, 100)
            },
            {
                "query": random.choice(["concert", "game", "exhibition"]),
                "date_time": "2024-01-01"
            }
        ]

        # Randomly select a search scenario
        scenario = random.choice(search_scenarios)

        with self.client.get(
            f"{self.base_url}/search", 
            json=scenario, 
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Search failed with status {response.status_code}")

    @task(2)  # Moderate weight for fetch tasks
    def fetch_events(self):
        """
        Retrieve events from different tables
        """
        tables = ['event', 'users', 'location']
        
        with self.client.get(
            f"{self.base_url}/fetch", 
            params={"table_name": random.choice(tables)}, 
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Fetch failed with status {response.status_code}")

    @task(1)  # Lower weight for insert/update/delete
    def insert_event(self):
        """
        Simulate inserting a new event
        """
        insert_payload = {
            "table_name": "event",
            "columns": [
                "name", "location_id", "date_time", "category", 
                "total_tickets", "available_tickets"
            ],
            "values": [
                (
                    f"Test Event {random.randint(1, 1000)}", 
                    random.choice(self.sample_locations), 
                    "2024-07-15T18:00:00Z",
                    random.choice(self.event_categories),
                    random.randint(100, 500),
                    random.randint(50, 300)
                )
            ]
        }

        with self.client.post(
            f"{self.base_url}/insert", 
            json=insert_payload, 
            catch_response=True
        ) as response:
            if "successfully" in response.text.lower():
                response.success()
            else:
                response.failure(f"Insert failed with status {response.status_code}")

    @task(1)
    def update_event(self):
        """
        Simulate updating an existing event
        """
        update_payload = {
            "table_name": "event",
            "columns": ["name", "available_tickets"],
            "values": [
                f"Updated Event {random.randint(1, 1000)}", 
                random.randint(10, 200)
            ],
            "primary_column": "event_id",
            "primary_value": random.randint(1, 100)  # Adjust range as needed
        }

        with self.client.put(
            f"{self.base_url}/update", 
            json=update_payload, 
            catch_response=True
        ) as response:
            if "successfully" in response.text.lower():
                response.success()
            else:
                response.failure(f"Update failed with status {response.status_code}")

    @task(1)
    def delete_event(self):
        """
        Simulate deleting an event
        """
        delete_payload = {
            "table_name": "event",
            "primary_column": "event_id",
            "primary_value": random.randint(1, 100)  # Adjust range as needed
        }

        with self.client.delete(
            f"{self.base_url}/delete", 
            json=delete_payload, 
            catch_response=True
        ) as response:
            if "successfully" in response.text.lower():
                response.success()
            else:
                response.failure(f"Delete failed with status {response.status_code}")

# Locust configuration notes:
"""
To run the Locust performance tests:

1. Install Locust:
   pip install locust

2. Run Locust:
   locust 

3. Open Locust web interface:
   http://localhost:8089

4. Configure test parameters:
   - Number of total users to simulate
   - Spawn rate (users per second)
   - Host URL

Performance Testing Scenarios:
- Baseline Load Test: 50-100 concurrent users
- Stress Test: Gradually increase users to find breaking point
- Soak Test: Sustained load over extended period (e.g., 1-2 hours)

Recommended Test Configurations:
1. Light Load: 50 users, 5 spawn rate
2. Medium Load: 200 users, 10 spawn rate
3. Heavy Load: 500 users, 20 spawn rate

Monitoring Recommendations:
- Track response times
- Monitor error rates
- Check server resource utilization
- Identify bottlenecks in search and database operations
"""
