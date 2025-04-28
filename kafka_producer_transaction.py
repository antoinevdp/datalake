from kafka import KafkaProducer
import time
import threading
import json
import random
import datetime
import uuid

# Nom du topic Kafka
topic_name = 'transaction_log'

# Fonction pour sérialiser les messages en JSON
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def generate_transaction_log(num_logs):
    """Generates a list of complex transaction logs in JSON format."""

    transaction_logs = []
    product_categories = ["electronics", "clothing", "books", "home_goods", "food"]
    payment_methods = ["credit_card", "paypal", "bank_transfer", "apple_pay", "google_pay", "cryptocurrency"]
    currencies = ["USD", "EUR", "GBP", "CAD", "JPY", "AUD"]
    countries = ["USA", "Canada", "UK", "Germany", "France", "Japan", "Australia", "Brazil", "India", "China"]
    cities = {
        "USA": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"],
        "Canada": ["Toronto", "Montreal", "Vancouver", "Calgary", "Ottawa"],
        "UK": ["London", "Birmingham", "Manchester", "Glasgow", "Liverpool"],
        "Germany": ["Berlin", "Hamburg", "Munich", "Cologne", "Frankfurt"],
        "France": ["Paris", "Marseille", "Lyon", "Toulouse", "Nice"],
        "Japan": ["Tokyo", "Osaka", "Kyoto", "Yokohama", "Nagoya"],
        "Australia": ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"],
        "Brazil": ["São Paulo", "Rio de Janeiro", "Brasília", "Salvador", "Fortaleza"],
        "India": ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai"],
        "China": ["Shanghai", "Beijing", "Guangzhou", "Shenzhen", "Hangzhou"],
    }
    names = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller","Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
        "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
        "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
        "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter", "Roberts","Liam", "Olivia", "Noah", "Emma", "Oliver", "Ava", "Elijah", "Sophia",
        "William", "Isabella", "James", "Charlotte", "Benjamin", "Amelia", "Lucas", "Mia", "Henry", "Evelyn", "Alexander", "Abigail", "Daniel",
        "Harper", "Matthew", "Emily", "Joseph", "Elizabeth", "David", "Sofia", "Samuel", "Madison", "John", "Scarlett", "Robert", "Grace", "Michael",
        "Chloe", "Christopher", "Victoria", "Andrew", "Aria", "Joshua", "Lily", "Anthony", "Aubrey", "Logan", "Zoey", "Ethan", "Penelope"
    ]

    for _ in range(num_logs):
        transaction_id = f"TXN-{str(uuid.uuid4())[:8]}"
        timestamp = (datetime.datetime.now() - datetime.timedelta(seconds=random.randint(0, 86400 * 30))).isoformat() + "Z"
        user_id = f"USER-{random.randint(1000, 9999)}"
        user_name = random.choice(names)
        product_id = f"PROD-{random.randint(100, 999)}"
        amount = round(random.uniform(10, 1000), 2)
        currency = random.choice(currencies)
        transaction_type = random.choice(["purchase", "refund", "payment", "withdrawal"])
        status = random.choice(["completed", "pending", "failed", "processing", "cancelled"])
        country = random.choice(countries)
        city = random.choice(cities[country])
        location = {"city": city, "country": country}
        payment_method = random.choice(payment_methods)
        product_category = random.choice(product_categories)
        quantity = random.randint(1, 10)
        shipping_address = {
            "street": f"{random.randint(100, 999)} Main St",
            "zip": f"{random.randint(10000, 99999)}",
            "city": city,
            "country": country,
        }
        device_info = {
            "os": random.choice(["Windows", "MacOS", "Linux", "Android", "iOS"]),
            "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
            "ip_address": f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
        }
        customer_rating = random.choice([None, random.randint(1, 5)])
        discount_code = random.choice([None, f"DISCOUNT-{random.randint(100, 999)}"])
        tax_amount = round(amount * random.uniform(0.05, 0.20), 2) if status == "completed" else 0.00

        log = {
            "transaction_id": transaction_id,
            "timestamp": timestamp,
            "user_id": user_id,
            "user_name": user_name,
            "product_id": product_id,
            "amount": amount,
            "currency": currency,
            "transaction_type": transaction_type,
            "status": status,
            "location": location,
            "payment_method": payment_method,
            "product_category": product_category,
            "quantity": quantity,
            "shipping_address": shipping_address,
            "device_info": device_info,
            "customer_rating": customer_rating,
            "discount_code": discount_code,
            "tax_amount": tax_amount,
        }
        transaction_logs.append(log)

    return transaction_logs


# Fonction qui envoie des messages dans Kafka
def send_messages(thread_id, num_messages, sleep_time = 0):
    logs = generate_transaction_log(num_messages)
    for log_number, log in enumerate(logs):
        log["thread"] = thread_id
        log["message_number"] = log_number
        log["timestamp_of_reception_log"] = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        producer.send(topic_name, value=log)
        print(f"Thread {thread_id} sent: {log}")
        time.sleep(sleep_time)

if __name__ == '__main__':
    # Create a kafka producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=json_serializer  # JSON serializer
    )

    threads = []
    num_threads = 5
    num_messages = 10
    sleep_time = 0

    # Create and start the threads
    for i in range(num_threads):
        thread = threading.Thread(target=send_messages, args=(i, num_messages, sleep_time))
        threads.append(thread)
        thread.start()

    # Wait for all the threads to complete
    for thread in threads:
        thread.join()

    #  Close the producer
    producer.flush()  # Make sure all the messages are sent
    producer.close()

