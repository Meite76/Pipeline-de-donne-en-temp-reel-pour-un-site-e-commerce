import json
import time
from kafka import KafkaProducer
from faker import Faker
import random

# Initialisation Faker pour générer des données fictives
fake = Faker()

# Connexion au broker Kafka local
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Sérialisation JSON
)

topic = 'ecommerce_events'

def generate_event():
    # Génère un événement utilisateur aléatoire simulant un clic ou un achat
    event = {
        'user_id': fake.uuid4(),
        'event_type': random.choice(['click', 'purchase']),
        'product_id': fake.ean13(),
        'timestamp': int(time.time()),
        'price': round(random.uniform(10, 500), 2) if random.random() < 0.3 else 0  # prix pour achat uniquement
    }
    return event

print(f"Envoi d'événements vers le topic '{topic}'...")

try:
    while True:
        event = generate_event()
        producer.send(topic, value=event)
        print(f"Envoyé: {event}")
        time.sleep(1)  # Pause 1 sec entre chaque événement
except KeyboardInterrupt:
    print("Arrêt du producteur...")
finally:
    producer.flush()
    producer.close()
