from confluent_kafka import Producer
import json

config = {
    'bootstrap.servers': 'localhost:9092,localhost:9094,localhost:9095',
}

producer = Producer(config)

# Mensaje VÁLIDO según el schema de pedidos
mensaje_valido = {
    'id_pedido': 'PED-999',
    'producto': 'Laptop',
    'cantidad': 2,
    'precio': 1500.00,
    'timestamp': '2025-10-20T10:00:00'
}

producer.produce(
    topic='pedidos',
    key='test',
    value=json.dumps(mensaje_valido)
)

producer.flush()
print("✅ Mensaje válido enviado correctamente")