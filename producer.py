from confluent_kafka import Producer
import json
import time
import random
from datetime import datetime

# Configuración del producer
config = {
    'bootstrap.servers': 'localhost:9092,localhost:9094,localhost:9095',
    'client.id': 'python-producer'
}

# Crear instancia del producer
producer = Producer(config)

def delivery_report(err, msg):
    """
    Callback que se ejecuta cuando un mensaje es entregado o falla.
    Muy útil para debugging y monitoreo.
    """
    if err is not None:
        print(f'❌ Error al enviar mensaje: {err}')
    else:
        print(f'✅ Mensaje enviado a {msg.topic()} [partición {msg.partition()}] en offset {msg.offset()}')

def enviar_pedidos():
    """
    Simula el envío de pedidos a Kafka
    """
    productos = ['Laptop', 'Mouse', 'Teclado', 'Monitor', 'Audífonos']
    
    print("\n🚀 Iniciando envío de pedidos...")
    
    for i in range(10):
        # Crear datos del pedido
        pedido = {
            'id_pedido': f'PED-{i+1:03d}',
            'producto': random.choice(productos),
            'cantidad': random.randint(1, 5),
            'precio': round(random.uniform(10.0, 500.0), 2),
            'timestamp': datetime.now().isoformat()
        }
        
        # Convertir a JSON
        mensaje = json.dumps(pedido)
        
        # Enviar mensaje
        # key es importante: mensajes con la misma key van a la misma partición
        producer.produce(
            topic='pedidos',
            key=str(i % 3),  # Distribuir en 3 particiones
            value=mensaje,
            callback=delivery_report
        )
        
        # Forzar el envío de mensajes pendientes
        producer.poll(0)
        
        time.sleep(1)
    
    # Esperar a que todos los mensajes sean enviados
    producer.flush()
    print("\n✨ Todos los pedidos han sido enviados\n")

def enviar_usuarios():
    """
    Simula el registro de usuarios
    """
    nombres = ['Ana', 'Carlos', 'María', 'Pedro', 'Lucía']
    ciudades = ['Bogotá', 'Medellín', 'Cali', 'Barranquilla', 'Cartagena']
    
    print("\n🚀 Iniciando registro de usuarios...")
    
    for i in range(5):
        usuario = {
            'id_usuario': f'USR-{i+1:03d}',
            'nombre': nombres[i],
            'ciudad': ciudades[i],
            'edad': random.randint(18, 65),
            'timestamp': datetime.now().isoformat()
        }
        
        mensaje = json.dumps(usuario)
        
        producer.produce(
            topic='usuarios',
            key=str(i),
            value=mensaje,
            callback=delivery_report
        )
        
        producer.poll(0)
        time.sleep(1)
    
    producer.flush()
    print("\n✨ Todos los usuarios han sido registrados\n")

if __name__ == '__main__':
    print("=" * 60)
    print("           PRODUCTOR DE MENSAJES KAFKA")
    print("=" * 60)
    
    # Enviar mensajes a ambos topics
    enviar_pedidos()
    enviar_usuarios()
    
    print("🎉 Proceso completado exitosamente")