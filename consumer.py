from confluent_kafka import Consumer, KafkaError
import json
import sys

def crear_consumer(group_id, topics):
    """
    Crea y configura un consumer de Kafka
    """
    config = {
        'bootstrap.servers': 'localhost:9092,localhost:9094,localhost:9095',
        'group.id': group_id,
        'auto.offset.reset': 'earliest',  # Leer desde el inicio
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 5000
    }
    
    consumer = Consumer(config)
    consumer.subscribe(topics)
    
    return consumer

def consumir_pedidos():
    """
    Consumer dedicado a procesar pedidos
    """
    consumer = crear_consumer('grupo-pedidos', ['pedidos'])
    
    print("=" * 60)
    print("           CONSUMER DE PEDIDOS")
    print("=" * 60)
    print("📦 Esperando mensajes de pedidos...\n")
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('📭 Fin de partición alcanzado')
                else:
                    print(f'❌ Error: {msg.error()}')
                continue
            
            # Procesar mensaje
            pedido = json.loads(msg.value().decode('utf-8'))
            
            print(f"\n🔹 NUEVO PEDIDO RECIBIDO:")
            print(f"   Topic: {msg.topic()} | Partición: {msg.partition()} | Offset: {msg.offset()}")
            print(f"   ID: {pedido['id_pedido']}")
            print(f"   Producto: {pedido['producto']}")
            print(f"   Cantidad: {pedido['cantidad']}")
            print(f"   Precio: ${pedido['precio']}")
            print(f"   Timestamp: {pedido['timestamp']}")
            print("-" * 60)
            
    except KeyboardInterrupt:
        print("\n⏹️  Consumer detenido por el usuario")
    finally:
        consumer.close()
        print("✅ Consumer cerrado correctamente")

def consumir_usuarios():
    """
    Consumer dedicado a procesar usuarios
    """
    consumer = crear_consumer('grupo-usuarios', ['usuarios'])
    
    print("=" * 60)
    print("           CONSUMER DE USUARIOS")
    print("=" * 60)
    print("👥 Esperando mensajes de usuarios...\n")
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('📭 Fin de partición alcanzado')
                else:
                    print(f'❌ Error: {msg.error()}')
                continue
            
            # Procesar mensaje
            usuario = json.loads(msg.value().decode('utf-8'))
            
            print(f"\n🔹 NUEVO USUARIO REGISTRADO:")
            print(f"   Topic: {msg.topic()} | Partición: {msg.partition()} | Offset: {msg.offset()}")
            print(f"   ID: {usuario['id_usuario']}")
            print(f"   Nombre: {usuario['nombre']}")
            print(f"   Ciudad: {usuario['ciudad']}")
            print(f"   Edad: {usuario['edad']}")
            print(f"   Timestamp: {usuario['timestamp']}")
            print("-" * 60)
            
    except KeyboardInterrupt:
        print("\n⏹️  Consumer detenido por el usuario")
    finally:
        consumer.close()
        print("✅ Consumer cerrado correctamente")

def consumir_todos():
    """
    Consumer que escucha ambos topics
    """
    consumer = crear_consumer('grupo-general', ['pedidos', 'usuarios'])
    
    print("=" * 60)
    print("        CONSUMER GENERAL (TODOS LOS TOPICS)")
    print("=" * 60)
    print("📨 Esperando mensajes de todos los topics...\n")
    
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f'❌ Error: {msg.error()}')
                continue
            
            # Procesar mensaje según el topic
            datos = json.loads(msg.value().decode('utf-8'))
            
            print(f"\n📬 Mensaje recibido de '{msg.topic()}'")
            print(f"   Partición: {msg.partition()} | Offset: {msg.offset()}")
            print(f"   Datos: {json.dumps(datos, indent=2, ensure_ascii=False)}")
            print("-" * 60)
            
    except KeyboardInterrupt:
        print("\n⏹️  Consumer detenido por el usuario")
    finally:
        consumer.close()
        print("✅ Consumer cerrado correctamente")

if __name__ == '__main__':
    print("\n🎯 Selecciona el tipo de consumer:")
    print("1. Consumer de Pedidos")
    print("2. Consumer de Usuarios")
    print("3. Consumer General (Todos los topics)")
    
    opcion = input("\nIngresa tu opción (1-3): ")
    
    if opcion == '1':
        consumir_pedidos()
    elif opcion == '2':
        consumir_usuarios()
    elif opcion == '3':
        consumir_todos()
    else:
        print("❌ Opción inválida")
        sys.exit(1)