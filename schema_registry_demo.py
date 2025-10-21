import requests
import json
from confluent_kafka import Producer, Consumer
import time

SCHEMA_REGISTRY_URL = 'http://localhost:8081'

# Definir schemas JSON para nuestros mensajes
PEDIDO_SCHEMA = {
    "type": "object",
    "properties": {
        "id_pedido": {"type": "string"},
        "producto": {"type": "string"},
        "cantidad": {"type": "integer"},
        "precio": {"type": "number"},
        "timestamp": {"type": "string"}
    },
    "required": ["id_pedido", "producto", "cantidad", "precio"]
}

USUARIO_SCHEMA = {
    "type": "object",
    "properties": {
        "id_usuario": {"type": "string"},
        "nombre": {"type": "string"},
        "ciudad": {"type": "string"},
        "edad": {"type": "integer"},
        "timestamp": {"type": "string"}
    },
    "required": ["id_usuario", "nombre", "ciudad", "edad"]
}

def registrar_schema(subject, schema):
    """
    Registra un schema en Schema Registry
    """
    url = f'{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions'
    
    payload = {
        "schema": json.dumps(schema),
        "schemaType": "JSON"
    }
    
    headers = {'Content-Type': 'application/vnd.schemaregistry.v1+json'}
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        
        schema_id = response.json()['id']
        print(f"✅ Schema registrado exitosamente con ID: {schema_id}")
        return schema_id
    
    except requests.exceptions.RequestException as e:
        print(f"❌ Error al registrar schema: {e}")
        return None

def obtener_schema(subject):
    """
    Obtiene un schema del Schema Registry
    """
    url = f'{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest'
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    
    except requests.exceptions.RequestException as e:
        print(f"❌ Error al obtener schema: {e}")
        return None

def listar_subjects():
    """
    Lista todos los subjects registrados
    """
    url = f'{SCHEMA_REGISTRY_URL}/subjects'
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    
    except requests.exceptions.RequestException as e:
        print(f"❌ Error al listar subjects: {e}")
        return []

def demo_schema_registry():
    """
    Demostración completa de Schema Registry
    """
    print("=" * 60)
    print("        DEMO: SCHEMA REGISTRY CON JSON")
    print("=" * 60)
    
    # 1. Registrar schemas
    print("\n📝 Paso 1: Registrando schemas...\n")
    
    pedido_id = registrar_schema('pedidos-value', PEDIDO_SCHEMA)
    time.sleep(1)
    usuario_id = registrar_schema('usuarios-value', USUARIO_SCHEMA)
    
    # 2. Listar todos los subjects
    print("\n📋 Paso 2: Listando todos los schemas registrados...\n")
    
    subjects = listar_subjects()
    print(f"Subjects encontrados: {subjects}")
    
    # 3. Obtener información de un schema específico
    print("\n🔍 Paso 3: Obteniendo información del schema de pedidos...\n")
    
    schema_info = obtener_schema('pedidos-value')
    if schema_info:
        print(f"Schema ID: {schema_info['id']}")
        print(f"Versión: {schema_info['version']}")
        print(f"Schema:\n{json.dumps(json.loads(schema_info['schema']), indent=2)}")
    
    # 4. Validación de compatibilidad
    print("\n✔️ Paso 4: Validación de schemas")
    print("Los schemas JSON definen la estructura esperada de los mensajes.")
    print("Esto ayuda a mantener la consistencia de datos en el sistema.\n")
    
    print("=" * 60)
    print("✨ Demo completada exitosamente")
    print("=" * 60)

if __name__ == '__main__':
    # Esperar a que Schema Registry esté listo
    print("⏳ Esperando a que Schema Registry esté disponible...")
    time.sleep(5)
    
    demo_schema_registry()