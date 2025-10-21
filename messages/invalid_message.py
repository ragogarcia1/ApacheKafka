from confluent_kafka import Producer
import json
import requests

SCHEMA_REGISTRY_URL = 'http://localhost:8081'

# Primero, obtengamos el schema registrado
def validar_mensaje_contra_schema(mensaje, subject):
    """
    Valida un mensaje contra el schema registrado
    """
    try:
        # Obtener el schema
        response = requests.get(f'{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest')
        schema_info = response.json()
        schema = json.loads(schema_info['schema'])
        
        print("\n📋 Schema registrado:")
        print(json.dumps(schema, indent=2))
        
        # Validar campos requeridos
        required_fields = schema.get('required', [])
        mensaje_fields = set(mensaje.keys())
        required_set = set(required_fields)
        
        missing_fields = required_set - mensaje_fields
        
        if missing_fields:
            print(f"\n❌ ERROR: Faltan campos requeridos: {missing_fields}")
            return False
        
        # Validar tipos de datos
        properties = schema.get('properties', {})
        for field, value in mensaje.items():
            if field in properties:
                expected_type = properties[field]['type']
                actual_type = type(value).__name__
                
                type_mapping = {
                    'string': 'str',
                    'integer': 'int',
                    'number': ['float', 'int'],
                }
                
                expected = type_mapping.get(expected_type, expected_type)
                
                if isinstance(expected, list):
                    if actual_type not in expected:
                        print(f"❌ ERROR: Campo '{field}' debe ser {expected}, pero es {actual_type}")
                        return False
                else:
                    if actual_type != expected:
                        print(f"❌ ERROR: Campo '{field}' debe ser {expected}, pero es {actual_type}")
                        return False
        
        print("\n✅ Mensaje cumple con el schema")
        return True
        
    except Exception as e:
        print(f"❌ Error al validar: {e}")
        return False

# Mensaje INVÁLIDO - falta campo requerido 'producto'
mensaje_invalido = {
    'id_pedido': 'PED-888',
    # 'producto': 'Mouse',  # ← FALTA ESTE CAMPO REQUERIDO
    'cantidad': 1,
    'precio': 25.00,
    'timestamp': '2025-10-20T10:00:00'
}

print("🧪 Probando validación de schema...\n")
print("📦 Mensaje a validar:")
print(json.dumps(mensaje_invalido, indent=2))

# Validar antes de enviar
es_valido = validar_mensaje_contra_schema(mensaje_invalido, 'pedidos-value')

if es_valido:
    # Enviar mensaje
    config = {'bootstrap.servers': 'localhost:9092'}
    producer = Producer(config)
    
    producer.produce(
        topic='pedidos',
        key='test',
        value=json.dumps(mensaje_invalido)
    )
    producer.flush()
    print("\n✅ Mensaje enviado (pero esto no debería pasar!)")
else:
    print("\n🚫 Mensaje rechazado por no cumplir el schema")
    print("💡 El Schema Registry protege la integridad de los datos")