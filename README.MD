# 🚀 Apache Kafka Workshop - Guía de Ejecución

## 📋 Tabla de Contenidos
1. [Requisitos Previos](#requisitos-previos)
2. [Estructura del Proyecto](#estructura-del-proyecto)
3. [Configuración Inicial](#configuración-inicial)
4. [Levantamiento del Cluster](#levantamiento-del-cluster)
5. [Creación de Topics](#creación-de-topics)
6. [Ejecución de Componentes](#ejecución-de-componentes)
7. [Validación con Schema Registry](#validación-con-schema-registry)
8. [Comandos de Monitoreo](#comandos-de-monitoreo)
9. [Comandos según el Entorno](#comandos-según-el-entorno)
10. [Solución de Problemas](#solución-de-problemas)
11. [Apagar el Entorno](#apagar-el-entorno)

---

## 📦 Requisitos Previos

### Software Necesario
- **Docker** y **Docker Compose** instalados
- **Python 3.8+** instalado
- **pip** (gestor de paquetes de Python)

### Librerías Python
```bash
pip install confluent-kafka requests
```

---

## 📁 Estructura del Proyecto

```
kafka-workshop/
│
├── docker-compose.yml           # Configuración del cluster Kafka
├── producer.py                  # Producer de mensajes
├── consumer.py                  # Consumer de mensajes
├── schema_registry_demo.py      # Demo de Schema Registry
│
└── scripts/                     # (Opcional) Scripts de utilidad
    ├── create_topics.sh         # Para Linux/Mac/Git Bash
    └── create_topics.ps1        # Para Windows PowerShell
```

---

## ⚙️ Configuración Inicial

### 1. Clonar o crear el directorio del proyecto
---

## 🐳 Levantamiento del Cluster

### Iniciar todos los servicios

```bash
docker-compose up -d
```

### Verificar que los contenedores estén corriendo

```bash
docker-compose ps
```

**Salida esperada:**
```
NAME                 STATUS              PORTS
kafka-broker-1       Up                  0.0.0.0:9092->9092/tcp
kafka-broker-2       Up                  0.0.0.0:9094->9094/tcp
kafka-broker-3       Up                  0.0.0.0:9095->9095/tcp
schema-registry      Up                  0.0.0.0:8081->8081/tcp
kafka-ui             Up                  0.0.0.0:8085->8085/tcp
```

### Ver logs de un broker específico

```bash
# Ver logs en tiempo real
docker-compose logs -f kafka-broker-1

# Ver últimas 100 líneas de logs
docker-compose logs --tail=100 kafka-broker-1
```

### ⏳ Esperar a que el cluster esté listo

**Importante:** Espera aproximadamente **30-45 segundos** después de levantar el cluster antes de crear topics o producir mensajes.

---

## 📊 Creación de Topics

Los topics deben crearse con **3 particiones** y **factor de replicación 3** para aprovechar el cluster completo.

### 🐧 Linux / Mac / Git Bash

```bash
# Topic: pedidos
docker exec kafka-broker-1 kafka-topics --create \
  --bootstrap-server kafka-broker-1:19092 \
  --topic pedidos \
  --partitions 3 \
  --replication-factor 3

# Topic: usuarios
docker exec kafka-broker-1 kafka-topics --create \
  --bootstrap-server kafka-broker-1:19092 \
  --topic usuarios \
  --partitions 3 \
  --replication-factor 3
```

### 🪟 Windows PowerShell

```powershell
# Topic: pedidos
docker exec kafka-broker-1 kafka-topics --create `
  --bootstrap-server kafka-broker-1:19092 `
  --topic pedidos `
  --partitions 3 `
  --replication-factor 3

# Topic: usuarios
docker exec kafka-broker-1 kafka-topics --create `
  --bootstrap-server kafka-broker-1:19092 `
  --topic usuarios `
  --partitions 3 `
  --replication-factor 3
```

### 🐚 Desde el Bash del Contenedor

```bash
# Entrar al bash del contenedor
docker exec -it kafka-broker-1 bash

# Una vez dentro, ejecutar:
kafka-topics --create \
  --bootstrap-server kafka-broker-1:19092 \
  --topic pedidos \
  --partitions 3 \
  --replication-factor 3

kafka-topics --create \
  --bootstrap-server kafka-broker-1:19092 \
  --topic usuarios \
  --partitions 3 \
  --replication-factor 3

# Salir del contenedor
exit
```

### Verificar topics creados

```bash
# Listar todos los topics
docker exec kafka-broker-1 kafka-topics --list \
  --bootstrap-server kafka-broker-1:19092

# Describir un topic específico
docker exec kafka-broker-1 kafka-topics --describe \
  --bootstrap-server kafka-broker-1:19092 \
  --topic pedidos
```

**Salida esperada de describe:**
```
Topic: pedidos
PartitionCount: 3
ReplicationFactor: 3
Partition: 0    Leader: 1    Replicas: 1,2,3    Isr: 1,2,3
Partition: 1    Leader: 2    Replicas: 2,3,1    Isr: 2,3,1
Partition: 2    Leader: 3    Replicas: 3,1,2    Isr: 3,1,2
```

---

## ▶️ Ejecución de Componentes

### 1. Schema Registry Demo

**Ejecutar primero** para registrar los schemas:

```bash
python schema_registry_demo.py
```

### 2. Producer

**En una terminal:**

```bash
python producer.py
```

Verás mensajes como:
```
✅ Mensaje enviado a pedidos [partición 0] en offset 0
✅ Mensaje enviado a pedidos [partición 1] en offset 0
...
```

### 3. Consumer(s)

**En otra(s) terminal(es):**

```bash
python consumer.py
```

Selecciona la opción deseada:
- **1** - Consumer de Pedidos
- **2** - Consumer de Usuarios  
- **3** - Consumer General (ambos topics)

**Tip:** Puedes abrir múltiples terminales y ejecutar diferentes consumers simultáneamente para ver cómo funcionan los consumer groups.

---

## 🔍 Comandos de Monitoreo

### Ver mensajes de un topic en tiempo real

#### 🐧 Linux / Mac / Git Bash
```bash
docker exec kafka-broker-1 kafka-console-consumer \
  --bootstrap-server kafka-broker-1:19092 \
  --topic pedidos \
  --from-beginning
```

#### 🪟 Windows PowerShell
```powershell
docker exec kafka-broker-1 kafka-console-consumer `
  --bootstrap-server kafka-broker-1:19092 `
  --topic pedidos `
  --from-beginning
```

#### 🐚 Desde el Bash del Contenedor
```bash
docker exec -it kafka-broker-1 bash

kafka-console-consumer \
  --bootstrap-server kafka-broker-1:19092 \
  --topic pedidos \
  --from-beginning
```

### Listar Consumer Groups

```bash
docker exec kafka-broker-1 kafka-consumer-groups \
  --bootstrap-server kafka-broker-1:19092 \
  --list
```

### Ver detalles de un Consumer Group

```bash
docker exec kafka-broker-1 kafka-consumer-groups \
  --bootstrap-server kafka-broker-1:19092 \
  --group grupo-pedidos \
  --describe
```

**Información importante que muestra:**
- `CURRENT-OFFSET`: Último mensaje procesado
- `LOG-END-OFFSET`: Último mensaje disponible
- `LAG`: Mensajes pendientes de procesar

### Ver información del Schema Registry

```bash
# Listar todos los schemas registrados
curl http://localhost:8081/subjects

# Ver detalles de un schema específico
curl http://localhost:8081/subjects/pedidos-value/versions/latest
```

---

## 🎯 Comandos según el Entorno

### Resumen de diferencias

| Acción | Linux/Mac/Git Bash | Windows PowerShell | Bash del Contenedor |
|--------|-------------------|-------------------|-------------------|
| **Continuar línea** | `\` | `` ` `` (backtick) | `\` |
| **Ejecutar comando** | `docker exec ...` | `docker exec ...` | Comando directo |
| **Bootstrap server** | `kafka-broker-X:19092` | `kafka-broker-X:19092` | `kafka-broker-X:19092` |

### 🔑 Puntos clave

#### Para comandos kafka-topics, kafka-console-consumer, etc:

**Desde fuera del contenedor (tu máquina):**
```bash
docker exec kafka-broker-1 kafka-topics ...
```

**Desde dentro del contenedor:**
```bash
docker exec -it kafka-broker-1 bash
kafka-topics ...  # Sin 'docker exec'
```

#### Para código Python:

**Siempre usa los puertos expuestos en tu máquina:**
```python
'bootstrap.servers': 'localhost:9092,localhost:9094,localhost:9095'
```

---

## 🧪 Flujo de Trabajo Sugerido para la Demo

### Paso 1: Preparación (5 min)
```bash
# 1. Levantar cluster
docker-compose up -d

# 2. Esperar 30 segundos
sleep 30

# 3. Crear topics
[Ejecutar comandos de creación según tu SO]

# 4. Verificar topics
docker exec kafka-broker-1 kafka-topics --list \
  --bootstrap-server kafka-broker-1:19092
```

### Paso 2: Schema Registry (2 min)
```bash
python schema_registry_demo.py
```

### Paso 3: Iniciar Consumers (2 min)
```bash
# Terminal 1: Consumer de pedidos
python consumer.py
# Seleccionar opción 1

# Terminal 2: Consumer de usuarios
python consumer.py
# Seleccionar opción 2
```

### Paso 4: Producir Mensajes (3 min)
```bash
# Terminal 3: Producer
python producer.py
```

**Observa cómo los mensajes aparecen en los consumers en tiempo real.**

### Paso 5: Monitoreo (3 min)
```bash
# Ver consumer groups activos
docker exec kafka-broker-1 kafka-consumer-groups \
  --bootstrap-server kafka-broker-1:19092 \
  --list

# Ver lag de un consumer
docker exec kafka-broker-1 kafka-consumer-groups \
  --bootstrap-server kafka-broker-1:19092 \
  --group grupo-pedidos \
  --describe
```

---

## 🐛 Solución de Problemas

### Error: "Connection to node ... could not be established"

**Problema:** Intentas conectarte a `localhost:9094` desde dentro del contenedor.

**Solución:** Usa los nombres internos de Docker:
```bash
--bootstrap-server kafka-broker-1:19092,kafka-broker-2:19093,kafka-broker-3:19095
```

### Error: "Topic 'X' already exists"

**Normal:** El topic ya fue creado previamente.

**Verificar:**
```bash
docker exec kafka-broker-1 kafka-topics --list \
  --bootstrap-server kafka-broker-1:19092
```

### Error: "No module named 'confluent_kafka'"

**Solución:**
```bash
pip install confluent-kafka requests
```

### Los consumers no reciben mensajes

**Verificaciones:**

1. ¿El topic existe?
```bash
docker exec kafka-broker-1 kafka-topics --list \
  --bootstrap-server kafka-broker-1:19092
```

2. ¿Hay mensajes en el topic?
```bash
docker exec kafka-broker-1 kafka-console-consumer \
  --bootstrap-server kafka-broker-1:19092 \
  --topic pedidos \
  --from-beginning \
  --max-messages 5
```

3. ¿El consumer está usando el bootstrap server correcto?
- Desde Python: `localhost:9092,localhost:9094,localhost:9095`
- Desde comandos docker exec: `kafka-broker-1:19092,kafka-broker-2:19093,kafka-broker-3:19095`

### Schema Registry no responde

**Verificar que esté corriendo:**
```bash
docker-compose ps schema-registry
curl http://localhost:8081/subjects
```

**Si no responde, reiniciar:**
```bash
docker-compose restart schema-registry
```

### Los brokers no inician

**Ver logs:**
```bash
docker-compose logs kafka-broker-1
docker-compose logs kafka-broker-2
docker-compose logs kafka-broker-3
```

**Limpiar y reiniciar:**
```bash
docker-compose down -v
docker-compose up -d
```

⚠️ **Advertencia:** `-v` elimina los volúmenes y **todos los datos**.

---

## 🛑 Apagar el Entorno

### Detener servicios (mantiene datos)

```bash
docker-compose stop
```

### Reiniciar servicios
```bash
docker-compose start
```

### Detener y eliminar contenedores (mantiene volúmenes)

```bash
docker-compose down
```

---

## 📚 Comandos Útiles de Referencia Rápida

### Topics
```bash
# Crear
docker exec kafka-broker-1 kafka-topics --create \
  --bootstrap-server kafka-broker-1:19092 \
  --topic <nombre> --partitions 3 --replication-factor 3

# Listar
docker exec kafka-broker-1 kafka-topics --list \
  --bootstrap-server kafka-broker-1:19092

# Describir
docker exec kafka-broker-1 kafka-topics --describe \
  --bootstrap-server kafka-broker-1:19092 --topic <nombre>

# Eliminar
docker exec kafka-broker-1 kafka-topics --delete \
  --bootstrap-server kafka-broker-1:19092 --topic <nombre>
```

### Consumer Groups
```bash
# Listar
docker exec kafka-broker-1 kafka-consumer-groups \
  --bootstrap-server kafka-broker-1:19092 --list

# Describir
docker exec kafka-broker-1 kafka-consumer-groups \
  --bootstrap-server kafka-broker-1:19092 \
  --group <nombre> --describe

# Resetear offsets
docker exec kafka-broker-1 kafka-consumer-groups \
  --bootstrap-server kafka-broker-1:19092 \
  --group <nombre> --reset-offsets --to-earliest \
  --topic <topic> --execute
```

### Schema Registry
```bash
# Listar subjects
curl http://localhost:8081/subjects

# Ver schema
curl http://localhost:8081/subjects/<subject>/versions/latest

# Eliminar schema
curl -X DELETE http://localhost:8081/subjects/<subject>
```

---

## 📝 Notas Importantes

- ⏰ **Siempre espera 30-45 segundos** después de levantar el cluster antes de crear topics
- 🔄 El cluster puede tomar tiempo en elegir leaders y sincronizar réplicas
- 💾 Los datos persisten entre reinicios **a menos que** uses `docker-compose down -v`
- 🖥️ Los comandos difieren ligeramente entre Linux/Mac y Windows (principalmente en continuación de líneas)


---
