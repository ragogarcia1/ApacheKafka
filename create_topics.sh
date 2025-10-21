#!/bin/bash

# Topic 1: pedidos (para simular órdenes de compra)
docker exec kafka-broker-1 kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic pedidos \
  --partitions 3 \
  --replication-factor 3

# Topic 2: usuarios (para simular registro de usuarios)
docker exec kafka-broker-1 kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic usuarios \
  --partitions 3 \
  --replication-factor 3

# Listar topics creados
docker exec kafka-broker-1 kafka-topics --list \
  --bootstrap-server localhost:9092

# Describir un topic
docker exec kafka-broker-1 kafka-topics --describe \
  --bootstrap-server localhost:9092 \
  --topic pedidos