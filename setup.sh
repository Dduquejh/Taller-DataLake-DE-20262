#!/usr/bin/env bash
set -Eeuo pipefail

# Ejecutar desde cualquier ubicación.
PROJECT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

AWS_DIR="$HOME/.aws"
SECRETS_DIR="$PROJECT_DIR/.secrets"
AIRFLOW_AWS_DIR="$SECRETS_DIR/aws"

log() {
    printf '\n[%s] %s\n' "$(date '+%H:%M:%S')" "$1"
}

fail() {
    printf '\nERROR: %s\n' "$1" >&2
    exit 1
}

# 1. Verificar requisitos.
log "Verificando requisitos..."

command -v docker >/dev/null 2>&1 ||
    fail "Docker no está instalado o no está en PATH."

docker compose version >/dev/null 2>&1 ||
    fail "Docker Compose no está disponible."

[[ -f "$PROJECT_DIR/docker-compose.yaml" ]] ||
[[ -f "$PROJECT_DIR/docker-compose.yml" ]] ||
    fail "No encuentro docker-compose.yaml ni docker-compose.yml."

[[ -f "$AWS_DIR/config" ]] ||
    fail "No existe $AWS_DIR/config."

[[ -f "$AWS_DIR/credentials" ]] ||
    fail "No existe $AWS_DIR/credentials."

docker info >/dev/null 2>&1 ||
    fail "Docker no está iniciado o tu usuario no tiene acceso."

# 2. Verificar que Compose monte las credenciales preparadas.
if ! grep -Fq './.secrets/aws:/home/airflow/.aws:ro' \
    "$PROJECT_DIR/docker-compose.yaml" 2>/dev/null; then
    if ! grep -Fq './.secrets/aws:/home/airflow/.aws:ro' \
        "$PROJECT_DIR/docker-compose.yml" 2>/dev/null; then
        fail "Configura en los servicios Airflow el montaje ./.secrets/aws:/home/airflow/.aws:ro"
    fi
fi

# 3. Copiar las credenciales sin mostrarlas.
# 3. Copiar las credenciales sin mostrarlas.
log "Preparando copia privada de las credenciales AWS..."

# Recuperar el acceso del usuario local a la carpeta.
sudo chown -R "$(id -u):$(id -g)" "$SECRETS_DIR" 2>/dev/null || true
chmod 700 "$SECRETS_DIR" 2>/dev/null || true

mkdir -p "$AIRFLOW_AWS_DIR"

install -m 600 "$AWS_DIR/config" \
    "$AIRFLOW_AWS_DIR/config"

install -m 600 "$AWS_DIR/credentials" \
    "$AIRFLOW_AWS_DIR/credentials"

# Permitir lectura al grupo de Airflow (GID 0).
sudo chgrp -R 0 "$SECRETS_DIR"
chmod 750 "$SECRETS_DIR" "$AIRFLOW_AWS_DIR"
chmod 640 "$AIRFLOW_AWS_DIR/config" \
          "$AIRFLOW_AWS_DIR/credentials"
# 4. Excluir secretos de Git.
log "Verificando .gitignore..."

touch "$PROJECT_DIR/.gitignore"

if ! grep -Fxq '.secrets/' "$PROJECT_DIR/.gitignore"; then
    printf '\n# Credenciales privadas para Docker/Airflow\n.secrets/\n' \
        >> "$PROJECT_DIR/.gitignore"
fi

if git ls-files --error-unmatch .secrets/aws/credentials \
    >/dev/null 2>&1; then
    fail "Las credenciales ya están rastreadas por Git. Debes retirarlas del índice antes de continuar."
fi

# 5. Levantar los servicios.
log "Levantando los servicios de Docker Compose..."

docker compose up -d

# 6. Esperar a que el scheduler esté disponible.
log "Esperando al scheduler..."

scheduler_ready=false

for attempt in $(seq 1 30); do
    if docker compose exec -T airflow-scheduler \
        airflow version >/dev/null 2>&1; then
        scheduler_ready=true
        break
    fi
    sleep 2
done

if [[ "$scheduler_ready" != true ]]; then
    docker compose ps
    docker compose logs --tail=60 airflow-scheduler
    fail "El scheduler no quedó disponible. Revisa los registros anteriores."
fi

# 7. Comprobar lectura de credenciales desde Airflow.
log "Comprobando acceso a los archivos AWS..."

docker compose exec -T airflow-scheduler sh -c '
    test -r /home/airflow/.aws/config &&
    test -r /home/airflow/.aws/credentials
' || fail "Airflow no puede leer las credenciales AWS."

echo "Credenciales legibles por Airflow: OK"

# 8. Mostrar errores de importación y DAGs.
log "Consultando errores de importación..."

docker compose exec -T airflow-scheduler \
    airflow dags list-import-errors

log "DAGs detectados..."

docker compose exec -T airflow-scheduler \
    airflow dags list

log "Estado de los servicios..."
docker compose ps

log "Preparación terminada."
