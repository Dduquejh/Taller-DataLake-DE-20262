#!/usr/bin/env bash
# Deja el data lake listo para una demo desde cero, conservando Bronze.
#
# Borra silver/, gold/ y quarantine/ (tablas, resúmenes de lote, estados de
# corrida y staging) y los lotes de escenario de Bronze (run_id=escenario-*).
# Las capturas reales de Bronze se conservan, así que todo se puede
# reconstruir con el DAG de reproceso.
#
# Requiere un perfil con permiso de borrado en Bronze y quarantine: el
# pipeline no lo tiene a propósito (ver terraform/iam/s3.tf).
#
# El bucket tiene versionado: "borrar" deja un marcador de borrado y las
# versiones anteriores siguen recuperables.
#
# Uso:
#   scripts/reset_demo.sh --profile admin          # muestra qué borraría
#   scripts/reset_demo.sh --profile admin --yes    # borra
#
# Antes: detener cualquier Airflow que escriba en el bucket.
set -euo pipefail

PROFILE=""
CONFIRM=false

while [ $# -gt 0 ]; do
  case "$1" in
    --profile) PROFILE="$2"; shift 2 ;;
    --yes) CONFIRM=true; shift ;;
    *) echo "Opción desconocida: $1" >&2; exit 2 ;;
  esac
done

AWS=(aws)
[ -n "$PROFILE" ] && AWS+=(--profile "$PROFILE")

ACCOUNT=$("${AWS[@]}" sts get-caller-identity --query Account --output text)
BUCKET="${S3_BUCKET:-noticias-data-lake-$ACCOUNT}"

echo "Bucket: s3://$BUCKET"

for prefix in silver/ gold/ quarantine/; do
  count=$("${AWS[@]}" s3 ls "s3://$BUCKET/$prefix" --recursive | wc -l)
  echo "  $prefix: $count objetos"
done

SCENARIO_KEYS=$("${AWS[@]}" s3 ls "s3://$BUCKET/bronze/" --recursive \
  | awk '{print $4}' | grep '/run_id=escenario-' || true)
echo "  bronze/ (lotes escenario-*): $(printf '%s' "$SCENARIO_KEYS" | grep -c . || true) objetos"

if [ "$CONFIRM" != true ]; then
  echo
  echo "Sin cambios. Repite con --yes para borrar."
  exit 0
fi

for prefix in silver/ gold/ quarantine/; do
  "${AWS[@]}" s3 rm "s3://$BUCKET/$prefix" --recursive --only-show-errors
done

while read -r key; do
  [ -n "$key" ] && "${AWS[@]}" s3 rm "s3://$BUCKET/$key" --only-show-errors
done <<< "$SCENARIO_KEYS"

echo
echo "Listo. La próxima ingesta construye Silver y Gold desde cero."
