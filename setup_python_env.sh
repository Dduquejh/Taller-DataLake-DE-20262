
#!/usr/bin/env bash
set -euo pipefail

echo "==> Creando entorno virtual..."
python -m venv .venv

echo "==> Activando entorno virtual..."
source .venv/bin/activate

echo "==> Actualizando pip..."
python -m pip install --upgrade pip

echo "==> Instalando dependencias..."
python -m pip install -r requirements.txt

echo "==> Verificando instalación..."
python -c "import boto3, feedparser, pandas, pyarrow, requests; print('Dependencias principales OK')"

echo "==> Instalación completada."
echo "Para activar el entorno virtual posteriormente, ejecuta:"
echo "source .venv/bin/activate"

