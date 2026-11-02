# Airflow en EC2

Este stack despliega el mismo `docker compose` de la raíz del repo en una
instancia EC2. Las credenciales salen del rol de la instancia (IMDSv2) y no
de llaves copiadas. Está pensado para encenderse durante la demo o una
ejecución y apagarse después.

## Qué crea

| Recurso | Detalle |
|---|---|
| Rol IAM `noticias-data-lake-airflow-ec2` y su instance profile | Tiene adjuntas las 3 políticas del taller (`-s3`, `-comprehend`, `-athena`), que se leen por nombre desde `terraform/iam`, más `AmazonSSMManagedInstanceCore`. |
| Security group `noticias-data-lake-airflow` | Salida libre y SSH solo desde EC2 Instance Connect. El 8080 se puede abrir a mano para una IP (ver [Acceso](#acceso)). |
| Instancia `noticias-data-lake-airflow` | Amazon Linux 2023 (la AMI sale de SSM), `t3.medium`, disco gp3 de 30 GiB cifrado, IMDSv2 obligatorio con hop limit 2. Usa la VPC y la subred por defecto. |

En el primer arranque, `user_data.sh.tftpl` hace lo siguiente:

1. Instala docker, git y los plugins compose y buildx.
2. Crea 2 GiB de swap.
3. Clona el repo en `/opt/noticias-data-lake`.
4. Escribe `.env`.
5. Levanta `docker compose up -d --build`.

### Diferencias con el despliegue local

- **Credenciales**: `docker-compose.yaml` define `AWS_PROFILE=default` y
  monta `.secrets/aws`. Si ese perfil no existe, boto3 falla con
  `ProfileNotFound` antes de llegar al rol. Por eso `user_data` crea
  `.secrets/aws/config` con solo `[default] region = ...` y sin archivo
  `credentials`, y boto3 termina usando el rol de la instancia. `setup.sh`
  no se usa en EC2, porque exige `~/.aws/credentials`.
- **Claves de Airflow**: `docker-compose.ec2.yaml` (en esta carpeta)
  reemplaza `AIRFLOW__API__SECRET_KEY` y `AIRFLOW__API_AUTH__JWT_SECRET`,
  que están publicadas en el repo, por valores aleatorios generados en
  `.env`. `user_data` lo copia a la raíz del repo y lo activa con
  `COMPOSE_FILE` en `.env`, así que en la instancia basta con escribir
  `docker compose ...`.
- `logs/` y `plugins/` se crean antes de levantar Compose. Si no existen,
  Docker los crea como root y Airflow no puede escribir los logs.

## Requisitos

- Haber aplicado `terraform/s3`, `terraform/iam` y `terraform/catalog`.
- AWS CLI v2 con el
  [Session Manager plugin](https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html).
- La rama que se despliega tiene que estar en GitHub. La instancia clona
  `repo_branch` (por defecto `main`) por HTTPS. Si el trabajo está en
  otra rama, primero hay que hacer push y luego pasar
  `-var repo_branch=<rama>`.

## Despliegue

`user_cli` no tiene permisos de IAM, EC2 ni SSM: se aplica con el perfil
administrador.

```bash
aws login --profile admin
cd terraform/ec2
terraform init
terraform plan  -var aws_profile=admin
terraform apply -var aws_profile=admin
```

Variables útiles (con `-var` o en un `terraform.tfvars`, que está en
`.gitignore`):

| Variable | Por defecto | Uso |
|---|---|---|
| `repo_branch` | `main` | Rama que se clona. |
| `instance_type` | `t3.medium` | No bajar de 4 GiB de RAM. |
| `root_volume_size` | `30` | GiB del disco gp3. |
| `key_name` | `null` | Par de llaves para SSH. Solo se usa al crear la instancia; cambiarlo la recrea. Sin él, usar Instance Connect o Session Manager. |
| `cpu_credits` | `unlimited` | `standard` evita cualquier cobro extra de CPU, pero estrangula la instancia si se le acaban los créditos. |

## Acceso

- **Terminal desde la consola, sin llaves**: EC2 → Instancias →
  `noticias-data-lake-airflow` → **Connect** → *EC2 Instance Connect* →
  usuario `ec2-user`. Terraform abre el 22 solo para la lista de prefijos
  `com.amazonaws.us-east-2.ec2-instance-connect`, que es el rango del
  servicio de AWS. No queda abierto a ninguna otra IP, y la llave que sube
  la consola dura 60 s. La instancia no tiene par de llaves.
- **UI de Airflow**: túnel de Session Manager (`ssm_port_forward_command`),
  o una regla manual del 8080 para la IP propia `/32` en el security group.
  Terraform no administra esa regla, así que un `apply` no la borra. Es HTTP
  sin cifrar; hay que borrar la regla al terminar la demo.
- **Session Manager** (`ssm_session_command`) funciona siempre, sin
  ninguna regla de entrada.

## Entrar al panel de Airflow

1. **Abrir el acceso al 8080**: en el security group
   `noticias-data-lake-airflow`, agregar a mano la regla *Custom TCP 8080*
   con origen `<tu IP>/32` (`curl https://checkip.amazonaws.com`). Como
   alternativa sin abrir puertos, usar el túnel:
   `eval "$(terraform output -raw ssm_port_forward_command)"`.
2. **Obtener la contraseña** del usuario `admin`. Se genera al crear el
   contenedor del api-server, así que cambia si se recrea. Desde
   `terraform/ec2` en la máquina local:

   ```bash
   ID=$(terraform output -raw instance_id)
   CMD=$(aws ssm send-command --profile admin --region us-east-2 --instance-ids "$ID" \
     --document-name AWS-RunShellScript \
     --parameters 'commands=["cd /opt/noticias-data-lake && docker compose exec -T airflow-apiserver cat /opt/airflow/simple_auth_manager_passwords.json.generated"]' \
     --query Command.CommandId --output text)
   aws ssm wait command-executed --profile admin --region us-east-2 --command-id "$CMD" --instance-id "$ID"
   aws ssm get-command-invocation --profile admin --region us-east-2 --command-id "$CMD" --instance-id "$ID" \
     --query StandardOutputContent --output text
   # -> {"admin": "<contraseña>"}
   ```

   O desde la terminal de la consola (EC2 Instance Connect o Session
   Manager):

   ```bash
   cd /opt/noticias-data-lake && sudo docker compose exec airflow-apiserver \
     cat /opt/airflow/simple_auth_manager_passwords.json.generated
   ```
3. **Entrar** a `http://<IP pública>:8080` (`terraform output -raw public_ip`),
   o a `http://localhost:8080` con el túnel, con el usuario `admin` y esa
   contraseña.
4. Al terminar la demo, **borrar la regla del 8080**.

## Verificación

Los comandos salen de `terraform output` y se ejecutan desde `terraform/ec2`. `eval` respeta las comillas del JSON de `--parameters`.

```bash
# 1. Terminal en la instancia
eval "$(terraform output -raw ssm_session_command)"

# Dentro de la sesión: seguir el arranque (5-10 min la primera vez, por el build)
sudo tail -f /var/log/cloud-init-output.log      # termina en "== Arranque terminado"
cd /opt/noticias-data-lake && sudo docker compose ps

# Comprobar que los contenedores usan el rol de la instancia y no llaves
sudo docker compose exec airflow-scheduler python -c \
  "import boto3; print(boto3.client('sts').get_caller_identity()['Arn'])"
# -> arn:aws:sts::<cuenta>:assumed-role/noticias-data-lake-airflow-ec2/i-...

# Contraseña del usuario admin (el comando lo da terraform output airflow_password_command)
cd /opt/noticias-data-lake && sudo docker compose exec airflow-apiserver \
  cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

```bash
# 2. UI: dejar el túnel abierto en una terminal local
eval "$(terraform output -raw ssm_port_forward_command)"
# y abrir http://localhost:8080 (usuario admin)
```

Los DAGs se crean pausados, igual que en local. Para actualizar el código
sin recrear la instancia, dentro de la sesión:

```bash
cd /opt/noticias-data-lake && sudo -u ec2-user git pull && sudo docker compose up -d --build
```

## Detener y destruir

**Detener** cuando no se use. Postgres y el historial de Airflow quedan en
el disco y los contenedores vuelven solos al arrancar (`restart:
unless-stopped`). La IP pública cambia en cada arranque.

```bash
aws ec2 stop-instances  --instance-ids $(terraform output -raw instance_id) --region us-east-2 --profile admin
aws ec2 start-instances --instance-ids $(terraform output -raw instance_id) --region us-east-2 --profile admin
```

**Destruir** al terminar el taller:

```bash
terraform destroy -var aws_profile=admin
```

Esto borra la instancia, el disco (con Postgres), el security group, el rol
y el instance profile. Las políticas del taller y los datos de S3 no se
tocan, porque pertenecen a `terraform/iam` y `terraform/s3`. Si se destruye
todo, este stack va primero: sus adjuntos dependen de las políticas de
`terraform/iam`.

> Los cambios en `user_data` (la plantilla, las variables del repo o las
> versiones de los plugins) y en la AMI **no** recrean la instancia
> actual: solo aplican a instancias nuevas. Para aplicarlos a la actual
> se usa `terraform apply -replace=aws_instance.airflow ...`, y se pierde
> la base de Postgres (los datos de S3 no).

## Costo estimado (us-east-2, on-demand)

| Concepto | Precio | Encendida 1 h | Encendida 8 h | 24/7 un mes |
|---|---|---|---|---|
| `t3.medium` | USD 0,0416/h | 0,042 | 0,33 | ~30,4 |
| IPv4 pública | USD 0,005/h | 0,005 | 0,04 | ~3,7 |
| gp3 30 GiB | USD 0,08/GiB-mes | 0,003 | 0,03 | 2,4 |
| **Total** | | **~0,05** | **~0,40** | **~36,5** |

- Con la instancia **detenida** solo se cobra el disco: unos USD 2,4 al
  mes. La IP automática se libera al detenerla.
- Session Manager no tiene costo. Comprehend, S3 y Athena se cobran aparte,
  igual que en local.
- Con `cpu_credits = "unlimited"` hay un cobro de USD 0,05 por vCPU-hora
  solo si el uso promedio de 24 h supera el 20 % por vCPU. En sesiones
  cortas eso es raro.
