"""
Pipeline completo: export local → upload → run na VPS.

Uso:
    cd dados_rfb_full_etl
    python code/pipeline_motivo.py

Passos executados automaticamente:
  1. Gera motivo_cnpj.csv.gz localmente (se não existir)
  2. Faz upload para /tmp/ na VPS via SFTP
  3. Roda import_motivo_vps.py na VPS via SSH
  4. Exibe o log em tempo real

Requer: pip install paramiko
"""
import os
import pathlib
import subprocess
import sys
import time

import paramiko
from dotenv import load_dotenv


def _find_dotenv() -> str:
    p = pathlib.Path().resolve()
    for _ in range(5):
        candidate = p / ".env"
        if candidate.is_file():
            return str(candidate)
        p = p.parent
    while True:
        raw = input("Caminho do .env: ").strip().strip("'\"")
        if raw.startswith("#") or not raw:
            continue
        p = pathlib.Path(raw)
        path = p if p.name == ".env" else p / ".env"
        if path.is_file():
            return str(path)
        print(f"  Não encontrado: {path}")


load_dotenv(_find_dotenv(), override=True)

VPS_HOST   = os.getenv("VPS_HOST", os.getenv("DB_HOST"))
ROOT_PASS  = os.getenv("ROOT_PASS")
OUTPUT_DIR = os.getenv("OUTPUT_FILES_PATH", ".")
LOCAL_GZ   = os.path.join(OUTPUT_DIR, "motivo_cnpj.csv.gz")
REMOTE_GZ  = "/tmp/motivo_cnpj.csv.gz"
SCRIPT_DIR = "/root/app/dados_rfb_full_etl"
LOG_FILE   = "/root/motivo_import.log"

if not VPS_HOST or not ROOT_PASS:
    raise SystemExit("ERRO: VPS_HOST ou ROOT_PASS não definidos no .env")

# ── 1. Export local (pula se arquivo já existe) ───────────────────────────────
if os.path.isfile(LOCAL_GZ):
    size_mb = os.path.getsize(LOCAL_GZ) / 1024 / 1024
    print(f"[1/3] motivo_cnpj.csv.gz já existe ({size_mb:.1f} MB) — pulando export.")
else:
    print("[1/3] Gerando motivo_cnpj.csv.gz localmente...")
    script = pathlib.Path(__file__).parent / "export_motivo_local.py"
    result = subprocess.run([sys.executable, str(script)], check=True)
    if not os.path.isfile(LOCAL_GZ):
        raise SystemExit("ERRO: export não gerou o arquivo.")

size_mb = os.path.getsize(LOCAL_GZ) / 1024 / 1024
print(f"Arquivo: {LOCAL_GZ} ({size_mb:.1f} MB)\n")

# ── Conexão SSH ───────────────────────────────────────────────────────────────
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(VPS_HOST, username="root", password=ROOT_PASS, timeout=30)
print(f"SSH conectado: root@{VPS_HOST}")

# ── 2. Upload via SFTP ────────────────────────────────────────────────────────
print(f"[2/3] Enviando para {VPS_HOST}:{REMOTE_GZ}...")
sftp = ssh.open_sftp()

def _progress(sent, total):
    pct = sent / total * 100
    mb  = sent / 1024 / 1024
    print(f"\r  {mb:.1f}/{size_mb:.1f} MB ({pct:.0f}%)", end="", flush=True)

sftp.put(LOCAL_GZ, REMOTE_GZ, callback=_progress)
sftp.close()
print(f"\nUpload concluído.\n")

# ── 3. Roda import na VPS em background e exibe log ──────────────────────────
print(f"[3/3] Iniciando import na VPS...")

# Garante que o script está atualizado (git pull do submodule)
ssh.exec_command(f"cd {SCRIPT_DIR} && git checkout main && git pull origin main 2>&1")
time.sleep(2)

cmd = (
    f"cd {SCRIPT_DIR} && "
    f"nohup /root/venv-etl/bin/python3 -u code/import_motivo_vps.py {REMOTE_GZ} "
    f"> {LOG_FILE} 2>&1 & echo PID:$!"
)
_, stdout, _ = ssh.exec_command(cmd)
print(stdout.read().decode().strip())
time.sleep(1)

print(f"Log: {LOG_FILE}")
print("Ctrl+C para parar de acompanhar (o processo continua na VPS)\n")

# Stream do log
try:
    while True:
        _, out, _ = ssh.exec_command(f"tail -n 40 {LOG_FILE} 2>/dev/null")
        content = out.read().decode()
        if content:
            os.system("cls" if os.name == "nt" else "clear")
            print(content, end="", flush=True)
        # Verifica se terminou
        _, done, _ = ssh.exec_command(f"grep -c 'Concluído\\|ERRO' {LOG_FILE} 2>/dev/null")
        if done.read().decode().strip() not in ("", "0"):
            time.sleep(2)
            _, final, _ = ssh.exec_command(f"tail -n 20 {LOG_FILE}")
            print(final.read().decode())
            break
        time.sleep(5)
except KeyboardInterrupt:
    print(f"\nAcompanhamento interrompido. Para ver o progresso na VPS: tail -f {LOG_FILE}")

ssh.close()
print("Feito.")
