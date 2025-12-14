"""
API FastAPI para processar CSV de delivery_success.
Deploy gratuito: Railway, Render, Fly.io
"""

from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import tempfile
import logging
import uuid
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv
from supabase import create_client, Client

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

load_dotenv()

logger.info("=" * 60)
logger.info("Iniciando aplicação Delivery Success CSV Processor")
logger.info("=" * 60)

app = FastAPI(
    title="Delivery Success CSV Processor",
    description="API para processar CSV de delivery_success e calcular pagamentos",
    version="1.0.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuração Supabase
logger.info("[ETAPA 1] Carregando variáveis de ambiente...")
SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    logger.error("[ERRO] Variáveis de ambiente não configuradas")
    logger.error(f"SUPABASE_URL: {'Configurado' if SUPABASE_URL else 'NÃO CONFIGURADO'}")
    logger.error(f"SUPABASE_SERVICE_KEY: {'Configurado' if SUPABASE_SERVICE_KEY else 'NÃO CONFIGURADO'}")
    raise ValueError("Variáveis SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY devem estar configuradas")

logger.info(f"[ETAPA 2] Conectando ao Supabase... URL: {SUPABASE_URL[:30]}...")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
logger.info("[ETAPA 2] Conexão com Supabase estabelecida com sucesso")

# Importar função principal do script original
import sys
from pathlib import Path

logger.info("[ETAPA 3] Configurando imports...")
# Adicionar diretório atual ao path para importar
sys.path.insert(0, str(Path(__file__).parent))

# Importar função de processamento
logger.info("[ETAPA 4] Importando módulo process_delivery_csv...")
from process_delivery_csv import process_csv_file
logger.info("[ETAPA 4] Módulo importado com sucesso")
logger.info("[INICIALIZAÇÃO] Aplicação pronta para receber requisições")

# Armazenar status dos jobs em memória (em produção, use Redis ou banco de dados)
jobs_status: Dict[str, Dict[str, Any]] = {}

def process_file_background(job_id: str, tmp_path: str, filename: str):
    """Processa arquivo em background e atualiza status."""
    try:
        logger.info(f"[JOB {job_id}] Iniciando processamento em background...")
        jobs_status[job_id]["status"] = "processing"
        jobs_status[job_id]["message"] = "Processando arquivo..."
        
        # Processar arquivo
        result = process_csv_file(tmp_path)
        
        # Atualizar status com sucesso
        jobs_status[job_id]["status"] = "completed"
        jobs_status[job_id]["result"] = {
            "ok": result["ok"],
            "inserted_delivery_success": result["inserted_delivery_success"],
            "inserted_pagamento_delivery": result["inserted_pagamento_delivery"],
            "format_detected": result["format_detected"],
            "filename": filename,
        }
        jobs_status[job_id]["message"] = "Processamento concluído com sucesso"
        logger.info(f"[JOB {job_id}] Processamento concluído com sucesso")
        
    except Exception as e:
        logger.error(f"[JOB {job_id}] Erro no processamento: {str(e)}", exc_info=True)
        jobs_status[job_id]["status"] = "error"
        jobs_status[job_id]["message"] = f"Erro: {str(e)}"
        jobs_status[job_id]["error"] = str(e)
    finally:
        # Limpar arquivo temporário
        try:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
                logger.info(f"[JOB {job_id}] Arquivo temporário removido")
        except Exception as cleanup_error:
            logger.warning(f"[JOB {job_id}] Erro ao remover arquivo temporário: {cleanup_error}")

@app.get("/")
async def root():
    return {
        "message": "Delivery Success CSV Processor API",
        "version": "1.0.0",
        "endpoints": {
            "POST /process": "Processa arquivo CSV e calcula pagamentos (retorna job_id imediatamente)",
            "GET /process/{job_id}": "Verifica status do processamento",
            "GET /health": "Health check"
        }
    }

@app.get("/health")
async def health():
    logger.info("[HEALTH CHECK] Verificando saúde da aplicação...")
    status = {
        "status": "ok",
        "supabase_configured": bool(SUPABASE_URL and SUPABASE_SERVICE_KEY)
    }
    logger.info(f"[HEALTH CHECK] Status: {status}")
    return status

@app.post("/process")
async def process_csv(file: UploadFile = File(...), background_tasks: BackgroundTasks = BackgroundTasks()):
    """
    Processa arquivo CSV de delivery_success.
    
    Aceita arquivo CSV via multipart/form-data.
    Retorna job_id imediatamente. Use GET /process/{job_id} para verificar status.
    """
    logger.info("=" * 60)
    logger.info("[PROCESSAMENTO] Nova requisição recebida")
    logger.info(f"[PROCESSAMENTO] Arquivo: {file.filename}")
    logger.info(f"[PROCESSAMENTO] Content-Type: {file.content_type}")
    
    # Validar tipo de arquivo
    logger.info("[ETAPA 1/3] Validando tipo de arquivo...")
    if not file.filename or not file.filename.endswith('.csv'):
        logger.error("[ERRO] Arquivo não é CSV")
        raise HTTPException(status_code=400, detail="Arquivo deve ser CSV")
    logger.info("[ETAPA 1/3] Validação OK - arquivo é CSV")
    
    # Gerar job_id único
    job_id = str(uuid.uuid4())
    logger.info(f"[JOB {job_id}] Job criado")
    
    # Salvar arquivo temporariamente
    logger.info("[ETAPA 2/3] Salvando arquivo temporariamente...")
    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv') as tmp_file:
        content = await file.read()
        file_size = len(content)
        logger.info(f"[ETAPA 2/3] Arquivo recebido: {file_size:,} bytes ({file_size / 1024 / 1024:.2f} MB)")
        tmp_file.write(content)
        tmp_path = tmp_file.name
    
    logger.info(f"[ETAPA 2/3] Arquivo salvo em: {tmp_path}")
    
    # Inicializar status do job
    jobs_status[job_id] = {
        "status": "queued",
        "message": "Arquivo recebido, aguardando processamento...",
        "filename": file.filename,
        "file_size": file_size,
        "created_at": None,
        "result": None,
        "error": None
    }
    
    # Adicionar task em background
    logger.info("[ETAPA 3/3] Adicionando task em background...")
    background_tasks.add_task(process_file_background, job_id, tmp_path, file.filename)
    
    logger.info(f"[JOB {job_id}] Resposta enviada imediatamente")
    logger.info("=" * 60)
    
    # Retornar resposta imediata
    return {
        "job_id": job_id,
        "status": "queued",
        "message": "Arquivo recebido e processamento iniciado em background",
        "filename": file.filename,
        "file_size": file_size,
        "check_status_url": f"/process/{job_id}"
    }

@app.get("/process/{job_id}")
async def get_job_status(job_id: str):
    """
    Verifica status do processamento de um job.
    
    Status possíveis:
    - queued: Aguardando processamento
    - processing: Processando
    - completed: Concluído com sucesso
    - error: Erro no processamento
    """
    if job_id not in jobs_status:
        raise HTTPException(status_code=404, detail="Job não encontrado")
    
    job = jobs_status[job_id]
    return {
        "job_id": job_id,
        "status": job["status"],
        "message": job["message"],
        "filename": job.get("filename"),
        "file_size": job.get("file_size"),
        "result": job.get("result"),
        "error": job.get("error")
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    logger.info(f"[SERVER] Iniciando servidor na porta {port}...")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")

