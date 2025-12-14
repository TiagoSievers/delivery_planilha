"""
API FastAPI para processar CSV de delivery_success.
Deploy gratuito: Railway, Render, Fly.io
"""

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import tempfile
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

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
SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL") or os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    raise ValueError("Variáveis SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY devem estar configuradas")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# Importar função principal do script original
import sys
from pathlib import Path

# Adicionar diretório atual ao path para importar
sys.path.insert(0, str(Path(__file__).parent))

# Importar função de processamento
from process_delivery_csv import process_csv_file

@app.get("/")
async def root():
    return {
        "message": "Delivery Success CSV Processor API",
        "version": "1.0.0",
        "endpoints": {
            "POST /process": "Processa arquivo CSV e calcula pagamentos",
            "GET /health": "Health check"
        }
    }

@app.get("/health")
async def health():
    return {"status": "ok", "supabase_configured": bool(SUPABASE_URL and SUPABASE_SERVICE_KEY)}

@app.post("/process")
async def process_csv(file: UploadFile = File(...)):
    """
    Processa arquivo CSV de delivery_success.
    
    Aceita arquivo CSV via multipart/form-data.
    Retorna estatísticas do processamento.
    """
    try:
        # Validar tipo de arquivo
        if not file.filename or not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="Arquivo deve ser CSV")
        
        # Salvar arquivo temporariamente
        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv') as tmp_file:
            content = await file.read()
            tmp_file.write(content)
            tmp_path = tmp_file.name
        
        try:
            # Processar usando função do script original
            result = process_csv_file(tmp_path)
            
            return {
                "ok": result["ok"],
                "inserted_delivery_success": result["inserted_delivery_success"],
                "inserted_pagamento_delivery": result["inserted_pagamento_delivery"],
                "format_detected": result["format_detected"],
                "filename": file.filename,
            }
        finally:
            # Limpar arquivo temporário
            try:
                os.unlink(tmp_path)
            except:
                pass
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao processar arquivo: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

