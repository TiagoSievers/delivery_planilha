#!/usr/bin/env python3
"""
Script Python para processar arquivos CSV de delivery_success localmente.
Processa o arquivo, insere em delivery_success e calcula pagamentos.
"""

import os
import sys
import csv
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv
from supabase import create_client, Client

# Carregar variáveis de ambiente (tenta .env, depois variáveis do sistema)
load_dotenv()

# ==== Configuração ====

SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL") or "https://hqkehwncoqcjplnvwzvz.supabase.co"
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imhxa2Vod25jb3FjanBsbnZ3enZ6Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MjQyNDA3NCwiZXhwIjoyMDc4MDAwMDc0fQ.yA4cu3oRFF36dVDLJGIiTOBQxscP7QE9RWkFeoYFXDQ"

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    print("[ERRO] Variáveis NEXT_PUBLIC_SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY devem estar configuradas")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# Colunas da tabela delivery_success (formato antigo - 36 colunas)
TARGET_COLS_OLD = [
    "data_entrega",
    "svc",
    "xpt",
    "mlp",
    "rotas",
    "ciclo_final",
    "clus",
    "driver",
    "placa",
    "id_veiculo",
    "veiculo",
    "hora_inicio",
    "hora_fim",
    "parada",
    "pacotes",
    "entregue",
    "insucessos",
    "ds",
    "orh_hours",
    "nao_visitado",
    "end_fechado",
    "cli_ausente",
    "mudou_se",
    "recusado",
    "avariado",
    "end_inacessivel",
    "falha",
    "roubado",
    "end_errado",
    "outros",
    "outros_insucessos_nao_mapeados",
    "at_station_problem_solving",
    "at_station",
    "at_station_aduana",
    "at_station_dev_buyer",
    "blocked_by_keyword",
]

# Mapeamento de meses em português
MONTHS_PT = {
    "jan": 1, "janeiro": 1,
    "fev": 2, "fevereiro": 2,
    "mar": 3, "março": 3, "marco": 3,
    "abr": 4, "abril": 4,
    "mai": 5, "maio": 5,
    "jun": 6, "junho": 6,
    "jul": 7, "julho": 7,
    "ago": 8, "agosto": 8,
    "set": 9, "setembro": 9,
    "out": 10, "outubro": 10,
    "nov": 11, "novembro": 11,
    "dez": 12, "dezembro": 12,
}

# ==== Helpers de parsing ====

def parse_number(value: Any) -> Optional[float]:
    """Converte valor para número float."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value) if value != float('inf') and value != float('-inf') else None
    if isinstance(value, str):
        cleaned = value.strip().replace(',', '.')
        cleaned = re.sub(r'[^0-9.-]', '', cleaned)
        if not cleaned or cleaned in ['-', '--', '.']:
            return 0.0
        try:
            parsed = float(cleaned)
            return parsed if parsed != float('inf') and parsed != float('-inf') else None
        except ValueError:
            return None
    return None

def parse_integer(value: Any) -> Optional[int]:
    """Converte valor para inteiro."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value) if value != float('inf') and value != float('-inf') else None
    if isinstance(value, str):
        numeric = re.sub(r'[^0-9-]', '', value)
        if not numeric or numeric == '-':
            return None
        try:
            return int(numeric)
        except ValueError:
            return None
    return None

def normalize_text(value: Any) -> Optional[str]:
    """Normaliza texto."""
    if value is None:
        return None
    if isinstance(value, str):
        trimmed = value.strip()
        return trimmed if trimmed else None
    return str(value)

def normalize_date(value: Any) -> Optional[str]:
    """Normaliza data para formato YYYY-MM-DD."""
    if not isinstance(value, str):
        return None
    
    trimmed = value.strip()
    if not trimmed:
        return None
    
    # Tentar parse direto (ISO, etc)
    try:
        parsed = datetime.fromisoformat(trimmed.replace('Z', '+00:00'))
        return parsed.strftime('%Y-%m-%d')
    except:
        pass
    
    # Formato português: "18 de jan. de 2025" ou "18 de janeiro de 2025"
    pt_match = re.match(r'(\d{1,2})\s+de\s+([a-zç]+)\.?\s*de\s+(\d{4})', trimmed, re.IGNORECASE)
    if pt_match:
        day, month_name, year = pt_match.groups()
        clean_month = month_name.lower().rstrip('.')
        month = MONTHS_PT.get(clean_month)
        if month:
            return f"{year}-{month:02d}-{int(day):02d}"
    
    # Formato DD/MM/YYYY ou DD-MM-YYYY
    parts = re.match(r'(\d{1,2})[\s\/\-](\d{1,2})[\s\/\-](\d{2,4})', trimmed)
    if parts:
        day, month, year = parts.groups()
        year_full = year.zfill(4)
        if len(year_full) == 2:
            year_full = f"20{year_full}"
        return f"{year_full}-{int(month):02d}-{int(day):02d}"
    
    return None

def detect_format(headers: List[str]) -> str:
    """Detecta se o CSV é formato antigo (36 colunas) ou novo (33 colunas)."""
    num_cols = len(headers)
    
    if num_cols == 33:
        has_cluster = any(h.lower().strip() == "cluster" for h in headers)
        has_ciclo = any(h.lower().strip() == "ciclo" and "final" not in h.lower() for h in headers)
        if has_cluster or has_ciclo:
            return "new"
    
    has_ciclo_final = any("ciclo_final" in h.lower() or "CICLO_FINAL" in h.upper() for h in headers)
    has_clus = any(h.lower().strip() == "clus" for h in headers)
    
    if has_ciclo_final or has_clus:
        return "old"
    
    return "old" if num_cols == 36 else "new"

def map_new_format_to_old(headers: List[str], values: List[str]) -> Dict[str, Any]:
    """Mapeia valores do formato novo (33 colunas) para formato antigo (36 colunas)."""
    result: Dict[str, Any] = {}
    header_map = {h.lower().strip(): i for i, h in enumerate(headers)}
    
    # Mapear colunas comuns diretamente
    common_mappings = {
        "data": "data_entrega",
        "svc": "svc",
        "xpt": "xpt",
        "mlp": "mlp",
        "rotas": "rotas",
        "driver": "driver",
        "placa": "placa",
        "id_veiculo": "id_veiculo",
        "veículo": "veiculo",
        "veiculo": "veiculo",
        "hora_inicio": "hora_inicio",
        "hora_fim": "hora_fim",
        "parada": "parada",
        "pacotes": "pacotes",
        "entregue": "entregue",
        "ds": "ds",
        "orh_hours": "orh_hours",
        "at_station": "at_station",
        "blocked_by_keyword": "blocked_by_keyword",
    }
    
    for new_key, old_key in common_mappings.items():
        idx = header_map.get(new_key)
        if idx is not None and idx < len(values):
            result[old_key] = values[idx].strip() if values[idx] else None
    
    # Mapear ciclo -> ciclo_final
    ciclo_idx = header_map.get("ciclo")
    if ciclo_idx is not None and ciclo_idx < len(values):
        result["ciclo_final"] = values[ciclo_idx].strip() if values[ciclo_idx] else None
    
    # Mapear cluster -> clus
    cluster_idx = header_map.get("cluster")
    if cluster_idx is not None and cluster_idx < len(values):
        result["clus"] = values[cluster_idx].strip() if values[cluster_idx] else None
    
    # Mapear Total de Insucessos
    total_insucessos_idx = header_map.get("total de insucessos") or header_map.get("total_insucessos")
    if total_insucessos_idx is not None and total_insucessos_idx < len(values):
        result["insucessos"] = values[total_insucessos_idx].strip() if values[total_insucessos_idx] else None
    
    # Consolidar insucessos do formato novo
    insucesso_fields = {
        "inaccessible_address": "end_inacessivel",
        "buyer_rejected": "recusado",
        "buyer_moved": "mudou_se",
        "buyer_absent": "cli_ausente",
        "business_closed": "end_fechado",
        "bad_address": "end_errado",
        "not_attempted": "nao_visitado",
    }
    
    for new_key, old_key in insucesso_fields.items():
        idx = header_map.get(new_key)
        if idx is not None and idx < len(values):
            value = parse_integer(values[idx])
            result[old_key] = value if value is not None else 0
        else:
            result[old_key] = 0
    
    # Outros motivos
    outros_idx = header_map.get("outros motivos") or header_map.get("outros_motivos")
    if outros_idx is not None and outros_idx < len(values):
        result["outros"] = values[outros_idx].strip() if values[outros_idx] else None
    else:
        result["outros"] = None
    
    # Colunas que não existem no formato novo
    result["nao_visitado"] = result.get("nao_visitado", 0)
    result["end_fechado"] = result.get("end_fechado", 0)
    result["cli_ausente"] = result.get("cli_ausente", 0)
    result["mudou_se"] = result.get("mudou_se", 0)
    result["recusado"] = result.get("recusado", 0)
    result["avariado"] = None
    result["end_inacessivel"] = result.get("end_inacessivel", 0)
    result["falha"] = None
    result["roubado"] = None
    result["end_errado"] = result.get("end_errado", 0)
    result["outros_insucessos_nao_mapeados"] = None
    result["at_station_problem_solving"] = None
    result["at_station_aduana"] = None
    result["at_station_dev_buyer"] = None
    
    return result

# ==== Helpers de cálculo de pagamentos ====

def determine_period(ciclo_final: Optional[str], hora_inicio: Optional[str], hora_fim: Optional[str]) -> str:
    """Determina período (AM/PM) baseado em ciclo_final ou hora."""
    if ciclo_final:
        label = ciclo_final.upper()
        if "AM" in label:
            return "AM"
        if "PM" in label:
            return "PM"
    
    def extract_hour(value: Optional[str]) -> Optional[int]:
        if not value:
            return None
        match = re.search(r'(\d{1,2})', value)
        if match:
            try:
                return int(match.group(1))
            except:
                return None
        return None
    
    hour = extract_hour(hora_inicio) or extract_hour(hora_fim)
    if hour is None:
        return "UNKNOWN"
    return "AM" if hour < 12 else "PM"

def normalize_region(svc: Optional[str]) -> str:
    """Normaliza região baseado em SVC."""
    if not svc:
        return "UNKNOWN"
    clean = svc.upper()
    if "CAP" in clean:
        return "CAPITAL"
    if "INT" in clean:
        return "INTERIOR"
    return "UNKNOWN"

def build_key(tipo_veiculo: Optional[str], apoio: Optional[str]) -> str:
    """Constrói chave para mapeamento de preços."""
    return f"{(tipo_veiculo or '').upper()}__{(apoio or '').upper()}"

DEFAULT_BONUS_PARADAS_80 = 20
DEFAULT_BONUS_PARADAS_110 = 40

def calculate_payments(supabase: Client, source_path: Optional[str] = None) -> int:
    """Calcula pagamentos baseado em delivery_success e valores_meli."""
    print("\n[PYTHON] Iniciando cálculo de pagamentos...")
    
    # Buscar todas as linhas de delivery_success
    print("[PYTHON] Buscando linhas de delivery_success...")
    delivery_rows = []
    page_size = 1000
    from_idx = 0
    
    while True:
        try:
            response = supabase.table("delivery_success").select("*").order("data_entrega").range(from_idx, from_idx + page_size - 1).execute()
            
            data = response.data if hasattr(response, 'data') else []
            if not data or len(data) == 0:
                break
            
            delivery_rows.extend(data)
            if len(data) < page_size:
                break
            
            from_idx += page_size
            print(f"[PYTHON] Carregadas {len(delivery_rows)} linhas...")
        except Exception as e:
            print(f"[PYTHON] Erro ao buscar delivery_success: {e}")
            raise
    
    print(f"[PYTHON] Total de linhas carregadas: {len(delivery_rows)}")
    
    # Buscar preços
    print("[PYTHON] Buscando preços de valores_meli...")
    pricing_rows = []
    from_idx = 0
    
    while True:
        try:
            response = supabase.table("valores_meli").select("*").order("tipo_de_veiculo").range(from_idx, from_idx + page_size - 1).execute()
            
            data = response.data if hasattr(response, 'data') else []
            if not data or len(data) == 0:
                break
            
            pricing_rows.extend(data)
            if len(data) < page_size:
                break
            
            from_idx += page_size
        except Exception as e:
            print(f"[PYTHON] Erro ao buscar valores_meli: {e}")
            raise
    
    print(f"[PYTHON] Total de preços carregados: {len(pricing_rows)}")
    
    # Criar mapa de preços
    pricing_map: Dict[str, Dict[str, Any]] = {}
    for pricing_row in pricing_rows:
        tipo_veiculo = normalize_text(pricing_row.get("tipo_de_veiculo"))
        apoio = normalize_text(pricing_row.get("apoio"))
        key = build_key(tipo_veiculo, apoio)
        pricing_map[key] = pricing_row
        
        if tipo_veiculo:
            pricing_map[build_key(tipo_veiculo, None)] = pricing_row
    
    # Calcular pagamentos
    print("[PYTHON] Calculando pagamentos...")
    batch_size = 200
    batched_payload: List[Dict[str, Any]] = []
    inserted = 0
    
    for index, raw in enumerate(delivery_rows):
        # Processar data_entrega
        data_entrega_raw = raw.get("data_entrega")
        if isinstance(data_entrega_raw, str):
            data_entrega = normalize_date(data_entrega_raw) or data_entrega_raw
        elif isinstance(data_entrega_raw, datetime):
            data_entrega = data_entrega_raw.strftime('%Y-%m-%d')
        else:
            data_entrega = data_entrega_raw
        
        svc = normalize_text(raw.get("svc"))
        tipo_regiao = normalize_region(svc)
        ciclo_final = normalize_text(raw.get("ciclo_final"))
        hora_inicio = normalize_text(raw.get("hora_inicio"))
        hora_fim = normalize_text(raw.get("hora_fim"))
        periodo = determine_period(ciclo_final, hora_inicio, hora_fim)
        tipo_veiculo = normalize_text(raw.get("veículo") or raw.get("veiculo"))
        apoio = normalize_text(raw.get("apoio") or raw.get("apoio_veiculo"))
        placa_veiculo = normalize_text(raw.get("placa"))
        driver_id = normalize_text(raw.get("driver"))
        paradas = parse_integer(raw.get("parada"))
        pacotes = parse_integer(raw.get("pacotes"))
        
        # Buscar preço
        pricing_row = pricing_map.get(build_key(tipo_veiculo, apoio)) or pricing_map.get(build_key(tipo_veiculo, None))
        
        tarifa_base_am = parse_number(pricing_row.get("tarifa_am") if pricing_row else None)
        tarifa_base_pm = parse_number(pricing_row.get("tarifa_pm") if pricing_row else None)
        
        tarifa_base: Optional[float] = None
        if periodo == "PM":
            tarifa_base = tarifa_base_pm or tarifa_base_am
        elif periodo == "AM":
            tarifa_base = tarifa_base_am or tarifa_base_pm
        else:
            tarifa_base = tarifa_base_am or tarifa_base_pm
        
        # Bonus paradas
        bonus_paradas = 0.0
        if isinstance(paradas, int):
            if paradas > 110:
                bonus_paradas = parse_number(pricing_row.get("acima_de_110") if pricing_row else None) or DEFAULT_BONUS_PARADAS_110
            elif paradas > 80:
                bonus_paradas = parse_number(pricing_row.get("acima_de_80") if pricing_row else None) or DEFAULT_BONUS_PARADAS_80
        
        # Bonus pacotes
        bonus_pacotes = 0.0
        bonus_60_90 = parse_number(pricing_row.get("c_60_90") if pricing_row else None)
        bonus_91_100 = parse_number(pricing_row.get("c_91_100") if pricing_row else None)
        bonus_gt_100 = parse_number(pricing_row.get("gt_100") if pricing_row else None)
        
        if isinstance(pacotes, int):
            if 60 <= pacotes <= 90:
                bonus_pacotes = bonus_60_90 or 0.0
            elif 91 <= pacotes <= 100:
                bonus_pacotes = bonus_91_100 or 0.0
            elif pacotes > 100:
                bonus_pacotes = bonus_gt_100 or 0.0
        
        adicional_km = parse_number(pricing_row.get("adicional_km") if pricing_row else None)
        bonus_sdd = parse_number(pricing_row.get("bonus_sdd") if pricing_row else None)
        outro_bonus = 0.0
        
        valor_total = (tarifa_base or 0.0) + bonus_paradas + bonus_pacotes + (adicional_km or 0.0) + (bonus_sdd or 0.0) + outro_bonus
        
        batched_payload.append({
            "source_path": source_path,
            "source_line": index + 1,
            "data_entrega": data_entrega,
            "svc": svc,
            "tipo_regiao": tipo_regiao,
            "tipo_periodo": periodo,
            "tipo_veiculo": tipo_veiculo,
            "apoio": apoio,
            "placa_veiculo": placa_veiculo,
            "driver_id": driver_id,
            "hora_inicio": hora_inicio,
            "hora_fim": hora_fim,
            "paradas": paradas,
            "pacotes": pacotes,
            "tarifa_base": tarifa_base,
            "bonus_paradas": bonus_paradas,
            "bonus_pacotes": bonus_pacotes,
            "adicional_km": adicional_km,
            "bonus_sdd": bonus_sdd,
            "outro_bonus": outro_bonus,
            "valor_total": valor_total,
            "observacoes": None,
            "raw_row": raw,
        })
        
        # Inserir em batches
        if len(batched_payload) >= batch_size:
            try:
                supabase.table("pagamento_delivery").insert(batched_payload).execute()
                inserted += len(batched_payload)
                print(f"[PYTHON] Inseridos {inserted} pagamentos...")
                batched_payload = []
            except Exception as e:
                print(f"[PYTHON] Erro ao inserir batch: {e}")
                raise
    
    # Inserir restante
    if batched_payload:
        try:
            supabase.table("pagamento_delivery").insert(batched_payload).execute()
            inserted += len(batched_payload)
        except Exception as e:
            print(f"[PYTHON] Erro ao inserir batch final: {e}")
            raise
    
    print(f"[PYTHON] Cálculo de pagamentos concluído: {inserted} linhas inseridas")
    return inserted

# ==== Função principal ====

def process_csv_file(csv_path: str) -> Dict[str, Any]:
    """Processa arquivo CSV e insere no Supabase."""
    print(f"\n[PYTHON] Processando arquivo: {csv_path}")
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {csv_path}")
    
    # Ler CSV
    print("[PYTHON] Lendo arquivo CSV...")
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        lines = [row for row in reader]
    
    if len(lines) < 2:
        raise ValueError("CSV precisa ter pelo menos header + 1 linha de dados")
    
    # Detectar formato
    headers = lines[0]
    format_type = detect_format(headers)
    print(f"[PYTHON] Formato detectado: {format_type}")
    print(f"[PYTHON] Número de colunas: {len(headers)}")
    
    data_lines = lines[1:]
    print(f"[PYTHON] Linhas de dados: {len(data_lines)}")
    
    # Processar linhas
    payload: List[Dict[str, Any]] = []
    
    for line_num, values in enumerate(data_lines, start=2):
        item: Dict[str, Any] = {}
        
        if format_type == "new":
            # Formato novo: mapear para formato antigo
            mapped = map_new_format_to_old(headers, values)
            
            # Converter data_entrega
            if mapped.get("data_entrega"):
                normalized_date = normalize_date(mapped["data_entrega"])
                if normalized_date:
                    item["data_entrega"] = normalized_date
                else:
                    if line_num <= 5:  # Só avisar nas primeiras linhas
                        print(f"[PYTHON] Aviso linha {line_num}: Não foi possível converter data: '{mapped['data_entrega']}'")
                    item["data_entrega"] = None
            else:
                item["data_entrega"] = None
            
            # Copiar outros campos
            for key in TARGET_COLS_OLD:
                if key != "data_entrega":
                    item[key] = mapped.get(key)
        else:
            # Formato antigo: processar normalmente
            for i, col_name in enumerate(TARGET_COLS_OLD):
                if i < len(values):
                    raw_value = values[i].strip() if values[i] else ""
                    
                    if col_name == "data_entrega":
                        normalized_date = normalize_date(raw_value)
                        if normalized_date:
                            item[col_name] = normalized_date
                        else:
                            if line_num <= 5:  # Só avisar nas primeiras linhas
                                print(f"[PYTHON] Aviso linha {line_num}: Não foi possível converter data: '{raw_value}'")
                            item[col_name] = None
                    else:
                        item[col_name] = raw_value if raw_value else None
                else:
                    item[col_name] = None
        
        if item:
            payload.append(item)
        
        if (line_num - 1) % 1000 == 0:
            print(f"[PYTHON] Processadas {line_num - 1} linhas...")
    
    print(f"[PYTHON] Total de linhas processadas: {len(payload)}")
    
    # Inserir em batches
    batch_size = 500
    inserted = 0
    
    print(f"[PYTHON] Inserindo em delivery_success (batch size: {batch_size})...")
    
    for i in range(0, len(payload), batch_size):
        chunk = payload[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (len(payload) + batch_size - 1) // batch_size
        
        try:
            supabase.table("delivery_success").insert(chunk).execute()
            inserted += len(chunk)
            print(f"[PYTHON] Batch {batch_num}/{total_batches} inserido ({inserted}/{len(payload)})")
        except Exception as e:
            print(f"[PYTHON] Erro ao inserir batch {batch_num}: {e}")
            print(f"[PYTHON] Detalhes do erro: {str(e)}")
            raise
    
    print(f"[PYTHON] Inserção concluída: {inserted} linhas em delivery_success")
    
    # Calcular pagamentos
    source_path = f"local/{os.path.basename(csv_path)}"
    payments_inserted = calculate_payments(supabase, source_path)
    
    return {
        "ok": True,
        "inserted_delivery_success": inserted,
        "inserted_pagamento_delivery": payments_inserted,
        "format_detected": format_type,
        "source_file": csv_path,
    }

# ==== Main ====

if __name__ == "__main__":
    # Arquivo padrão ou argumento
    default_file = "2.0 SRM - BSC Supplier_Delivery Success - DS_Tabela (3).csv"
    
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
    else:
        csv_file = default_file
    
    csv_path = Path(csv_file)
    if not csv_path.is_absolute():
        csv_path = Path(__file__).parent / csv_path
    
    try:
        result = process_csv_file(str(csv_path))
        print("\n[OK] Processamento concluído com sucesso!")
        print(f"   - Linhas inseridas em delivery_success: {result['inserted_delivery_success']}")
        print(f"   - Linhas inseridas em pagamento_delivery: {result['inserted_pagamento_delivery']}")
        print(f"   - Formato detectado: {result['format_detected']}")
    except Exception as e:
        print(f"\n[ERRO] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
