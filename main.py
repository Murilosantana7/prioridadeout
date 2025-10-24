# -*- coding: utf-8 -*-
import pandas as pd
import gspread
import requests
from datetime import datetime, timedelta, time as dt_time
import re
import time
import os
import json

# --- Constantes do Script ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1nMLHR6Xp5xzQjlhwXufecG1INSQS4KrHn41kqjV9Rmk'
NOME_ABA = 'Tabela dinâmica 2'

# --- (FUNÇÃO 'aguardar_horario_correto' FOI REMOVIDA) ---

# --- Função de Autenticação (CORRIGIDA) ---
def autenticar_e_criar_cliente():
    """
    Autentica usando o Secret do GitHub e já retorna o CLIENTE gspread.
    """
    
    creds_json_str = os.environ.get('GCP_SA_KEY_JSON')
    
    if not creds_json_str:
        print("❌ Erro: Variável de ambiente 'GCP_SA_KEY_JSON' não definida.")
        return None
    
    try:
        creds_dict = json.loads(creds_json_str)
        # --- MUDANÇA AQUI ---
        # Esta função agora retorna o *cliente* pronto para usar.
        cliente = gspread.service_account_from_dict(creds_dict, scopes=SCOPES)
        print("✅ Cliente gspread autenticado com Service Account.")
        return cliente # Retorna o cliente
    except Exception as e:
        print(f"❌ Erro ao autenticar com Service Account: {e}")
        return None

# --- Função de Webhook (Sem alteração) ---
def enviar_webhook(mensagem_txt):
    """Envia a mensagem de texto lendo a URL do Secret do GitHub."""
    webhook_url = os.environ.get('SEATALK_WEBHOOK_URL') 
    if not webhook_url:
        print("❌ Erro: Variável 'SEATALK_WEBHOOK_URL' não definida.")
        return
    try:
        payload = {
            "tag": "text",
            "text": { "format": 1, "content": f"```\n{mensagem_txt}\n```" }
        }
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        print("✅ Mensagem enviada com sucesso para o Seatalk.")
    except requests.exceptions.RequestException as err:
        print(f"❌ Erro ao enviar mensagem para o webhook: {err}")

# --- Funções Originais do Script (Sem Alteração) ---
def minutos_para_hhmm(minutos):
    horas = minutos // 60
    mins = minutos % 60
    return f"{horas:02d}:{mins:02d}h"

def turno_atual():
    agora = datetime.utcnow().time()
    if agora >= dt_time(6, 0) and agora < dt_time(14, 0): return "T1"
    elif agora >= dt_time(14, 0) and agora < dt_time(22, 0): return "T2"
    else: return "T3"

def ordenar_turnos(pendentes_por_turno):
    ordem_turnos = ['T1', 'T2', 'T3']
    t_atual = turno_atual()
    idx = ordem_turnos.index(t_atual)
    nova_ordem = ordem_turnos[idx:] + ordem_turnos[:idx]
    turnos_existentes = {k: v for k, v in pendentes_por_turno.items() if k in nova_ordem}
    return sorted(turnos_existentes.items(), key=lambda x: nova_ordem.index(x[0]))

def periodo_dia_customizado(agora_utc):
    hoje = agora_utc.date()
    inicio_dia = datetime.combine(hoje, dt_time(6, 0))
    if agora_utc < inicio_dia:
        inicio_dia -= timedelta(days=1)
    fim_dia = inicio_dia + timedelta(days=1) - timedelta(seconds=1)
    return inicio_dia, fim_dia

def padronizar_doca(doca_str):
    match = re.search(r'(\d+)$', doca_str)
    return match.group(1) if match else "--"

# --- Função Principal (CORRIGIDA) ---
def main():
    print(f"🔄 Iniciando script às {datetime.utcnow().strftime('%H:%M:%S')} UTC...")
    
    # --- MUDANÇA AQUI ---
    # Chamamos a nova função. 'cliente' agora é o cliente, não as credenciais.
    cliente = autenticar_e_criar_cliente()
    
    if not cliente: # Se a autenticação falhou
        print("Encerrando script devido a falha na autenticação.")
        enviar_webhook("Falha na autenticação do Google. Verifique o Secret 'GCP_SA_KEY_JSON' e as permissões da planilha.")
        return

    try:
        # --- MUDANÇA AQUI ---
        # A linha 'gspread.authorize()' foi REMOVIDA.
        # Usamos o 'cliente' diretamente.
        planilha = cliente.open_by_key(SPREADSHEET_ID)
        aba = planilha.worksheet(NOME_ABA)
        valores = aba.get_all_values()
    except gspread.exceptions.APIError as e:
        # Erro mais detalhado para permissões
        try:
            error_json = json.loads(e.response.text)
            error_message = error_json.get('error', {}).get('message', str(e))
        except:
            error_message = str(e)
        print(f"❌ Erro na API do Google: {error_message}")
        enviar_webhook(f"Erro na API do Google: {error_message}.\n\nVerifique se a conta de serviço (o email ...@gserviceaccount.com) tem permissão de 'Editor' na planilha.")
        return
    except Exception as e:
        print(f"❌ Erro ao abrir planilha: {e}")
        enviar_webhook(f"Erro ao abrir planilha: {e}")
        return

    # --- O RESTO DO SCRIPT CONTINUA IGUAL ---
    
    df = pd.DataFrame(valores[1:], columns=valores[0])
    df.columns = df.columns.str.strip()
    
    try:
        header_eta_planejado = valores[0][1].strip() # Coluna B
        header_origem = valores[0][28].strip()       # Coluna AC
        header_chegada_lt = valores[0][3].strip()    # Coluna D
        NOME_COLUNA_PACOTES = valores[0][5].strip()  # Coluna F
    except IndexError as e:
        print(f"❌ Erro: A planilha não tem colunas suficientes. Detalhe: {e}")
        enviar_webhook(f"Erro no script: A planilha não tem colunas suficientes.")
        return
        
    print("INFO: Colunas de dados localizadas.")
    
    required_cols = [
        'LH Trip Nnumber', 'Satus 2.0', 'Add to Queue Time', 'Doca', 'Turno 2', 
        header_eta_planejado, header_origem, header_chegada_lt, NOME_COLUNA_PACOTES
    ]
    
    for col in required_cols:
        if col not in df.columns:
            if col == 'ETA Planejado' and header_eta_planejado != col:
                 df.rename(columns={header_eta_planejado: 'ETA Planejado'}, inplace=True)
                 continue 
            print(f"❌ Coluna obrigatória '{col}' não encontrada no DataFrame.")
            enviar_webhook(f"Erro no script: Coluna obrigatória '{col}' não foi encontrada.")
            return
            
    if header_eta_planejado != 'ETA Planejado':
        df.rename(columns={header_eta_planejado: 'ETA Planejado'}, inplace=True)
        
    df['LH Trip Nnumber'] = df['LH Trip Nnumber'].astype(str).str.strip()
    df['Satus 2.0'] = df['Satus 2.0'].astype(str).str.strip()
    df['Doca'] = df['Doca'].astype(str).str.strip()
    df['Turno 2'] = df['Turno 2'].astype(str).str.strip()
    df[header_origem] = df[header_origem].astype(str).str.strip() 
    
    df['Add to Queue Time'] = pd.to_datetime(df['Add to Queue Time'], errors='coerce') 
    df['ETA Planejado'] = pd.to_datetime(df['ETA Planejado'], format='%d/%m/%Y %H:%M', errors='coerce')
    df[header_chegada_lt] = pd.to_datetime(df[header_chegada_lt], format='%d/%m/%Y %H:%M', errors='coerce')
    df[NOME_COLUNA_PACOTES] = pd.to_numeric(df[NOME_COLUNA_PACOTES], errors='coerce').fillna(0).astype(int)
    df['Satus 2.0'] = df['Satus 2.0'].replace({'Pendente Recepção': 'pendente recepção', 'Pendente De Chegada': 'pendente de chegada'})
    df = df[~df['Satus 2.0'].str.lower().str.contains('finalizado', na=False)]

    agora_utc = datetime.utcnow()
    inicio_dia, fim_dia = periodo_dia_customizado(agora_utc)
    print(f"Intervalo considerado para pendentes (UTC): {inicio_dia} até {fim_dia}")

    em_doca, em_fila, pendentes_por_turno = [], [], {}
    pendentes_status = ['pendente de chegada', 'pendente recepção']

    for _, row in df.iterrows():
        trip, status = row['LH Trip Nnumber'], str(row['Satus 2.0']).strip().lower()
        origem = row[header_origem] if pd.notna(row[header_origem]) and row[header_origem].strip() != '' else '--'
        pacotes = row[NOME_COLUNA_PACOTES]
        eta_pendente, turno = row['ETA Planejado'], row['Turno 2']

        if status in pendentes_status and pd.notna(eta_pendente) and inicio_dia <= eta_pendente <= fim_dia:
            if turno not in pendentes_por_turno:
                pendentes_por_turno[turno] = {'lts': 0, 'pacotes': 0}
            pendentes_por_turno[turno]['lts'] += 1
            pendentes_por_turno[turno]['pacotes'] += pacotes 
            
        entrada_cd, doca = row['Add to Queue Time'], row['Doca'] if pd.notna(row['Doca']) and row['Doca'].strip() != '' else '--'
        eta_planejado_val, chegada_lt_val = row['ETA Planejado'], row[header_chegada_lt]
        eta_str = eta_planejado_val.strftime('%d/%m %H:%M') if pd.notna(eta_planejado_val) else '--/-- --:--'
        chegada_str = chegada_lt_val.strftime('%d/%m %H:%M') if pd.notna(chegada_lt_val) else '--/-- --:--'
        
        minutos = None
        if pd.notna(entrada_cd):
            minutos = int((agora_utc - entrada_cd).total_seconds() / 60)

        if status == 'em doca' and minutos is not None:
            msg_doca = f"- {trip}  |  Doca: {padronizar_doca(doca)}  |  ETA: {eta_str}  |  Chegada: {chegada_str}  |  Tempo CD: {minutos_para_hhmm(minutos)}  |  {origem}"
            em_doca.append((minutos, msg_doca))
        elif 'fila' in status and minutos is not None:
            msg_fila = f"- {trip}  |  ETA: {eta_str}  |  Chegada: {chegada_str}  |  Tempo CD: {minutos_para_hhmm(minutos)}  |  {origem}"
            em_fila.append((minutos, msg_fila))

    em_doca.sort(key=lambda x: x[0], reverse=True)
    em_fila.sort(key=lambda x: x[0], reverse=True)
    mensagem = []

    if em_doca:
        mensagem.append(f"🚛 Em Doca: {len(em_doca)} LT(s)\n" + "\n".join([x[1] for x in em_doca]))
    if em_fila:
        mensagem.append(f"🔴 Em Fila: {len(em_fila)} LT(s)\n" + "\n".join([x[1] for x in em_fila]))

    total_lts_pendentes = sum(d['lts'] for d in pendentes_por_turno.values())
    total_pacotes_pendentes = sum(d['pacotes'] for d in pendentes_por_turno.values())

    if total_lts_pendentes > 0:
        mensagem.append(f"⏳ Pendentes para chegar: {total_lts_pendentes} LT(s) ({total_pacotes_pendentes} pacotes)")
        for turno, dados in ordenar_turnos(pendentes_por_turno):
            mensagem.append(f"- {dados['lts']} LTs ({dados['pacotes']} pacotes) no {turno}")
    elif not em_doca and not em_fila:
        mensagem.append("✅ Nenhuma pendência no momento.")

    if not mensagem:
        print("ℹ️ Nenhuma LT em doca, em fila ou pendente. Nenhuma mensagem será enviada.")
        return

    mensagem_final = "\n\n".join(mensagem)
    print("📤 Enviando mensagem formatada...")
    enviar_webhook("Segue as LH´s com mais tempo de Pátio:\n\n" + mensagem_final)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"❌ Ocorreu um erro inesperado na função main: {e}")
        try:
            enviar_webhook(f"Ocorreu um erro crítico no script de monitoramento de LTs:\n\n{e}")
        except:
            print("❌ Falha ao enviar a mensagem de erro para o webhook.")
    
    print(f"Execução finalizada às {datetime.utcnow().strftime('%H:%M:%S')} UTC.")
