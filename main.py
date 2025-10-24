import pandas as pd
import gspread
import requests
from datetime import datetime, timedelta, time as dt_time # Renomeei 'time' para 'dt_time'
import re
import time
from google.oauth2 import service_account

# --- Constantes do Script 1 ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1nMLHR6Xp5xzQjlhwXufecG1INSQS4KrHn41kqjV9Rmk'
NOME_ABA = 'Tabela dinâmica 2'
WEBHOOK_URL = "https://openapi.seatalk.io/webhook/group/ATSiL-5DRiGnHdV0t2XLlg"

# --- Constante de Autenticação (do Modelo do Script 2) ---
# Use o nome do seu arquivo JSON. 'credentials.json' era o nome do seu script original.
SERVICE_ACCOUNT_FILE = 'credentials.json' 

# --- Função de Espera (do Modelo do Script 2) ---
def aguardar_horario_correto():
    """
    Verifica se é hora cheia (XX:00) ou meia hora (XX:30).
    Se não for, aguarda 30 segundos e verifica novamente.
    """
    while True:
        agora = datetime.now()
        minutos_atuais = agora.minute
        
        # Verifica se é hora cheia (00) ou meia hora (30)
        if minutos_atuais == 0 or minutos_atuais == 30:
            print(f"✅ Horário correto detectado: {agora.strftime('%H:%M:%S')}")
            print("Iniciando execução do código...")
            break
        else:
            # Calcula quanto tempo falta para o próximo horário válido
            if minutos_atuais < 30:
                minutos_faltando = 30 - minutos_atuais
                proximo_horario = f"{agora.hour:02d}:30"
            else:
                minutos_faltando = 60 - minutos_atuais
                proxima_hora = (agora.hour + 1) % 24
                proximo_horario = f"{proxima_hora:02d}:00"
            
            print(f"⏳ Horário atual: {agora.strftime('%H:%M:%S')}")
            print(f"   Aguardando até {proximo_horario} (faltam {minutos_faltando} minutos)")
            print(f"   Próxima verificação em 30 segundos...")
            
            # Aguarda 30 segundos antes de verificar novamente
            time.sleep(30)

# --- Função de Autenticação (Modificada para o Modelo do Script 2) ---
def autenticar():
    creds = None
    try:
        # Carrega as credenciais diretamente do arquivo JSON
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    except FileNotFoundError:
        print(f"Erro: O arquivo de conta de serviço '{SERVICE_ACCOUNT_FILE}' não foi encontrado.")
        print("Por favor, baixe o arquivo JSON e coloque na mesma pasta do script.")
        return None
    except Exception as e:
        print(f"Erro ao carregar credenciais: {e}")
        return None
    
    print("✅ Autenticação com Google Service Account bem-sucedida.")
    return creds

# --- Funções Originais do Script 1 (Sem Alteração) ---

def enviar_webhook(mensagem_txt):
    """Envia a mensagem de texto formatada para o Seatalk."""
    try:
        payload = {
            "tag": "text",
            "text": {
                "format": 1,
                "content": f"```\n{mensagem_txt}\n```"
            }
        }
        response = requests.post(WEBHOOK_URL, json=payload)
        response.raise_for_status()
        print("✅ Mensagem enviada com sucesso para o Seatalk.")
    except requests.exceptions.RequestException as err:
        print(f"❌ Erro ao enviar mensagem para o webhook: {err}")

def minutos_para_hhmm(minutos):
    horas = minutos // 60
    mins = minutos % 60
    return f"{horas:02d}:{mins:02d}h"

def turno_atual():
    agora = datetime.now().time()
    if agora >= datetime.strptime("06:00", "%H:%M").time() and agora < datetime.strptime("14:00", "%H:%M").time():
        return "T1"
    elif agora >= datetime.strptime("14:00", "%H:%M").time() and agora < datetime.strptime("22:00", "%H:%M").time():
        return "T2"
    else:
        return "T3"

def ordenar_turnos(pendentes_por_turno):
    ordem_turnos = ['T1', 'T2', 'T3']
    t_atual = turno_atual()
    idx = ordem_turnos.index(t_atual)
    nova_ordem = ordem_turnos[idx:] + ordem_turnos[:idx]
    
    turnos_existentes = {k: v for k, v in pendentes_por_turno.items() if k in nova_ordem}
    
    return sorted(turnos_existentes.items(), key=lambda x: nova_ordem.index(x[0]))

def periodo_dia_customizado(agora):
    hoje = agora.date()
    inicio_dia = datetime.combine(hoje, dt_time(6, 0)) # Usando dt_time aqui
    if agora < inicio_dia:
        inicio_dia -= timedelta(days=1)
    fim_dia = inicio_dia + timedelta(days=1) - timedelta(seconds=1)
    return inicio_dia, fim_dia

def padronizar_doca(doca_str):
    match = re.search(r'(\d+)$', doca_str)
    if match:
        return match.group(1)
    else:
        return "--"

def main():
    print("🔄 Iniciando script...")
    creds = autenticar()
    
    # Adicionando verificação de credenciais (boa prática do script 2)
    if not creds:
        print("Encerrando script devido a falha na autenticação.")
        # Tenta enviar um aviso, embora possa falhar se a rede estiver fora
        try:
            enviar_webhook("Falha na autenticação do Google. Verifique o arquivo .json e as permissões da planilha.")
        except:
            pass
        return

    cliente = gspread.authorize(creds)
    planilha = cliente.open_by_key(SPREADSHEET_ID)
    aba = planilha.worksheet(NOME_ABA)
    valores = aba.get_all_values()

    df = pd.DataFrame(valores[1:], columns=valores[0])
    df.columns = df.columns.str.strip()
    
    try:
        # A=0, B=1, D=3, F=5, AC=28
        header_eta_planejado = valores[0][1].strip() # Coluna B
        header_origem = valores[0][28].strip()       # Coluna AC
        header_chegada_lt = valores[0][3].strip()    # Coluna D
        NOME_COLUNA_PACOTES = valores[0][5].strip()  # Coluna F
        
    except IndexError as e:
        print(f"❌ Erro: A planilha não tem colunas suficientes (pelo menos até a Coluna AC). Detalhe: {e}")
        enviar_webhook(f"Erro no script: A planilha não tem colunas suficientes (pelo menos até a Coluna AC).")
        return
        
    print(f"INFO: Usando Coluna B ('{header_eta_planejado}') para ETA.")
    print(f"INFO: Usando Coluna AC ('{header_origem}') para Origem.")
    print(f"INFO: Usando Coluna D ('{header_chegada_lt}') para Chegada LT.")
    print(f"INFO: Usando Coluna F ('{NOME_COLUNA_PACOTES}') para Pacotes.")
    
    required_cols = [
        'LH Trip Nnumber', 'Satus 2.0', 'Add to Queue Time', 'Doca', 'Turno 2', 
        header_eta_planejado,  # Col B
        header_origem,         # Col AC
        header_chegada_lt,     # Col D
        NOME_COLUNA_PACOTES    # Col F
    ]
    
    for col in required_cols:
        if col not in df.columns:
            if col == 'ETA Planejado' and header_eta_planejado != col:
                 df.rename(columns={header_eta_planejado: 'ETA Planejado'}, inplace=True)
                 print(f"AVISO: Renomeando '{header_eta_planejado}' para 'ETA Planejado' internamente.")
                 continue 
                 
            print(f"❌ Coluna obrigatória '{col}' não encontrada no DataFrame.")
            print(f"   Colunas encontradas: {list(df.columns)}")
            enviar_webhook(f"Erro no script: Coluna obrigatória '{col}' não foi encontrada na planilha.")
            return
            
    if header_eta_planejado != 'ETA Planejado':
        df.rename(columns={header_eta_planejado: 'ETA Planejado'}, inplace=True)
        
    # --- Conversão de Tipos ---
    df['LH Trip Nnumber'] = df['LH Trip Nnumber'].astype(str).str.strip()
    df['Satus 2.0'] = df['Satus 2.0'].astype(str).str.strip()
    df['Doca'] = df['Doca'].astype(str).str.strip()
    df['Turno 2'] = df['Turno 2'].astype(str).str.strip()
    df[header_origem] = df[header_origem].astype(str).str.strip() 
    
    df['Add to Queue Time'] = pd.to_datetime(df['Add to Queue Time'], errors='coerce') 
    df['ETA Planejado'] = pd.to_datetime(df['ETA Planejado'], format='%d/%m/%Y %H:%M', errors='coerce')
    df[header_chegada_lt] = pd.to_datetime(df[header_chegada_lt], format='%d/%m/%Y %H:%M', errors='coerce')
    
    df[NOME_COLUNA_PACOTES] = pd.to_numeric(df[NOME_COLUNA_PACOTES], errors='coerce').fillna(0).astype(int)

    df['Satus 2.0'] = df['Satus 2.0'].replace({
        'Pendente Recepção': 'pendente recepção',
        'Pendente De Chegada': 'pendente de chegada'
    })

    df = df[~df['Satus 2.0'].str.lower().str.contains('finalizado', na=False)]

    agora = datetime.now()
    inicio_dia, fim_dia = periodo_dia_customizado(agora)

    print(f"Intervalo considerado para pendentes: {inicio_dia} até {fim_dia}")

    em_doca = []
    em_fila = []
    pendentes_por_turno = {} 
    pendentes_status = ['pendente de chegada', 'pendente recepção']

    for _, row in df.iterrows():
        trip = row['LH Trip Nnumber']
        status = str(row['Satus 2.0']).strip().lower()
        origem = row[header_origem] if pd.notna(row[header_origem]) and row[header_origem].strip() != '' else '--'
        pacotes = row[NOME_COLUNA_PACOTES]
        
        # --- Lógica para Pendentes ---
        eta_pendente = row['ETA Planejado'] # Esta é a coluna B
        turno = row['Turno 2']

        if status in pendentes_status:
            if pd.notna(eta_pendente):
                if inicio_dia <= eta_pendente <= fim_dia:
                    if turno not in pendentes_por_turno:
                        pendentes_por_turno[turno] = {'lts': 0, 'pacotes': 0}
                    
                    pendentes_por_turno[turno]['lts'] += 1
                    pendentes_por_turno[turno]['pacotes'] += pacotes 
            
        # --- Lógica para Em Doca / Em Fila ---
        entrada_cd = row['Add to Queue Time']
        doca = row['Doca'] if pd.notna(row['Doca']) and row['Doca'].strip() != '' else '--'
        
        eta_planejado_val = row['ETA Planejado']    # Coluna B
        chegada_lt_val = row[header_chegada_lt]     # Coluna D

        eta_str = eta_planejado_val.strftime('%d/%m %H:%M') if pd.notna(eta_planejado_val) else '--/-- --:--'
        chegada_str = chegada_lt_val.strftime('%d/%m %H:%M') if pd.notna(chegada_lt_val) else '--/-- --:--'
        
        minutos = None
        if pd.notna(entrada_cd):
            minutos = int((agora - entrada_cd).total_seconds() / 60)

        if status == 'em doca' and minutos is not None:
            doca_formatada = padronizar_doca(doca)
            msg_doca = f"- {trip}  |  Doca: {doca_formatada}  |  ETA: {eta_str}  |  Chegada: {chegada_str}  |  Tempo CD: {minutos_para_hhmm(minutos)}  |  {origem}"
            em_doca.append((minutos, msg_doca))
            
        elif 'fila' in status and minutos is not None:
            msg_fila = f"- {trip}  |  ETA: {eta_str}  |  Chegada: {chegada_str}  |  Tempo CD: {minutos_para_hhmm(minutos)}  |  {origem}"
            em_fila.append((minutos, msg_fila))

    em_doca.sort(key=lambda x: x[0], reverse=True)
    em_fila.sort(key=lambda x: x[0], reverse=True)

    mensagem = []

    if em_doca:
        total_em_doca = len(em_doca) 
        mensagem.append(f"🚛 Em Doca: {total_em_doca} LT(s)\n" + "\n".join([x[1] for x in em_doca]))
    if em_fila:
        total_em_fila = len(em_fila) 
        mensagem.append(f"🔴 Em Fila: {total_em_fila} LT(s)\n" + "\n".join([x[1] for x in em_fila]))

    total_lts_pendentes = sum(dados['lts'] for dados in pendentes_por_turno.values())
    total_pacotes_pendentes = sum(dados['pacotes'] for dados in pendentes_por_turno.values())

    if total_lts_pendentes > 0:
        mensagem.append(f"⏳ Pendentes para chegar: {total_lts_pendentes} LT(s) ({total_pacotes_pendentes} pacotes)")
        
        for turno, dados in ordenar_turnos(pendentes_por_turno):
            lts = dados['lts']
            pacotes = dados['pacotes']
            
            mensagem.append(f"- {lts} LTs ({pacotes} pacotes) no {turno}")
            
    else:
        # Só adiciona a mensagem de "Nenhuma pendência" se não houver NADA (nem em doca, nem em fila)
        if not em_doca and not em_fila:
             mensagem.append("✅ Nenhuma pendência no momento.")
        # Se tiver algo em doca/fila, mas 0 pendentes, não polui a msg.

    # Evita enviar mensagem vazia
    if not mensagem:
        print("ℹ️ Nenhuma LT em doca, em fila ou pendente. Nenhuma mensagem será enviada.")
        return

    mensagem_final = "\n\n".join(mensagem)
    print("📤 Enviando mensagem formatada...")
    enviar_webhook("Segue as LH´s com mais tempo de Pátio:\n\n" + mensagem_final)


if __name__ == '__main__':
    # 1. Aguarda o horário correto (do modelo 2)
    aguardar_horario_correto()
    
    # 2. Roda a lógica principal (do script 1)
    try:
        main()
    except Exception as e:
        print(f"❌ Ocorreu um erro inesperado na função main: {e}")
        try:
            # Tenta enviar o erro
            enviar_webhook(f"Ocorreu um erro crítico no script de monitoramento de LTs:\n\n{e}")
        except:
            print("❌ Falha ao enviar a mensagem de erro para o webhook.")
    
    print(f"Execução finalizada às {datetime.now().strftime('%H:%M:%S')}.")
