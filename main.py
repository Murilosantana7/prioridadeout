import pandas as pd
import gspread
# A linha 'Credentials' não é mais necessária
import requests
import time
from datetime import datetime, timedelta
from pytz import timezone
import os  # Importado para ler os segredos
import json # Importado para ler o segredo JSON

# --- CONSTANTES GLOBAIS ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
# O SPREADSHEET_ID e WEBHOOK_URL serão lidos dos segredos
NOME_ABA = 'Base Pending Tratado'
INTERVALO = 'A:F'
# SERVICE_ACCOUNT_FILE foi removido

# --- AUTENTICAÇÃO NOVA (DO SCRIPT NOVO) ---
def autenticar_google():
    """Autentica usando o Secret do GitHub e já retorna o CLIENTE gspread."""
    creds_json_str = os.environ.get('GCP_SA_KEY_JSON')
    if not creds_json_str:
        print("❌ Erro: Variável de ambiente 'GCP_SA_KEY_JSON' não definida.")
        return None
    try:
        creds_dict = json.loads(creds_json_str)
        # gspread.service_account_from_dict lida com as 'creds' e autoriza
        cliente = gspread.service_account_from_dict(creds_dict, scopes=SCOPES)
        print("✅ Cliente gspread autenticado com Service Account.")
        return cliente
    except Exception as e:
        print(f"❌ Erro ao autenticar com Service Account: {e}")
        return None

def identificar_turno(hora):
    if 6 <= hora < 14:
        return "Turno 1"
    elif 14 <= hora < 22:
        return "Turno 2"
    else:
        return "Turno 3"

# --- MODIFICADO ---
def obter_dados_expedicao(cliente, spreadsheet_id):
    # Não precisa mais de 'creds', já recebe o 'cliente' pronto
    if not cliente:
        return None, "⚠️ Não foi possível autenticar o cliente."

    try:
        # Abre a planilha pelo ID lido dos segredos
        planilha = cliente.open_by_key(spreadsheet_id)
        aba = planilha.worksheet(NOME_ABA)
        dados = aba.get(INTERVALO)
    except Exception as e:
        return None, f"⚠️ Erro ao acessar planilha: {e}"

    if not dados or len(dados) < 2:
        return None, "⚠️ Nenhum dado encontrado na planilha."

    df = pd.DataFrame(dados[1:], columns=dados[0])
    df.columns = df.columns.str.strip()

    for col in ['Doca', 'LH Trip Number', 'Station Name', 'CPT']:
        if col not in df.columns:
            return None, f"⚠️ Coluna '{col}' não encontrada."

    df = df[df['LH Trip Number'].str.strip() != '']
    df['CPT'] = pd.to_datetime(df['CPT'], dayfirst=True, errors='coerce')
    df = df.dropna(subset=['CPT'])
    df['Turno'] = df['CPT'].dt.hour.apply(identificar_turno)

    return df, None

def formatar_doca(doca):
    doca = doca.strip()
    if not doca or doca == '-':
        return "Doca --"
    elif doca.startswith("EXT.OUT"):
        numeros = ''.join(filter(str.isdigit, doca))
        return f"Doca {numeros}"
    elif not doca.startswith("Doca"):
        return f"Doca {doca}"
    else:
        return doca

def montar_mensagem(df):
    agora = datetime.now(timezone('America/Sao_Paulo')).replace(tzinfo=None)
    limite_2h = agora + timedelta(hours=2)
    turno_atual = identificar_turno(agora.hour)

    mensagens = []
    totais = df['Turno'].value_counts().to_dict()

    df_2h = df[(df['CPT'] >= agora) & (df['CPT'] < limite_2h)].copy()
    if df_2h.empty:
        mensagens.append("🚛 LTs pendentes:\n\n✅ Sem LT pendente para as próximas 2h.\n")
    else:
        mensagens.append("🚛 LTs pendentes:\n")
        df_2h['Hora'] = df_2h['CPT'].dt.hour

        for hora, grupo in df_2h.groupby('Hora', sort=True):
            qtd_lhs = len(grupo)
            mensagens.append(f"{qtd_lhs} LH{'s' if qtd_lhs > 1 else ''} pendente{'s' if qtd_lhs > 1 else ''} às {hora:02d}h\n")
            for _, row in grupo.iterrows():
                lt = row['LH Trip Number'].strip()
                destino = row['Station Name'].strip()
                cpt = row['CPT']
                cpt_str = cpt.strftime('%H:%M')
                doca = formatar_doca(row['Doca'])

                minutos = int((cpt - agora).total_seconds() // 60)
                if minutos < 0:
                    prefixo = "❗️"
                    status = "(ATRASADO)"
                elif minutos <= 10:
                    prefixo = "⚠️"
                    status = f"(FALTAM {minutos} MIN)"
                else:
                    prefixo = ""
                    status = ""

                mensagens.append(f"{prefixo} {lt} | {doca} | Destino: {destino} | CPT: {cpt_str} {status}".strip())
            mensagens.append("")

    mensagens.append("─" * 40)
    mensagens.append("LH´s pendentes para os próximos turnos:\n")

    prioridades_turno = {
        'Turno 1': ['Turno 2', 'Turno 3'],
        'Turno 2': ['Turno 3', 'Turno 1'],
        'Turno 3': ['Turno 1', 'Turno 2']
    }

    for turno in prioridades_turno.get(turno_atual, []):
        qtd = totais.get(turno, 0)
        mensagens.append(f"⚠️ {qtd} LH{'s' if qtd != 1 else ''} pendente{'s' if qtd != 1 else ''} no {turno}")

    return "\n".join(mensagens)

# --- MODIFICADO ---
def enviar_webhook(mensagem, webhook_url):
    # A URL é recebida como parâmetro
    if not webhook_url:
        print("❌ Erro: WEBHOOK_URL não fornecida.")
        return
    try:
        payload = {
            "tag": "text",
            "text": {
                "format": 1,
                "content": mensagem
            }
        }
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        print("✅ Mensagem enviada com sucesso.")
    except Exception as e:
        print(f"❌ Erro ao enviar mensagem: {e}")

# --- MODIFICADO ---
def enviar_em_blocos(mensagem, webhook_url, limite=3000):
    # Repassa a webhook_url
    linhas = mensagem.split('\n')
    bloco = []
    for linha in linhas:
        bloco.append(linha)
        if len("\n".join(bloco)) > limite:
            bloco.pop()
            enviar_webhook("```\n" + "\n".join(bloco) + "\n```", webhook_url) # Passa a URL
            time.sleep(1)
            bloco = [linha]
    if bloco:
        enviar_webhook("```\n" + "\n".join(bloco) + "\n```", webhook_url) # Passa a URL

# --- MODIFICADO ---
def main():
    # Carrega as variáveis de ambiente fornecidas pelo GitHub Actions
    webhook_url = os.environ.get('SEATALK_WEBHOOK_URL')
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')
    
    # Validação para garantir que os segredos foram carregados
    if not webhook_url or not spreadsheet_id:
        print("❌ Erro: Variáveis de ambiente SEATALK_WEBHOOK_URL e/ou SPREADSHEET_ID não definidas.")
        print("Verifique os 'Secrets' do repositório no GitHub.")
        return

    # Autentica primeiro para obter o cliente
    cliente = autenticar_google()
    if not cliente:
        print("❌ Falha na autenticação. Encerrando.")
        return

    # Passa o cliente e o ID da planilha
    df, erro = obter_dados_expedicao(cliente, spreadsheet_id)
    if erro:
        print(erro)
        return
    
    mensagem = montar_mensagem(df)
    
    # Passa a webhook_url para o envio
    enviar_em_blocos(mensagem, webhook_url)

if __name__ == "__main__":
    main()
