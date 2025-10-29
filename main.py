import pandas as pd
import gspread
import requests  # ← Voltamos ao requests (já instalado!)
import time
from datetime import datetime, timedelta
from pytz import timezone
import os
import json

# --- CONSTANTES ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
NOME_ABA = 'Base Pending Tratado'
INTERVALO = 'A:F'
MINUTOS_ALERTA = [30, 20, 10]
USER_ID_LUIS = "1508081817"  # ← ID DO LUIS TIBÉRIO


def autenticar_google():
    creds_json_str = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON')
    if not creds_json_str:
        print("❌ Erro: Variável de ambiente 'GOOGLE_SERVICE_ACCOUNT_JSON' não definida.")
        return None

    try:
        creds_dict = json.loads(creds_json_str)
        cliente = gspread.service_account_from_dict(creds_dict, scopes=SCOPES)
        print("✅ Cliente autenticado.")
        return cliente
    except Exception as e:
        print(f"❌ Erro ao autenticar: {e}")
        return None


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


def obter_dados_expedicao(cliente, spreadsheet_id):
    if not cliente:
        return None, "⚠️ Cliente não autenticado."

    try:
        planilha = cliente.open_by_key(spreadsheet_id)
        aba = planilha.worksheet(NOME_ABA)
        dados = aba.get(INTERVALO)
    except Exception as e:
        return None, f"⚠️ Erro ao acessar planilha: {e}"

    if not dados or len(dados) < 2:
        return None, "⚠️ Nenhum dado encontrado."

    df = pd.DataFrame(dados[1:], columns=dados[0])
    df.columns = df.columns.str.strip()

    for col in ['Doca', 'LH Trip Number', 'Station Name', 'CPT']:
        if col not in df.columns:
            return None, f"⚠️ Coluna '{col}' não encontrada."

    df = df[df['LH Trip Number'].str.strip() != '']
    df['CPT'] = pd.to_datetime(df['CPT'], dayfirst=True, errors='coerce')
    df = df.dropna(subset=['CPT'])

    return df, None


def montar_mensagem_alerta(df):
    agora = datetime.now(timezone('America/Sao_Paulo')).replace(tzinfo=None)

    df = df.copy()
    df['minutos_restantes'] = ((df['CPT'] - agora).dt.total_seconds() // 60).astype(int)
    df_filtrado = df[df['minutos_restantes'].isin(MINUTOS_ALERTA)]

    if df_filtrado.empty:
        return None

    mensagens = []
    for minuto in sorted(MINUTOS_ALERTA, reverse=True):
        grupo = df_filtrado[df_filtrado['minutos_restantes'] == minuto]
        if not grupo.empty:
            mensagens.append(f"⚠️ Atenção!!!")
            mensagens.append(f"{minuto}min para o CPT.\n")
            for _, row in grupo.iterrows():
                lt = row['LH Trip Number'].strip()
                destino = row['Station Name'].strip()
                doca = formatar_doca(row['Doca'])
                cpt_str = row['CPT'].strftime('%H:%M')
                mensagens.append(f"🚛 {lt} | {doca} | Destino: {destino} | CPT: {cpt_str}")
            mensagens.append("")

    if mensagens and mensagens[-1] == "":
        mensagens.pop()

    return "\n".join(mensagens)


def enviar_webhook_com_mencao(mensagem_texto: str, webhook_url: str, user_id: str = "1508081817"):
    """
    Envia mensagem com menção REAL pelo ID, usando requests (SEM httpx).
    """
    if not webhook_url:
        print("❌ WEBHOOK_URL não definida.")
        return

    # Texto base com tag <at>
    texto_base = "🚨 ALERTA DE CPT IMINENTE<at id=\"{user_id}\"></at>"
    mensagem_completa = f"{texto_base}\n\n{mensagem_texto}"
    offset = len("🚨 ALERTA DE CPT IMINENTE")  # Calcula posição da tag

    payload = {
        "tag": "text",
        "text": {
            "format": 2,  # ← OBRIGATÓRIO para marcações
            "content": mensagem_completa.format(user_id=user_id),
            "at_list": [
                {
                    "id": user_id,
                    "len": 0,
                    "offset": offset
                }
            ]
        }
    }

    try:
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        print("✅ Mensagem com menção REAL enviada com sucesso.")
    except Exception as e:
        print(f"❌ Falha ao enviar mensagem: {e}")


def main():
    webhook_url = os.environ.get('SEATALK_WEBHOOK_URL')
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')

    if not webhook_url or not spreadsheet_id:
        print("❌ Variáveis SEATALK_WEBHOOK_URL ou SPREADSHEET_ID não definidas.")
        return

    cliente = autenticar_google()
    if not cliente:
        return

    df, erro = obter_dados_expedicao(cliente, spreadsheet_id)
    if erro:
        print(erro)
        return

    mensagem = montar_mensagem_alerta(df)

    if mensagem:
        enviar_webhook_com_mencao(mensagem, webhook_url, USER_ID_LUIS)
    else:
        print("✅ Nenhuma LT nos critérios de alerta. Nada enviado.")


if __name__ == "__main__":
    main()
