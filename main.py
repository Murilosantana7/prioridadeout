import pandas as pd
import gspread
import requests
import time
from datetime import datetime, timedelta
from pytz import timezone
import os
import json

# --- CONSTANTES ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
NOME_ABA = 'Base Pending Tratado'
INTERVALO = 'A:F'


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

    # FILTRO: todas as LTs que estão nas próximas 4h (0 a 240 minutos)
    df_filtrado = df[(df['minutos_restantes'] >= 0) & (df['minutos_restantes'] <= 240)]

    if df_filtrado.empty:
        return None

    mensagens = []

    # 👇👇👇 INCLUI A MENÇÃO VISUAL NO TOPO 👇👇👇
    mensagens.append("@Luis Tiberio | COP | SOC SP5")
    mensagens.append("")
    mensagens.append("🚨 ALERTA DE CPT IMINENTE")
    mensagens.append("📋 LISTA DE LTs NAS PRÓXIMAS 4H\n")

    # Ordena por CPT (mais cedo primeiro)
    df_filtrado = df_filtrado.sort_values('CPT')

    for _, row in df_filtrado.iterrows():
        lt = row['LH Trip Number'].strip()
        destino = row['Station Name'].strip()
        doca = formatar_doca(row['Doca'])
        cpt_str = row['CPT'].strftime('%H:%M')
        minutos = int(row['minutos_restantes'])

        if minutos <= 10:
            icone = "❗️"
        elif minutos <= 30:
            icone = "⚠️"
        else:
            icone = "✅"

        mensagens.append(f"{icone} {lt} | {doca} | Destino: {destino} | CPT: {cpt_str} | Faltam {minutos} min")

    return "\n".join(mensagens)


def enviar_webhook(mensagem_texto: str, webhook_url: str):
    """
    Envia mensagem simples de texto (formato 1) — mais compatível.
    """
    if not webhook_url:
        print("❌ WEBHOOK_URL não definida.")
        return

    payload = {
        "tag": "text",
        "text": {
            "format": 1,  # ← Simples e confiável
            "content": mensagem_texto
        }
    }

    try:
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
        print("✅ Mensagem enviada com sucesso.")
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
        enviar_webhook(mensagem, webhook_url)
    else:
        print("✅ Nenhuma LT nas próximas 4h. Nada enviado.")


if __name__ == "__main__":
    main()
