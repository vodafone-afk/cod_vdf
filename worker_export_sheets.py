import os
import json
import time
from datetime import datetime
import mysql.connector
import gspread
from google.oauth2.service_account import Credentials

# ---------------- CONFIG ----------------
RC_TABLE = "RC"
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "30"))
SHEET_TAB = os.getenv("SHEET_TAB", "Perguntas")  # nome do separador no Sheets

# Ordem das colunas a enviar (igual ao teu schema)
RC_COLUMNS = [
    "ID","nome_completo","nif","tel_contacto","morada","cp7","morada_app","cp4_app","cp3_app","localidade_app",
    "email","pacote","tel_fixo","int_fixa","tel_1","tel_2","tel_3","tel_4","tar_1","tar_2","tar_3","tar_4",
    "FE","NTCB","IBAN","banco","SFID","nome","tipo_fatura","ze_sem_ze",
    "pm_op_1","pm_op_2","pm_op_3","pm_op_4",
    "pm_nome_1","pm_nome_2","pm_nome_3","pm_nome_4",
    "pm_nif_1","pm_nif_2","pm_nif_3","pm_nif_4",
    "pm_cvp_1","pm_cvp_2","pm_cvp_3","pm_cvp_4",
    "pm_kmat_1","pm_kmat_2","pm_kmat_3","pm_kmat_4",
    "pf_nome","pf_nif","pf_contacto","pf_operadora","pf_fixo","pf_cvp",
    "res_nome","res_morada","res_cp4","res_cp3","res_localidade","res_operadora","res_cliente","res_fixo",
    "res_movel_1","res_movel_2","res_movel_3","res_movel_4",
    "data_geracao","res_int_movel","res_srv_tv","res_srv_internet","res_srv_voz","res_srv_movel","res_srv_int_movel",
    "oferta_novo_cliente","plataforma_meses","oferta_extra","num_boxes_adicionais","valor_boxes_adicionais",
    "origem_venda","origem_venda_outra","cor","mac",
]

# ---------------- HELPERS ----------------
def env_required(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v

def mysql_connect():
    return mysql.connector.connect(
        host=env_required("DB_HOST"),
        user=env_required("DB_USER"),
        password=env_required("DB_PASS"),
        database=env_required("DB_NAME"),
        port=int(os.getenv("DB_PORT", "3306")),
        connection_timeout=10,
    )

def gspread_client():
    # service account JSON vem do secret e é escrito para ficheiro pelo workflow
    sa_path = env_required("GSERVICE_JSON_PATH")  # ex: service_account.json
    spreadsheet_id = env_required("SPREADSHEET_ID")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(spreadsheet_id)
    ws = sh.worksheet(SHEET_TAB)
    return ws

def ensure_header(ws):
    # Se a sheet estiver vazia, escreve o header
    try:
        first_row = ws.row_values(1)
    except Exception:
        first_row = []
    if not first_row:
        ws.append_row(RC_COLUMNS, value_input_option="RAW")

def fetch_pending(conn):
    cols_sql = ", ".join(f"`{c}`" for c in RC_COLUMNS)
    sql = f"""
        SELECT {cols_sql}
        FROM `{RC_TABLE}`
        WHERE `sheets_status`='PENDING'
        ORDER BY `ID` ASC
        LIMIT %s
    """
    cur = conn.cursor(dictionary=True)
    cur.execute(sql, (BATCH_SIZE,))
    rows = cur.fetchall()
    cur.close()
    return rows

def mark_status(conn, rc_id: int, status: str, err: str | None):
    cur = conn.cursor()
    cur.execute(
        f"""
        UPDATE `{RC_TABLE}`
        SET `sheets_status`=%s,
            `sheets_sent_at` = CASE WHEN %s='DONE' THEN NOW() ELSE `sheets_sent_at` END,
            `sheets_last_error`=%s
        WHERE `ID`=%s
        LIMIT 1
        """,
        (status, status, err, rc_id),
    )
    conn.commit()
    cur.close()

def to_row_values(r: dict):
    vals = []
    for c in RC_COLUMNS:
        v = r.get(c)
        # converter datetime/date para string legível
        if hasattr(v, "strftime"):
            v = v.strftime("%Y-%m-%d %H:%M:%S")
        vals.append("" if v is None else v)
    return vals

# ---------------- MAIN ----------------
def main():
    ws = gspread_client()
    ensure_header(ws)

    conn = mysql_connect()
    rows = fetch_pending(conn)

    if not rows:
        print("No pending RCs.")
        conn.close()
        return

    # Append em batch (mais rápido)
    values = [to_row_values(r) for r in rows]
    ws.append_rows(values, value_input_option="RAW")

    # Só depois de inserir, marcar DONE
    for r in rows:
        rc_id = int(r["ID"])
        mark_status(conn, rc_id, "DONE", None)

    conn.close()
    print(f"Exported {len(rows)} RC(s) to Sheets tab '{SHEET_TAB}'.")

if __name__ == "__main__":
    main()
