r"""
 ___      ___  ________   ________   ________   ________  ________   ________    _______      
|\  \    /  /||\   __  \ |\   ___ \ |\   __  \ |\  _____\|\   __  \ |\   ___  \ |\  ___ \     
\ \  \  /  / /\ \  \|\  \\ \  \_|\ \\ \  \|\  \\ \  \__/ \ \  \|\  \\ \  \\ \  \\ \   __/|    
 \ \  \/  / /  \ \  \\\  \\ \  \ \\ \\ \   __  \\ \   __\ \ \  \\\  \\ \  \\ \  \\ \  \_|/__  
  \ \    / /    \ \  \\\  \\ \  \_\\ \\ \  \ \  \\ \  \_|  \ \  \\\  \\ \  \\ \  \\ \  \_|\ \ 
   \ \__/ /      \ \_______\\ \_______\\ \__\ \__\\ \__\    \ \_______\\ \__\\ \__\\ \_______|
    \|__|/        \|_______| \|_______| \|__|\|__| \|__|     \|_______| \|__| \|__| \|_______|
                                                                                              
                                                                                                                                                                                          

Desenvolvido por : David Pereira
© 2026 Vodafone – Todos os direitos reservados.
Uso interno. Reprodução ou distribuição não autorizada.

############################################################################################################
------------------------------------------------------------------------------------------------------------
############################################################################################################

OBS :
    - FA 
    - PORTABILIDADE MOVEL
    - PORTABILIDADE FIXA
    - ALTEERAÇAO TITULAR
    - RESCISAO (FEITO)

TO DO LIST (V 1.0.0)

    - FAZER COM QUE A PORTABILIDADE MOVEL GERE VARIOS PDFS MOVEIS DE ACORDO COM A PESSOA OU OPERADORA
    - ADICIONAR COLUNAS A TABELA RC
    - METER TODOS OS DADOS PARA A TABELA RC DEPOIS DE GERAR O PDF
    - COPIAR OS DADOS NO BOTAO DE "Editar" DA TABELA RC PARA OS CAMPOS DO FORMULARIO

TO DO LIST (V 1.1.0)

    - REMOVER ALGUNS BUGS DESECESSARIOS E DEIXAR O FORMULARIO MAIS CLEAN
    - FAZER UM FORUM

"""


import tkinter as tk
from tkinter import messagebox, ttk
import fitz  # PyMuPDFC
import os
import datetime
import ttkbootstrap as ttkb
import mysql.connector
import subprocess
import time
import re
import threading
import urllib.request
import urllib.error

import json
from typing import List, Tuple, Optional

# ====================== GOOGLE SHEETS (EXPORT PERGUNTAS) ======================
# Nota: requer "pip install google-api-python-client google-auth"
try:
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
except Exception:
    Credentials = None
    build = None


# ====================== RESTART APP (após registo) ======================
def restart_app():
    """Reinicia o programa (útil após registo) abrindo uma nova instância e terminando a atual.
    Funciona tanto em .py (python.exe) como em .exe (PyInstaller), usando sys.executable.
    """
    import sys, os
    try:
        args = [sys.executable] + sys.argv
        subprocess.Popen(args, close_fds=True)
    finally:
        os._exit(0)

# ====================== MYSQL CONFIG ======================
DB_CONFIG = {
    "host": "sql7.freesqldatabase.com",
    "user": "sql7813184",
    "password": "bF9SQDhXW5",
    "database": "sql7813184",
    "port": 3306
}



# ====================== EURUS (AI via servidor/Tailscale) ======================
# Dica: coloca a URL do servidor EURUS (com Tailscale) numa variável de ambiente EURUS_BASE_URL,
# por exemplo: EURUS_BASE_URL="http://100.x.y.z:8000"
EURUS_BASE_URL = os.environ.get("EURUS_BASE_URL", "http://127.0.0.1:8000")
EURUS_API_TOKEN = os.environ.get("EURUS_API_TOKEN", "")  # opcional (se ativares X-API-KEY no servidor)
EURUS_TIMEOUT = 25  # segundos


# ====================== CONTROLO DE EDIÇÃO (RC) ======================
# Quando o utilizador clica em "Editar" num contrato na aba "Contratos Gerados",
# guardamos aqui o ID para que o próximo "GERAR" faça UPDATE pelo ID (e não pelo NIF).
CURRENT_RC_ID = None

# ====================== UTILIZADOR ATUAL (Vendedor) ======================
# Antes existia persistência local via pasta/ficheiro 'dont_touch_me'.
# Como agora o acesso é controlado via BD (UUID + tabela vendedores),
# guardamos o vendedor autenticado em memória para ser usado no resto da app.
CURRENT_VENDEDOR_NOME = ''
CURRENT_VENDEDOR_SFID = ''


CURRENT_VENDEDOR_MAC = 0  # 1 = utilizador Mac, 0 = não-Mac

# ====================== CORES OFICIAIS VODAFONE ======================
VODA_RED = "#E60000"
VODA_DARK = "#121212"
VODA_LIGHT_GRAY = "#F5F5F5"
VODA_WHITE = "#FFFFFF"
VODA_GRAY = "#333333"
VODA_HIGHLIGHT = "#FFFF00"

# ====================== MYSQL FUNÇÕES ======================
def ligar_db():
    return mysql.connector.connect(**DB_CONFIG, use_pure=True) # importante para correr o .exe


def ensure_vendedores_mac_column(conn):
    """Garante que a tabela `vendedores` tem a coluna `mac` (TINYINT)."""
    try:
        cur = conn.cursor()
        # adicionar coluna se não existir
        cur.execute("SHOW COLUMNS FROM vendedores LIKE 'mac'")
        if not cur.fetchone():
            try:
                cur.execute("ALTER TABLE vendedores ADD COLUMN mac TINYINT NULL DEFAULT 0")
                conn.commit()
            except Exception:
                # corrida paralela / já existe
                try:
                    conn.rollback()
                except Exception:
                    pass
        cur.close()
    except Exception:
        pass



# ====================== MELHORIAS (TABELA + INSERT) ======================
def ensure_melhorias_table(conn):
    """Garante que a tabela `melhorias` existe e tem as colunas necessárias.

    Colunas:
      - id (PK)
      - sfid
      - titulo
      - texto
      - resposta (texto do BackOffice)
      - visualizacao (0/1: se o vendedor já visualizou a resposta)
    """
    cur = conn.cursor()
    # 1) Criar tabela (schema novo)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS melhorias (
            id INT AUTO_INCREMENT PRIMARY KEY,
            sfid VARCHAR(200) NOT NULL,
            titulo VARCHAR(200) NOT NULL,
            texto TEXT NOT NULL,
            resposta TEXT NULL,
            visualizacao TINYINT(1) NOT NULL DEFAULT 0
        )
        """
    )
    conn.commit()

    # 2) Migrar schema antigo (caso já exista sem as novas colunas)
    def _ensure_col(col_name: str, col_sql: str):
        try:
            cur.execute("SHOW COLUMNS FROM melhorias LIKE %s", (col_name,))
            exists = cur.fetchone()
            if not exists:
                cur.execute(f"ALTER TABLE melhorias ADD COLUMN {col_name} {col_sql}")
                conn.commit()
        except Exception:
            # Se falhar (permissões/versão), não quebra a app.
            try:
                conn.rollback()
            except Exception:
                pass

    _ensure_col("resposta", "TEXT NULL")
    _ensure_col("visualizacao", "TINYINT(1) NOT NULL DEFAULT 0")

    cur.close()


def inserir_melhoria(sfid: str, titulo: str, texto: str):
    """Insere uma sugestão na tabela `melhorias`."""
    conn = ligar_db()
    try:
        ensure_melhorias_table(conn)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO melhorias (sfid, titulo, texto) VALUES (%s, %s, %s)",
            (sfid, titulo, texto),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()



def listar_melhorias_por_sfid(sfid: str):
    """Lista melhorias de um SFID (mais recentes primeiro)."""
    conn = ligar_db()
    try:
        ensure_melhorias_table(conn)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, sfid, titulo, texto, resposta, visualizacao
            FROM melhorias
            WHERE sfid = %s
            ORDER BY id DESC
            """,
            (sfid,),
        )
        rows = cur.fetchall() or []
        cur.close()
        return rows
    finally:
        conn.close()


def contar_respostas_nao_vistas(sfid: str) -> int:
    """Conta respostas existentes mas ainda não visualizadas (visualizacao=0)."""
    conn = ligar_db()
    try:
        ensure_melhorias_table(conn)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*)
            FROM melhorias
            WHERE sfid = %s
              AND resposta IS NOT NULL
              AND TRIM(resposta) <> ''
              AND (visualizacao IS NULL OR visualizacao = 0)
            """,
            (sfid,),
        )
        n = cur.fetchone()
        cur.close()
        return int(n[0] or 0) if n else 0
    finally:
        conn.close()


def marcar_melhoria_como_visualizada(melhoria_id: int, sfid: str):
    """Marca a melhoria como visualizada (visualizacao=1)."""
    conn = ligar_db()
    try:
        ensure_melhorias_table(conn)
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE melhorias
               SET visualizacao = 1
             WHERE id = %s AND sfid = %s
            """,
            (int(melhoria_id), str(sfid)),
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()



# ====================== RC SCHEMA + UPSERT ======================

RC_REQUIRED_COLUMNS = {
    # Morada de instalação (APP)
    "morada_app": "VARCHAR(255) NULL",
    "cp4_app": "VARCHAR(10) NULL",
    "cp3_app": "VARCHAR(10) NULL",
    "localidade_app": "VARCHAR(120) NULL",

    # Portabilidade Móvel (por linha 1..4)
    "pm_op_1": "VARCHAR(40) NULL",
    "pm_op_2": "VARCHAR(40) NULL",
    "pm_op_3": "VARCHAR(40) NULL",
    "pm_op_4": "VARCHAR(40) NULL",

    "pm_nome_1": "VARCHAR(255) NULL",
    "pm_nome_2": "VARCHAR(255) NULL",
    "pm_nome_3": "VARCHAR(255) NULL",
    "pm_nome_4": "VARCHAR(255) NULL",

    "pm_nif_1": "VARCHAR(32) NULL",
    "pm_nif_2": "VARCHAR(32) NULL",
    "pm_nif_3": "VARCHAR(32) NULL",
    "pm_nif_4": "VARCHAR(32) NULL",

    "pm_cvp_1": "VARCHAR(64) NULL",
    "pm_cvp_2": "VARCHAR(64) NULL",
    "pm_cvp_3": "VARCHAR(64) NULL",
    "pm_cvp_4": "VARCHAR(64) NULL",

    "pm_kmat_1": "VARCHAR(64) NULL",
    "pm_kmat_2": "VARCHAR(64) NULL",
    "pm_kmat_3": "VARCHAR(64) NULL",
    "pm_kmat_4": "VARCHAR(64) NULL",

    # Portabilidade Fixa (completa)
    "pf_nome": "VARCHAR(255) NULL",
    "pf_nif": "VARCHAR(32) NULL",
    "pf_contacto": "VARCHAR(32) NULL",
    "pf_operadora": "VARCHAR(40) NULL",
    "pf_fixo": "VARCHAR(32) NULL",
    "pf_cvp": "VARCHAR(64) NULL",

    # Rescisão (completa)
    "res_nome": "VARCHAR(255) NULL",
    "res_morada": "VARCHAR(255) NULL",
    "res_cp4": "VARCHAR(10) NULL",
    "res_cp3": "VARCHAR(10) NULL",
    "res_localidade": "VARCHAR(120) NULL",
    "res_operadora": "VARCHAR(40) NULL",
    "res_cliente": "VARCHAR(64) NULL",
    "res_fixo": "VARCHAR(32) NULL",
    "res_movel_1": "VARCHAR(32) NULL",
    "res_movel_2": "VARCHAR(32) NULL",
    "res_movel_3": "VARCHAR(32) NULL",
    "res_movel_4": "VARCHAR(32) NULL",
    "res_int_movel": "VARCHAR(32) NULL",
    "res_srv_tv": "TINYINT(1) NULL",
    "res_srv_internet": "TINYINT(1) NULL",
    "res_srv_voz": "TINYINT(1) NULL",
    "res_srv_movel": "TINYINT(1) NULL",
    "res_srv_int_movel": "TINYINT(1) NULL",
    # Perguntas obrigatórias (Supervisor)
    "oferta_novo_cliente": "VARCHAR(255) NULL",
    "plataforma_meses": "VARCHAR(255) NULL",
    "oferta_extra": "VARCHAR(500) NULL",
    "num_boxes_adicionais": "INT NULL",
    "valor_boxes_adicionais": "VARCHAR(120) NULL",
    "origem_venda": "VARCHAR(255) NULL",
    "origem_venda_outra": "VARCHAR(255) NULL",
    "data_geracao": "DATE NULL",
    "mac": "TINYINT NULL DEFAULT 0",
    "sheets_status": "TINYINT NULL DEFAULT 0",
    "sheets_sent_at": "DATETIME NULL",
    "sheets_last_error": "TEXT NULL",
    "sheets_lock_at": "DATETIME NULL",
    "edited": "TINYINT NULL DEFAULT 0",

    # Débito Direto
    "NTCB": "VARCHAR(255) NULL",
    "banco": "VARCHAR(255) NULL",
    "PR": "TINYINT(1) NOT NULL DEFAULT 0",

    # Comercial (override para PDF)
    "SFID_COMERCIAL": "VARCHAR(200) NULL",
    "NOME_COMERCIAL": "VARCHAR(255) NULL",

    # ==========================================
    # ADICIONA ESTAS DUAS LINHAS AQUI:
    "ID_RC": "VARCHAR(100) NULL",
    "NUM_COMERCIAL": "VARCHAR(100) NULL",
    # ==========================================

}

def ensure_rc_columns(conn):
    """Garante que a tabela RC tem todas as colunas necessárias (adiciona as que faltarem)."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'RC'
        """,
        (DB_CONFIG["database"],),
    )
    existentes = {row[0] for row in cur.fetchall()}
    for col, ddl in RC_REQUIRED_COLUMNS.items():
        if col not in existentes:
            try:
                cur.execute(f"ALTER TABLE RC ADD COLUMN {col} {ddl}")
            except Exception:
                # Se já existir por corrida paralela, ignora
                pass
    conn.commit()
    cur.close()




def notificar_discord(uuid_pc, nome_vendedor):
    """Avisa no Discord sempre que um assistente entra na app."""
    # SUBSTITUI O LINK ABAIXO PELO URL DO TEU WEBHOOK DO DISCORD
    webhook_url = "https://discord.com/api/webhooks/1361381347071102986/Ps_Hmly2htbbRzm9Vy1Mx4yDc3BV4mn6locYKJHVai1PsffCrNhF1eF8NpmPIpS78DqI"
    
    if not webhook_url or webhook_url == "COLA_AQUI_O_TEU_LINK_DO_WEBHOOK":
        return

    # A mensagem exata que pediste!
    mensagem = {
        "content": f"🟢 **UUID : {uuid_pc}** (`{nome_vendedor}`) entrou na app."
    }
    
    try:
        req = urllib.request.Request(webhook_url, method="POST")
        req.add_header('Content-Type', 'application/json; charset=utf-8')
        req.add_header('User-Agent', 'VodafoneApp/1.0')
        urllib.request.urlopen(req, data=json.dumps(mensagem).encode('utf-8'), timeout=3)
    except Exception:
        # Se falhar (ex: sem net no segundo exato), ignora para não crashar a app deles
        pass

def _rc_row_from_form(dados: dict, nome_vendedor: str, sfid: str) -> dict:
    """Mapeia os dados do formulário para colunas RC (inclui campos novos)."""
    morada = f'{dados.get("rua_faturacao","")}, {dados.get("localidade_faturacao","")}'.strip().strip(",")
    cp7 = f'{dados.get("cp4_faturacao","")}-{dados.get("cp3_faturacao","")}'.strip("-")

    row = {
        "nome_completo": dados.get("nome_completo", ""),
        "nif": dados.get("nif", ""),
        "tel_contacto": dados.get("contacto", ""),
        "morada": morada,
        "cp7": cp7,
        "email": dados.get("email", ""),
        "pacote": dados.get("plano_valor", ""),
        "tel_fixo": dados.get("fixo", ""),
        "int_fixa": dados.get("velocidade_internet", ""),
        "data_geracao": datetime.date.today().isoformat(),

        # Perguntas obrigatórias (Supervisor)
        "oferta_novo_cliente": dados.get("oferta_novo_cliente", ""),
        "plataforma_meses": dados.get("plataforma_meses", ""),
        "oferta_extra": dados.get("oferta_extra", ""),
        "num_boxes_adicionais": int(dados.get("num_boxes_adicionais", 0) or 0),
        "valor_boxes_adicionais": dados.get("valor_boxes_adicionais", ""),
        "origem_venda": dados.get("origem_venda", ""),
        "origem_venda_outra": dados.get("origem_venda_outra", ""),


        "tel_1": dados.get("telemovel1", ""),
        "tel_2": dados.get("telemovel2", ""),
        "tel_3": dados.get("telemovel3", ""),
        "tel_4": dados.get("telemovel4", ""),

        "tar_1": dados.get("gigas_min_telemovel1", ""),
        "tar_2": dados.get("gigas_min_telemovel2", ""),
        "tar_3": dados.get("gigas_min_telemovel3", ""),
        "tar_4": dados.get("gigas_min_telemovel4", ""),

        "FE": int(bool(dados.get("fatura_eletronica", 0))),
        "NTCB": dados.get("ntcb", ""),
        "IBAN": dados.get("iban", ""),
        "banco": dados.get("banco_nome", ""),
        "PR": int(bool(dados.get("pagamento_recorrente", 0))),

        "SFID": sfid,
        "SFID_COMERCIAL": dados.get("sfid_comercial", ""),
        "NOME_COMERCIAL": dados.get("nome_comercial", ""),

        # ==========================================
        # ADICIONA ESTAS DUAS LINHAS AQUI:
        "ID_RC": dados.get("num_rc", ""),
        "NUM_COMERCIAL": dados.get("tel_vendedor", ""),
        # ==========================================

        "mac": int(dados.get("mac", globals().get("CURRENT_VENDEDOR_MAC", 0)) or 0),
        "sheets_status": int(dados.get("sheets_status", 0) or 0),
        "edited": int(dados.get("edited", 0) or 0),
        "sheets_sent_at": dados.get("sheets_sent_at", None),
        "sheets_last_error": dados.get("sheets_last_error", None),
        "nome": nome_vendedor,
        "tipo_fatura": dados.get("fatura_tipo", ""),
        "ze_sem_ze": int(bool(dados.get("ze_sem_ze", 0))),

        # Instalação (APP)
        "morada_app": dados.get("rua_instalacao", ""),
        "cp4_app": dados.get("cp4_instalacao", ""),
        "cp3_app": dados.get("cp3_instalacao", ""),
        "localidade_app": dados.get("localidade_instalacao", ""),

        # Portabilidade fixa
        "pf_nome": dados.get("nome_pf", ""),
        "pf_nif": dados.get("nif_pf", ""),
        "pf_contacto": dados.get("contacto_pf", ""),
        "pf_operadora": dados.get("operador_pf", ""),
        "pf_fixo": dados.get("fixo_pf", ""),
        "pf_cvp": dados.get("cvp_pf", ""),

        # Rescisão
        "res_nome": dados.get("entry_res_nome", ""),
        "res_morada": dados.get("entry_res_morada", ""),
        "res_cp4": dados.get("entry_res_cp4", ""),
        "res_cp3": dados.get("entry_res_cp3", ""),
        "res_localidade": dados.get("entry_res_localidade", ""),
        "res_operadora": dados.get("combo_operadora_res", ""),
        "res_cliente": dados.get("entry_res_cliente", ""),
        "res_fixo": dados.get("entry_res_fixo", ""),
        "res_movel_1": dados.get("res_movel_1", ""),
        "res_movel_2": dados.get("res_movel_2", ""),
        "res_movel_3": dados.get("res_movel_3", ""),
        "res_movel_4": dados.get("res_movel_4", ""),
        "res_int_movel": dados.get("entry_res_int_movel", ""),
        "res_srv_tv": 1 if dados.get("res_srv_tv", False) else 0,
        "res_srv_internet": 1 if dados.get("res_srv_internet", False) else 0,
        "res_srv_voz": 1 if dados.get("res_srv_voz", False) else 0,
        "res_srv_movel": 1 if dados.get("res_srv_movel", False) else 0,
        "res_srv_int_movel": 1 if dados.get("res_srv_int_movel", False) else 0,
    }

    # Portabilidade móvel por linha (1..4): op/nome/nif/cvp/kmat
    for i in range(1, 5):
        row[f"pm_op_{i}"] = dados.get(f"pm_op_{i}", "")
        row[f"pm_nome_{i}"] = dados.get(f"pm_nome_{i}", "")
        row[f"pm_nif_{i}"] = dados.get(f"pm_nif_{i}", "")
        row[f"pm_cvp_{i}"] = dados.get(f"pm_cvp_{i}", "")
        row[f"pm_kmat_{i}"] = dados.get(f"pm_kmat_{i}", "")
    return row

def upsert_rc(dados: dict, nome_vendedor: str, sfid: str):
    """
    Guarda RC SEM assumir que o NIF é único.

    Regra:
    - Se existir um ID selecionado (ex.: o utilizador clicou em "Editar" num contrato),
      faz UPDATE pelo ID.
    - Caso contrário, faz INSERT (mesmo que o NIF já exista).
    """
    conn = ligar_db()
    try:
        ensure_rc_columns(conn)
        cur = conn.cursor()

        row = _rc_row_from_form(dados, nome_vendedor, sfid)

        # ID do registo (quando estás a editar um contrato já existente)
        rc_id = None
        # 1) pode vir no dict (mais explícito)
        try:
            rc_id = dados.get("rc_id")
        except Exception:
            rc_id = None
        # 2) fallback para global (se estiveres a usar essa abordagem)
        if (rc_id is None or str(rc_id).strip() == "") and "CURRENT_RC_ID" in globals():
            rc_id = globals().get("CURRENT_RC_ID")

        # normalizar
        try:
            if rc_id is not None and str(rc_id).strip() != "":
                rc_id = int(rc_id)
            else:
                rc_id = None
        except Exception:
            rc_id = None



        # Determinar se o "ator" atual é Mac (para marcação de edits)
        # Nota: usamos tanto o flag do vendedor (CURRENT_VENDEDOR_MAC) como a plataforma.
        is_actor_mac = False
        try:
            if int(globals().get("CURRENT_VENDEDOR_MAC", 0) or 0) == 1:
                is_actor_mac = True
        except Exception:
            pass
        try:
            if sys.platform == "darwin":
                is_actor_mac = True
        except Exception:
            pass
        # --- Bridge Mac edits: preservar estado do Sheets e marcar edição ---
        existing_mac = None
        existing_sheets_status = None
        if rc_id is not None:
            try:
                cur_chk = conn.cursor(dictionary=True)
                cur_chk.execute("SELECT mac, sheets_status FROM RC WHERE ID=%s", (int(rc_id),))
                ex = cur_chk.fetchone() or {}
                try:
                    existing_mac = ex.get("mac")
                    existing_sheets_status = ex.get("sheets_status")
                except Exception:
                    existing_mac = None
                    existing_sheets_status = None
                cur_chk.close()
            except Exception:
                existing_mac = None
                existing_sheets_status = None

            # Se o registo já tinha mac definido, mantém (não deixar o Windows sobrescrever)
            try:
                if existing_mac is not None:
                    row["mac"] = int(existing_mac)
            except Exception:
                pass

            # Se já estava sincronizado, mantém sheets_status=1 para garantir UPDATE no Sheets (não voltar a marcar como pendente)
            try:
                if existing_sheets_status is not None and int(existing_sheets_status) == 1:
                    row["sheets_status"] = 1
            except Exception:
                pass
            # Marcar como editado se o registo for de Mac OU se o utilizador atual estiver em Mac
            # (isto garante que ao clicar "Editar" -> "Preencher contrato" no Mac, um Windows fará UPDATE no Sheets)
            try:
                if (existing_mac is not None and int(existing_mac) == 1) or is_actor_mac:
                    row["edited"] = 1
            except Exception:
                pass

        cols = list(row.keys())

        if rc_id is not None:
            # UPDATE pelo ID (não pelo NIF)
            set_cols = [c for c in cols]  # atualiza tudo o que existe no row
            set_sql = ", ".join([f"{c}=%s" for c in set_cols])
            sql = f"UPDATE RC SET {set_sql} WHERE ID=%s"
            values = [row[c] for c in set_cols] + [rc_id]
            cur.execute(sql, tuple(values))
            conn.commit()
            return rc_id

        # INSERT (permite NIF duplicado)
        cols_sql = ", ".join(cols)
        placeholders = ", ".join(["%s"] * len(cols))
        sql = f"INSERT INTO RC ({cols_sql}) VALUES ({placeholders})"
        values = [row[c] for c in cols]
        cur.execute(sql, tuple(values))
        conn.commit()
        new_id = getattr(cur, "lastrowid", None)

        # atualiza o global (se existir), para permitir "editar imediatamente" após gravar
        try:
            globals()["CURRENT_RC_ID"] = int(new_id) if new_id is not None else None
        except Exception:
            pass

        return new_id

    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


# ====================== GOOGLE SHEETS (EXPORT PERGUNTAS) ======================

GOOGLE_SHEETS_SPREADSHEET_ID = "1AHsOe9b9yJHJPCqJDtH178uU51TDMHSR1jBgGL0P1YU"
GOOGLE_SHEETS_TAB_NAME = "PERGUNTAS"
GOOGLE_SHEETS_START_ROW = 30  # começa a escrever aqui (A30)


def _gsh_base_dir() -> str:
    """Diretório base onde corre o programa (compatível com .py e .exe)."""
    try:
        return os.path.dirname(os.path.abspath(__file__))
    except Exception:
        return os.getcwd()


# ====================== GOOGLE SHEETS - CREDENCIAIS VIA BD (TABELA Json) ======================
# Objetivo: evitar depender de ficheiro .json local com credenciais.
# Estrutura esperada na BD:
#   CREATE TABLE Json (id INT AUTO_INCREMENT PRIMARY KEY, json LONGTEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
#   INSERT INTO Json (json) VALUES ('{...json completo da service account...}');

def ensure_json_table(conn):
    # Garante que existe a tabela `Json` com uma coluna `json` (LONGTEXT).
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS Json (
            id INT AUTO_INCREMENT PRIMARY KEY,
            json LONGTEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.commit()
    cur.close()


def _gsh_try_repair_service_account_json(raw: str) -> str:
    """Tenta reparar JSON de Service Account quando o campo 'private_key' foi guardado com quebras de linha reais
    (em vez de \n). Mantém o resto do JSON intacto e devolve a string reparada; se não conseguir, devolve o raw."""
    try:
        import re
        s = raw
        # Captura o bloco do private_key mesmo que tenha \n ou newlines reais dentro da string
        pattern = r'("private_key"\s*:\s*")(?P<body>-----BEGIN PRIVATE KEY-----.*?-----END PRIVATE KEY-----\s*)(")'
        m = re.search(pattern, s, flags=re.DOTALL)
        if not m:
            return raw

        body = m.group("body")
        # Normaliza CRLF e transforma newlines reais em \n
        body_norm = body.replace("\r\n", "\n").replace("\r", "\n")
        body_escaped = body_norm.replace("\n", "\\n")

        repaired = s[:m.start("body")] + body_escaped + s[m.end("body"):]
        return repaired
    except Exception:
        return raw


def _gsh_get_service_account_info_from_db() -> Optional[dict]:
    # Vai buscar o JSON da Service Account à BD (tabela Json, coluna json).
    try:
        conn = ligar_db()
    except Exception:
        return None
    try:
        try:
            ensure_json_table(conn)
        except Exception:
            pass
        cur = conn.cursor()
        cur.execute("SELECT json FROM Json ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        cur.close()
        if not row or not row[0]:
            return None
        raw = row[0]
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode('utf-8', errors='ignore')
        raw = str(raw).strip()
        try:
            data = json.loads(raw)
        except Exception:
            raw2 = _gsh_try_repair_service_account_json(raw)
            if raw2 != raw:
                data = json.loads(raw2)
            else:
                raise
        if not (isinstance(data, dict) and data.get('type') == 'service_account' and data.get('client_email') and data.get('private_key')):
            return None
        return data
    except Exception:
        return None
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _gsh_find_service_account_json(base_dir: Optional[str] = None) -> Optional[str]:
    """Procura um ficheiro JSON de Service Account na mesma pasta do programa."""
    base_dir = base_dir or _gsh_base_dir()
    try:
        for fn in os.listdir(base_dir):
            if not fn.lower().endswith(".json"):
                continue
            full = os.path.join(base_dir, fn)
            try:
                with open(full, "r", encoding="utf-8") as f:
                    data = json.load(f)
                # Validação mínima (não expõe chave)
                if isinstance(data, dict) and data.get("type") == "service_account" and data.get("client_email"):
                    return full
            except Exception:
                continue
    except Exception:
        return None
    return None


GOOGLE_IMPORT_ERROR = None
try:
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
except Exception as e:
    Credentials = None
    build = None
    GOOGLE_IMPORT_ERROR = repr(e)

from typing import Tuple
import sys
import re

def exportar_perguntas_para_google_sheets(dados: dict) -> Tuple[bool, str]:
    """Envia as Perguntas Obrigatórias para o Google Sheets (tab PERGUNTAS) em formato de tabela por colunas.

    - Linha 30: cabeçalhos (SFID, VENDEDOR, e perguntas)
    - Linha 31 em diante: 1 linha por contrato (append na próxima linha vazia)
    Retorna (ok, mensagem). Não levanta exceção para não crashar o programa.
    """
    # Import local (evita problemas de import circular)
    from tkinter import messagebox

    # Regra: este export é suportado apenas em Windows.
    # (Em macOS os RCs ficam pendentes para um Windows os enviar - bridge.)
    if not sys.platform.startswith("win"):
        return (False, "Export para Google Sheets só é suportado em Windows (bridge).")

    # Se não tiveres as libs instaladas, não bloqueia a app com mensagem enganadora
    if Credentials is None or build is None:
        so = "macOS" if sys.platform == "darwin" else ("Windows" if sys.platform.startswith("win") else sys.platform)
        detalhe = f"{GOOGLE_IMPORT_ERROR}" if "GOOGLE_IMPORT_ERROR" in globals() else "Desconhecido"
        messagebox.showerror(
            "Erro Google Sheets",
            "Não foi possível carregar as bibliotecas do Google Sheets.\n\n"
            f"Sistema: {so}\n"
            f"Detalhe técnico: {detalhe}\n\n"
            "Solução típica:\n"
            "- Se corres como .py: instala 'google-api-python-client' e 'google-auth'\n"
            "- Se é app empacotada (PyInstaller): faltam hidden-imports no build"
        )
        return False, "Bibliotecas Google Sheets em falta."

    sa_info = _gsh_get_service_account_info_from_db()
    if not sa_info:
        return False, (
            "Não encontrei o JSON da Service Account na BD (tabela Json, coluna json). "
            "Insere 1 registo com o conteúdo completo do Service Account JSON e volta a tentar."
        )

    try:
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
        service = build("sheets", "v4", credentials=creds)

        # Dados do vendedor (vem do teu mecanismo atual de login)
        try:
            nome_vendedor, sfid = ler_dados()
        except Exception:
            nome_vendedor, sfid = "", ""

        # Normalizações
        n_boxes_raw = (dados.get("num_boxes_adicionais") or "0")
        try:
            n_boxes_int = int(str(n_boxes_raw).strip() or "0")
        except Exception:
            n_boxes_int = 0

        valor_boxes = (dados.get("valor_boxes_adicionais") or "").strip()
        if n_boxes_int <= 0:
            valor_boxes = ""

        origem = (dados.get("origem_venda") or "").strip()
        origem_outra = (dados.get("origem_venda_outra") or "").strip()
        if origem.lower().startswith("outra") and origem_outra:
            origem_final = f"{origem} {origem_outra}".strip()
        else:
            origem_final = origem

        # Cabeçalhos (linha 30)
        header_row = GOOGLE_SHEETS_START_ROW
        headers = [
            "ID",
            "SFID",
            "NOME COMERCIAL",
            "QUAL A  OFERTA NOVO CLIENTE ? (Exemplo: Oferta do desconto 10€/mês OU oferta 2º mês)",
            "QUAL A   PLATAFORMA (se tiver) E QUANTOS MESES? (exemplo Netflix durante 3 meses)",
            "OFERECES-TE ALGUMA OFERTA EXTRA AO CLIENTE? (exemplo, combinei que lhe pagaria o valor de incumrpimento contratual, ou valor de dupla faturação, ou que lhe iria oferecer o super wifi ou então crédito de 0€ para pagar o super wifi)",
            "Nº DE BOXES ADICONAIS?",
            "VALOR DAS BOXES ADICONAIS?",
            "QUAL A ORIGEM DA VENDA?",
            # EXTRA (Supervisor)
            "MORADA COMPLETA",
            "CÓDIGO POSTAL",
            "LOCALIDADE",
            "NOME COMPLETO DO CLIENTE",
            "NIF",
            "NOME DO PLANO",
            "CONTACTO DO CLIENTE",
        ]

        # Range dos cabeçalhos: A30:P30 (16 colunas)
        header_range = f"{GOOGLE_SHEETS_TAB_NAME}!A{header_row}:P{header_row}"

        # Se os cabeçalhos ainda não estiverem corretos, escreve/normaliza
        existing = service.spreadsheets().values().get(
            spreadsheetId=GOOGLE_SHEETS_SPREADSHEET_ID,
            range=header_range,
        ).execute()
        existing_vals = existing.get("values", [])
        existing_row = existing_vals[0] if existing_vals else []

        def _norm_row(r):
            r = list(r or [])
            r = r + [""] * (len(headers) - len(r))
            return [str(x).strip() for x in r[:len(headers)]]

        if _norm_row(existing_row) != _norm_row(headers):
            service.spreadsheets().values().update(
                spreadsheetId=GOOGLE_SHEETS_SPREADSHEET_ID,
                range=header_range,
                valueInputOption="USER_ENTERED",
                body={"values": [headers]},
            ).execute()

        # Dados começam na linha 31
        start_data_row = GOOGLE_SHEETS_START_ROW + 1

        # ID do registo (quando vem da BD)
        rc_id = ""
        try:
            rc_id = dados.get("rc_id") or dados.get("id") or dados.get("ID") or ""
        except Exception:
            rc_id = ""

        # Identidade do vendedor: para bridge (Mac->Windows) usamos SEMPRE o que vem no registo (RC),
        # e só em último caso fazemos fallback para o vendedor logado.
        try:
            sfid_local = (dados.get("sfid") or dados.get("SFID") or dados.get("vendedor_sfid") or dados.get("sfid_vendedor") or "").strip()
        except Exception:
            sfid_local = ""
        try:
            nome_vendedor_local = (dados.get("vendedor") or dados.get("nome") or dados.get("NOME") or dados.get("nome_vendedor") or "").strip()
        except Exception:
            nome_vendedor_local = ""
        if not sfid_local:
            try:
                sfid_local = (globals().get("CURRENT_VENDEDOR_SFID") or globals().get("sfid") or "").strip()
            except Exception:
                sfid_local = sfid_local or ""
        if not nome_vendedor_local:
            try:
                nome_vendedor_local = (globals().get("CURRENT_VENDEDOR_NOME") or globals().get("nome_vendedor") or "").strip()
            except Exception:
                nome_vendedor_local = nome_vendedor_local or ""


        # EXTRA (Supervisor)
        morada_completa = (dados.get("rua_faturacao") or "").strip()
        cp4 = (dados.get("cp4_faturacao") or "").strip()
        cp3 = (dados.get("cp3_faturacao") or "").strip()
        codigo_postal = f"{cp4}-{cp3}".strip("-") if (cp4 or cp3) else ""
        localidade = (dados.get("localidade_faturacao") or "").strip()
        nome_cliente = (dados.get("nome_completo") or "").strip()
        nif_cliente = (dados.get("nif") or "").strip()
        nome_plano = (dados.get("plano_valor") or "").strip()
        contacto_cliente = (dados.get("tel_contacto") or dados.get("contacto") or "").strip()

        values_row = [
            str(rc_id).strip(),
            (sfid_local or "").strip(),
            (nome_vendedor_local or "").strip(),
            (dados.get("oferta_novo_cliente") or "").strip(),
            (dados.get("plataforma_meses") or "").strip(),
            (dados.get("oferta_extra") or "").strip(),
            str(n_boxes_int),
            valor_boxes,
            origem_final,
            # EXTRA (Supervisor)
            morada_completa,
            codigo_postal,
            localidade,
            nome_cliente,
            nif_cliente,
            nome_plano,
            contacto_cliente,
        ]

        # UPSERT POR ID (evita duplicar)
        row_info = ""
        rc_id_str = str(rc_id).strip()
        target_row = None

        if rc_id_str:
            try:
                id_range = f"{GOOGLE_SHEETS_TAB_NAME}!A{start_data_row}:A"
                id_resp = service.spreadsheets().values().get(
                    spreadsheetId=GOOGLE_SHEETS_SPREADSHEET_ID,
                    range=id_range,
                ).execute()
                id_values = id_resp.get("values", []) or []

                for idx, row in enumerate(id_values, start=0):
                    cell = (row[0] if row else "")
                    if str(cell).strip() == rc_id_str:
                        target_row = start_data_row + idx
                        break
            except Exception:
                target_row = None

        if target_row is not None:
            update_range = f"{GOOGLE_SHEETS_TAB_NAME}!A{target_row}:P{target_row}"
            service.spreadsheets().values().update(
                spreadsheetId=GOOGLE_SHEETS_SPREADSHEET_ID,
                range=update_range,
                valueInputOption="USER_ENTERED",
                body={"values": [values_row]},
            ).execute()
            row_info = f" (atualizado na linha {target_row})"
        else:
            append_range = f"{GOOGLE_SHEETS_TAB_NAME}!A{start_data_row}:P"
            resp = service.spreadsheets().values().append(
                spreadsheetId=GOOGLE_SHEETS_SPREADSHEET_ID,
                range=append_range,
                valueInputOption="USER_ENTERED",
                insertDataOption="INSERT_ROWS",
                body={"values": [values_row]},
            ).execute()

            try:
                updated_range = (resp.get("updates", {}) or {}).get("updatedRange", "") or ""
                m_row = re.search(r'!\w+(\d+)', updated_range)
                if m_row:
                    row_info = f" (adicionado na linha {m_row.group(1)})"
            except Exception:
                row_info = ""

        return True, f"Perguntas exportadas para Google Sheets ({GOOGLE_SHEETS_TAB_NAME}){row_info}."
    except Exception as e:
        return False, f"Falha ao exportar para Google Sheets: {e}"



def inserir_rc(dados, nome_vendedor, sfid):
    conn = ligar_db()
    cursor = conn.cursor()

    #quero inserir estes -> entry_rua_instalacao, entry_cp4_instalacao, entry_cp3_instalacao, entry_localidade_instalacao

    sql = """
    INSERT INTO RC (
        nome_completo, nif, tel_contacto, morada, cp7, email, pacote,
        tel_fixo, int_fixa,
        tel_1, tel_2, tel_3, tel_4,
        tar_1, tar_2, tar_3, tar_4,
        FE, NTCB, IBAN, banco, SFID, nome, tipo_fatura, ze_sem_ze
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,
              %s,%s,%s,%s,
              %s,%s,%s,%s,
              %s,%s,%s,%s,%s,%s,%s,%s)
    """

    
    
    valores = (
        dados["nome_completo"],
        dados["nif"],
        dados["contacto"],
        f'{dados["rua_faturacao"]}, {dados["localidade_faturacao"]}',
        f'{dados["cp4_faturacao"]}-{dados["cp3_faturacao"]}',
        dados["email"],
        dados["plano_valor"],
        dados["fixo"],
        dados["velocidade_internet"],
        dados["telemovel1"],
        dados["telemovel2"],
        dados["telemovel3"],
        dados["telemovel4"],
        dados["gigas_min_telemovel1"],
        dados["gigas_min_telemovel2"],
        dados["gigas_min_telemovel3"],
        dados["gigas_min_telemovel4"],
        int(dados["fatura_eletronica"]),
        dados["contacto"],
        dados["iban"],
        "N/A",
        sfid,
        nome_vendedor,
        dados["fatura_tipo"],
        int(dados["ze_sem_ze"])
    )

    
  

    cursor.execute(sql, valores)
    conn.commit()
    cursor.close()
    conn.close()


def montar_campos_portabilidade_movel(port, ssfid, dia, mes, ano, nome_vendedor="", ze_sem_ze=False):
    """
    Monta a lista de campos (na ordem do COORDS_4) para 1 página de Portabilidade Móvel.

    Regras (supervisor):
    - 1 portabilidade por (NIF + Operadora).
    - Números dentro da mesma portabilidade têm de ser da mesma operadora.

    NOTA IMPORTANTE:
    - O template (COORDS_4) tem campos duplicados específicos para MEO (NÚMERO MEO + CVP MEO),
      e campos gerais (NÚMERO + CVP). Para evitar texto duplicado:
        * Se operadora == "MEO": preenche apenas os campos "MEO" e deixa os gerais em branco.
        * Caso contrário: preenche apenas os campos gerais e deixa os campos "MEO" em branco.
    """
    op = (port.get("operadora") or "").strip()

    # Até 4 linhas por portabilidade (o template só tem 4 slots)
    linhas = port.get("linhas") or []
    linhas = [l for l in linhas if (l.get("num") or "").strip()][:4]

    meo_nums  = ["", "", "", ""]
    nums      = ["", "", "", ""]
    cvps      = ["", "", "", ""]
    cvps_meo  = ["", "", "", ""]
    sims      = ["", "", "", ""]  # Nº Cartão SIM (KMAT) 1..4

    for i, item in enumerate(linhas):
        num  = (item.get("num") or "").strip()
        cvp  = (item.get("cvp") or "").strip()
        sim  = (item.get("kmat") or "").strip()  # Nº Cartão SIM (KMAT)
        sims[i] = sim

        if op.upper() == "MEO":
            meo_nums[i] = num
            cvps_meo[i] = cvp
        else:
            nums[i] = num
            cvps[i] = cvp

    nome = (port.get("titular_nome") or "").strip()
    nif  = (port.get("titular_nif") or "").strip()

    meo_x  = "X" if op.upper() == "MEO" else ""
    nos_x  = "X" if op.upper() == "NOS" else ""
    nowo_x = "X" if op.upper() == "NOWO" else ""
    outro  = op if op.upper() not in ["MEO", "NOS", "NOWO"] else ""

    campos_4 = [
        "" if ze_sem_ze else ssfid,   # 1) SFID
        "" if ze_sem_ze else (nome_vendedor or "").strip(),  # 1b) Nome/Assinatura Comercial
        *meo_nums,                    # 2-5) NÚMERO MEO 1..4
        *nums,                        # 6-9) NÚMERO 1..4 (geral)
        *cvps,                        # 10-13) CVP 1..4 (geral)
        *sims,                        # 14-17) Nº Cartão SIM (KMAT) 1..4
        nome,                         # 14) NOME COMPLETO
        nif,                          # 15) NIF
        meo_x,                        # 16) MEO (X)
        nos_x,                        # 17) NOS (X)
        nowo_x,                       # 18) NOWO (X)
        outro,                        # 19) Outro Operador (texto)
        *cvps_meo,                    # 20-23) CVP MEO 1..4
        dia, mes, ano,
        "x" if 1 == 1 else "",
    ]
    return campos_4
def inserir_vendedor(nome, sfid):
    conn = ligar_db()
    try:
        ensure_vendedores_mac_column(conn)
    except Exception:
        pass
    cursor = conn.cursor()

    try:
        mac_val = int(globals().get('CURRENT_VENDEDOR_MAC', 0) or 0)
    except Exception:
        mac_val = 0

    cursor.execute(
        "INSERT IGNORE INTO vendedores (nome, SFID, mac) VALUES (%s,%s,%s)",
        (nome, sfid, mac_val)
    )

    conn.commit()
    cursor.close()
    conn.close()
# ====================== SHEETS BRIDGE (MAC -> WINDOWS) ======================
def _is_windows_runtime() -> bool:
    try:
        return sys.platform.startswith("win")
    except Exception:
        return False

def _can_export_sheets() -> bool:
    """Só Windows E só vendedores com mac=0 podem exportar para Sheets."""
    try:
        mac_flag = int(globals().get("CURRENT_VENDEDOR_MAC", 0) or 0)
    except Exception:
        mac_flag = 0
    return _is_windows_runtime() and mac_flag == 0

def _rc_set_sheets_status(rc_id: int, status: int, sent_at: Optional[str] = None, last_error: Optional[str] = None) -> None:
    """Atualiza o estado de exportação do RC no MySQL.
    status:
      0 = pendente
      1 = enviado OK
      2 = reservado/em processamento (evita duplicados)
    """
    try:
        conn = ligar_db()
    except Exception:
        return
    try:
        try:
            ensure_rc_columns(conn)
        except Exception:
            pass
        cur = conn.cursor()
        # sent_at vem como string 'YYYY-MM-DD HH:MM:SS' (MySQL DATETIME)
        if int(status) == 1:
            cur.execute(
                """UPDATE RC
                   SET sheets_status=%s,
                       sheets_sent_at=%s,
                       sheets_last_error=%s,
                       edited=0
                   WHERE ID=%s""",
                (int(status), sent_at, last_error, int(rc_id))
            )
        else:
            cur.execute(
                """UPDATE RC
                   SET sheets_status=%s,
                       sheets_sent_at=%s,
                       sheets_last_error=%s
                   WHERE ID=%s""",
                (int(status), sent_at, last_error, int(rc_id))
            )
        conn.commit()
        cur.close()
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
    finally:
        try:
            conn.close()
        except Exception:
            pass

def _split_cp7(cp7: str) -> Tuple[str, str]:
    cp7 = (cp7 or "").strip()
    m = re.match(r"^(\d{4})\s*[- ]\s*(\d{3})$", cp7)
    if m:
        return m.group(1), m.group(2)
    # fallback: tenta apanhar apenas dígitos
    digits = re.sub(r"\D", "", cp7)
    if len(digits) >= 7:
        return digits[:4], digits[4:7]
    return "", ""

def _split_morada(morada: str) -> Tuple[str, str]:
    """Tenta separar rua/localidade a partir de um texto 'rua, localidade'."""
    s = (morada or "").strip()
    if "," in s:
        a, b = s.split(",", 1)
        return a.strip(), b.strip()
    return s, ""

def _adapt_rc_row_to_dados_for_sheets(row: dict) -> dict:
    """Converte uma linha da tabela RC (MySQL) num 'dados' compatível com exportar_perguntas_para_google_sheets()."""
    # Morada/CP (na RC tens 'morada' e 'cp7' e também campos APP em separado)
    rua_fat, local_fat = _split_morada(row.get("morada", ""))
    cp4_fat, cp3_fat = _split_cp7(row.get("cp7", ""))

    # valor/plano: na RC tens 'pacote' (texto). O sheets aceita qualquer string.
    plano_valor = (row.get("pacote", "") or "").strip()

    # Perguntas obrigatórias (já existem como colunas na RC)
    dados = {
        "rc_id": row.get("ID"),
        "sfid": row.get("SFID", ""),
        "vendedor": row.get("nome", ""),

        # Comercial (override PDF)
        "sfid_comercial": row.get("SFID_COMERCIAL", ""),
        "nome_comercial": row.get("NOME_COMERCIAL", ""),

        # Identificação do cliente (para preencher colunas M/N/P no Sheets)
        "nome_completo": row.get("nome_completo", ""),
        "nif": row.get("nif", ""),
        "tel_contacto": row.get("tel_contacto", ""),


        "rua_faturacao": rua_fat,
        "cp4_faturacao": cp4_fat,
        "cp3_faturacao": cp3_fat,
        "localidade_faturacao": local_fat,
        "plano_valor": plano_valor,

        "oferta_novo_cliente": row.get("oferta_novo_cliente", ""),
        "plataforma_meses": row.get("plataforma_meses", ""),
        "oferta_extra": row.get("oferta_extra", ""),
        "num_boxes_adicionais": row.get("num_boxes_adicionais", 0) or 0,
        "valor_boxes_adicionais": row.get("valor_boxes_adicionais", ""),
        "origem_venda": row.get("origem_venda", ""),
        "origem_venda_outra": row.get("origem_venda_outra", ""),
    }
    return dados

def sync_mac_pending_rc_to_sheets(limit: int = 8) -> Tuple[int, int]:
    """Windows (mac=0): envia para o Sheets:
      A) RCs de utilizadores Mac (mac=1) pendentes: sheets_status IS NULL/0
      B) RCs de utilizadores Mac editados: edited=1 (faz UPSERT por ID no Sheets)

    Regras:
      - Só Windows pode exportar (_can_export_sheets()).
      - Só vendedores Windows (CURRENT_VENDEDOR_MAC == 0) fazem bridge.
      - Após sucesso: sheets_status=1, edited=0, sheets_sent_at=NOW()
      - Se falhar: repõe sheets_status ao estado anterior (0 ou 1) e mantém edited=1 (para tentar novamente)
      - Recupera locks presos (sheets_status=2) com lock antigo.
    Retorna (processados, enviados_ok).
    """
    if not _can_export_sheets():
        return (0, 0)

    try:
        conn = ligar_db()
    except Exception:
        return (0, 0)

    processed = 0
    ok_count = 0
    try:
        try:
            ensure_rc_columns(conn)
        except Exception:
            pass

        # Recuperar locks presos (ex.: app fechou a meio)
        try:
            cur_fix = conn.cursor()
            cur_fix.execute(
                """UPDATE RC
                       SET sheets_status=0, sheets_lock_at=NULL
                     WHERE mac=1
                       AND sheets_status=2
                       AND (sheets_lock_at IS NULL OR sheets_lock_at < (NOW() - INTERVAL 20 MINUTE))"""
            )
            conn.commit()
            cur_fix.close()
        except Exception:
            pass

        cur = conn.cursor(dictionary=True)

        # Candidatos: pendentes OU editados (não depende do sheets_status para os editados)
        # Nota: priorizamos edited=1 para refletir alterações rapidamente
        cur.execute(
            """SELECT ID, COALESCE(sheets_status,0) AS sheets_status, COALESCE(edited,0) AS edited
                   FROM RC
                  WHERE mac=1
                    AND ( (sheets_status IS NULL OR sheets_status=0) OR edited=1 )
                  ORDER BY (CASE WHEN edited=1 THEN 0 ELSE 1 END), ID ASC
                  LIMIT %s""",
            (int(limit) * 4,)
        )
        candidates = cur.fetchall() or []

        for r in candidates:
            if processed >= int(limit):
                break

            rc_id = r.get("ID")
            if rc_id is None:
                continue

            orig_status = r.get("sheets_status", 0)
            try:
                orig_status = int(orig_status) if orig_status is not None else 0
            except Exception:
                orig_status = 0
            orig_edited = 1 if str(r.get("edited", 0)) == "1" else 0

            # Reservar atomicamente (1 Windows ganha). Mantém a condição "precisa sync" no WHERE.
            cur2 = conn.cursor()
            cur2.execute(
                """UPDATE RC
                       SET sheets_status=2, sheets_lock_at=NOW()
                     WHERE ID=%s
                       AND mac=1
                       AND (sheets_status IS NULL OR sheets_status<>2)
                       AND ( (sheets_status IS NULL OR sheets_status=0) OR edited=1 )""",
                (int(rc_id),)
            )
            conn.commit()
            affected = getattr(cur2, "rowcount", 0)
            cur2.close()

            if affected != 1:
                continue  # outro Windows já reservou

            # Buscar dados completos
            cur.execute("SELECT * FROM RC WHERE ID=%s", (int(rc_id),))
            row = cur.fetchone()
            if not row:
                _rc_set_sheets_status(int(rc_id), orig_status, None, "RC não encontrado após reserva.")
                continue

            processed += 1
            dados = _adapt_rc_row_to_dados_for_sheets(row)

            try:
                ok, msg = exportar_perguntas_para_google_sheets(dados)
            except Exception as e:
                ok, msg = False, f"Erro exportar Sheets: {e!r}"

            if ok:
                ok_count += 1
                try:
                    cur_ok = conn.cursor()
                    cur_ok.execute(
                        """UPDATE RC
                               SET sheets_status=1,
                                   edited=0,
                                   sheets_sent_at=NOW(),
                                   sheets_last_error=NULL,
                                   sheets_lock_at=NULL
                             WHERE ID=%s""",
                        (int(rc_id),)
                    )
                    conn.commit()
                    cur_ok.close()
                except Exception:
                    # fallback no helper antigo
                    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    _rc_set_sheets_status(int(rc_id), 1, ts, None)
                    try:
                        cur_clear = conn.cursor()
                        cur_clear.execute("UPDATE RC SET edited=0, sheets_lock_at=NULL WHERE ID=%s", (int(rc_id),))
                        conn.commit()
                        cur_clear.close()
                    except Exception:
                        pass
            else:
                # Repor status anterior e manter edited=1 se era edição
                try:
                    cur_fail = conn.cursor()
                    cur_fail.execute(
                        """UPDATE RC
                               SET sheets_status=%s,
                                   sheets_last_error=%s,
                                   sheets_lock_at=NULL,
                                   edited=%s
                             WHERE ID=%s""",
                        (int(orig_status), str(msg)[:2000], int(orig_edited), int(rc_id))
                    )
                    conn.commit()
                    cur_fail.close()
                except Exception:
                    _rc_set_sheets_status(int(rc_id), orig_status, None, str(msg)[:2000])

        cur.close()

    finally:
        try:
            conn.close()
        except Exception:
            pass

    return (processed, ok_count)
def sync_mac_edited_rc_to_sheets(limit: int = 6) -> Tuple[int, int]:
    """Windows mac=0: procura RCs de utilizadores Mac (mac=1) com edited=1 e faz UPDATE no Sheets.
    Retorna (processados, enviados_ok).
    """
    if not _can_export_sheets():
        return (0, 0)

    try:
        conn = ligar_db()
    except Exception:
        return (0, 0)

    processed = 0
    ok_count = 0
    try:
        try:
            ensure_rc_columns(conn)
        except Exception:
            pass

        cur = conn.cursor(dictionary=True)

        # Buscar IDs editados (registos já sincronizados, mas alterados no Mac)
        cur.execute(
            """SELECT ID
               FROM RC
               WHERE mac=1 AND edited=1
               ORDER BY ID ASC
               LIMIT %s""",
            (int(limit) * 3,)
        )
        candidates = [int(r.get("ID")) for r in (cur.fetchall() or []) if r and r.get("ID") is not None]

        for rc_id in candidates:
            if processed >= int(limit):
                break

            # Reservar atomicamente usando sheets_status=2 como lock temporário
            cur2 = conn.cursor()
            cur2.execute(
                """UPDATE RC
                   SET sheets_status=2
                   WHERE ID=%s AND mac=1 AND edited=1 AND (sheets_status IS NULL OR sheets_status=1 OR sheets_status=0)""",
                (int(rc_id),)
            )
            conn.commit()
            affected = getattr(cur2, "rowcount", 0)
            cur2.close()

            if affected != 1:
                continue

            # Buscar dados completos e enviar para Sheets (a função já faz UPSERT por ID)
            cur.execute("SELECT * FROM RC WHERE ID=%s", (int(rc_id),))
            row = cur.fetchone() or {}
            dados = _adapt_rc_row_to_dados_for_sheets(row)

            ok, msg = exportar_perguntas_para_google_sheets(dados)
            processed += 1

            sent_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if ok:
                ok_count += 1
                try:
                    cur3 = conn.cursor()
                    cur3.execute(
                        """UPDATE RC
                           SET edited=0,
                               sheets_status=1,
                               sheets_sent_at=%s,
                               sheets_last_error=NULL
                           WHERE ID=%s""",
                        (sent_at, int(rc_id))
                    )
                    conn.commit()
                    cur3.close()
                except Exception:
                    pass
            else:
                try:
                    cur3 = conn.cursor()
                    cur3.execute(
                        """UPDATE RC
                           SET edited=1,
                               sheets_status=1,
                               sheets_sent_at=%s,
                               sheets_last_error=%s
                           WHERE ID=%s""",
                        (sent_at, str(msg)[:1800], int(rc_id))
                    )
                    conn.commit()
                    cur3.close()
                except Exception:
                    pass

    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    return (processed, ok_count)

def _trigger_sync_mac_rc_background(limit: int = 4) -> None:
    """Dispara a sincronização em background (não bloqueia UI)."""
    if not _can_export_sheets():
        return
    try:
        threading.Thread(target=lambda: (sync_mac_pending_rc_to_sheets(limit=limit), sync_mac_edited_rc_to_sheets(limit=max(2, int(limit)))), daemon=True).start()
    except Exception:
        pass


# ====================== FUNÇÕES DE DADOS ======================
def ler_dados():
    """Obtém nome e SSFID do vendedor autenticado.

    Antigamente estes dados eram lidos de um ficheiro local (pasta 'dont_touch_me').
    Como o controlo passou a ser feito via Base de Dados (UUID), mantemos estes
    valores em memória após login.
    """
    return (globals().get('CURRENT_VENDEDOR_NOME') or ''), (globals().get('CURRENT_VENDEDOR_SFID') or '')


PDF_ORIGINAL = "contrato_adesao.pdf"
# NOTA: o nome do PDF de saída é calculado dinamicamente (NIF + Nome do novo titular).
PDF_SAIDA = "contrato_prenchido.pdf"

def _safe_filename(value: str, max_len: int = 120) -> str:
    """Sanitiza nomes de ficheiro para Windows (remove caracteres inválidos)."""
    v = (value or "").strip()
    v = v.replace("\\", " ")
    v = re.sub(r'[<>:"/\\|?*]', "_", v)  # inválidos no Windows
    v = re.sub(r"\s+", " ", v).strip()
    v = v.replace(" ", "_")
    v = re.sub(r"_+", "_", v).strip("_")
    return (v[:max_len] if len(v) > max_len else v) or "contrato"

# ===================================================================
#  AQUI É ONDE TU VAIS METER AS COORDENADAS CERTAS (x, y)
#  Cada linha = 1 campo (na ordem que aparece abaixo)
#  Exemplo: (150, 230)  →  x=150 (esquerda/direita), y=230 (cima/baixo)
# ===================================================================


COORDS = [
    (112, 102),   # 1. Nome Completo (feito)
    (99, 121),   # 2. NIF (feito)
    (84, 146),   # 3. Rua Faturação (feito)
    (108, 165),   # 4. CP4 Faturação (feito) 
    (175, 165),   # 5. CP3 Faturação (feito)
    (225, 165),   # 6. Localidade Faturação (feito)
    (128, 704),   # 7. Rua Instalação (feito)
    (110, 730),   # 8. CP4 Instalação (feito)
    (177, 730),   # 9. CP3 Instalação (feito)
    (228, 730),   # 10. Localidade Instalação (feito)
    (452, 121),   # 11. Contacto (feito)
    (370, 165),   # 12. Email (feito)
    (81, 228),   # 13. Plano + Valor (feito)
    (454, 246),   # 14. Velocidade Internet (feito)
    (96, 282),   # 15. Telemóvel 1 (feito) # 300-282=18
    (96, 300),   # 16. Telemóvel 2 (feito)
    (96, 318),   # 17. Telemóvel 3 (feito)
    (96, 336),   # 18. Telemóvel 4 (feito)
    (96, 354),   # 19. Net Móvel 1 (feito)
    (96, 372),   # 20. Net Móvel 2 (feito)
    (230, 282),   # 21. Gigas/Min Telemóvel 1(feito)
    (230, 300),   # 22. Gigas/Min Telemóvel 2(feito)
    (230, 318),   # 23. Gigas/Min Telemóvel 3(feito)
    (230, 336),   # 24. Gigas/Min Telemóvel 4(feito)
    (230, 354),   # 25. Gigas Net Móvel 1(feito)
    (230, 372),   # 26. Gigas Net Móvel 2(feito)
    (112, 434),   # 27. Fatura Electrónica → "X" na caixa(feito)
    (95, 459),  # 28. Detalhada / Resumida → "X" na caixa certa(feito)
    (120, 546),  # 29. IBAN(feito)
    (432,282), # telemovel 1
    (432,300), # telemovel 2
    (432,318), # telemovel 3
    (432,336), # telemovel 4
    (432,354), # net movel 1
    (432,372), # net movel 2
    (279, 245), # fixo
    (432,228), # cruz "X"
    (230,282),
    (230,300),
    (230,318),
    (230,336),
    (400, 527),  # NTCB (Nome titular conta bancária)
    (85, 563),   # Banco
    (537, 546),  # Pagamento Recorrente (X)
]

COORDS_3 = [
    (335, 439),   # 30. SFID (A PREENCHER PELA VODAFONE)
    (450, 439),   # 30b. Nome/Assinatura Comercial
    (50,439),   # 31. Dia
    (90,439),  # 32. Mês
    (130,439),  # 33. Ano
]

COORDS_4 = [
    # == PORTABILIDADE MOVEL ==
    (260, 780),  # SFID
    (465, 780),  # Nome/Assinatura Comercial (feito)
    (60, 317),  # NÚMERO MEO 1 (feito)
    (60, 337),  # NÚMERO MEO 2 (feito)
    (60, 357),  # NÚMERO MEO 3 (feito)
    (60, 377),  # NÚMERO MEO 4 (feito)
    (60, 317),  # NÚMERO 1 (feito)
    (60, 337),  # Nºtel 2 (feito)
    (60, 357),  # NÚMERO 3 (feito)
    (60, 377),  # num 4 (feito)
    (205, 317),  # CVP 1 (feito)
    (205, 337),  # CVP 2 (feito)
    (205, 357),  # CVP 3 (feito)
    (205, 377),  # CVP 4 (feito)
    (405, 317),  # Nº Cartão SIM 1 (KMAT)
    (405, 337),  # Nº Cartão SIM 2 (KMAT)
    (405, 357),  # Nº Cartão SIM 3 (KMAT)
    (405, 377),  # Nº Cartão SIM 4 (KMAT)
    (135,135), # NOME COMPLETO (feito)
    (135,157), # NIF (FEITO)
    (215, 237), # MEO (X)
    (284, 237), # NOS (X)
    (367, 237), # NOWO (X)
    (435, 237), # Outro Operador (feito)
    (205, 317), # CVP MEO (feito)
    (205, 337), # CVP MEO (feito)
    (205, 357), # CVP MEO (feito)
    (205, 377), # CVP MEO (feito)
    (73, 675),   # 31. Dia
    (98, 675),  # 32. Mês   
    (122, 675),  # 33. Ano
    (45,446)
]

COORDS_5 = [
    # == PORTABILIDADE FIXO ==
    (135,142), # NOME COMPLETO (feito)
    (135,165), # NIF (FEITO)
    (420, 165), # CONTACTO (FEITO)
    (170, 282), # FIXO (FEITO)
    (405, 282), # CVP (FEITO)
    (215, 250), # MEO (X)
    (284, 250), # NOS (X)
    (367, 250), # NOWO (X)
    (435, 251), # Outro Operador (feito)
    (260, 775),  # SFID (feito)
    (440, 775),  # Nome/Assinatura Comercial
    (75,657),   # Dia
    (100,657),  # Mês   
    (125,657),  # Ano

]

COORDS_6 = [
    # == ALTERAÇAO DE TITULARIDADE ==
    (150, 145), # FIXO 1 (FEITO --)
    (415, 145), # FIXO 2 (FEITO --)
    (152, 193), # MÓVEL 1 (FEITO --)
    (415, 193), # MÓVEL 2 (FEITO --)
    (152, 213), # MÓVEL 3 (FEITO --)
    (415, 213), # MÓVEL 4 (FEITO --)
    (75, 107), # NOME DO CLIENTE (FEITO -- )
    (435, 107), # CONTA (FEITO --)
    (150, 321), # NIF ANTIGO
    (419, 321), # NIF NOVO
    (67, 719), # DIA
    (95, 719), # MES 
    (118, 719), # ANO
    (230, 780), # SFID
    (360, 780), # Nome/Assinatura Comercial (ADICIONADO)
    (44,652), # cruz
    # PARA BAIXO NAO FUNCIONA NAO MEXER !!  
    (150, 355), # TEL ANTIGO (FEITO --)
    (419, 355), # TEL NOVO (FEITO --)
    (150, 391), # SIM ANTIGO (FEITO --)
    (419, 391), # SIM NOVO (FEITO --)
    (72, 717),   # 31. Dia
    (95, 717),  # 32. Mês
    (119, 717),  # 33. Ano
    (230, 779),  # SFID (feito)
]

COORDS_7 = [
    # == RESCISÃO DE CONTRATO ==
    (125, 487), # MÓVEL 1 (FEITO)
    (236, 487), # MÓVEL 2 (FEITO)
    (347, 487), # MÓVEL 3 (FEITO)
    (458, 487), # MÓVEL 4 (FEITO)
    (57, 70), # NOME DO CLIENTE (FEITO)
    (57, 90), # NOVA MORADA (FEITO)
    (57, 110), # CP4 (FEITO)
    (87, 110), # CP3 (FEITO)
    (110, 110), # LOCALIDADE (FEITO)
    (57, 180), # combo operadora (FEITO)
    (208, 246), # NÚMERO DE CLIENTE (FEITO)
    (126, 438), # FIXO (FEITO)
    (175,689),   # Dia
    (205,689),  # Mês   
    (230,689),  # Ano
    (80, 689),  # LOCALIDADE (LOCALIDADE DA DATA)(FEITO)
    (81,352),
    (81,385), # 385 - 352 = 33
    (81,418),  # 385 + 33 = 418
    (81,468),
    (81,517),  # Internet Móvel (checkbox)
    (125,538), # Nº Internet Móvel

]

_PDF_FONTNAME = None
_PDF_FONTFILE = None

def _find_pdf_fontfile() -> Optional[str]:
    """Devolve um caminho para uma fonte TrueType que suporte o simbolo EUR (\u20ac), quando existir no SO."""
    # Windows
    try:
        windir = os.environ.get("WINDIR") or os.environ.get("SystemRoot") or "C:\\Windows"
    except Exception:
        windir = "C:\\Windows"

    candidates = [
        # Windows (quase sempre existe)
        os.path.join(windir, "Fonts", "arial.ttf"),
        os.path.join(windir, "Fonts", "segoeui.ttf"),
        os.path.join(windir, "Fonts", "calibri.ttf"),

        # macOS
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
        "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/Library/Fonts/Arial Unicode.ttf",
        "/Library/Fonts/Arial.ttf",

        # Linux (fallbacks comuns)
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    ]

    for p in candidates:
        try:
            if p and os.path.exists(p):
                return p
        except Exception:
            continue
    return None


def _pdf_prepare_font_once():
    """Resolve e guarda (em memoria) a fonte a usar no PDF (uma vez por execucao)."""
    global _PDF_FONTNAME, _PDF_FONTFILE
    if _PDF_FONTNAME is not None:
        return
    ff = _find_pdf_fontfile()
    if ff:
        _PDF_FONTNAME = "VDF_EURO_FONT"
        _PDF_FONTFILE = ff
    else:
        _PDF_FONTNAME = ""
        _PDF_FONTFILE = None


def _pdf_insert(page, x: float, y: float, texto, fontsize: int = 11, color=(0, 0, 0)):
    """Insercao robusta de texto no PDF, com suporte ao simbolo EUR (€) e cores."""
    if texto is None:
        return
    s = str(texto)
    if not s.strip():
        return

    _pdf_prepare_font_once()

    # Tenta com fonte embutida (se existir)
    try:
        if _PDF_FONTNAME and _PDF_FONTFILE:
            try:
                page.insert_font(fontname=_PDF_FONTNAME, fontfile=_PDF_FONTFILE)
            except Exception:
                pass
            page.insert_text((x, y), s, fontsize=fontsize, fontname=_PDF_FONTNAME, color=color)
            return
    except Exception:
        pass

    # Fallback: fonte default
    try:
        page.insert_text((x, y), s, fontsize=fontsize, color=color)
    except Exception:
        # Ultimo recurso: se o viewer nao suportar, escreve "EUR" para nao aparecer ponto.
        try:
            page.insert_text((x, y), s.replace("\u20ac", "EUR").replace("€", "EUR"), fontsize=fontsize, color=color)
        except Exception:
            pass


def obter_primeiro_ultimo(nome_completo):
    """Devolve apenas o primeiro e o último nome de uma string."""
    if not nome_completo:
        return ""
    partes = str(nome_completo).strip().split()
    if len(partes) >= 2:
        return f"{partes[0]} {partes[-1]}"
    return str(nome_completo).strip()

def preencher_pdf(dados):
    if not os.path.exists(PDF_ORIGINAL):
        messagebox.showerror("Erro", f"Não encontrei o ficheiro {PDF_ORIGINAL}")
        return

    # Obter a data de hoje (com zeros à esquerda)
    data_hoje = datetime.date.today()
    dia = f"{data_hoje.day:02d}"
    mes = f"{data_hoje.month:02d}"
    ano = str(data_hoje.year)

    # Lê vendedor / sfid
    nome_vendedor, ssfid = ler_dados()


    # Override (Perguntas Obrigatórias): estes valores vão para o PDF
    ssfid_pdf = (dados.get("sfid_comercial") or "").strip() or (ssfid or "").strip()
    nome_pdf = (dados.get("nome_comercial") or "").strip() or (nome_vendedor or "").strip()

    # Se "Ze sem ZE" estiver ativo, no PDF não deve aparecer nem SFID nem Nome Comercial
    if dados.get("ze_sem_ze", False):
        ssfid_pdf = ""
        nome_pdf = ""


    # Nome do PDF de saída: NIF + Nome (novo titular / topo do formulário)
    out_base = _safe_filename(f"{dados.get('nif', '')}_{dados.get('nome_completo', '')}")
    output_pdf = f"{out_base}.pdf"
    doc = fitz.open(PDF_ORIGINAL)

    # ==============================
    # 1) PÁGINA 1 (index 0) - campos
    # ==============================
    page_0 = doc[0]

    campos = [
        dados["nome_completo"],
        dados["nif"],
        dados["rua_faturacao"],
        dados["cp4_faturacao"],
        dados["cp3_faturacao"],
        dados["localidade_faturacao"],
        dados["rua_instalacao"],
        dados["cp4_instalacao"],
        dados["cp3_instalacao"],
        dados["localidade_instalacao"],
        dados["contacto"],
        dados["email"],
        dados["plano_valor"],
        dados["velocidade_internet"],
        dados["telemovel1"],
        dados["telemovel2"],
        dados["telemovel3"],
        dados["telemovel4"],
        dados["net_movel1"],
        dados["net_movel2"],
        dados["gigas_min_telemovel1"],
        dados["gigas_min_telemovel2"],
        dados["gigas_min_telemovel3"],
        dados["gigas_min_telemovel4"],
        dados["gigas_net_movel1"],
        dados["gigas_net_movel2"],
        "X" if dados["fatura_eletronica"] else "",
        "DETALHADA" if dados["fatura_tipo"] == "DETALHADA" else "RESUMIDA",
        dados["iban"].strip(),
        "X" if dados["telemovel1"] else "",
        "X" if dados["telemovel2"] else "",
        "X" if dados["telemovel3"] else "",
        "X" if dados["telemovel4"] else "",
        "X" if dados["net_movel1"] else "",
        "X" if dados["net_movel2"] else "",
        dados["fixo_pf"],
        "X" if 1 == 1 else "",
        dados["pm_gigas_1"],
        dados["pm_gigas_2"],
        dados["pm_gigas_3"],
        dados["pm_gigas_4"],
        dados.get("ntcb", ""),
        dados.get("banco_nome", ""),
        "X" if int(dados.get("pagamento_recorrente", 0) or 0) == 1 else "",
    ]

    for (x, y), texto in zip(COORDS, campos):
        if texto:
            _pdf_insert(page_0, x, y, texto, fontsize=11)

    # ==============================
    # 2) PÁGINA 4 (index 3) - campos_3
    # ==============================
    page_3 = doc[3]
    campos_3 = [
        "" if dados["ze_sem_ze"] else ssfid_pdf,
        (nome_pdf or "").strip(),
        dia,
        mes,
        ano,
    ]

    for (x, y), texto in zip(COORDS_3, campos_3):
        if texto:
            _pdf_insert(page_3, x, y, texto, fontsize=11)
            
    # === NOME CINZENTO (FA - Titular do Serviço) ===
    nome_fa = obter_primeiro_ultimo(dados.get("nome_completo", ""))
    if nome_fa:
        _pdf_insert(page_3, 5, 350, nome_fa, fontsize=6, color=(0.6, 0.6, 0.6))

    # ===========================================================
    # 3) PORTABILIDADE MÓVEL - PÁGINA 5 (index 4) COM REGRAS NOVAS
    #   - 1 portabilidade por (NIF + Operadora)
    #   - duplica a página 5 conforme necessário
    # ===========================================================
    portabilidades = dados.get("portabilidades", []) or []

    # Guardar o template original (limpo) da página 5 para duplicar
    # (abre um doc separado para inserir a página sem texto)
    src_template = fitz.open(PDF_ORIGINAL)

    # Se não houver portabilidades: remove página 5
    if len(portabilidades) == 0:
        # Remove a página 5 (index 4)
        try:
            doc.delete_page(4)
            shift_after_ports = -1
        except:
            shift_after_ports = 0
    else:
        # Vamos preencher a 1ª portabilidade na página 5 existente (index 4)
        # e inserir cópias limpas para as restantes.
        extra = len(portabilidades) - 1
        shift_after_ports = extra  # páginas depois da 5 deslocam +extra

        for idx_port, port in enumerate(portabilidades):
            if idx_port == 0:
                page_port = doc[4]
            else:
                insert_at = 4 + idx_port
                doc.insert_pdf(src_template, from_page=4, to_page=4, start_at=insert_at)
                page_port = doc[insert_at]

            campos_4 = montar_campos_portabilidade_movel(
                port=port,
                ssfid=ssfid_pdf,
                dia=dia,
                mes=mes,
                ano=ano,
                nome_vendedor=nome_pdf,
                ze_sem_ze=dados.get("ze_sem_ze", False),
            )

            for (x, y), texto in zip(COORDS_4, campos_4):
                if texto:
                    _pdf_insert(page_port, x, y, texto, fontsize=11)
                    
            # === NOME CINZENTO (PM - 1º nome da página) ===
            nome_pm = obter_primeiro_ultimo(port.get("titular_nome", ""))
            if nome_pm:
                _pdf_insert(page_port, 158, 678, nome_pm, fontsize=6, color=(0.6, 0.6, 0.6))

    src_template.close()

    # ===========================================================
    # 4) RESTO DAS PÁGINAS
    # Atenção: índices mudaram se inserimos/removemos páginas depois da 5
    # Original:
    #   page_5 = doc[5]  (index 5)
    #   page_6 = doc[6]  (index 6)
    #   page_7 = doc[7]  (index 7)
    # Agora:
    #   page_5 = doc[5 + shift_after_ports]
    #   page_6 = doc[6 + shift_after_ports]
    #   page_7 = doc[7 + shift_after_ports + shift_after_pf + shift_after_tit]
    # ===========================================================

    # Página 6 do PDF (original index 5) -> COORDS_5 / campos_5
    shift_after_pf = 0
    pf_any_preenchido = any((dados.get(k) or "").strip() for k in ["nome_pf","nif_pf","contacto_pf","fixo_pf","cvp_pf"])

    # Se a Portabilidade Fixa estiver toda vazia, remove a página para não ir em branco.
    if not pf_any_preenchido:
        try:
            doc.delete_page(5 + shift_after_ports)
            shift_after_pf = -1
        except:
            shift_after_pf = 0
    else:
        try:
            page_5 = doc[5 + shift_after_ports]

            op_pf = (dados.get("operador_pf") or "").strip()

            meo_x  = "X" if op_pf.upper() == "MEO" else ""
            nos_x  = "X" if op_pf.upper() == "NOS" else ""
            nowo_x = "X" if op_pf.upper() == "NOWO" else ""
            outro  = op_pf if op_pf.upper() not in ["MEO", "NOS", "NOWO"] else ""

            campos_5 = [
                dados.get("nome_pf", "").strip(),
                dados.get("nif_pf", "").strip(),
                dados.get("contacto_pf", "").strip(),
                dados.get("fixo_pf", "").strip(),
                dados.get("cvp_pf", "").strip(),
                meo_x,
                nos_x,
                nowo_x,
                outro,
                "" if dados.get("ze_sem_ze") else ssfid_pdf,
                (nome_pdf or "").strip(),
                dia,
                mes,
                ano,
            ]

            for (x, y), texto in zip(COORDS_5, campos_5):
                if texto:
                    _pdf_insert(page_5, x, y, texto, fontsize=11)
                    
            # === NOME CINZENTO (PF - 1º nome da página) ===
            nome_pf = obter_primeiro_ultimo(dados.get("nome_pf", ""))
            if nome_pf:
                _pdf_insert(page_5, 158, 660, nome_pf, fontsize=6, color=(0.6, 0.6, 0.6))
        except:
            pass


    # ===========================================================
    # ALTERAÇÃO DE TITULARIDADE (original index 6) - dinâmica
    # Regra:
    # - NIF do topo (dados["nif"]) = NIF NOVO
    # - NIFs nas linhas (linhas_pm) = NIFs ANTIGOS
    # - 1 alteração por cada NIF antigo diferente do NIF novo
    # ===========================================================
    titularidades = dados.get("titularidades", []) or []
    base_tit_index = 6 + shift_after_ports + shift_after_pf  # índice da página de titularidade no doc atual
    shift_after_tit = 0

    try:
        src_template_tit = fitz.open(PDF_ORIGINAL)

        if len(titularidades) == 0:
            # Sem alterações => remove a página de titularidade para não ir em branco
            try:
                doc.delete_page(base_tit_index)
                shift_after_tit = -1
            except:
                shift_after_tit = 0
        else:
            extra_tit = len(titularidades) - 1
            shift_after_tit = extra_tit

            for idx_tit, tit in enumerate(titularidades):
                if idx_tit == 0:
                    page_tit = doc[base_tit_index]
                else:
                    insert_at = base_tit_index + idx_tit
                    doc.insert_pdf(src_template_tit, from_page=6, to_page=6, start_at=insert_at)
                    page_tit = doc[insert_at]

                campos_6 = montar_campos_titularidade(
                    tit=tit,
                    ssfid=ssfid_pdf,
                    nome_vendedor=nome_pdf,
                    dia=dia,
                    mes=mes,
                    ano=ano,
                    ze_sem_ze=dados.get("ze_sem_ze", False),
                )

                for (x, y), texto in zip(COORDS_6, campos_6):
                    if texto:
                        _pdf_insert(page_tit, x, y, texto, fontsize=11)
                        
                # === NOME CINZENTO (AT - Titular Antigo da página) ===
                nome_at = obter_primeiro_ultimo(tit.get("tit_nome", ""))
                if nome_at:
                    _pdf_insert(page_tit, 158, 722, nome_at, fontsize=6, color=(0.6, 0.6, 0.6))
    except:
        # se falhar, não interrompe o contrato inteiro
        shift_after_tit = 0
    finally:
        try:
            src_template_tit.close()
        except:
            pass

    # Página 8 do PDF (original index 7) -> COORDS_7 / campos_7
    # Regra: se TODOS os campos de rescisão estiverem vazios, removemos a página
    # Nota: a combobox da operadora tem sempre um default, por isso NÃO conta como "preenchido".
    try:
        idx_res = 7 + shift_after_ports + shift_after_pf + shift_after_tit

        resc_text_keys = [
            "res_movel_1", "res_movel_2", "res_movel_3", "res_movel_4",
            "entry_res_nome", "entry_res_morada", "entry_res_cp4", "entry_res_cp3",
            "entry_res_localidade", "entry_res_cliente", "entry_res_fixo",
            "entry_res_int_movel",
        ]
        resc_flag_keys = ["res_srv_voz", "res_srv_internet", "res_srv_tv", "res_srv_movel", "res_srv_int_movel"]

        resc_any_preenchido = any((dados.get(k) or "").strip() for k in resc_text_keys) or any(bool(dados.get(k)) for k in resc_flag_keys)

        if not resc_any_preenchido:
            # Sem rescisão => remove a página para não ir em branco
            try:
                doc.delete_page(idx_res)
            except Exception:
                pass
        else:
            # Preencher normalmente
            try:
                page_7 = doc[idx_res]
                campos_7 = [
                    dados["res_movel_1"],
                    dados["res_movel_2"],
                    dados["res_movel_3"],
                    dados["res_movel_4"],
                    dados["entry_res_nome"],
                    dados["entry_res_morada"],
                    dados["entry_res_cp4"],
                    dados["entry_res_cp3"],
                    dados["entry_res_localidade"],
                    dados["combo_operadora_res"],
                    dados["entry_res_cliente"],
                    dados["entry_res_fixo"],
                    dia,
                    mes,
                    ano,
                    (dados["entry_res_localidade"] or "").lower(),
                    "X" if dados.get("res_srv_voz") else "",
                    "X" if dados.get("res_srv_internet") else "",
                    "X" if dados.get("res_srv_tv") else "",
                    "X" if dados.get("res_srv_movel") else "",
                    "X" if dados.get("res_srv_int_movel") else "",
                    dados.get("entry_res_int_movel",""),
                ]

                for (x, y), texto in zip(COORDS_7, campos_7):
                    if texto:
                        _pdf_insert(page_7, x, y, texto, fontsize=11)
                        
                # === NOME CINZENTO (Rescisão - 1º nome da página) ===
                nome_res = obter_primeiro_ultimo(dados.get("entry_res_nome", ""))
                if nome_res:
                    _pdf_insert(page_7, 75, 770, nome_res, fontsize=8, color=(0.6, 0.6, 0.6))
            except Exception:
                pass
    except Exception:
        pass


    # ===========================================================
    # JUNÇÃO DOS 3 PDFs (FOLHA DE ROSTO + RC + CONTRATO)
    # ===========================================================
    
    # 1. Obter o 1º e último nome do cliente para a folha de rosto
    nomes = dados["nome_completo"].split()
    if len(nomes) >= 2:
        primeiro_ultimo = f"{nomes[0]} {nomes[-1]}"
    else:
        primeiro_ultimo = dados["nome_completo"]

    # 2. Caminhos para as pastas
    pasta_rostos = "FOLHAS_ROSTO"
    pasta_rc = "RC"               
    
    caminho_rosto = f"{pasta_rostos}/folha_rosto.pdf"
    
    # ===========================================================
    # VERIFICAÇÃO DE RC DUPLICADA NA PASTA
    # ===========================================================
    num_rc = dados.get('num_rc', '').strip()
    ficheiros_encontrados = []
    caminho_rc = f"{pasta_rc}/{num_rc}.pdf" # Caminho por defeito
    
    # Só procura se a pasta existir e se o utilizador tiver preenchido o nº da RC
    if os.path.exists(pasta_rc) and num_rc:
        for ficheiro in os.listdir(pasta_rc):
            # Procura por ficheiros que comecem pelo nº da RC e sejam PDF
            if ficheiro.startswith(num_rc) and ficheiro.lower().endswith(".pdf"):
                ficheiros_encontrados.append(ficheiro)
    
    # Lógica de aviso se houver mais que um
    if len(ficheiros_encontrados) > 1:
        resposta = messagebox.askyesno(
            "RC Duplicada Encontrada!", 
            f"Atenção! Encontrei {len(ficheiros_encontrados)} ficheiros para a RC {num_rc} na pasta '{pasta_rc}'.\n"
            f"Isto acontece se descarregaste o ficheiro mais do que uma vez (ex: '{ficheiros_encontrados[1]}').\n\n"
            f"Queres continuar e usar o ficheiro '{ficheiros_encontrados[0]}' ou preferes cancelar (NÃO) para ires limpar a pasta primeiro?"
        )
        if not resposta:
            doc.close() # Importante: fechar o contrato base antes de cancelar
            return # CANCELA A GERAÇÃO AQUI MESMO
            
        caminho_rc = f"{pasta_rc}/{ficheiros_encontrados[0]}"
    elif len(ficheiros_encontrados) == 1:
        caminho_rc = f"{pasta_rc}/{ficheiros_encontrados[0]}"
    # ===========================================================

    # 3. Abrir e preencher Folha de Rosto
    if not os.path.exists(caminho_rosto):
        messagebox.showwarning("Aviso", f"Folha de rosto não encontrada:\n{caminho_rosto}\nO contrato vai ser gerado sem ela.")
        doc_rosto = None
    else:
        doc_rosto = fitz.open(caminho_rosto)
        try:
            # Página 1 (índice 0)
            page_rosto = doc_rosto[0] 
            
            # 3.1 Preencher o nome no topo
            _pdf_insert(page_rosto, 150, 115, primeiro_ultimo, fontsize=12)

            # 3.2 RECOLHER NIFS ÚNICOS E NOMES PARA O CC
            pessoas_cc = {}
            
            # --- NOVA LÓGICA: Extrair APENAS o 1º nome ---
            def formatar_nome(nome_completo):
                partes = nome_completo.split()
                if partes: # Se o nome não estiver vazio
                    return partes[0] # Vai buscar apenas a 1ª palavra
                return nome_completo
            # ----------------------------------------------------

            # Cliente principal
            nif_cli = dados.get("nif", "").strip()
            if nif_cli: 
                pessoas_cc[nif_cli] = formatar_nome(dados.get("nome_completo", "").strip())

            # Portabilidade Fixa
            nif_pf = dados.get("nif_pf", "").strip()
            if nif_pf: 
                pessoas_cc[nif_pf] = formatar_nome(dados.get("nome_pf", "").strip())

            # Portabilidade Móvel (1 a 4)
            for i in range(1, 5):
                n_mov = dados.get(f"pm_nif_{i}", "").strip()
                if n_mov: 
                    pessoas_cc[n_mov] = formatar_nome(dados.get(f"pm_nome_{i}", "").strip())

            # Alteração de Titularidade (NIF Antigo)
            for tit in dados.get("titularidades", []):
                n_tit = tit.get("nif_antigo", "").strip()
                if n_tit: 
                    pessoas_cc[n_tit] = formatar_nome(tit.get("tit_nome", "").strip())

            # 3.3 Escrever os nomes à frente de "cartão de cidadão"
            # Adiciona o " de Sr(a)s. " antes de juntar os nomes
            nomes_juntos = " de Sr(a)s. " + ", ".join(pessoas_cc.values())
            
            # Inserir com letra mais pequena (fontsize=8.5) e em Negrito (fontname="hebo")
            try:
                page_rosto.insert_text((373, 331), nomes_juntos, fontsize=8.5, fontname="hebo", color=(0, 0, 0))
            except Exception:
                # Fallback caso haja algum erro com a fonte bold, escreve normal mas pequenino
                _pdf_insert(page_rosto, 290, 385, nomes_juntos, fontsize=8.5)

        except Exception as e:
            print("Erro ao preencher rosto:", e)

    # 4. Abrir RC
    if not os.path.exists(caminho_rc):
        messagebox.showwarning("Aviso", f"RC não encontrada:\n{caminho_rc}\nO contrato vai ser gerado sem ela.")
        doc_rc = None
    else:
        doc_rc = fitz.open(caminho_rc)

    # 5. Juntar tudo num PDF final vazio
    doc_final = fitz.Document()
    
    # Ordem de inserção: 1º Folha de Rosto, 2º RC, 3º Contrato Vodafone
    if doc_rosto:
        doc_final.insert_pdf(doc_rosto)
    if doc_rc:
        doc_final.insert_pdf(doc_rc)
        
    doc_final.insert_pdf(doc) # Insere o Contrato que acabou de ser preenchido pela tua app

    # 6. Guardar o novo PDF com o nome pedido
    out_base = _safe_filename(f"{dados.get('nif', '')}_{dados.get('nome_completo', '')}")
    output_pdf = f"pdf_novo_{out_base}.pdf" 

    doc_final.save(output_pdf, garbage=4, deflate=True)

    # 7. Fechar os documentos da memória
    doc_final.close()
    if doc_rosto: doc_rosto.close()
    if doc_rc: doc_rc.close()
    doc.close()

    messagebox.showinfo("PRONTO!", f"Documento final (Rosto + RC + Contrato) gerado com sucesso!\n{output_pdf}")
    os.startfile(output_pdf)
# ======================= FUNÇÃO GERAR CONTRATO =======================

def gerar():
    global num_1, cvp_1, num_2, cvp_2, num_3, cvp_3, num_4, cvp_4
    global num1_meo, num2_meo, num3_meo, num4_meo
    global fixo_1, fixo_2, movel_1, movel_2, movel_3, movel_4
    global res_movel_1, res_movel_2, res_movel_3, res_movel_4

    fixo_1 = fixo_2 = movel_1 = movel_2 = movel_3 = movel_4 = ""
    res_movel_1 = res_movel_2 = res_movel_3 = res_movel_4 = ""

    # RESCISÃO (seguro contra index)
    res_movel_1 = res_telemoveis[0].get().strip() if len(res_telemoveis) > 0 else ""
    res_movel_2 = res_telemoveis[1].get().strip() if len(res_telemoveis) > 1 else ""
    res_movel_3 = res_telemoveis[2].get().strip() if len(res_telemoveis) > 2 else ""
    res_movel_4 = res_telemoveis[3].get().strip() if len(res_telemoveis) > 3 else ""

    # Reset por segurança (mantive como tinhas)
    num_1 = cvp_1 = ""
    num_2 = cvp_2 = ""
    num_3 = cvp_3 = ""
    num_4 = cvp_4 = ""

    num_1_meo = num_2_meo = num_3_meo = num_4_meo = ""


    # =========================
    # TELEFONES DO CONTRATO (agora vêm da Portabilidade Móvel com setinha)
    # - usa os primeiros 4 números preenchidos na Portabilidade Móvel
    # =========================
    tels_pm = ["", "", "", ""]
    gigas_pm = ["", "", "", ""]
    try:
        if "linhas_pm" in globals():
            pares = []
            for row in linhas_pm:
                n = (row.get("num").get() if row.get("num") else "").strip()
                g = (row.get("gigas").get() if row.get("gigas") else "").strip()
                if n:
                    pares.append((n, g))
            for i in range(min(4, len(pares))):
                tels_pm[i] = pares[i][0]
                gigas_pm[i] = pares[i][1]
    except Exception:
        pass

    # =========================
    # EXTRA: PORTABILIDADE MÓVEL (guardar CVP/KMAT/Operadora/Titular por linha 1..4)
    # =========================
    pm_ops = ["", "", "", ""]
    pm_nomes = ["", "", "", ""]
    pm_nifs = ["", "", "", ""]
    pm_cvps = ["", "", "", ""]
    pm_kmats = ["", "", "", ""]
    try:
        if "linhas_pm" in globals():
            for i in range(min(4, len(linhas_pm))):
                row = linhas_pm[i]
                pm_ops[i] = (row.get("op").get() if row.get("op") else "").strip()
                pm_nomes[i] = (row.get("nome").get() if row.get("nome") else "").strip()
                pm_nifs[i] = (row.get("nif").get() if row.get("nif") else "").strip()
                pm_cvps[i] = (row.get("cvp").get() if row.get("cvp") else "").strip()
                pm_kmats[i] = (row.get("kmat").get() if row.get("kmat") else "").strip()
    except Exception:
        pass

    dados = {
        "num_rc": entry_num_rc.get().strip(),
        "nome_completo": entry_nome.get().strip(),
        "nif": entry_nif.get().strip(),
        "rua_faturacao": entry_rua_faturacao.get().strip(),
        "cp4_faturacao": entry_cp4_faturacao.get().strip(),
        "cp3_faturacao": entry_cp3_faturacao.get().strip(),
        "localidade_faturacao": entry_localidade_faturacao.get().strip(),
        "rua_instalacao": entry_rua_instalacao.get().strip(),
        "cp4_instalacao": entry_cp4_instalacao.get().strip(),
        "cp3_instalacao": entry_cp3_instalacao.get().strip(),
        "localidade_instalacao": entry_localidade_instalacao.get().strip(),
        "contacto": entry_contacto.get().strip(),
        "email": entry_email.get().strip(),
        "plano_valor": entry_plano.get().strip(),
        "velocidade_internet": entry_velocidade.get().strip(),

        # Comercial (obrigatório - vai para PDF e BD, não mexe no Excel)
        "sfid_comercial": entry_sfid_comercial.get().strip(),
        "nome_comercial": entry_nome_comercial.get().strip(),

        # Perguntas obrigatórias (Supervisor)
        "oferta_novo_cliente": entry_oferta_novo.get().strip(),
        "plataforma_meses": entry_plataforma.get().strip(),
        "oferta_extra": entry_oferta_extra.get().strip(),
        "num_boxes_adicionais": combo_num_boxes.get().strip(),
        "valor_boxes_adicionais": entry_valor_boxes.get().strip() if combo_num_boxes.get().strip() not in ("", "0") else "",
        "origem_venda": combo_origem_venda.get().strip(),
        "origem_venda_outra": entry_origem_outra.get().strip() if combo_origem_venda.get().strip().startswith("Outra") else "",


        "telemovel1": tels_pm[0],
        "telemovel2": tels_pm[1],
        "telemovel3": tels_pm[2],
        "telemovel4": tels_pm[3],

        "net_movel1": "",
        "net_movel2": "",

        "gigas_min_telemovel1": gigas_pm[0],
        "gigas_min_telemovel2": gigas_pm[1],
        "gigas_min_telemovel3": gigas_pm[2],
        "gigas_min_telemovel4": gigas_pm[3],
        "gigas_net_movel1": "",
        "gigas_net_movel2": "",

        # NOTA: estes campos vinham de globais pm_gigas_1..4, que podem estar None
        # quando existe menos de 4 linhas de Portabilidade Móvel.
        # A origem correta (e segura) é a lista gigas_pm, construída a partir de linhas_pm.
        "pm_gigas_1": gigas_pm[0],
        "pm_gigas_2": gigas_pm[1],
        "pm_gigas_3": gigas_pm[2],
        "pm_gigas_4": gigas_pm[3],

        "pm_op_1": pm_ops[0], "pm_op_2": pm_ops[1], "pm_op_3": pm_ops[2], "pm_op_4": pm_ops[3],
        "pm_nome_1": pm_nomes[0], "pm_nome_2": pm_nomes[1], "pm_nome_3": pm_nomes[2], "pm_nome_4": pm_nomes[3],
        "pm_nif_1": pm_nifs[0], "pm_nif_2": pm_nifs[1], "pm_nif_3": pm_nifs[2], "pm_nif_4": pm_nifs[3],
        "pm_cvp_1": pm_cvps[0], "pm_cvp_2": pm_cvps[1], "pm_cvp_3": pm_cvps[2], "pm_cvp_4": pm_cvps[3],
        "pm_kmat_1": pm_kmats[0], "pm_kmat_2": pm_kmats[1], "pm_kmat_3": pm_kmats[2], "pm_kmat_4": pm_kmats[3],

        "fatura_eletronica": var_fatura_eletronica.get(),
        "fatura_tipo": combo_fatura.get(),
        "iban": entry_iban.get().strip(),
        "ntcb": entry_ntcb.get().strip(),
        "banco_nome": entry_banco.get().strip(),
        "pagamento_recorrente": var_pagamento_recorrente.get(),
        "ze_sem_ze": var_ze_sem_ze.get(),

        "fixo": fixo.get().strip(),

        # Portabilidade Fixa (Telefone Fixo)
        "nome_pf": entry_pf_nome.get().strip(),
        "nif_pf": entry_pf_nif.get().strip(),
        "contacto_pf": entry_pf_contacto.get().strip(),
        "operador_pf": combo_pf_operador.get().strip(),
        "fixo_pf": entry_pf_fixo.get().strip(),
        "cvp_pf": entry_pf_cvp.get().strip(),


        # Mantive estes campos como tinhas (não interferem com a nova portabilidade)
        "num_1": num_1, "cvp_1": cvp_1,
        "num_2": num_2, "cvp_2": cvp_2,
        "num_3": num_3, "cvp_3": cvp_3,
        "num_4": num_4, "cvp_4": cvp_4,
        "num1_meo": num_1_meo,
        "num2_meo": num_2_meo,
        "num3_meo": num_3_meo,
        "num4_meo": num_4_meo,

        # Titularidade
        "fixo_1": fixo_1,
        "fixo_2": fixo_2,
        "movel_1": movel_1,
        "movel_2": movel_2,
        "movel_3": movel_3,
        "movel_4": movel_4,
        "entry_nif_antigo": entry_nif_antigo.get().strip(),
        "entry_nif_novo": entry_nif_novo.get().strip(),
        "entry_tit_nome": entry_tit_nome.get().strip(),
        "entry_tit_conta": entry_tit_conta.get().strip(),

        # Rescisão
        "res_movel_1": res_movel_1,
        "res_movel_2": res_movel_2,
        "res_movel_3": res_movel_3,
        "res_movel_4": res_movel_4,
        "entry_res_nome": entry_res_nome.get().strip(),
        "entry_res_morada": entry_res_morada.get().strip(),
        "entry_res_cp4": entry_res_cp4.get().strip(),
        "entry_res_cp3": entry_res_cp3.get().strip(),
        "entry_res_localidade": entry_res_localidade.get().strip(),
        "combo_operadora_res": combo_operadora_res.get().strip(),
        "entry_res_cliente": entry_res_cliente.get().strip(),
        "entry_res_fixo": entry_res_fixo.get().strip(),
        "entry_res_int_movel": entry_res_int_movel.get().strip() if "entry_res_int_movel" in globals() else "",

        # Serviços a cancelar (Rescisão)
        "res_srv_voz": bool(res_srv_voz.get()) if "res_srv_voz" in globals() else False,
        "res_srv_internet": bool(res_srv_internet.get()) if "res_srv_internet" in globals() else False,
        "res_srv_tv": bool(res_srv_tv.get()) if "res_srv_tv" in globals() else False,
        "res_srv_movel": bool(res_srv_movel.get()) if "res_srv_movel" in globals() else False,
        "res_srv_int_movel": bool(res_srv_int_movel.get()) if "res_srv_int_movel" in globals() else False,
    }

    # =========================
    # NOVO: PORTABILIDADES AGRUPADAS
    # =========================
    try:
        dados["portabilidades"] = obter_portabilidades_agrupadas()
    except Exception:
        dados["portabilidades"] = []

    # =========================
    # NOVO: TITULARIDADES (por NIF antigo != NIF novo)
    # =========================
    try:
        nome_novo = dados.get("nome_completo", "")
        nif_novo = dados.get("nif", "")
        conta_nova = ""
        try:
            conta_nova = entry_tit_conta.get().strip()
        except Exception:
            conta_nova = ""
        dados["titularidades"] = obter_titularidades_agrupadas(
            nif_novo=nif_novo,
            conta_nova=conta_nova,
            nome_fallback=""
        )
    except Exception:
        dados["titularidades"] = []


    # Validação básica (mantive)
    if not dados["nome_completo"] or not dados["nif"]:
        messagebox.showwarning("Atenção", "Nome e NIF obrigatórios!")
        return

    # -----------------------------
    # Validação: Perguntas Obrigatórias (Oferta/Plataforma/Extra/Origem)
    # -----------------------------
    # Nota: Nº de boxes já tem default "0" e o valor das boxes é opcional.

    # Comercial (obrigatório - vai para PDF e BD, não mexe no Excel)
    if not (dados.get("sfid_comercial") or "").strip():
        messagebox.showwarning("Validação", "Preenche o SFID Comercial (Perguntas Obrigatórias).")
        try:
            entry_sfid_comercial.focus_set()
        except Exception:
            pass
        return

    if not (dados.get("nome_comercial") or "").strip():
        messagebox.showwarning("Validação", "Preenche o Nome Comercial (Perguntas Obrigatórias).")
        try:
            entry_nome_comercial.focus_set()
        except Exception:
            pass
        return

    if not (dados.get("oferta_novo_cliente") or "").strip():
        messagebox.showwarning("Validação", "Preenche a oferta do novo cliente (Perguntas Obrigatórias).")
        try:
            entry_oferta_novo_cliente.focus_set()
        except Exception:
            pass
        return

    if not (dados.get("plataforma_meses") or "").strip():
        messagebox.showwarning("Validação", "Preenche a plataforma (se tiver) e quantos meses (Perguntas Obrigatórias).")
        try:
            entry_plataforma_meses.focus_set()
        except Exception:
            pass
        return

    if not (dados.get("oferta_extra") or "").strip():
        messagebox.showwarning("Validação", "Preenche se ofereces-te alguma oferta extra ao cliente (Perguntas Obrigatórias).")
        try:
            entry_oferta_extra.focus_set()
        except Exception:
            pass
        return

    origem = (dados.get("origem_venda") or "").strip()
    if not origem:
        messagebox.showwarning("Validação", "Seleciona a origem da venda (Perguntas Obrigatórias).")
        try:
            combo_origem_venda.focus_set()
        except Exception:
            pass
        return

    # Se a origem for "OUTRA", obriga a descrição
    if origem.upper().startswith("OUTRA"):
        outra_txt = (dados.get("origem_venda_outra") or "").strip()
        if not outra_txt:
            messagebox.showwarning("Validação", "Selecionaste 'OUTRA' na origem da venda. Escreve qual é a origem.")
            try:
                entry_origem_outra.focus_set()
            except Exception:
                pass
            return


    nome_vendedor, sfid = ler_dados()

    try:
        inserir_vendedor(nome_vendedor, sfid)
        # Guarda tudo na RC (com colunas auto-criadas se faltarem)
        dados["rc_id"] = globals().get("CURRENT_RC_ID")
        try:
            dados["mac"] = int(globals().get("CURRENT_VENDEDOR_MAC", 0) or 0)
        except Exception:
            dados["mac"] = 0
        rc_id_saved = upsert_rc(dados, nome_vendedor, sfid)
        try:
            dados["rc_id"] = rc_id_saved
        except Exception:
            pass

        # Guardar flag do vendedor (Mac) no registo RC
        try:
            dados["mac"] = int(globals().get("CURRENT_VENDEDOR_MAC", 0) or 0)
        except Exception:
            dados["mac"] = 0

        # Exportar perguntas obrigatórias para Google Sheets (não bloqueia o fluxo)
        # Regras:
        #   - Só Windows (runtime) e vendedores com mac=0 exportam diretamente
        #   - RCs criados por utilizadores Mac (mac=1) ficam pendentes (sheets_status=0)
        #   - Em Windows, a app também faz "bridge" e tenta enviar RCs pendentes de Mac de 15 em 15 minutos
        try:
            if _can_export_sheets():
                ok_gsh, msg_gsh = exportar_perguntas_para_google_sheets(dados)
                if ok_gsh:
                    try:
                        ts_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        _rc_set_sheets_status(int(rc_id_saved), 1, ts_now, None)
                    except Exception:
                        pass
                else:
                    try:
                        _rc_set_sheets_status(int(rc_id_saved), 0, None, str(msg_gsh)[:4000])
                    except Exception:
                        pass
                    # Aviso leve: não impede gerar contrato
                    try:
                        messagebox.showwarning("Google Sheets", msg_gsh)
                    except Exception:
                        pass

                # Trigger extra: aproveita e tenta enviar alguns RCs pendentes de Mac em background
                _trigger_sync_mac_rc_background(limit=4)

            else:
                # Bypass (macOS ou vendedor marcado como mac=1). Deixa pendente para o bridge Windows.
                try:
                    _rc_set_sheets_status(int(rc_id_saved), 0, None, "Pendente: export para Sheets será feito por um Windows (bridge).")
                except Exception:
                    pass
        except Exception:
            pass
        except Exception:
            pass


    except Exception as e:
        messagebox.showerror("Erro BD", f"Erro ao gravar na base de dados:\n{e}")
        return

    preencher_pdf(dados)
    messagebox.showinfo("Sucesso", "Contrato atualizado/gerado com sucesso!")


# ======================= FUNÇÃO PRINCIPAL (INTERFACE) =======================
def abrir_interface(nome, ssfid, janela=None):
    """Abre a interface principal.

    Nota: se 'janela' for fornecida, a interface é construída na mesma janela
    (evita destruir a aplicação e erros de estilo do ttkbootstrap).
    """

    if janela is None:
        janela = tk.Tk()
    else:
        # Reutilizar a janela do login: remover todos os widgets existentes
        try:
            for w in list(janela.winfo_children()):
                w.destroy()
        except Exception:
            pass
        try:
            janela.deiconify()
        except Exception:
            pass

    janela.title("Aplicação Vodafone")
    janela.geometry("950x800")

    # ===================== SHEETS BRIDGE (SCHEDULER) =====================
    # Só corre em Windows e apenas para vendedores com mac=0.
    # Faz um sync logo no arranque e depois a cada 15 minutos (não bloqueia a UI).
    def _start_sheets_bridge_scheduler():
        if not _can_export_sheets():
            return

        def _tick():
            _trigger_sync_mac_rc_background(limit=6)
            try:
                janela.after(15 * 60 * 1000, _tick)
            except Exception:
                pass

        # Run inicial (pequeno delay para a UI abrir)
        try:
            janela.after(2000, _tick)
        except Exception:
            pass

    _start_sheets_bridge_scheduler()


    # ===================== VODAFONE LOOK & FEEL =====================
    VDF_RED = "#E60000"
    VDF_RED_DARK = "#B80000"
    VDF_WHITE = "#FFFFFF"
    VDF_BG = "#F6F7F9"
    VDF_CARD = "#FFFFFF"
    VDF_TEXT = "#111111"
    VDF_MUTED = "#6B7280"
    VDF_BORDER = "#E5E7EB"

    janela.configure(bg=VDF_BG)

    style = ttk.Style(janela)
    # Escolher um tema base mais "clean" quando disponível
    try:
        style.theme_use("clam")
    except Exception:
        pass

    # Fonte base (mantém compatibilidade com o que já tens)
    BASE_FONT = ("Arial", 11)
    TITLE_FONT = ("Arial", 18, "bold")
    SECTION_FONT = ("Arial", 11, "bold")
    SUBSECTION_FONT = ("Arial", 10, "bold")

    # Estilos ttk (Vodafone: vermelho + branco, cantos/alto contraste)
    style.configure(".", font=BASE_FONT)
    style.configure("TFrame", background=VDF_BG)
    style.configure("Card.TFrame", background=VDF_CARD, relief="flat")
    style.configure("TLabel", background=VDF_BG, foreground=VDF_TEXT)
    style.configure("Muted.TLabel", background=VDF_BG, foreground=VDF_MUTED)
    style.configure("Title.TLabel", background=VDF_BG, foreground=VDF_RED, font=TITLE_FONT)

    style.configure("TNotebook", background=VDF_BG, borderwidth=0)
    style.configure("TNotebook.Tab", padding=(14, 10), font=("Arial", 10, "bold"))
    style.map("TNotebook.Tab",
              background=[("selected", VDF_WHITE)],
              foreground=[("selected", VDF_RED), ("!selected", VDF_TEXT)])

    style.configure("TSeparator", background=VDF_BORDER)

    style.configure("TEntry", padding=(10, 8))
    style.map("TEntry",
              fieldbackground=[("!disabled", VDF_WHITE)],
              bordercolor=[("focus", VDF_RED), ("!focus", VDF_BORDER)],
              lightcolor=[("focus", VDF_RED), ("!focus", VDF_BORDER)],
              darkcolor=[("focus", VDF_RED), ("!focus", VDF_BORDER)])

    style.configure("Primary.TButton",
                    font=("Arial", 11, "bold"),
                    padding=(16, 10),
                    background=VDF_RED,
                    foreground=VDF_WHITE,
                    borderwidth=0)
    style.map("Primary.TButton",
              background=[("active", VDF_RED_DARK), ("pressed", VDF_RED_DARK)],
              foreground=[("active", VDF_WHITE), ("pressed", VDF_WHITE)])

    style.configure("Ghost.TButton",
                    font=("Arial", 10, "bold"),
                    padding=(12, 8),
                    background=VDF_WHITE,
                    foreground=VDF_RED,
                    borderwidth=1)
    style.map("Ghost.TButton",
              background=[("active", "#FFF1F1"), ("pressed", "#FFE4E4")],
              foreground=[("active", VDF_RED_DARK), ("pressed", VDF_RED_DARK)])

    style.configure("Section.TLabel", background=VDF_CARD, foreground=VDF_TEXT, font=SECTION_FONT)
    style.configure("SubSection.TLabel", background=VDF_CARD, foreground=VDF_TEXT, font=SUBSECTION_FONT)
    style.configure("CardMuted.TLabel", background=VDF_CARD, foreground=VDF_MUTED)

    style.configure("Card.TSeparator", background=VDF_BORDER)

    # Helper: hover “dinâmico” nos botões ttk (sem mexer na tua lógica)
    def _bind_hover(btn, normal_style, hover_style=None):
        # ttk não tem hover por estilo universal; usamos map já feito,
        # mas ainda assim podemos dar cursor e micro-feedback
        try:
            btn.configure(cursor="hand2")
        except Exception:
            pass

    # ===================== LAYOUT BASE =====================

    # Notebook com abas
    notebook = ttk.Notebook(janela)
    notebook.pack(fill="both", expand=True, padx=14, pady=14)

    aba_gerar = ttk.Frame(notebook)
    aba_lista = ttk.Frame(notebook)
    aba_melhorias = ttk.Frame(notebook)


    aba_eurus = ttk.Frame(notebook)
    notebook.add(aba_gerar, text="📝 Gerar Contrato")
    notebook.add(aba_lista, text="📂 Contratos Gerados")
    notebook.add(aba_melhorias, text="🛠 Melhorias")

    
    notebook.add(aba_eurus, text="🤖 EURUS")

    # ===================== ABA EURUS =====================
    # Chat EURUS (via servidor HTTP — recomendado usar Tailscale)

    def eurus_call_chat(base_url: str, message: str, history: list):
        payload = {"message": message, "history": history or []}
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")

        req = urllib.request.Request(
            base_url.rstrip("/") + "/chat",
            data=data,
            method="POST",
            headers={"Content-Type": "application/json; charset=utf-8"}
        )
        if EURUS_API_TOKEN:
            req.add_header("X-API-KEY", EURUS_API_TOKEN)

        with urllib.request.urlopen(req, timeout=EURUS_TIMEOUT) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
            return json.loads(raw)

    def eurus_health(base_url: str):
        req = urllib.request.Request(base_url.rstrip("/") + "/health", method="GET")
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read().decode("utf-8", errors="ignore"))

    # --- Scroll da aba EURUS (para ecrãs pequenos) ---
    eurus_outer = ttk.Frame(aba_eurus)
    eurus_outer.pack(fill="both", expand=True)

    eurus_canvas = tk.Canvas(eurus_outer, highlightthickness=0, bd=0)
    eurus_vsb = ttk.Scrollbar(eurus_outer, orient="vertical", command=eurus_canvas.yview)
    eurus_canvas.configure(yscrollcommand=eurus_vsb.set)

    eurus_vsb.pack(side="right", fill="y")
    eurus_canvas.pack(side="left", fill="both", expand=True)

    eurus_scroll_frame = ttk.Frame(eurus_canvas)
    eurus_window_id = eurus_canvas.create_window((0, 0), window=eurus_scroll_frame, anchor="nw")

    def _eurus_on_frame_configure(event=None):
        try:
            eurus_canvas.configure(scrollregion=eurus_canvas.bbox("all"))
        except Exception:
            pass

    def _eurus_on_canvas_configure(event):
        # Faz o frame interno acompanhar a largura do canvas
        try:
            eurus_canvas.itemconfigure(eurus_window_id, width=event.width)
        except Exception:
            pass

    eurus_scroll_frame.bind("<Configure>", _eurus_on_frame_configure)
    eurus_canvas.bind("<Configure>", _eurus_on_canvas_configure)

    # Scroll com roda do rato (apenas quando o cursor está na aba EURUS)
    def _eurus_mousewheel(event):
        try:
            delta = getattr(event, "delta", 0)
            if delta:
                eurus_canvas.yview_scroll(int(-1 * (delta / 120)), "units")
            else:
                num = getattr(event, "num", None)
                if num == 4:
                    eurus_canvas.yview_scroll(-3, "units")
                elif num == 5:
                    eurus_canvas.yview_scroll(3, "units")
        except Exception:
            pass

    def _eurus_focus(event=None):
        try:
            eurus_canvas.focus_set()
        except Exception:
            pass

    eurus_canvas.bind("<Enter>", _eurus_focus)
    eurus_canvas.bind("<MouseWheel>", _eurus_mousewheel)
    eurus_canvas.bind("<Button-4>", _eurus_mousewheel)
    eurus_canvas.bind("<Button-5>", _eurus_mousewheel)

    header_eu = ttk.Frame(eurus_scroll_frame)
    header_eu.pack(fill="x", padx=14, pady=14)

    ttk.Label(
        header_eu,
        text="EURUS — Assistente IA Versao 1.0",
        font=("Arial", 16, "bold"),
        foreground=VDF_RED,
        background=VDF_BG
    ).pack(anchor="w")

    ttk.Label(
        header_eu,
        text="Faz perguntas rápidas sobre processos, regras e dúvidas.",
        style="Muted.TLabel"
    ).pack(anchor="w", pady=(4, 0))

    card_eu = tk.Frame(eurus_scroll_frame, bg=VDF_CARD, bd=1, relief="solid", highlightthickness=0)
    card_eu.pack(fill="both", expand=True, padx=14, pady=(0, 14))

    # Linha de configuração (servidor oculto)
    top_cfg = tk.Frame(card_eu, bg=VDF_CARD)
    top_cfg.pack(fill="x", padx=12, pady=(12, 6))

    # Default do servidor (Tailscale). NÃO mostramos o URL ao utilizador.
    eurus_url_var = tk.StringVar(value="http://100.73.127.32:8000/")

    ttk.Label(top_cfg, text="Servidor EURUS:", style="CardMuted.TLabel").pack(side="left")
    ttk.Label(top_cfg, text="Privado (Tailscale)", style="CardMuted.TLabel").pack(side="left", padx=(8, 10))

    lbl_status = ttk.Label(top_cfg, text="Status: a verificar…", style="CardMuted.TLabel")
    lbl_status.pack(side="left", padx=(0, 10))

    # Chat box (read-only) + scrollbar
    chat_wrap = tk.Frame(card_eu, bg=VDF_CARD)
    chat_wrap.pack(fill="both", expand=True, padx=12, pady=(0, 10))

    chat_scroll = ttk.Scrollbar(chat_wrap, orient="vertical")
    chat_scroll.pack(side="right", fill="y")

    chat_box = tk.Text(
        chat_wrap,
        wrap="word",
        bd=0,
        highlightthickness=0,
        height=18,
        font=("Segoe UI", 11),
        padx=10,
        pady=10,
        yscrollcommand=chat_scroll.set
    )
    chat_box.pack(side="left", fill="both", expand=True)
    chat_scroll.config(command=chat_box.yview)

    # Melhor espaçamento (fica menos "cru")
    try:
        chat_box.configure(spacing1=2, spacing2=4, spacing3=6)
    except Exception:
        pass

    # Tags para deixar o chat mais legível
    try:
        chat_box.tag_configure("who_user", font=("Segoe UI", 11, "bold"))
        chat_box.tag_configure("who_bot", font=("Segoe UI", 11, "bold"))
        chat_box.tag_configure("msg", font=("Segoe UI", 11))
    except Exception:
        pass

    chat_box.configure(state="disabled")

    bottom = tk.Frame(card_eu, bg=VDF_CARD)
    bottom.pack(fill="x", padx=12, pady=(0, 12))

    entry_msg = ttk.Entry(bottom)
    entry_msg.pack(side="left", fill="x", expand=True, padx=(0, 10))

    btn_send = ttk.Button(bottom, text="Enviar")
    btn_send.pack(side="right")

    # Se a janela for pequena, o utilizador consegue fazer scroll até ao input.
    # Também permite Enter para enviar.
    try:
        entry_msg.bind("<Return>", lambda e: btn_send.invoke())
    except Exception:
        pass

    eurus_history = []  # lista de turnos (role/content) enviada ao servidor

    def _chat_append(who: str, text: str):
        """
        Append ao chat com quebras de linha reais e tags (mais legível).
        Nota: alguns modelos devolvem o texto \n (duas letras) em vez de quebra de linha.
        """
        try:
            msg = (text or "")
            msg = msg.replace("\r\n", "\n").replace("\\n", "\n").strip()

            chat_box.configure(state="normal")

            if who.lower().startswith("tu"):
                chat_box.insert("end", f"{who}: ", ("who_user",))
            else:
                chat_box.insert("end", f"{who}: ", ("who_bot",))

            chat_box.insert("end", msg + "\n\n", ("msg",))
            chat_box.see("end")
            chat_box.configure(state="disabled")
        except Exception:
            pass

    def _set_status(s: str):
        try:
            lbl_status.configure(text=s)
        except Exception:
            pass

    def _safe_after(ms: int, func):
        try:
            janela.after(ms, func)
        except RuntimeError:
            # janela ainda não está em mainloop ou já fechou
            pass
        except Exception:
            pass

    def _do_ping():
        base_url = (eurus_url_var.get() or "").strip()
        if not base_url:
            _set_status("Status: URL vazia")
            return

        _set_status("Status: a verificar…")

        def worker():
            try:
                h = eurus_health(base_url)
                model = h.get("model", "?")
                _safe_after(0, lambda model=model: _set_status(f"Status: ONLINE (model={model})"))
            except Exception as e:
                err = str(e)
                _safe_after(0, lambda err=err: _set_status(f"Status: OFFLINE ({err})"))

        threading.Thread(target=worker, daemon=True).start()

    def _send_message():
        msg = entry_msg.get().strip()
        if not msg:
            return

        entry_msg.delete(0, "end")
        _chat_append("Tu", msg)

        base_url = (eurus_url_var.get() or "").strip()

        def worker():
            try:
                res = eurus_call_chat(base_url, msg, eurus_history)
                # res: {ok: bool, reply: str, decision: {...}}
                if isinstance(res, dict):
                    ok = bool(res.get("ok", False))
                    reply = (res.get("reply") or "").strip()
                    if ok:
                        eurus_history.append({"role": "user", "content": msg})
                        eurus_history.append({"role": "assistant", "content": reply})
                        _safe_after(0, lambda r=reply: _chat_append("EURUS", r))
                    else:
                        err = res.get("error") or "Erro desconhecido"
                        _safe_after(0, lambda err=err: _chat_append("EURUS", f"Erro: {err}"))
                else:
                    _safe_after(0, lambda: _chat_append("EURUS", "Erro: resposta inválida do servidor."))
            except Exception as e:
                err = str(e)
                _safe_after(0, lambda err=err: _chat_append("EURUS", f"Erro: {err}"))

        threading.Thread(target=worker, daemon=True).start()

    btn_send.configure(command=_send_message)

    def _ping_btn():
        _do_ping()

    btn_ping = ttk.Button(top_cfg, text="Testar ligação", command=_ping_btn)
    btn_ping.pack(side="right")

    # Mensagem inicial
    _chat_append("EURUS", "Em manutençao.")
    #Olá! sou o modelo de AI feito para te ajudar !.

    # Ping inicial (em background)
    _do_ping()
    # ===================== ABA MELHORIAS
    # Área para o colaborador submeter sugestões/bugs diretamente para a BD (tabela: melhorias)

    header_m = ttk.Frame(aba_melhorias)
    header_m.pack(fill="x", padx=14, pady=14)

    ttk.Label(header_m, text="Melhorias", font=("Arial", 16, "bold"),
              foreground=VDF_RED, background=VDF_BG).pack(anchor="w")
    ttk.Label(header_m, text="Submete sugestões, bugs ou pedidos de melhoria. A mensagem será guardada na base de dados.",
              style="Muted.TLabel").pack(anchor="w", pady=(4, 0))

    card_m = tk.Frame(aba_melhorias, bg=VDF_CARD, bd=1, relief="solid", highlightthickness=0)
    card_m.pack(fill="both", expand=False, padx=14, pady=(0, 14))

    # SFID
    row_ms = tk.Frame(card_m, bg=VDF_CARD)
    row_ms.pack(fill="x", padx=12, pady=(12, 6))
    ttk.Label(row_ms, text="SFID", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
    entry_m_sfid = ttk.Entry(row_ms, width=30)
    entry_m_sfid.pack(side="left", padx=10)
    # bloquear edição (apenas leitura)
    entry_m_sfid.insert(0, str(ssfid).strip())
    entry_m_sfid.configure(state="readonly")

    # Título
    row_mt = tk.Frame(card_m, bg=VDF_CARD)
    row_mt.pack(fill="x", padx=12, pady=6)
    ttk.Label(row_mt, text="Título", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
    entry_m_titulo = ttk.Entry(row_mt, width=60)
    entry_m_titulo.pack(side="left", padx=10)

    # Texto (multi-linha)
    row_mx = tk.Frame(card_m, bg=VDF_CARD)
    row_mx.pack(fill="both", expand=True, padx=12, pady=6)
    ttk.Label(row_mx, text="Texto", background=VDF_CARD, foreground=VDF_TEXT).pack(anchor="w")
    txt_m_texto = tk.Text(row_mx, height=10, wrap="word", bd=1, relief="solid")
    txt_m_texto.pack(fill="both", expand=True, pady=(6, 0))

    def submeter_melhoria():
        sfid_local = entry_m_sfid.get().strip()
        titulo_local = entry_m_titulo.get().strip()
        texto_local = txt_m_texto.get("1.0", "end").strip()

        if not sfid_local:
            messagebox.showwarning("Validação", "Preenche o SFID.")
            return
        if not titulo_local:
            messagebox.showwarning("Validação", "Preenche o Título.")
            return
        if not texto_local:
            messagebox.showwarning("Validação", "Escreve a mensagem no campo Texto.")
            return

        try:
            inserir_melhoria(sfid_local, titulo_local, texto_local)
        except Exception as e:
            messagebox.showerror("Erro", f"Não foi possível submeter a melhoria.\n\nDetalhes: {e}")
            return

        # Trigger: em Windows (mac=0) tenta também despachar RCs pendentes de Mac para Sheets
        try:
            _trigger_sync_mac_rc_background(limit=2)
        except Exception:
            pass

        messagebox.showinfo("Submetido", "A tua mensagem foi submetida com sucesso.")
        entry_m_titulo.delete(0, "end")
        txt_m_texto.delete("1.0", "end")

        # Atualizar lista (se existir na UI)
        try:
            _carregar_lista_melhorias()
        except Exception:
            pass

    btn_row = tk.Frame(card_m, bg=VDF_CARD)
    btn_row.pack(fill="x", padx=12, pady=(10, 12))
    ttk.Button(btn_row, text="SUBMETER", style="Primary.TButton", command=submeter_melhoria).pack(anchor="e")

    # ===================== LISTA DE MELHORIAS + RESPOSTA BACKOFFICE =====================
    # Mostra apenas as melhorias do próprio SFID e, caso exista resposta, permite visualizar.
    # Quando o utilizador clica em "Ver Resposta", marca visualizacao=1 e o ponto vermelho desaparece.

    # Ponto vermelho (avisador) — só aparece se existir resposta por ver
    dot_row = tk.Frame(aba_melhorias, bg=VDF_BG)
    dot_row.pack(fill="x", padx=14, pady=(0, 4))

    lbl_dot = tk.Label(dot_row, text="●", fg="#D10000", bg=VDF_BG, font=("Arial", 14, "bold"))
    lbl_dot.pack(side="left")
    lbl_dot.pack_forget()  # começa escondido

    lbl_dot_txt = ttk.Label(dot_row, text="Tens resposta(s) do BackOffice por visualizar.", style="Muted.TLabel")
    lbl_dot_txt.pack(side="left", padx=(8, 0))
    lbl_dot_txt.pack_forget()

    # Lista
    lista_card = tk.Frame(aba_melhorias, bg=VDF_CARD, bd=1, relief="solid", highlightthickness=0)
    lista_card.pack(fill="both", expand=False, padx=14, pady=(0, 10))

    ttk.Label(lista_card, text="As tuas melhorias", background=VDF_CARD, foreground=VDF_TEXT,
              font=("Arial", 11, "bold")).pack(anchor="w", padx=12, pady=(10, 6))

    tree_frame = tk.Frame(lista_card, bg=VDF_CARD)
    tree_frame.pack(fill="both", expand=True, padx=12, pady=(0, 10))

    cols = ("id", "titulo", "texto")
    tree_m = ttk.Treeview(tree_frame, columns=cols, show="headings", height=5)
    tree_m.heading("id", text="ID")
    tree_m.heading("titulo", text="Título")
    tree_m.heading("texto", text="Texto")
    tree_m.column("id", width=60, anchor="center")
    tree_m.column("titulo", width=260, anchor="w")
    tree_m.column("texto", width=520, anchor="w")

    sb_y = ttk.Scrollbar(tree_frame, orient="vertical", command=tree_m.yview)
    tree_m.configure(yscrollcommand=sb_y.set)
    tree_m.pack(side="left", fill="both", expand=True)
    sb_y.pack(side="right", fill="y")

    # Resposta BackOffice (só aparece quando existir resposta na linha selecionada)
    resp_card = tk.Frame(aba_melhorias, bg=VDF_CARD, bd=1, relief="solid", highlightthickness=0)
    resp_card.pack(fill="both", expand=False, padx=14, pady=(0, 14))
    resp_card.pack_forget()

    # Cabeçalho
    resp_header = tk.Frame(resp_card, bg=VDF_CARD)
    resp_header.pack(fill="x", padx=12, pady=(10, 6))

    ttk.Label(
        resp_header,
        text="Resposta da Back office",
        background=VDF_CARD,
        foreground=VDF_TEXT,
        font=("Arial", 11, "bold")
    ).pack(side="left")

    ttk.Label(
        resp_header,
        text="(detalhe da melhoria selecionada)",
        style="Muted.TLabel"
    ).pack(side="left", padx=(10, 0))

    # Corpo: Título, Texto, Resposta
    resp_body = tk.Frame(resp_card, bg=VDF_CARD)
    resp_body.pack(fill="both", expand=True, padx=12, pady=(0, 10))

    # Linha: Título
    row_rt = tk.Frame(resp_body, bg=VDF_CARD)
    row_rt.pack(fill="x", pady=(0, 6))
    ttk.Label(row_rt, text="Título:", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
    entry_resp_titulo = ttk.Entry(row_rt, width=80)
    entry_resp_titulo.pack(side="left", padx=(10, 0), fill="x", expand=True)
    entry_resp_titulo.configure(state="readonly")

    # Caixa: Texto (do pedido)
    ttk.Label(resp_body, text="Texto:", background=VDF_CARD, foreground=VDF_TEXT).pack(anchor="w")
    txt_resp_texto = tk.Text(resp_body, height=4, wrap="word", bd=1, relief="solid")
    txt_resp_texto.pack(fill="both", expand=False, pady=(6, 10))
    txt_resp_texto.configure(state="disabled")

    # Caixa: Resposta (BackOffice)
    ttk.Label(resp_body, text="Resposta:", background=VDF_CARD, foreground=VDF_TEXT).pack(anchor="w")
    txt_resp = tk.Text(resp_body, height=6, wrap="word", bd=1, relief="solid")
    txt_resp.pack(fill="both", expand=True, pady=(6, 0))
    txt_resp.configure(state="disabled")

    btn_resp_row = tk.Frame(resp_card, bg=VDF_CARD)
    btn_resp_row.pack(fill="x", padx=12, pady=(8, 12))

    # Guardamos info da seleção atual
    selected_melhoria = {"id": None, "resposta": "", "visualizacao": 0, "titulo": "", "texto": ""}

    def _refresh_dot():
        try:
            n = contar_respostas_nao_vistas(str(ssfid).strip())
        except Exception:
            n = 0

        if n > 0:
            try:
                lbl_dot.pack(side="left")
                lbl_dot_txt.pack(side="left", padx=(8, 0))
            except Exception:
                pass
        else:
            try:
                lbl_dot.pack_forget()
                lbl_dot_txt.pack_forget()
            except Exception:
                pass

    def _carregar_lista_melhorias():
        # limpar
        for item in tree_m.get_children():
            tree_m.delete(item)

        try:
            rows = listar_melhorias_por_sfid(str(ssfid).strip())
        except Exception as e:
            # não bloqueia o resto da UI
            rows = []

        # Guardar em tags/dicionário para acesso rápido (id -> (resposta, visualizacao))
        tree_m._meta = {}
        for r in rows:
            # r = (id, sfid, titulo, texto, resposta, visualizacao)
            mid = r[0]
            titulo = r[2] or ""
            texto = (r[3] or "").replace("\n", " ").strip()
            resposta = r[4] or ""
            visual = int(r[5] or 0)

            tree_m.insert("", "end", values=(mid, titulo, texto[:180] + ("..." if len(texto) > 180 else "")))
            tree_m._meta[str(mid)] = {"resposta": resposta, "visualizacao": visual, "titulo": titulo, "texto": (r[3] or "")}

        _refresh_dot()

    def _on_select_melhoria(event=None):
        sel = tree_m.selection()
        if not sel:
            return
        vals = tree_m.item(sel[0], "values") or []
        if not vals:
            return

        mid = str(vals[0])
        meta = (getattr(tree_m, "_meta", {}) or {}).get(mid, {})
        resposta = (meta.get("resposta") or "").strip()
        visual = int(meta.get("visualizacao") or 0)
        titulo_full = (meta.get("titulo") or "").strip()
        texto_full = (meta.get("texto") or "")

        selected_melhoria["id"] = mid
        selected_melhoria["resposta"] = resposta
        selected_melhoria["visualizacao"] = visual
        selected_melhoria["titulo"] = titulo_full
        selected_melhoria["texto"] = texto_full

        if resposta:
            # mostrar card com resposta (layout completo: título + texto + resposta)
            try:
                entry_resp_titulo.configure(state="normal")
                entry_resp_titulo.delete(0, "end")
                entry_resp_titulo.insert(0, titulo_full)
                entry_resp_titulo.configure(state="readonly")
            except Exception:
                pass

            try:
                txt_resp_texto.configure(state="normal")
                txt_resp_texto.delete("1.0", "end")
                txt_resp_texto.insert("1.0", (texto_full or "").strip())
                txt_resp_texto.configure(state="disabled")
            except Exception:
                pass

            txt_resp.configure(state="normal")
            txt_resp.delete("1.0", "end")
            txt_resp.insert("1.0", resposta)
            txt_resp.configure(state="disabled")
            try:
                resp_card.pack(fill="both", expand=False, padx=14, pady=(0, 14))
            except Exception:
                pass
        else:
            # se não há resposta, esconde
            try:
                # limpar conteúdos para não ficar "resto" visual
                try:
                    entry_resp_titulo.configure(state="normal")
                    entry_resp_titulo.delete(0, "end")
                    entry_resp_titulo.configure(state="readonly")
                except Exception:
                    pass
                try:
                    txt_resp_texto.configure(state="normal")
                    txt_resp_texto.delete("1.0", "end")
                    txt_resp_texto.configure(state="disabled")
                except Exception:
                    pass
                try:
                    txt_resp.configure(state="normal")
                    txt_resp.delete("1.0", "end")
                    txt_resp.configure(state="disabled")
                except Exception:
                    pass

                resp_card.pack_forget()
            except Exception:
                pass

    def _ver_resposta_e_marcar():
        mid = selected_melhoria.get("id")
        resposta = (selected_melhoria.get("resposta") or "").strip()
        if not mid or not resposta:
            return

        # Marcar visualização apenas se ainda não foi marcado
        if int(selected_melhoria.get("visualizacao") or 0) == 0:
            try:
                marcar_melhoria_como_visualizada(int(mid), str(ssfid).strip())
            except Exception:
                pass

        # refrescar lista e dot
        _carregar_lista_melhorias()

        # manter resposta visível
        _on_select_melhoria()

    btn_ver_resp = ttk.Button(
        btn_resp_row,
        text="Ver resposta / marcar como lida",
        style="Primary.TButton",
        command=_ver_resposta_e_marcar
    )
    btn_ver_resp.pack(anchor="e")

    tree_m.bind("<<TreeviewSelect>>", _on_select_melhoria)

    # Carregar lista ao abrir a aba
    _carregar_lista_melhorias()



    # Canvas e scrollbar dentro da aba_gerar
    canvas = tk.Canvas(aba_gerar, borderwidth=0, highlightthickness=0, bg=VDF_BG)
    scrollbar = ttk.Scrollbar(aba_gerar, orient="vertical", command=canvas.yview)
    canvas.configure(yscrollcommand=scrollbar.set)

    canvas.grid(row=0, column=0, sticky="nsew")
    scrollbar.grid(row=0, column=1, sticky="ns")

    # Expandir corretamente
    aba_gerar.grid_rowconfigure(0, weight=1)
    aba_gerar.grid_columnconfigure(0, weight=1)

    # Frame interno para todo o conteúdo
    main_frame = ttk.Frame(canvas)
    canvas.create_window((0, 0), window=main_frame, anchor="nw")

    def ajustar_scroll(event):
        canvas.configure(scrollregion=canvas.bbox("all"))
    main_frame.bind("<Configure>", ajustar_scroll)

    def scroll_rato(event):
        canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
    canvas.bind_all("<MouseWheel>", scroll_rato)

    # Header / Hero
    header = ttk.Frame(main_frame)
    header.pack(fill="x", padx=8, pady=(6, 10))

    # Imagem/Logo (opcional) — coloca aqui as imagens que enviaste (basta estarem na mesma pasta do .py/.exe)
    try:
        img_path = os.path.join(os.path.dirname(__file__), "Screenshot_4.png")
        if os.path.exists(img_path):
            _img_header = tk.PhotoImage(file=img_path)
            lbl_img = tk.Label(header, image=_img_header, bg=VDF_BG)
            lbl_img.image = _img_header  # manter referência
            lbl_img.pack(side="left", padx=(0, 12))
    except Exception:
        pass

    title_box = tk.Frame(header, bg=VDF_BG)
    title_box.pack(side="left", fill="x", expand=True)


    def resetar_formulario():
        """Limpa todos os campos do formulário (aba Gerar Contrato) e cancela modo edição (remove CURRENT_RC_ID)."""
        # Cancelar modo de edição
        try:
            globals()['CURRENT_RC_ID'] = None
        except Exception:
            pass

        def _limpar_recursivo(widget):
            # Limpar inputs conhecidos
            try:
                # Entries (tk e ttk)
                if isinstance(widget, (ttk.Entry, tk.Entry)):
                    widget.delete(0, "end")

                # Combobox
                elif isinstance(widget, ttk.Combobox):
                    try:
                        valores = widget.cget("values") or ()
                    except Exception:
                        valores = ()
                    # se existir "0" nas opções (ex.: nº boxes), volta a 0; caso contrário fica vazio
                    try:
                        if "0" in valores:
                            widget.set("0")
                        else:
                            widget.set("")
                    except Exception:
                        pass

                # Spinbox
                elif isinstance(widget, (ttk.Spinbox, tk.Spinbox)):
                    try:
                        widget.delete(0, "end")
                        widget.insert(0, "0")
                    except Exception:
                        pass

                # Text
                elif isinstance(widget, tk.Text):
                    try:
                        widget.delete("1.0", "end")
                    except Exception:
                        pass

                # Checkbutton / Radiobutton (tentar limpar variável associada)
                elif isinstance(widget, (ttk.Checkbutton, tk.Checkbutton, ttk.Radiobutton, tk.Radiobutton)):
                    for opt in ("variable", "textvariable"):
                        try:
                            varname = widget.cget(opt)
                            if varname:
                                # checkbuttons normalmente são 0/1; radiobutton pode ser string
                                try:
                                    widget.setvar(varname, 0)
                                except Exception:
                                    widget.setvar(varname, "")
                        except Exception:
                            pass
            except Exception:
                pass

            # Recursivo
            try:
                for child in widget.winfo_children():
                    _limpar_recursivo(child)
            except Exception:
                pass

        # Limpar tudo dentro do main_frame (form)
        _limpar_recursivo(main_frame)

        # Voltar ao topo do formulário
        try:
            canvas.yview_moveto(0)
        except Exception:
            pass

        try:
            messagebox.showinfo("Reset", "Formulário limpo com sucesso. O modo de edição foi cancelado.")
        except Exception:
            pass

    # Botão RESET (limpa tudo e remove o ID de edição)
    btn_reset = ttk.Button(header, text="Resetar (Limpar tudo)", style="Ghost.TButton", command=resetar_formulario)
    btn_reset.pack(side="right", padx=(10, 0))
    _bind_hover(btn_reset, "Ghost.TButton")

    tk.Label(
        title_box,
        text="CONTRATO VODAFONE - PREENCHIMENTO PERFEITO",
        bg=VDF_BG,
        fg=VDF_TEXT,
        font=("Arial", 14, "bold")
    ).pack(anchor="w")

    tk.Label(
        title_box,
        text="Preenche os campos por ordem. Os obrigatórios têm asterisco (*). As secções avançadas abrem por setas.",
        bg=VDF_BG,
        fg=VDF_TEXT,
        font=("Arial", 10)
    ).pack(anchor="w", pady=(2, 0))

    # “Card” principal do formulário (fundo branco)
    card = ttk.Frame(main_frame, style="Card.TFrame")
    card.pack(fill="x", padx=18, pady=8)

    # Container interno com padding (para ficar mais “Vodafone / limpo”)
    frame = ttk.Frame(card, style="Card.TFrame")
    frame.pack(padx=18, pady=18, fill="x")

    def linha(texto, obrigatorio=True, largura=50):
        # Linha com look mais clean
        row = tk.Frame(frame, bg=VDF_CARD)
        row.pack(fill="x", pady=6)

        lbl_txt = texto + (" *" if obrigatorio else "")
        lbl = ttk.Label(row, text=lbl_txt, width=55, anchor="w")
        # Forçar “background branco” na label desta linha (card)
        lbl.configure(style="Section.TLabel") if False else None  # (não altera, mantém compatibilidade)

        # Para garantir fundo branco, usamos tk.Label opcional? Mantemos ttk, mas o card é branco.
        lbl.pack(side="left", padx=(2, 10))

        e = ttk.Entry(row, width=largura, font=("Arial", 11))
        e.pack(side="left", padx=10)
        return e
    
    
    # ==========================================================
    # PERGUNTAS OBRIGATÓRIAS (SUPERVISOR) — antes dos dados do cliente
    # ==========================================================
    global entry_sfid_comercial, entry_nome_comercial
    global entry_oferta_novo, entry_plataforma, entry_oferta_extra
    global combo_num_boxes, entry_valor_boxes, combo_origem_venda, entry_origem_outra
    global _row_valor_boxes, _row_origem_outra

    ttk.Label(frame, text="PERGUNTAS OBRIGATÓRIAS", style="Section.TLabel").pack(anchor="w", pady=(0, 8))

    entry_sfid_comercial = linha("SFID COMERCIAL (Ex: 54472442)", obrigatorio=True, largura=25)
    entry_nome_comercial = linha("NOME COMERCIAL (Ex: Duarte Afonso)", obrigatorio=True, largura=55)

    entry_oferta_novo = linha("QUAL A OFERTA NOVO CLIENTE ? (Ex: desconto 10€/mês ou oferta 2º mês)")

    entry_plataforma = linha("QUAL A PLATAFORMA (se tiver) E QUANTOS MESES? (Ex: Netflix durante 3 meses)", obrigatorio=True, largura=60)
    entry_oferta_extra = linha("OFERECES-TE ALGUMA OFERTA EXTRA AO CLIENTE? (Ex: pagar incumprimento, dupla faturação, super wifi, crédito 0€)", obrigatorio=True, largura=70)

    def linha_combo(texto, valores, obrigatorio=True, largura=25):
        row = tk.Frame(frame, bg=VDF_CARD)
        row.pack(fill="x", pady=6)
        lbl_txt = texto + (" *" if obrigatorio else "")
        ttk.Label(row, text=lbl_txt, width=55, anchor="w").pack(side="left", padx=(2, 10))
        cb = ttk.Combobox(row, values=valores, width=largura, state="readonly")
        cb.pack(side="left", padx=10)
        return row, cb

    # Nº de boxes adicionais (0 a 4)
    _, combo_num_boxes = linha_combo("Nº DE BOXES ADICIONAIS?", [str(i) for i in range(0, 5)], obrigatorio=False, largura=5)
    combo_num_boxes.set("0")

    # Valor das boxes adicionais (apenas se nº boxes > 0)
    _row_valor_boxes = tk.Frame(frame, bg=VDF_CARD)
    ttk.Label(_row_valor_boxes, text="VALOR DAS BOXES ADICIONAIS?", width=55, anchor="w").pack(side="left", padx=(2, 10))
    entry_valor_boxes = ttk.Entry(_row_valor_boxes, width=25, font=("Arial", 11))
    entry_valor_boxes.pack(side="left", padx=10)

    # Âncora para manter a posição desta linha (evita ir para o fim ao fazer pack mais tarde)
    _anchor_after_boxes = tk.Frame(frame, bg=VDF_CARD)
    _anchor_after_boxes.pack(fill="x", pady=0)

    def _toggle_valor_boxes(*_):
        try:
            n = (combo_num_boxes.get() or "").strip()
        except Exception:
            n = "0"
        if n and n != "0":
            if not _row_valor_boxes.winfo_ismapped():
                # Repack na posição correta (antes da âncora), para não aparecer no fim do formulário
                _row_valor_boxes.pack(fill="x", pady=6, before=_anchor_after_boxes)
        else:
            if _row_valor_boxes.winfo_ismapped():
                _row_valor_boxes.pack_forget()
            try:
                entry_valor_boxes.delete(0, "end")
            except Exception:
                pass

    try:
        combo_num_boxes.bind("<<ComboboxSelected>>", _toggle_valor_boxes)
    except Exception:
        pass
    _toggle_valor_boxes()

    # Origem da venda (opções fixas + "Outra")
    ORIGEM_VENDA_OPCOES = [
        "LEADS (RECENTES - MENOS DE 15 DIAS)",
        "LEADS ANTIGAS (LEADS COM + DE 15 DIAS)",
        "LEADS PEDENTES (Leads que eram vossas, e terminavam agora a FID)",
        "CONTACTOS PRÓPRIOS (Base de dados vossa)",
        "BD NOS (contatos a terminar fidelização com a NOS)",
        "50-50 (contacto enviado diretamente entre vendedores)",
        "50-50 (através do excel 50-50)",
        "INSTALAÇÕES FALHADAS NOS",
        "NºS 118",
        "NºS BD MINADA",
        "PORTA A PORTA",
        "RECOMENDAÇÕES DE CLIENTES VOSSOS",
        "RECOMENDAÇÕES DE PARCEIROS (técnicos, agentes imobiliários etc)",
        "FLYER (publicidade nas caixas de correio)",
        "REDES SOCIAIS (através de publicações na internet)",
        "LEADS FOLHAS",
        "Outra: (escrever)"
    ]
    _, combo_origem_venda = linha_combo("QUAL A ORIGEM DA VENDA?", ORIGEM_VENDA_OPCOES, obrigatorio=True, largura=55)
    combo_origem_venda.set(ORIGEM_VENDA_OPCOES[0])

    _row_origem_outra = tk.Frame(frame, bg=VDF_CARD)
    ttk.Label(_row_origem_outra, text="Outra (especificar)", width=55, anchor="w").pack(side="left", padx=(2, 10))
    entry_origem_outra = ttk.Entry(_row_origem_outra, width=55, font=("Arial", 11))
    entry_origem_outra.pack(side="left", padx=10)

    def _toggle_origem_outra(*_):
        try:
            v = (combo_origem_venda.get() or "").strip()
        except Exception:
            v = ""
        if v.startswith("Outra"):
            if not _row_origem_outra.winfo_ismapped():
                _row_origem_outra.pack(fill="x", pady=6)
        else:
            if _row_origem_outra.winfo_ismapped():
                _row_origem_outra.pack_forget()
            try:
                entry_origem_outra.delete(0, "end")
            except Exception:
                pass

    try:
        combo_origem_venda.bind("<<ComboboxSelected>>", _toggle_origem_outra)
    except Exception:
        pass
    _toggle_origem_outra()

    ttk.Separator(frame, orient="horizontal").pack(fill="x", pady=16)

    # === CAMPOS COMEÇA DADOS FA ===

    ttk.Label(frame, text="DADOS DO VENDEDOR E RC", style="Section.TLabel").pack(anchor="w", pady=(0, 8))
    global entry_tel_vendedor, entry_num_rc
    entry_num_rc = linha("Nº da RC")

    ttk.Separator(frame, orient="horizontal").pack(fill="x", pady=16)

    global entry_nome, entry_nif
    ttk.Label(frame, text="DADOS DO CLIENTE", style="Section.TLabel").pack(anchor="w", pady=(0, 8))
    entry_nome = linha("NOME COMPLETO DO CLIENTE")
    entry_nif = linha("NIF DO CLIENTE")

    # Nota de validação (vermelho) — para evitar confusões na venda
    nota_nif = tk.Frame(frame, bg=VDF_CARD)
    nota_nif.pack(fill="x", pady=(0, 6))
    ttk.Label(
        nota_nif,
        text="(VERIFICA NA APP SE TÉM DÍVIDA)",
        background=VDF_CARD,
        foreground=VDF_RED,
        font=("Arial", 10, "bold")
    ).pack(anchor="w", padx=(2, 0))

    ttk.Separator(frame, orient="horizontal").pack(fill="x", pady=16)
    heading_MORADA_DE_FATURAÇÃO = tk.Frame(frame, bg=VDF_CARD)
    heading_MORADA_DE_FATURAÇÃO.pack(fill="x", pady=(0, 8), anchor="w")
    ttk.Label(heading_MORADA_DE_FATURAÇÃO, text="MORADA DE FATURAÇÃO", style="Section.TLabel").pack(side="left")
    ttk.Label(heading_MORADA_DE_FATURAÇÃO, text="(MORADA QUE O CLIENTE TE INFORMOU)", background=VDF_CARD, foreground=VDF_RED, font=("Arial", 10, "bold")).pack(side="left", padx=(10, 0))
    global entry_rua_faturacao
    entry_rua_faturacao = linha("Rua + Nº Porta + Andar/Fração")
    
    global entry_cp4_faturacao, entry_cp3_faturacao, entry_localidade_faturacao
    entry_cp4_faturacao = linha("Primeiros 4 números CP")
    entry_cp3_faturacao = linha("Últimos 3 números CP")
    entry_localidade_faturacao = linha("Localidade")

    ttk.Separator(frame, orient="horizontal").pack(fill="x", pady=16)
    heading_MORADA_DE_INSTALAÇÃO_App = tk.Frame(frame, bg=VDF_CARD)
    heading_MORADA_DE_INSTALAÇÃO_App.pack(fill="x", pady=(0, 8), anchor="w")
    ttk.Label(heading_MORADA_DE_INSTALAÇÃO_App, text="MORADA DE INSTALAÇÃO (App)", style="Section.TLabel").pack(side="left")
    ttk.Label(heading_MORADA_DE_INSTALAÇÃO_App, text="(PREENCHER APENAS SE A MORADA NA APP FOR DIFERENTE DA MORADA DE CIMA)", background=VDF_CARD, foreground=VDF_RED, font=("Arial", 10, "bold")).pack(side="left", padx=(10, 0))

    global entry_rua_instalacao, entry_cp4_instalacao, entry_cp3_instalacao, entry_localidade_instalacao
    entry_rua_instalacao = linha("Rua + Nº Porta + Andar/Fração")
    entry_cp4_instalacao = linha("Primeiros 4 números CP")
    entry_cp3_instalacao = linha("Últimos 3 números CP")
    entry_localidade_instalacao = linha("Localidade")

    # ---------------------------
    # Botão: copiar MORADA DE FATURAÇÃO -> MORADA DE INSTALAÇÃO (App)
    # ---------------------------
    def copiar_morada_faturacao_para_instalacao():
        try:
            rua = (entry_rua_faturacao.get() if "entry_rua_faturacao" in globals() else "").strip()
            cp4 = (entry_cp4_faturacao.get() if "entry_cp4_faturacao" in globals() else "").strip()
            cp3 = (entry_cp3_faturacao.get() if "entry_cp3_faturacao" in globals() else "").strip()
            loc = (entry_localidade_faturacao.get() if "entry_localidade_faturacao" in globals() else "").strip()

            if not (rua or cp4 or cp3 or loc):
                messagebox.showwarning("Aviso", "Preenche primeiro a Morada de Faturação (em cima).")
                return

            # Rua
            try:
                entry_rua_instalacao.delete(0, tk.END)
                entry_rua_instalacao.insert(0, rua)
            except Exception:
                pass

            # CP4 / CP3 / Localidade
            try:
                entry_cp4_instalacao.delete(0, tk.END)
                entry_cp4_instalacao.insert(0, cp4)
            except Exception:
                pass
            try:
                entry_cp3_instalacao.delete(0, tk.END)
                entry_cp3_instalacao.insert(0, cp3)
            except Exception:
                pass
            try:
                entry_localidade_instalacao.delete(0, tk.END)
                entry_localidade_instalacao.insert(0, loc)
            except Exception:
                pass
        except Exception as e:
            messagebox.showerror("Erro", f"Não foi possível copiar a morada: {e}")

    frame_btn_morada = tk.Frame(frame, bg=VDF_CARD)
    frame_btn_morada.pack(fill="x", pady=(4, 0))
    btn_copiar_morada = ttk.Button(
        frame_btn_morada,
        text="COPIAR DADOS DA MORADA DE CIMA",
        style="Ghost.TButton",
        command=copiar_morada_faturacao_para_instalacao
    )
    btn_copiar_morada.pack(anchor="center", pady=(6, 0))
    try:
        _bind_hover(btn_copiar_morada, "Ghost.TButton")
    except Exception:
        pass

    ttk.Separator(frame, orient="horizontal").pack(fill="x", pady=16)
    ttk.Label(frame, text="CONTACTOS E SERVIÇO", style="Section.TLabel").pack(anchor="w", pady=(0, 8))

    global entry_contacto, entry_email, entry_plano, entry_velocidade
    entry_contacto = linha("CONTACTO DO CLIENTE")
    entry_email = linha("EMAIL")
    entry_plano = linha("PLANO E VALOR")
    entry_velocidade = linha("VELOCIDADE DA INTERNET FIXA")


    # ==========================================================
    # TELEMÓVEIS + NET MÓVEL + GIGAS E MINUTOS (em linhas lado-a-lado)
    # Pedido do supervisor:
    # Tel 1 | GIGAS E MINUTOS
    # Tel 2 | GIGAS E MINUTOS
    # Tel 3 | GIGAS E MINUTOS
    # Tel 4 | GIGAS E MINUTOS
    # Net Móvel 1 | GIGAS E MINUTOS  (campo Net mais pequeno)
    # Net Móvel 2 | GIGAS E MINUTOS  (campo Net mais pequeno)
    # ==========================================================

    # (PORTABILIDADE MÓVEL sem setinha removida — agora só existe a versão expansível)

# ----- PORTABILIDADE MÓVEL (TOPO) -----
    ttk.Separator(frame, orient="horizontal").pack(fill="x", pady=16)
    ttk.Label(frame, text="TELEMOVEIS INCLUIDOS NO PLANO", style="Section.TLabel").pack(anchor="w", pady=(0, 8))
    global frame_pm_holder
    frame_pm_holder = tk.Frame(frame, bg=VDF_CARD)
    frame_pm_holder.pack(anchor="w", fill="x", pady=(0, 8))

    # ----- PORTABILIDADE FIXA (TOPO) -----
    global fixo
    ttk.Separator(frame, orient="horizontal").pack(fill="x", pady=16)
    ttk.Label(frame, text="PORTABILIDADE FIXA", style="Section.TLabel").pack(anchor="w", pady=(0, 8))

    # Mantém compatibilidade com código antigo que usa fixo.get()
    # (o campo "Telemóvel fixo" foi removido porque agora a Portabilidade Fixa é feita no bloco próprio)
    fixo = entry_contacto

    global frame_pf_holder
    frame_pf_holder = tk.Frame(frame, bg=VDF_CARD)
    frame_pf_holder.pack(anchor="w", fill="x", pady=(0, 8))

    # ----- FATURAÇÃO -----
    global var_fatura_eletronica, var_ze_sem_ze, combo_fatura, entry_iban, entry_ntcb, entry_banco, var_pagamento_recorrente
    ttk.Separator(frame, orient="horizontal").pack(fill="x", pady=16)
    ttk.Label(frame, text="FATURAÇÃO", style="Section.TLabel").pack(anchor="w", pady=(0, 8))

    row_fat = tk.Frame(frame, bg=VDF_CARD); row_fat.pack(fill="x", pady=6, anchor="w")
    ttk.Label(row_fat, text="Fatura Eletrónica?", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
    var_fatura_eletronica = tk.BooleanVar()
    cb_fat = ttk.Checkbutton(row_fat, variable=var_fatura_eletronica)
    cb_fat.pack(side="left", padx=20)

    row_ze = tk.Frame(frame, bg=VDF_CARD); row_ze.pack(fill="x", pady=6, anchor="w")
    ttk.Label(row_ze, text="ZE sem ZE", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
    var_ze_sem_ze = tk.BooleanVar()
    cb_ze = ttk.Checkbutton(row_ze, variable=var_ze_sem_ze)
    cb_ze.pack(side="left", padx=20)

    row_tipo = tk.Frame(frame, bg=VDF_CARD); row_tipo.pack(fill="x", pady=6, anchor="w")
    ttk.Label(row_tipo, text="Fatura Detalhada ou Resumida?", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
    combo_fatura = ttk.Combobox(row_tipo, values=["DETALHADA", "RESUMIDA"], width=15, state="readonly")
    combo_fatura.set("DETALHADA")
    combo_fatura.pack(side="left", padx=20)

    entry_iban = linha("IBAN (facultativo)", False)

    # --- DÉBITO DIRETO (NOVO) ---
    global entry_ntcb, entry_banco, var_pagamento_recorrente
    entry_ntcb = linha("Nome Titular Conta Bancária (NTCB)", False)
    entry_banco = linha("Banco", False)

    row_pr = tk.Frame(frame, bg=VDF_CARD); row_pr.pack(fill="x", pady=6, anchor="w")
    ttk.Label(row_pr, text="Pagamento Recorrente", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
    var_pagamento_recorrente = tk.IntVar(value=0)
    ttk.Checkbutton(row_pr, variable=var_pagamento_recorrente).pack(side="left", padx=20)


    # ---------- FRAME DAS SEÇÕES EXPANSÍVEIS ----------
    frame_opcoes = ttk.Frame(main_frame, style="Card.TFrame")
    frame_opcoes.pack(fill="x", padx=18, pady=(6, 10))

    inner_opcoes = ttk.Frame(frame_opcoes, style="Card.TFrame")
    inner_opcoes.pack(fill="x", padx=18, pady=18)

    ttk.Label(inner_opcoes, text="OPÇÕES AVANÇADAS", style="Section.TLabel").pack(anchor="w", pady=(0, 8))
    ttk.Label(
        inner_opcoes,
        text="Rescisão.",
        style="CardMuted.TLabel"
    ).pack(anchor="w", pady=(0, 12))


    # ---------- PORTABILIDADE MÓVEL (COM SETINHA) ----------
    pm_aberto = tk.BooleanVar(value=False)

    btn_pm = ttk.Button(frame_pm_holder, text="▶ Telemoveis incluidos no plano", style="Ghost.TButton")
    btn_pm.pack(anchor="w", pady=4)
    _bind_hover(btn_pm, "Ghost.TButton")

    frame_pm_extra = ttk.Frame(frame_pm_holder, style="Card.TFrame")

    def toggle_pm():
        if pm_aberto.get():
            frame_pm_extra.pack_forget()
            btn_pm.config(text="▶ Telemoveis incluidos no plano")
            pm_aberto.set(False)
        else:
            frame_pm_extra.pack(fill="x", pady=10)
            btn_pm.config(text="Telemoveis incluidos no plano")
            pm_aberto.set(True)

    btn_pm.config(command=toggle_pm)

    # ======================================================================
    # PORTABILIDADE MÓVEL (BLOCO COMPLETO)
    # Ordem por linha:
    # Nº TELEMÓVEL || COPIAR DADOS FA || NOME || NIF || OPERADORA || CVP || Nº KMAT
    # + Botão global: Copiar dados do FA
    # + Botão global: Clear
    # + Botão por linha: Copiar dados FA (apenas essa linha)
    # + Agrupamento: 1 portabilidade por (Nome, NIF, Operadora)
    # ======================================================================

    frame_linhas_pm = tk.Frame(frame_pm_extra, bg=VDF_CARD)
    frame_linhas_pm.pack(anchor="w", pady=8, fill="x")

    MAX_LINHAS_PM = 4
    OPERADORAS_PM = ["MEO", "NOS", "NOWO", "UZO", "WOO", "DIGI", "Lycamobile", "Amigo", "Vodafone"]

    global linhas_pm
    linhas_pm = []

    def _set_globals_linhas_pm():
        """
        Globais por linha:
        pm_num_1..4, pm_gigas_1..4, pm_nome_1..4, pm_nif_1..4, pm_op_1..4, pm_cvp_1..4, pm_kmat_1..4
        """
        global pm_num_1, pm_gigas_1, pm_nome_1, pm_nif_1, pm_op_1, pm_cvp_1, pm_kmat_1
        global pm_num_2, pm_gigas_2, pm_nome_2, pm_nif_2, pm_op_2, pm_cvp_2, pm_kmat_2
        global pm_num_3, pm_gigas_3, pm_nome_3, pm_nif_3, pm_op_3, pm_cvp_3, pm_kmat_3
        global pm_num_4, pm_gigas_4, pm_nome_4, pm_nif_4, pm_op_4, pm_cvp_4, pm_kmat_4

        pm_num_1 = pm_gigas_1 = pm_nome_1 = pm_nif_1 = pm_op_1 = pm_cvp_1 = pm_kmat_1 = None
        pm_num_2 = pm_gigas_2 = pm_nome_2 = pm_nif_2 = pm_op_2 = pm_cvp_2 = pm_kmat_2 = None
        pm_num_3 = pm_gigas_3 = pm_nome_3 = pm_nif_3 = pm_op_3 = pm_cvp_3 = pm_kmat_3 = None
        pm_num_4 = pm_gigas_4 = pm_nome_4 = pm_nif_4 = pm_op_4 = pm_cvp_4 = pm_kmat_4 = None

        if len(linhas_pm) >= 1:
            pm_num_1  = linhas_pm[0]["num"]
            pm_gigas_1 = linhas_pm[0]["gigas"]
            pm_nome_1 = linhas_pm[0]["nome"]
            pm_nif_1  = linhas_pm[0]["nif"]
            pm_op_1   = linhas_pm[0]["op"]
            pm_cvp_1  = linhas_pm[0]["cvp"]
            pm_kmat_1 = linhas_pm[0]["kmat"]

        if len(linhas_pm) >= 2:
            pm_num_2  = linhas_pm[1]["num"]
            pm_gigas_2 = linhas_pm[1]["gigas"]
            pm_nome_2 = linhas_pm[1]["nome"]
            pm_nif_2  = linhas_pm[1]["nif"]
            pm_op_2   = linhas_pm[1]["op"]
            pm_cvp_2  = linhas_pm[1]["cvp"]
            pm_kmat_2 = linhas_pm[1]["kmat"]

        if len(linhas_pm) >= 3:
            pm_num_3  = linhas_pm[2]["num"]
            pm_gigas_3 = linhas_pm[2]["gigas"]
            pm_nome_3 = linhas_pm[2]["nome"]
            pm_nif_3  = linhas_pm[2]["nif"]
            pm_op_3   = linhas_pm[2]["op"]
            pm_cvp_3  = linhas_pm[2]["cvp"]
            pm_kmat_3 = linhas_pm[2]["kmat"]

        if len(linhas_pm) >= 4:
            pm_num_4  = linhas_pm[3]["num"]
            pm_gigas_4 = linhas_pm[3]["gigas"]
            pm_nome_4 = linhas_pm[3]["nome"]
            pm_nif_4  = linhas_pm[3]["nif"]
            pm_op_4   = linhas_pm[3]["op"]
            pm_cvp_4  = linhas_pm[3]["cvp"]
            pm_kmat_4 = linhas_pm[3]["kmat"]

    def _dados_fa():
        """
        Lê dados do FA (campos principais do contrato).

        Como a secção "PORTABILIDADE MÓVEL" sem setinha foi removida, já não existem
        entry_tel1..4 / entry_net1..2 / entry_gigas1.. etc. Portanto, aqui devolvemos:
        - telefones vazios (porque já não há inputs FA para isso)
        - nome/nif do topo (entry_nome / entry_nif)
        """
        telefones = ["", "", "", ""]
        nome = (entry_nome.get() if "entry_nome" in globals() else "").strip()
        nif = (entry_nif.get() if "entry_nif" in globals() else "").strip()
        return telefones, nome, nif

    def _copiar_fa_para_linha(idx_linha: int):
        """
        Copia FA para uma linha específica:
        - nº correspondente (tel[idx_linha])
        - nome/nif do FA
        Não mexe em Operadora/CVP/KMAT.
        """
        telefones_fa, nome_fa, nif_fa = _dados_fa()

        if idx_linha < 0 or idx_linha >= len(linhas_pm):
            return

        tel = telefones_fa[idx_linha] if idx_linha < len(telefones_fa) else ""
        row = linhas_pm[idx_linha]

        row["num"].delete(0, tk.END)
        if tel:
            row["num"].insert(0, tel)

        row["nome"].delete(0, tk.END)
        if nome_fa:
            row["nome"].insert(0, nome_fa)

        row["nif"].delete(0, tk.END)
        if nif_fa:
            row["nif"].insert(0, nif_fa)

        _set_globals_linhas_pm()

    def limpar_todos_os_campos_pm():
        """
        Clear global: limpa todos os campos de todas as linhas.
        Mantém as linhas existentes.
        """
        for row in linhas_pm:
            row["num"].delete(0, tk.END)
            if "gigas" in row:
                row["gigas"].delete(0, tk.END)
            row["nome"].delete(0, tk.END)
            row["nif"].delete(0, tk.END)
            row["cvp"].delete(0, tk.END)
            row["kmat"].delete(0, tk.END)
            # se quiseres também resetar a operadora:
            # row["op"].set("NOS")

        _set_globals_linhas_pm()

    def copiar_dados_fa_global():
        """
        Copia FA para todas as linhas:
        - preenche números em sequência (até 4)
        - preenche nome/nif do FA nas linhas que receberem número
        """
        telefones_fa, nome_fa, nif_fa = _dados_fa()

        # limpa antes
        limpar_todos_os_campos_pm()

        pos = 0
        for numero in telefones_fa:
            if not numero:
                continue

            if pos >= len(linhas_pm):
                adicionar_linha_pm()

            row = linhas_pm[pos]

            row["num"].delete(0, tk.END)
            row["num"].insert(0, numero)

            if nome_fa:
                row["nome"].delete(0, tk.END)
                row["nome"].insert(0, nome_fa)

            if nif_fa:
                row["nif"].delete(0, tk.END)
                row["nif"].insert(0, nif_fa)

            pos += 1

        _set_globals_linhas_pm()

    def adicionar_linha_pm():
        if len(linhas_pm) >= MAX_LINHAS_PM:
            messagebox.showwarning("Limite", "Máximo de 4 números.")
            return

        idx = len(linhas_pm) + 1

        linha = tk.Frame(frame_linhas_pm, bg=VDF_CARD)
        linha.pack(anchor="w", pady=4, fill="x")

        # Label "Nº 1/2/3/4"
        ttk.Label(linha, text=f"Nº {idx}", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")

        # 1) Nº TELEMÓVEL
        entry_num = ttk.Entry(linha, width=11)
        entry_num.pack(side="left", padx=(8, 10))

        # 2) GIGAS
        ttk.Label(linha, text="Gigas", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
        entry_gigas = ttk.Entry(linha, width=13)
        entry_gigas.pack(side="left", padx=(8, 12))

        # Botão individual: copiar Nome + NIF do cliente (topo) para ESTA linha
        def _copiar_nome_nif_para_esta_linha():
            try:
                nome_cli = (globals().get("entry_nome").get() if globals().get("entry_nome") else "").strip()
                nif_cli = (globals().get("entry_nif").get() if globals().get("entry_nif") else "").strip()

                if not nome_cli and not nif_cli:
                    messagebox.showinfo("Info", "Preencha primeiro o Nome/NIF do cliente (no topo).")
                    return

                try:
                    entry_nome.delete(0, tk.END)
                    if nome_cli:
                        entry_nome.insert(0, nome_cli)
                except Exception:
                    pass

                try:
                    entry_nif.delete(0, tk.END)
                    if nif_cli:
                        entry_nif.insert(0, nif_cli)
                except Exception:
                    pass
            except Exception as e:
                messagebox.showerror("Erro", f"Não foi possível copiar Nome/NIF: {e}")

        btn_copiar_linha = ttk.Button(
            linha,
            text="Copiar",
            width=6,
            command=_copiar_nome_nif_para_esta_linha
        )
        btn_copiar_linha.pack(side="left", padx=(0, 12))

        # 3) NOME
        ttk.Label(linha, text="Nome", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
        entry_nome = ttk.Entry(linha, width=30)
        entry_nome.pack(side="left", padx=(8, 12))

        # 4) NIF
        ttk.Label(linha, text="NIF", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
        entry_nif = ttk.Entry(linha, width=10)
        entry_nif.pack(side="left", padx=(8, 12))

        # 5) OPERADORA
        ttk.Label(linha, text="Operadora", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
        combo_op = ttk.Combobox(linha, values=OPERADORAS_PM, width=10, state="readonly")
        combo_op.set("NOS")
        combo_op.pack(side="left", padx=(8, 12))

        # 6) CVP
        ttk.Label(linha, text="CVP", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
        entry_cvp = ttk.Entry(linha, width=14)
        entry_cvp.pack(side="left", padx=(8, 12))

        # 7) Nº Cartão SIM (KMAT)
        ttk.Label(linha, text="Nº Cartão SIM", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
        entry_kmat = ttk.Entry(linha, width=14)
        entry_kmat.pack(side="left", padx=(8, 0))

        linhas_pm.append({
            "frame": linha,
            "num": entry_num,
            "gigas": entry_gigas,
            "nome": entry_nome,
            "nif": entry_nif,
            "op": combo_op,
            "cvp": entry_cvp,
            "kmat": entry_kmat,
        })

        _set_globals_linhas_pm()

    def copiar_nome_nif_cliente_para_portabilidade_movel():
        """
        Copia Nome + NIF do topo (DADOS DO CLIENTE) para todas as linhas existentes
        na Portabilidade Móvel.
        """
        try:
            nome_cli = (entry_nome.get() if "entry_nome" in globals() else "").strip()
            nif_cli = (entry_nif.get() if "entry_nif" in globals() else "").strip()

            if not (nome_cli or nif_cli):
                messagebox.showwarning("Aviso", "Preenche primeiro o Nome e/ou NIF do cliente (em cima).")
                return

            if "linhas_pm" not in globals() or not linhas_pm:
                messagebox.showwarning("Aviso", "Não existem linhas de Portabilidade Móvel para preencher.")
                return

            for row in linhas_pm:
                try:
                    row["nome"].delete(0, tk.END)
                    if nome_cli:
                        row["nome"].insert(0, nome_cli)
                except Exception:
                    pass
                try:
                    row["nif"].delete(0, tk.END)
                    if nif_cli:
                        row["nif"].insert(0, nif_cli)
                except Exception:
                    pass

            try:
                _set_globals_linhas_pm()
            except Exception:
                pass

        except Exception as e:
            messagebox.showerror("Erro", f"Não foi possível copiar Nome/NIF: {e}")

    # ---------------------------
    # Botões globais (lado a lado)
    # ---------------------------
    frame_botoes_pm = tk.Frame(frame_pm_extra, bg=VDF_CARD)
    frame_botoes_pm.pack(anchor="w", pady=(8, 6))

    btn_clear_pm = ttk.Button(
        frame_botoes_pm,
        text="🧹 Clear",
        style="Ghost.TButton",
        command=limpar_todos_os_campos_pm
    )
    btn_clear_pm.pack(side="left")
    _bind_hover(btn_clear_pm, "Ghost.TButton")

    # Botão adicionar linha
    btn_add_pm = ttk.Button(
        frame_pm_extra,
        text="➕ Adicionar Nº (Portabilidade)",
        style="Ghost.TButton",
        command=adicionar_linha_pm
    )
    btn_add_pm.pack(anchor="w", pady=(0, 6))
    _bind_hover(btn_add_pm, "Ghost.TButton")

    # 1ª linha por defeito
    adicionar_linha_pm()

    def obter_portabilidades_agrupadas():
        """
        1 portabilidade por (NIF + Operadora).
        - NIFs diferentes => portabilidades diferentes
        - Mesmo NIF mas operadoras diferentes => 1 portabilidade por operadora
        """
        grupos = {}

        for row in linhas_pm:
            num = row["num"].get().strip()
            if not num:
                continue

            nome = row["nome"].get().strip()
            nif = row["nif"].get().strip()
            op = row["op"].get().strip()

            # Vodafone não faz portabilidade móvel
            if op.lower() == "vodafone":
                continue
            cvp = row["cvp"].get().strip()
            kmat = row["kmat"].get().strip()

            if not nif or not op:
                continue

            key = (nif, op)
            if key not in grupos:
                grupos[key] = {
                    "titular_nome": nome,
                    "titular_nif": nif,
                    "operadora": op,
                    "linhas": []
                }
            else:
                if not grupos[key]["titular_nome"] and nome:
                    grupos[key]["titular_nome"] = nome

            grupos[key]["linhas"].append({
                "num": num,
                "cvp": cvp,
                "kmat": kmat
            })

        return list(grupos.values())

    # disponibiliza para o resto do código
    globals()["obter_portabilidades_agrupadas"] = obter_portabilidades_agrupadas

#========================================================================

    # ================= ZONA DE CONTEÚDO (ANTES DOS BOTÕES FINAIS) =================
    frame_conteudo = ttk.Frame(main_frame, style="Card.TFrame")
    frame_conteudo.pack(fill="x", padx=18, pady=(0, 10))

    inner_conteudo = ttk.Frame(frame_conteudo, style="Card.TFrame")
    inner_conteudo.pack(fill="x", padx=18, pady=18)

    # ================= PORTABILIDADE TELEMÓVEL FIXO =================
    pf_aberto = tk.BooleanVar(value=False)

    def toggle_portabilidade_fixa():
        if pf_aberto.get():
            frame_pf_extra.pack_forget()
            btn_pf.config(text="▶ Portabilidade Telemóvel Fixo")
            pf_aberto.set(False)
        else:
            frame_pf_extra.pack(fill="x", pady=10)
            btn_pf.config(text="Portabilidade Telemóvel Fixo")
            pf_aberto.set(True)


    btn_pf = ttk.Button(
        frame_pf_holder,
        text="▶ Portabilidade Telemóvel Fixo",
        style="Ghost.TButton",
        command=toggle_portabilidade_fixa
    )
    btn_pf.pack(anchor="w", pady=6)
    _bind_hover(btn_pf, "Ghost.TButton")

    frame_pf_extra = ttk.Frame(frame_pf_holder, style="Card.TFrame")
    # NÃO dar pack aqui

    # ------------------ CONTEÚDO FIXO ------------------
    ttk.Label(
        frame_pf_extra,
        text="PORTABILIDADE TELEMÓVEL FIXO",
        style="SubSection.TLabel"
    ).pack(anchor="w", pady=(0, 8))

    def linha_pf(parent, texto, largura=30):
        row = tk.Frame(parent, bg=VDF_CARD)
        row.pack(anchor="w", pady=4)
        ttk.Label(row, text=texto, background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
        entry = ttk.Entry(row, width=largura)
        entry.pack(side="left", padx=10)
        return entry

    global entry_pf_nome, entry_pf_nif, entry_pf_contacto, entry_pf_fixo, entry_pf_cvp, combo_pf_operador, fixos, telemoveis

    entry_pf_nome = linha_pf(frame_pf_extra, "Nome")
    entry_pf_nif = linha_pf(frame_pf_extra, "NIF")
    entry_pf_contacto = linha_pf(frame_pf_extra, "Contacto")

    row_operador = tk.Frame(frame_pf_extra, bg=VDF_CARD)
    row_operador.pack(anchor="w", pady=4)
    ttk.Label(row_operador, text="Operador atual", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
    combo_pf_operador = ttk.Combobox(
        row_operador,
        values=["MEO", "NOS", "NOWO", "Vodafone"],
        width=15,
        state="readonly"
    )
    combo_pf_operador.set("MEO")
    combo_pf_operador.pack(side="left", padx=10)

    row_fixo = tk.Frame(frame_pf_extra, bg=VDF_CARD)
    row_fixo.pack(anchor="w", pady=4)

    ttk.Label(row_fixo, text="Telefone Fixo", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
    entry_pf_fixo = ttk.Entry(row_fixo, width=20)
    entry_pf_fixo.pack(side="left", padx=10)

    ttk.Label(row_fixo, text="CVP do Fixo", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
    entry_pf_cvp = ttk.Entry(row_fixo, width=20)
    entry_pf_cvp.pack(side="left", padx=10)

    def copiar_dados_fa_fixo():
        entry_pf_nome.delete(0, tk.END)
        entry_pf_nome.insert(0, entry_nome.get())

        entry_pf_nif.delete(0, tk.END)
        entry_pf_nif.insert(0, entry_nif.get())

        entry_pf_contacto.delete(0, tk.END)
        entry_pf_contacto.insert(0, entry_contacto.get())

        #entry_contacto - variavel do contacto 

    ttk.Button(
        frame_pf_extra,
        text="📋 Copiar dados do FA",
        style="Ghost.TButton",
        command=copiar_dados_fa_fixo
    ).pack(anchor="w", pady=10)

    # ========================================================
    frame_conteudo = ttk.Frame(main_frame, style="Card.TFrame")
    frame_conteudo.pack(fill="x", padx=18, pady=(0, 10))

    inner_conteudo2 = ttk.Frame(frame_conteudo, style="Card.TFrame")
    inner_conteudo2.pack(fill="x", padx=18, pady=18)

    # ================= ALTERAÇÃO DE TITULARIDADE =================
    frame_titularidade = ttk.Frame(inner_conteudo2, style="Card.TFrame")
    frame_titularidade.pack(fill="x", pady=6)

    tit_aberto = tk.BooleanVar(value=False)
   

    frame_tit_extra = ttk.Frame(frame_titularidade, style="Card.TFrame")
    # NÃO dar pack aqui

    ttk.Label(
        frame_tit_extra,
        text="ALTERAÇÃO DE CONTRATO – FIXO E/OU MÓVEL",
        style="SubSection.TLabel"
    ).pack(anchor="w", pady=(0, 8))

    def linha(parent, texto, largura=30):
        row = tk.Frame(parent, bg=VDF_CARD)
        row.pack(anchor="w", pady=4)
        ttk.Label(row, text=texto, width=30, background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
        e = ttk.Entry(row, width=largura)
        e.pack(side="left", padx=10)
        return e


    global entry_tit_nome, entry_tit_conta

    entry_tit_nome = linha(frame_tit_extra, "Nome do Cliente")
    entry_tit_conta = linha(frame_tit_extra, "Conta (opcional)")

    ttk.Separator(frame_tit_extra, orient="horizontal").pack(fill="x", pady=10)

    ttk.Label(
        frame_tit_extra,
        text="DADOS DO SERVIÇO FIXO",
        style="SubSection.TLabel"
    ).pack(anchor="w", pady=(0, 6))

    frame_fixos_campos = ttk.Frame(frame_tit_extra, style="Card.TFrame")
    frame_fixos_campos.pack(anchor="w")

    fixos = []

    def adicionar_fixo():
        if len(fixos) >= 2:
            return
        e = linha(frame_fixos_campos, f"Nº Telefone Fixo {len(fixos)+1}")
        fixos.append(e)

    adicionar_fixo()  # começa com 1

    ttk.Button(
        frame_tit_extra,
        text="+ Adicionar Telefone Fixo",
        style="Ghost.TButton",
        command=adicionar_fixo
    ).pack(anchor="w", pady=6)

    ttk.Separator(frame_tit_extra, orient="horizontal").pack(fill="x", pady=10)

    ttk.Label(
        frame_tit_extra,
        text="DADOS DO SERVIÇO MÓVEL",
        style="SubSection.TLabel"
    ).pack(anchor="w", pady=(0, 6))

    frame_moveis_campos = ttk.Frame(frame_tit_extra, style="Card.TFrame")
    frame_moveis_campos.pack(anchor="w")

    telemoveis = []

    def adicionar_telemovel():
        if len(telemoveis) >= 4:
            return
        e = linha(frame_moveis_campos, f"Nº Telemóvel {len(telemoveis)+1}")
        telemoveis.append(e)

    adicionar_telemovel()  # começa com 1

    ttk.Button(
        frame_tit_extra,
        text="+ Adicionar Telemóvel",
        style="Ghost.TButton",
        command=adicionar_telemovel
    ).pack(anchor="w", pady=6)

    ttk.Separator(frame_tit_extra, orient="horizontal").pack(fill="x", pady=10)


    global  entry_nif_antigo, entry_nif_novo

    

    ttk.Label(
        frame_tit_extra,
        text="ALTERAÇÃO DE TITULARIDADE",
        style="SubSection.TLabel"
    ).pack(anchor="w", pady=(0, 6))

    entry_nif_antigo = linha(frame_tit_extra, "NIF Antigo Titular")
    entry_nif_novo = linha(frame_tit_extra, "NIF Novo Titular")

    #======================================================
    frame_conteudo = ttk.Frame(main_frame, style="Card.TFrame")
    frame_conteudo.pack(fill="x", padx=18, pady=(0, 10))
    inner_conteudo3 = ttk.Frame(frame_conteudo, style="Card.TFrame")
    inner_conteudo3.pack(fill="x", padx=18, pady=18)
    #=================================================

    # ================= RESCISÃO =================
    frame_rescisao = ttk.Frame(inner_conteudo3, style="Card.TFrame")
    frame_rescisao.pack(fill="x", pady=6)

    res_aberto = tk.BooleanVar(value=False)

    def toggle_rescisao():
        if res_aberto.get():
            frame_rescisao_extra.pack_forget()
            btn_rescisao.config(text="▶ Rescisão")
            res_aberto.set(False)
        else:
            frame_rescisao_extra.pack(fill="x", pady=10)
            btn_rescisao.config(text="Rescisão")
            res_aberto.set(True)

    btn_rescisao = ttk.Button(
        frame_rescisao,
        text="▶ Rescisão",
        style="Ghost.TButton",
        command=toggle_rescisao
    )
    btn_rescisao.pack(anchor="w")
    _bind_hover(btn_rescisao, "Ghost.TButton")

    frame_rescisao_extra = ttk.Frame(frame_rescisao, style="Card.TFrame")
    # NÃO fazer pack aqui

    def linha(parent, texto, largura=40):
        row = tk.Frame(parent, bg=VDF_CARD)
        row.pack(anchor="w", pady=4)
        ttk.Label(row, text=texto, width=28, background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
        e = ttk.Entry(row, width=largura)
        e.pack(side="left", padx=10)
        return e

    ttk.Label(
        frame_rescisao_extra,
        text="DADOS DO CLIENTE",
        style="SubSection.TLabel"
    ).pack(anchor="w", pady=(0, 8))

    global entry_res_nome, entry_res_morada, entry_res_cp4, entry_res_cp3, entry_res_localidade, combo_operadora_res, entry_res_cliente, entry_res_fixo, entry_res_int_movel

    entry_res_nome = linha(frame_rescisao_extra, "Nome")
    entry_res_morada = linha(frame_rescisao_extra, "Morada", 55)

    row_cp = tk.Frame(frame_rescisao_extra, bg=VDF_CARD)
    row_cp.pack(anchor="w", pady=4)
    ttk.Label(row_cp, text="Código Postal", width=28, background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
    entry_res_cp4 = ttk.Entry(row_cp, width=8)
    entry_res_cp4.pack(side="left")
    ttk.Label(row_cp, text=" - ", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
    entry_res_cp3 = ttk.Entry(row_cp, width=6)
    entry_res_cp3.pack(side="left", padx=5)
    entry_res_localidade = ttk.Entry(row_cp, width=20)
    entry_res_localidade.pack(side="left", padx=10)

    ttk.Separator(frame_rescisao_extra, orient="horizontal").pack(fill="x", pady=10)

    ttk.Label(
        frame_rescisao_extra,
        text="Operadora",
        style="SubSection.TLabel"
    ).pack(anchor="w", pady=(0, 6))

    style = ttk.Style()

    style.configure(
        "Small.TCombobox",
        font=("Segoe UI", 6)
    )

    combo_operadora_res = ttk.Combobox(
        frame_rescisao_extra,
        state="readonly",
        width=110,
        style="Small.TCombobox",
        values=[
            "NOS Comunicações, S.A.: Apartado 52111, EC Campo Grande, 1721-501 Lisboa",
            "MEO - Serviço Comunicações e Multimédia, S.A., Apartado 1423 EC Pedro Hispano (Porto) 4106-005 Porto",
            "NOWO Communications, S.A.: Serviço de Cliente, Apartado 200, Loja CTT Palmela, 2951-901 Palmela"
        ]
    )
    combo_operadora_res.pack(anchor="w", pady=6)
    combo_operadora_res.current(0)

    ttk.Separator(frame_rescisao_extra, orient="horizontal").pack(fill="x", pady=10)

    entry_res_cliente = linha(frame_rescisao_extra, "Nº Cliente")
    entry_res_fixo = linha(frame_rescisao_extra, "Nº Telefone Fixo")
    entry_res_int_movel = linha(frame_rescisao_extra, "Nº Internet Móvel", 30)

    # =========================
    # Serviços a cancelar (piscos no PDF)
    # =========================
    global res_srv_voz, res_srv_internet, res_srv_tv, res_srv_movel, res_srv_int_movel

    res_srv_voz = tk.BooleanVar(value=False)
    res_srv_internet = tk.BooleanVar(value=False)
    res_srv_tv = tk.BooleanVar(value=False)
    res_srv_movel = tk.BooleanVar(value=False)
    res_srv_int_movel = tk.BooleanVar(value=False)

    ttk.Label(
        frame_rescisao_extra,
        text="Serviços a cancelar (assinala os que vão ser cancelados)",
        style="SubSection.TLabel"
    ).pack(anchor="w", pady=(8, 6))

    row_srv = tk.Frame(frame_rescisao_extra, bg=VDF_CARD)
    row_srv.pack(anchor="w", pady=(0, 6))

    ttk.Checkbutton(row_srv, text="Voz / Fixo", variable=res_srv_voz).pack(side="left", padx=(0, 14))
    ttk.Checkbutton(row_srv, text="Internet", variable=res_srv_internet).pack(side="left", padx=(0, 14))
    ttk.Checkbutton(row_srv, text="TV", variable=res_srv_tv).pack(side="left", padx=(0, 14))
    ttk.Checkbutton(row_srv, text="Móvel", variable=res_srv_movel).pack(side="left", padx=(0, 14))
    ttk.Checkbutton(row_srv, text="Internet Móvel", variable=res_srv_int_movel).pack(side="left", padx=(0, 14))

    ttk.Separator(frame_rescisao_extra, orient="horizontal").pack(fill="x", pady=10)

    ttk.Label(
        frame_rescisao_extra,
        text="NÚMEROS DE TELEMÓVEL",
        style="SubSection.TLabel"
    ).pack(anchor="w", pady=(0, 6))

    global res_telemoveis

    frame_res_telemoveis = ttk.Frame(frame_rescisao_extra, style="Card.TFrame")
    frame_res_telemoveis.pack(anchor="w")

    res_telemoveis = []

    def adicionar_telemovel_res():
        if len(res_telemoveis) >= 4:
            return
        e = linha(frame_res_telemoveis, f"Nº Telemóvel {len(res_telemoveis)+1}")
        res_telemoveis.append(e)

    adicionar_telemovel_res()

    ttk.Button(
        frame_rescisao_extra,
        text="+ Adicionar Telemóvel",
        style="Ghost.TButton",
        command=adicionar_telemovel_res
    ).pack(anchor="w", pady=6)

    def buscar_dados_fa_rescisao():

        #DADOS :  entry_rua_faturacao || entry_cp4_faturacao || entry_cp3_faturacao || entry_localidade_faturacao
        
        # Nome
        entry_res_nome.delete(0, tk.END)
        entry_res_nome.insert(0, entry_nome.get())

        # Morada
        entry_res_morada.delete(0, tk.END)
        entry_res_morada.insert(0, entry_rua_faturacao.get())

        # Código Postal + Localidade
        entry_res_cp4.delete(0, tk.END)
        entry_res_cp4.insert(0, entry_cp4_faturacao.get())

        entry_res_cp3.delete(0, tk.END)
        entry_res_cp3.insert(0, entry_cp3_faturacao.get())

        entry_res_localidade.delete(0, tk.END)
        entry_res_localidade.insert(0, entry_localidade_faturacao.get())

        # Telefone fixo
        entry_res_fixo.delete(0, tk.END)
        entry_res_fixo.insert(0, entry_pf_fixo.get())

        # Telemóveis
        fa_tels = []
        try:
            if "linhas_pm" in globals():
                for i in range(min(4, len(linhas_pm))):
                    fa_tels.append((linhas_pm[i]["num"].get() or "").strip())
        except Exception:
            fa_tels = []


        for i, tel in enumerate(fa_tels):
            if tel.strip():
                if i >= len(res_telemoveis):
                    adicionar_telemovel_res()
                res_telemoveis[i].delete(0, tk.END)
                res_telemoveis[i].insert(0, tel)

    ttk.Button(
        frame_rescisao_extra,
        text="📋 Buscar dados do FA",
        style="Ghost.TButton",
        command=buscar_dados_fa_rescisao
    ).pack(anchor="w", pady=10)

    #======================FIM===========================

    # “Barra” final de ação (visual Vodafone)
    footer = ttk.Frame(main_frame)
    footer.pack(fill="x", padx=18, pady=(6, 18))

    # Botão principal (mantém o teu command=gerar)
    btn_gerar = ttk.Button(
        footer,
        text="GERAR CONTRATO",
        style="Primary.TButton",
        command=gerar
    )
    btn_gerar.pack(fill="x", pady=(10, 6))


    def on_configure(event):
        canvas.configure(scrollregion=canvas.bbox("all"))
    main_frame.bind("<Configure>", on_configure)

    # ------------------ Aba Contratos Gerados ------------------
    def carregar_contratos():
        for widget in aba_lista.winfo_children():
            widget.destroy()  # limpar aba

        # Cabeçalho da aba lista com estilo limpo
        aba_lista.configure(style="TFrame")
        top_lista = ttk.Frame(aba_lista)
        top_lista.pack(fill="x", padx=14, pady=14)
        ttk.Label(top_lista, text="Contratos Gerados", font=("Arial", 16, "bold"), foreground=VDF_RED, background=VDF_BG).pack(anchor="w")
        ttk.Label(top_lista, text="Seleciona um contrato para preencher automaticamente o formulário.",
                  style="Muted.TLabel").pack(anchor="w", pady=(4, 0))

        # Botão "Atualizar" (refresh) — recarrega a lista sem precisar reiniciar a aplicação
        btn_refresh = ttk.Button(top_lista, text="Atualizar", command=carregar_contratos)
        btn_refresh.pack(side="right", anchor="e")

        nome_vendedor, sfid = ler_dados()
        try:
            conn = ligar_db()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM RC WHERE SFID = %s", (sfid,))
            contratos = cursor.fetchall()
            cursor.close()
            conn.close()
        except Exception as e:
            messagebox.showerror("Erro BD", f"Erro ao carregar contratos:\n{e}")
            return


        # ------------------ Pesquisa (NIF / Nome) ------------------
        filtros = tk.Frame(aba_lista, bg=VDF_BG)
        filtros.pack(fill="x", padx=14, pady=(0, 10))

        var_filtro_nif = tk.StringVar()
        var_filtro_nome = tk.StringVar()

        tk.Label(filtros, text="Pesquisar NIF:", bg=VDF_BG, fg=VDF_TEXT, font=("Arial", 10, "bold")).pack(side="left")
        ent_filtro_nif = ttk.Entry(filtros, width=18, textvariable=var_filtro_nif)
        ent_filtro_nif.pack(side="left", padx=(8, 16))

        tk.Label(filtros, text="Pesquisar Nome:", bg=VDF_BG, fg=VDF_TEXT, font=("Arial", 10, "bold")).pack(side="left")
        ent_filtro_nome = ttk.Entry(filtros, width=30, textvariable=var_filtro_nome)
        ent_filtro_nome.pack(side="left", padx=(8, 0))

        # Container onde os "cards" vão ser renderizados (para permitir re-render ao filtrar)
        list_container = tk.Frame(aba_lista, bg=VDF_BG)
        list_container.pack(fill="both", expand=True)

        def criar_card(contrato):
                    # Card por contrato (branco com borda suave)
                    outer = tk.Frame(list_container, bg=VDF_BG)
                    outer.pack(fill="x", padx=14, pady=6)

                    frame_contrato = tk.Frame(outer, bg=VDF_WHITE, bd=1, relief="solid", highlightthickness=0)
                    frame_contrato.pack(fill="x")

                    left = tk.Frame(frame_contrato, bg=VDF_WHITE)
                    left.pack(side="left", fill="x", expand=True, padx=12, pady=10)

                    title_txt = f"{contrato['nome_completo']} | {contrato['nif']}"
                    tk.Label(left, text=title_txt, bg=VDF_WHITE, fg=VDF_TEXT, font=("Arial", 11, "bold")).pack(anchor="w")
                    tk.Label(left, text=f"SFID: {contrato.get('SFID', '')}", bg=VDF_WHITE, fg=VDF_MUTED, font=("Arial", 9)).pack(anchor="w", pady=(2, 0))
                    tk.Label(left, text=f"MAC: {contrato.get('mac', 0)}", bg=VDF_WHITE, fg=VDF_MUTED, font=("Arial", 9)).pack(anchor="w", pady=(2, 0))

                    def editar_closure(c=contrato):
                        # Guardar ID do registo para UPDATE pelo ID (permite NIF duplicado)
                        try:
                            globals()['CURRENT_RC_ID'] = c.get('ID', c.get('id', None))
                        except Exception:
                            globals()['CURRENT_RC_ID'] = None
                        # função para evitar 0 → vazio
                        def valor(campo):
                            v = c.get(campo, "")
                            if v == 0 or v == "0" or v is None:
                                return ""
                            return str(v)

                        # ====================================================
                        # ADICIONA ESTAS 4 LINHAS AQUI PARA O RC E COMERCIAL:
                        entry_num_rc.delete(0, tk.END)
                        entry_num_rc.insert(0, valor("ID_RC"))
                        
                        # ====================================================

                        entry_nome.delete(0, tk.END)
                        entry_nome.insert(0, valor("nome_completo"))
                        entry_nif.delete(0, tk.END)
                        entry_nif.insert(0, valor("nif"))
                        entry_rua_faturacao.delete(0, tk.END)
                        entry_rua_faturacao.insert(0, valor("morada").split(",")[0] if "," in valor("morada") else valor("morada"))
                        entry_localidade_faturacao.delete(0, tk.END)
                        entry_localidade_faturacao.insert(0, valor("morada").split(",")[1] if "," in valor("morada") else "")
                        entry_cp4_faturacao.delete(0, tk.END)
                        entry_cp4_faturacao.insert(0, valor("cp7").split("-")[0] if "-" in valor("cp7") else valor("cp7"))
                        entry_cp3_faturacao.delete(0, tk.END)
                        entry_cp3_faturacao.insert(0, valor("cp7").split("-")[1] if "-" in valor("cp7") else "")
                        entry_email.delete(0, tk.END)
                        entry_email.insert(0, valor("email"))
                        entry_plano.delete(0, tk.END)
                        entry_plano.insert(0, valor("pacote"))
                        entry_velocidade.delete(0, tk.END)
                        entry_velocidade.insert(0, valor("int_fixa"))
                        # Comercial (override PDF)
                        try:
                            entry_sfid_comercial.delete(0, tk.END)
                            entry_sfid_comercial.insert(0, valor("SFID_COMERCIAL"))
                        except Exception:
                            pass
                        try:
                            entry_nome_comercial.delete(0, tk.END)
                            entry_nome_comercial.insert(0, valor("NOME_COMERCIAL"))
                        except Exception:
                            pass

                        # Perguntas obrigatórias (Supervisor)
                        try:
                            entry_oferta_novo.delete(0, tk.END)
                            entry_oferta_novo.insert(0, valor("oferta_novo_cliente"))
                        except Exception:
                            pass
                        try:
                            entry_plataforma.delete(0, tk.END)
                            entry_plataforma.insert(0, valor("plataforma_meses"))
                        except Exception:
                            pass
                        try:
                            entry_oferta_extra.delete(0, tk.END)
                            entry_oferta_extra.insert(0, valor("oferta_extra"))
                        except Exception:
                            pass
                        try:
                            combo_num_boxes.set(valor("num_boxes_adicionais") if valor("num_boxes_adicionais") != "" else "0")
                            try:
                                _toggle_valor_boxes()
                            except Exception:
                                pass
                            _toggle_valor_boxes()
                            entry_valor_boxes.delete(0, tk.END)
                            entry_valor_boxes.insert(0, valor("valor_boxes_adicionais"))
                        except Exception:
                            pass
                        try:
                            ov = valor("origem_venda")
                            if ov:
                                combo_origem_venda.set(ov)
                            else:
                                # fallback: se existia "origem_venda_outra" mas não há opção, set Outra
                                if valor("origem_venda_outra"):
                                    combo_origem_venda.set("Outra: (escrever)")
                            _toggle_origem_outra()
                            entry_origem_outra.delete(0, tk.END)
                            entry_origem_outra.insert(0, valor("origem_venda_outra"))
                        except Exception:
                            pass

                        entry_contacto.delete(0, tk.END)
                        entry_contacto.insert(0, valor("tel_contacto"))
                        entry_iban.delete(0, tk.END)
                        entry_iban.insert(0, valor("IBAN"))
                        entry_ntcb.delete(0, tk.END)
                        entry_ntcb.insert(0, valor("NTCB"))
                        entry_banco.delete(0, tk.END)
                        entry_banco.insert(0, valor("banco"))
                        try:
                            var_pagamento_recorrente.set(int(valor("PR") or 0))
                        except Exception:
                            var_pagamento_recorrente.set(0)
                        # Preencher Portabilidade Móvel (com setinha) a partir do RC:
                        # - tel_1..tel_4 -> num (linhas_pm)
                        # - nome/nif do contrato -> nome/nif das linhas (para ficar coerente)
                        nums = [valor("tel_1"), valor("tel_2"), valor("tel_3"), valor("tel_4")]
                        # entry_rua_instalacao, entry_cp4_instalacao, entry_cp3_instalacao, entry_localidade_instalacao
                        entry_rua_instalacao.delete(0, tk.END)
                        entry_rua_instalacao.insert(0, valor("morada_app"))
                        entry_cp4_instalacao.delete(0, tk.END)
                        entry_cp4_instalacao.insert(0, valor("cp4_app"))
                        entry_cp3_instalacao.delete(0, tk.END)
                        entry_cp3_instalacao.insert(0, valor("cp3_app"))
                        entry_localidade_instalacao.delete(0, tk.END)
                        entry_localidade_instalacao.insert(0, valor("localidade_app"))


                        # garantir pelo menos 4 linhas (para não dar erro no editar)
                        try:
                            while len(linhas_pm) < 4:
                                adicionar_linha_pm()
                        except Exception:
                            pass

                        for i in range(4):
                            try:
                                row = linhas_pm[i]

                                # número (se não existir, manter a linha completamente em branco)
                                numero = (nums[i] or "").strip()

                                # limpar sempre a linha toda primeiro
                                row["num"].delete(0, tk.END)
                                row["nome"].delete(0, tk.END)
                                row["nif"].delete(0, tk.END)
                                row["cvp"].delete(0, tk.END)
                                row["kmat"].delete(0, tk.END)
                                if "gigas" in row:
                                    row["gigas"].delete(0, tk.END)
                                try:
                                    row["op"].set("")
                                except Exception:
                                    pass

                                # Se não há número nesta posição, não preencher mais nada nesta linha
                                if not numero:
                                    continue

                                # número
                                row["num"].insert(0, numero)

                                # CVP / KMAT / Operadora / Titular (agora RC guarda)
                                cvp_val = valor(f"pm_cvp_{i+1}")
                                kmat_val = valor(f"pm_kmat_{i+1}")
                                op_val = valor(f"pm_op_{i+1}")
                                nome_val = valor(f"pm_nome_{i+1}")
                                nif_val = valor(f"pm_nif_{i+1}")
                                gigas_val = valor(f"tar_{i+1}")  # já estava na RC

                                if cvp_val:
                                    row["cvp"].insert(0, cvp_val)
                                if kmat_val:
                                    row["kmat"].insert(0, kmat_val)
                                if "gigas" in row and gigas_val:
                                    row["gigas"].insert(0, gigas_val)

                                # titular por linha (se existir; senão cai no nome/nif do contrato)
                                row["nome"].insert(0, nome_val if nome_val else valor("nome_completo"))
                                row["nif"].insert(0, nif_val if nif_val else valor("nif"))

                                # operadora (se não existir, mantém default NOS)
                                try:
                                    row["op"].set(op_val if op_val else "NOS")
                                except Exception:
                                    pass
                            except Exception:
                                pass
                            except Exception:
                                pass

                        try:
                            _set_globals_linhas_pm()
                        except Exception:
                            pass

                        entry_iban.delete(0, tk.END)
                        entry_iban.insert(0, valor("IBAN"))
                        var_fatura_eletronica.set(bool(c.get("FE", 0)))
                        var_ze_sem_ze.set(bool(c.get("ze_sem_ze", 0)))
                        combo_fatura.set(valor("tipo_fatura"))


                        # Portabilidade Fixa (repôr tudo)
                        try:
                            entry_pf_nome.delete(0, tk.END); entry_pf_nome.insert(0, valor("pf_nome"))
                            entry_pf_nif.delete(0, tk.END); entry_pf_nif.insert(0, valor("pf_nif"))
                            entry_pf_contacto.delete(0, tk.END); entry_pf_contacto.insert(0, valor("pf_contacto"))
                            combo_pf_operador.set(valor("pf_operadora"))
                            entry_pf_fixo.delete(0, tk.END); entry_pf_fixo.insert(0, valor("pf_fixo"))
                            entry_pf_cvp.delete(0, tk.END); entry_pf_cvp.insert(0, valor("pf_cvp"))
                        except Exception:
                            pass

                        # Rescisão (repôr tudo)
                        try:
                            entry_res_nome.delete(0, tk.END); entry_res_nome.insert(0, valor("res_nome"))
                            entry_res_morada.delete(0, tk.END); entry_res_morada.insert(0, valor("res_morada"))
                            entry_res_cp4.delete(0, tk.END); entry_res_cp4.insert(0, valor("res_cp4"))
                            entry_res_cp3.delete(0, tk.END); entry_res_cp3.insert(0, valor("res_cp3"))
                            entry_res_localidade.delete(0, tk.END); entry_res_localidade.insert(0, valor("res_localidade"))
                            combo_operadora_res.set(valor("res_operadora"))
                            entry_res_cliente.delete(0, tk.END); entry_res_cliente.insert(0, valor("res_cliente"))
                            entry_res_fixo.delete(0, tk.END); entry_res_fixo.insert(0, valor("res_fixo"))
                            if "entry_res_int_movel" in globals():
                                entry_res_int_movel.delete(0, tk.END); entry_res_int_movel.insert(0, valor("res_int_movel"))

                            # serviços a cancelar (piscos)
                            try:
                                res_srv_tv.set(bool(int(valor("res_srv_tv") or 0)))
                                res_srv_internet.set(bool(int(valor("res_srv_internet") or 0)))
                                res_srv_voz.set(bool(int(valor("res_srv_voz") or 0)))
                                res_srv_movel.set(bool(int(valor("res_srv_movel") or 0)))
                                if "res_srv_int_movel" in globals():
                                    res_srv_int_movel.set(bool(int(valor("res_srv_int_movel") or 0)))
                            except Exception:
                                pass

                            # telemóveis rescisão (até 4)
                            res_vals = [valor("res_movel_1"), valor("res_movel_2"), valor("res_movel_3"), valor("res_movel_4")]
                            for j in range(min(4, len(res_telemoveis))):
                                res_telemoveis[j].delete(0, tk.END)
                                if res_vals[j]:
                                    res_telemoveis[j].insert(0, res_vals[j])
                        except Exception:
                            pass

                        notebook.select(aba_gerar)  # mudar para aba gerar

                    # Botão editar com look Vodafone
                    btn = tk.Button(frame_contrato, text="Editar", command=editar_closure,
                                    bg=VDF_RED, fg=VDF_WHITE, bd=0, padx=14, pady=8,
                                    activebackground=VDF_RED_DARK, activeforeground=VDF_WHITE,
                                    font=("Arial", 10, "bold"), cursor="hand2")
                    btn.pack(side="right", padx=12, pady=10)


        def aplicar_filtro(*_):
            # Limpar cards atuais
            for w in list_container.winfo_children():
                w.destroy()

            f_nif = (var_filtro_nif.get() or "").strip()
            f_nome = (var_filtro_nome.get() or "").strip().lower()

            filtrados = []
            for c in contratos:
                nif_c = str(c.get("nif", "") or "")
                nome_c = str(c.get("nome_completo", "") or "").lower()
                if f_nif and f_nif not in nif_c:
                    continue
                if f_nome and f_nome not in nome_c:
                    continue
                filtrados.append(c)

            if not filtrados:
                tk.Label(list_container, text="Nenhum contrato encontrado com esses filtros.",
                         bg=VDF_BG, fg=VDF_MUTED, font=("Arial", 10)).pack(anchor="w", padx=14, pady=12)
                return

            for c in filtrados:
                criar_card(c)

        # Atualizar automaticamente enquanto escreve
        try:
            var_filtro_nif.trace_add("write", aplicar_filtro)
            var_filtro_nome.trace_add("write", aplicar_filtro)
        except Exception:
            # Compatibilidade com versões antigas do Tk
            var_filtro_nif.trace("w", aplicar_filtro)
            var_filtro_nome.trace("w", aplicar_filtro)

        # Render inicial (sem filtros)
        aplicar_filtro()

    carregar_contratos()
    janela.mainloop()


def obter_titularidades_agrupadas(nif_novo: str, conta_nova: str = "", nome_fallback: str = ""):
    """
    Regra:
    - O NIF do topo (NIF DO CLIENTE) é o NIF NOVO.
    - Os NIFs por linha (Portabilidade Móvel) são considerados NIFs ANTIGOS.
    - Se nif_antigo == nif_novo => não há alteração de titular para essa linha.
    - Se nif_antigo != nif_novo => gera alteração de titular agrupada por nif_antigo,
      contendo apenas os números associados a esse nif_antigo.
    - O NOME a imprimir na Alteração de Titularidade deve ser SEMPRE o do titular ANTIGO,
      ou seja, o "Nome" associado ao NIF antigo na(s) linha(s) da Portabilidade Móvel.

    NOVO (pedido do Diogo):
    - Se o FIXO (Portabilidade Telemóvel Fixo) tiver um NIF diferente do NIF NOVO,
      então esse FIXO tem de entrar na Alteração de Titularidade.
    - Se, ao mesmo tempo, algum TELEMÓVEL também tiver alteração de titular,
      deve ficar tudo na MESMA alteração (desde que o NIF antigo seja o mesmo).
      Ex.: Fixo + Móvel em nome da Maria (NIF antigo), novo titular João (NIF novo) => mesma página.

    Retorna lista de alterações:
    [
      {"nif_antigo":..., "nif_novo":..., "movels":[...], "tit_nome":..., "tit_conta":..., "fixos":[...]},
      ...
    ]
    """
    nif_novo = (nif_novo or "").strip()
    if not nif_novo:
        return []

    # linhas_pm é criado na secção da Portabilidade Móvel (cada linha tem widgets: num, nome, nif, oper, cvp, kmat)
    if "linhas_pm" not in globals():
        linhas_pm_local = []
    else:
        linhas_pm_local = linhas_pm

    grupos_mov = {}    # nif_antigo -> lista de números móveis
    grupos_fix = {}    # nif_antigo -> lista de números fixos
    grupos_nome = {}   # nif_antigo -> nome antigo (primeiro encontrado)

    # ---------------------------
    # 1) Capturar alterações via MÓVEIS (linhas_pm)
    # ---------------------------
    for row in linhas_pm_local:
        try:
            num = (row.get("num").get() if row.get("num") else "").strip()
            nif_antigo = (row.get("nif").get() if row.get("nif") else "").strip()
            nome_antigo = (row.get("nome").get() if row.get("nome") else "").strip()
        except Exception:
            continue

        if not num or not nif_antigo:
            continue
        if nif_antigo == nif_novo:
            continue

        if nif_antigo not in grupos_nome and nome_antigo:
            grupos_nome[nif_antigo] = nome_antigo

        grupos_mov.setdefault(nif_antigo, [])
        if num not in grupos_mov[nif_antigo]:
            grupos_mov[nif_antigo].append(num)

    # ---------------------------
    # 2) Capturar alterações via FIXO (Portabilidade Telemóvel Fixo)
    # ---------------------------
    try:
        fixo_num = (entry_pf_fixo.get() if "entry_pf_fixo" in globals() else "").strip()
        fixo_nif_antigo = (entry_pf_nif.get() if "entry_pf_nif" in globals() else "").strip()
        fixo_nome_antigo = (entry_pf_nome.get() if "entry_pf_nome" in globals() else "").strip()
    except Exception:
        fixo_num = ""
        fixo_nif_antigo = ""
        fixo_nome_antigo = ""

    if fixo_num and fixo_nif_antigo and fixo_nif_antigo != nif_novo:
        if fixo_nif_antigo not in grupos_nome and fixo_nome_antigo:
            grupos_nome[fixo_nif_antigo] = fixo_nome_antigo

        grupos_fix.setdefault(fixo_nif_antigo, [])
        if fixo_num not in grupos_fix[fixo_nif_antigo]:
            grupos_fix[fixo_nif_antigo].append(fixo_num)

    # ---------------------------
    # 3) Construir titularidades: união de chaves (móveis + fixo)
    # ---------------------------
    chaves = list(dict.fromkeys(list(grupos_mov.keys()) + list(grupos_fix.keys())))  # mantém ordem estável

    titularidades = []
    for nif_antigo in chaves:
        movs = (grupos_mov.get(nif_antigo) or [])[:4]
        fixs = (grupos_fix.get(nif_antigo) or [])[:2]

        titularidades.append({
            "nif_antigo": nif_antigo,
            "nif_novo": nif_novo,
            "movels": movs,
            "tit_nome": (grupos_nome.get(nif_antigo) or nome_fallback or "").strip(),
            "tit_conta": (conta_nova or "").strip(),
            "fixos": fixs
        })

    return titularidades


def montar_campos_titularidade(tit, ssfid, nome_vendedor, dia, mes, ano, ze_sem_ze=False):
    """
    Monta os campos para a página de Alteração de Titularidade (COORDS_6),
    na ordem usada no teu código original (campos_6).

    Espera tit = {
      "nif_antigo":..., "nif_novo":..., "movels":[...],
      "tit_nome": (NOME ANTIGO TITULAR), "tit_conta":...
    }
    """
    movels = tit.get("movels") or []
    while len(movels) < 4:
        movels.append("")

    fixos = tit.get("fixos") or []
    while len(fixos) < 2:
        fixos.append("")

    campos_6 = [
        fixos[0],
        fixos[1],
        movels[0],
        movels[1],
        movels[2],
        movels[3],
        (tit.get("tit_nome") or ""),
        (tit.get("tit_conta") or ""),
        (tit.get("nif_antigo") or ""),
        (tit.get("nif_novo") or ""),
        dia,
        mes,
        ano,
        "" if ze_sem_ze else ssfid,
        "" if ze_sem_ze else (nome_vendedor or ""),
        "x" if 1 == 1 else ""
    ]
    return campos_6

def obter_uuid():
    """
    Obtém um identificador estável da máquina (Windows/macOS/Linux).

    Ordem de tentativas (por SO):

    Windows:
      1) WMIC (Windows mais antigos)
      2) PowerShell CIM (Windows mais novos)
      3) MachineGuid (registo) como fallback final

    macOS:
      1) ioreg IOPlatformUUID (Hardware UUID)

    Linux:
      1) /etc/machine-id
      2) /var/lib/dbus/machine-id (fallback)

    Retorna:
      - string com UUID/ID estável
      - ou None se não conseguir
    """
    import subprocess
    import sys
    import os

    def _clean(s: str) -> str:
        s = (s or "").strip()
        # Proteções básicas contra outputs estranhos
        s = s.replace("\ufeff", "").strip()
        return s

    # =========================
    # macOS
    # =========================
    if sys.platform == "darwin":
        # 1) Hardware UUID via ioreg (IOPlatformUUID)
        try:
            # Exemplo de output:
            # "IOPlatformUUID" = "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
            cmd = ["ioreg", "-rd1", "-c", "IOPlatformExpertDevice"]
            out = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode(errors="ignore")

            for line in out.splitlines():
                if "IOPlatformUUID" in line:
                    # Extrai o conteúdo entre aspas após o '='
                    # linha típica: '  "IOPlatformUUID" = "...."'
                    parts = line.split("=", 1)
                    if len(parts) == 2:
                        candidate = parts[1].strip().strip('"').strip()
                        candidate = _clean(candidate)
                        if candidate:
                            return candidate
        except Exception:
            pass

        return None

    # =========================
    # Linux (extra, não interfere)
    # =========================
    if sys.platform.startswith("linux"):
        for path in ("/etc/machine-id", "/var/lib/dbus/machine-id"):
            try:
                if os.path.exists(path):
                    with open(path, "r", encoding="utf-8", errors="ignore") as f:
                        candidate = _clean(f.read())
                        if candidate:
                            return candidate
            except Exception:
                pass
        return None

    # =========================
    # Windows (o teu código original, intacto)
    # =========================

    # 1) WMIC (pode não existir em Windows 11 recentes)
    try:
        out = subprocess.check_output(
            "wmic csproduct get uuid",
            shell=True,
            stderr=subprocess.STDOUT
        ).decode(errors="ignore")

        lines = [ln.strip() for ln in out.splitlines() if ln.strip()]
        # Normalmente:
        # lines[0] = "UUID"
        # lines[1] = "<uuid>"
        if len(lines) >= 2:
            candidate = _clean(lines[1])
            if candidate and candidate.upper() != "UUID":
                return candidate
    except Exception:
        pass

    # 2) PowerShell CIM (método recomendado em sistemas mais recentes)
    try:
        cmd = [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy", "Bypass",
            "-Command",
            "(Get-CimInstance Win32_ComputerSystemProduct).UUID"
        ]
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode(errors="ignore")
        candidate = _clean(out)
        if candidate and candidate.upper() != "UUID":
            return candidate
    except Exception:
        pass

    # 3) Fallback final: MachineGuid (estável por instalação do Windows)
    try:
        import winreg
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Microsoft\Cryptography") as key:
            guid, _ = winreg.QueryValueEx(key, "MachineGuid")
            candidate = _clean(str(guid))
            if candidate:
                return candidate
    except Exception:
        pass

    return None


# ======================= LOGIN =======================
def abrir_login():
    """Abre o login com integração do UUID e base de dados."""
    uuid_pc = obter_uuid()

    # Se UUID existe na base de dados → entrar direto
    try:
        conn = ligar_db()
        cursor = conn.cursor(dictionary=True)
        try:
            ensure_vendedores_mac_column(conn)
        except Exception:
            pass
        cursor.execute("SELECT * FROM vendedores WHERE UUID = %s", (uuid_pc,))
        vendedor = cursor.fetchone()
    except Exception as e:
        messagebox.showerror(
            "Erro de ligação à Base de Dados",
            f"Não foi possível ligar à BD.\n\nDetalhes:\n{e}"
        )
        return

    if vendedor:
        # UUID já existe → entrar direto
        cursor.close()
        conn.close()
        globals()['CURRENT_VENDEDOR_NOME'] = vendedor.get('nome','')
        globals()['CURRENT_VENDEDOR_SFID'] = vendedor.get('SFID','')
        # mac: 1 = utilizador Mac, 0 = não-Mac
        try:
            mac_val = vendedor.get('mac', None)
        except Exception:
            mac_val = None
        if mac_val is None:
            mac_val = 1 if sys.platform == 'darwin' else 0
            # tenta persistir para futuras sessões
            try:
                conn2 = ligar_db()
                try:
                    ensure_vendedores_mac_column(conn2)
                    cur2 = conn2.cursor()
                    cur2.execute("UPDATE vendedores SET mac=%s WHERE UUID=%s", (int(mac_val), uuid_pc))
                    conn2.commit()
                    cur2.close()
                finally:
                    conn2.close()
            except Exception:
                pass
        globals()['CURRENT_VENDEDOR_MAC'] = int(mac_val) if str(mac_val).strip() != '' else 0

        # ==========================================
        # CHAMA O DISCORD AQUI!
        # ==========================================
        notificar_discord(uuid_pc, vendedor.get('nome', 'Desconhecido'))
        abrir_interface(vendedor["nome"], vendedor["SFID"])
        return

    # Caso não exista UUID → abrir login para introduzir nome e SFID

    # NOTA: Não chamamos abrir_interface() dentro do login.mainloop(),
    # porque isso cria um mainloop aninhado e pode deixar a UI \"em branco\" na 1ª execução.
    next_user = {'nome': None, 'sfid': None}
    # Quando o utilizador cria a conta pela 1ª vez, vamos encerrar a aplicação e pedir para abrir novamente.
    # Isto evita inconsistências de estilo/layout que podem ocorrer em executáveis.
    exit_after_register = {'value': False}

    login = ttkb.Window(themename="cosmo")
    login.title("Vodafone | Acesso à Ferramenta")
    login.geometry("480x620")
    login.resizable(False, False)

    bg_frame = tk.Frame(login, bg=VODA_RED)
    bg_frame.place(relwidth=1, relheight=1)

    dark_top = tk.Frame(bg_frame, bg="#B80000", height=280)
    dark_top.place(relwidth=1, rely=0)

    tk.Label(bg_frame, text="VODAFONE", font=("Helvetica", 48, "bold"),
             fg="white", bg="#B80000").place(relx=0.5, y=100, anchor="center")
    tk.Label(bg_frame, text="Field Tool", font=("Helvetica", 20),
             fg="white", bg="#B80000").place(relx=0.5, y=160, anchor="center")

    form = tk.Frame(bg_frame, bg=VODA_RED)
    form.place(relx=0.5, rely=0.58, anchor="center")

    tk.Label(form, text="PRIMEIRO E ULTIMO NOME", font=("Helvetica", 10, "bold"),
             fg="white", bg=VODA_RED).pack()
    nome_entry = ttkb.Entry(form, font=("Arial", 14), bootstyle="light")
    nome_entry.pack(padx=40, ipadx=20, ipady=10)

    tk.Label(form, text="SSFID", font=("Helvetica", 10, "bold"),
             fg="white", bg=VODA_RED).pack(pady=(20, 5))
    ssfid_entry = ttkb.Entry(form, font=("Arial", 14), bootstyle="light")
    ssfid_entry.pack(padx=40, ipadx=20, ipady=10)

    # É utilizador MAC?
    mac_var = tk.IntVar(value=1 if sys.platform == 'darwin' else 0)
    try:
        cb_mac = ttk.Checkbutton(form, text="É utilizador MAC ?", variable=mac_var)
        cb_mac.pack(pady=(12, 0))
    except Exception:
        pass


    def registar():
        n = nome_entry.get().strip()
        s = ssfid_entry.get().strip()

        if not n or not s:
            messagebox.showerror("Erro", "Preenche os dois campos!")
            return

        # Verificar se SFID já existe
        cursor.execute("SELECT * FROM vendedores WHERE SFID = %s", (s,))
        if cursor.fetchone():
            messagebox.showerror("Erro Ético", "Este SFID já existe! Acesso negado.")
            return

        # Inserir novo vendedor na tabela
        mac_val = int(mac_var.get() or 0)

        # Garantir coluna mac na tabela vendedores
        try:
            ensure_vendedores_mac_column(conn)
        except Exception:
            pass

        cursor.execute("INSERT INTO vendedores (UUID, nome, SFID, mac) VALUES (%s, %s, %s, %s)",
                       (uuid_pc, n, s, mac_val))
        conn.commit()
        cursor.close()
        conn.close()

        # ==========================================
        # CHAMA O DISCORD AQUI TAMBÉM! (Para malta nova)
        # ==========================================
        notificar_discord(uuid_pc, n)


        # Sinalizar ao fluxo principal que o registo foi concluído
        next_user['nome'] = n
        next_user['sfid'] = s
        globals()['CURRENT_VENDEDOR_NOME'] = n
        globals()['CURRENT_VENDEDOR_SFID'] = s
        globals()['CURRENT_VENDEDOR_MAC'] = int(mac_val)
        exit_after_register['value'] = True

        # Feedback ao utilizador e encerrar para que volte a abrir manualmente.
        messagebox.showinfo(
            'Registo',
            'Conta criada com sucesso.\n\nA aplicação vai desligar agora.\nAbre o programa novamente para entrar.'
        )
        # Sair do mainloop de forma limpa (o destroy será feito após o mainloop terminar).
        try:
            login.after(50, login.quit)
        except Exception:
            try:
                login.quit()
            except Exception:
                pass


    ttkb.Button(form, text="ACEDER À FERRAMENTA, DO FIELDS MARKETING VODAFONE",
                command=registar, bootstyle="danger",
                width=32, padding=15).pack(pady=35)

    tk.Label(bg_frame, text="Ferramenta exclusiva para técnicos Vodafone\nPortugal © 2025",
             font=("Arial", 9), fg="#ff9999", bg=VODA_RED).place(relx=0.5, rely=0.94, anchor="center")

    login.mainloop()

    # Fechar a janela do login (se ainda estiver aberta).
    try:
        login.destroy()
    except Exception:
        pass

    # Se acabou de criar conta, termina aqui e pede ao utilizador para abrir novamente.
    if exit_after_register.get('value'):
        return

    # Caso contrário, abrir a interface principal.
    if next_user.get('nome') and next_user.get('sfid'):
        globals()['CURRENT_VENDEDOR_NOME'] = (next_user.get('nome') or '')
        globals()['CURRENT_VENDEDOR_SFID'] = (next_user.get('sfid') or '')
        # se ainda não estiver definido, assume 1 em macOS, 0 noutros
        try:
            _tmp = globals().get('CURRENT_VENDEDOR_MAC', None)
        except Exception:
            _tmp = None
        if _tmp is None:
            globals()['CURRENT_VENDEDOR_MAC'] = 1 if sys.platform == 'darwin' else 0
        abrir_interface(next_user['nome'], next_user['sfid'])

# ======================= INICIAR APP =======================
if __name__ == "__main__":
    abrir_login()

    # cod completo é este e esta complementado com AI
