import tkinter as tk
from tkinter import messagebox, ttk
import fitz  # PyMuPDF
import os
import datetime
import ttkbootstrap as ttkb
import mysql.connector
import subprocess

# ====================== MYSQL CONFIG ======================
DB_CONFIG = {
    "host": "sql7.freesqldatabase.com",
    "user": "sql7812774",
    "password": "yKbXpElFuM",
    "database": "sql7812774",
    "port": 3306
}

# ====================== CORES OFICIAIS VODAFONE ======================
VODA_RED = "#E60000"
VODA_DARK = "#121212"
VODA_LIGHT_GRAY = "#F5F5F5"
VODA_WHITE = "#FFFFFF"
VODA_GRAY = "#333333"
VODA_HIGHLIGHT = "#FFFF00"

# ====================== CAMINHO DO FICHEIRO ======================
desktop = os.path.join(os.environ["USERPROFILE"], "Desktop")
folder_path = os.path.join(desktop, "dont_touch_me")
file_path = os.path.join(folder_path, "dont_touch_me.txt")

# Criar pasta se não existir
os.makedirs(folder_path, exist_ok=True)

# Criar ficheiro se não existir
if not os.path.exists(file_path):
    with open(file_path, "w", encoding="utf-8") as f:
        f.write("Nome:\nSSFID:")



# ====================== MYSQL FUNÇÕES ======================
def ligar_db():
    return mysql.connector.connect(**DB_CONFIG)


def inserir_rc(dados, nome_vendedor, sfid):
    conn = ligar_db()
    cursor = conn.cursor()

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


def inserir_vendedor(nome, sfid):
    conn = ligar_db()
    cursor = conn.cursor()

    cursor.execute(
        "INSERT IGNORE INTO vendedores (nome, SFID) VALUES (%s,%s)",
        (nome, sfid)
    )

    conn.commit()
    cursor.close()
    conn.close()

# ====================== FUNÇÕES DE DADOS ======================
def ler_dados():
    """Lê nome e SSFID do ficheiro."""
    nome = ssfid = ""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            for linha in f:
                if linha.startswith("Nome:"):
                    nome = linha.split(":", 1)[1].strip()
                elif linha.startswith("SSFID:"):
                    ssfid = linha.split(":", 1)[1].strip()
    except:
        pass
    return nome, ssfid


def guardar_dados(nome, ssfid):
    """Guarda nome e SSFID no ficheiro e bloqueia escrita."""
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(f"Nome:{nome}\nSSFID:{ssfid}")
    try:
        os.chmod(file_path, 0o444)
    except:
        pass

PDF_ORIGINAL = "contrato_adesao.pdf"
PDF_SAIDA = "contrato_preenchido.pdf"

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
]

COORDS_3 = [
    (335, 439),   # 30. ZE / Sem ZE → "Nome + SFID" na caixa certa (X)
    (50,439),   # 31. Dia
    (90,439),  # 32. Mês   
    (130,439),  # 33. Ano
]

COORDS_4 = [
    # == PORTABILIDADE MOVEL ==

    (100,100), # NOME COMPLETO
]


def preencher_pdf(dados):
    if not os.path.exists(PDF_ORIGINAL):
        messagebox.showerror("Erro", f"Não encontrei o ficheiro {PDF_ORIGINAL}")
        return
    
    

    
    # Obter a data de hoje
    data_hoje = datetime.date.today()

    # busca dia mes ano
    dia = data_hoje.day
    mes = data_hoje.month
    ano = data_hoje.year

    doc = fitz.open(PDF_ORIGINAL)
    page = doc[0]
    page_3 = doc[3]
    page_4 = doc[4]

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
        "DETALHADA" if dados["fatura_tipo"] == "DETALHADA" else "RESUMIDA",   # ajusta se a caixa for outra
        dados["iban"] if dados["iban"].strip() else "—",
        "X" if dados["telemovel1"] else "",
        "X" if dados["telemovel2"] else "",
        "X" if dados["telemovel3"] else "",
        "X" if dados["telemovel4"] else "",
        "X" if dados["net_movel1"] else "",
        "X" if dados["net_movel2"] else "",
        dados["fixo"],
        
    ] # ainda nao esta completo lol, so da a 1 pagina mas consegues gerar as cenas

    try:
        os.chmod(file_path, 0o666)
    except:
        pass

    nome, ssfid = ler_dados()

    campos_3 = [
        "" if dados["ze_sem_ze"] else ssfid,
        dia,
        mes,
        ano,
        
    ]

    campos_4 = [
        dados["nome_completo_movel"],
        dia,
        mes,
        ano,
        
    ]

    

    for (x, y), texto in zip(COORDS, campos):
        if texto:
            page.insert_text((x, y), str(texto).upper(), fontsize=11, color=(0,0,0))

    # pagina 4 do pdf
    for (x, y), texto in zip(COORDS_3, campos_3):
        if texto:
            page_3.insert_text((x, y), str(texto).upper(), fontsize=11, color=(0,0,0))

    # pagina 5 do pdf
    for (x, y), texto in zip(COORDS_4, campos):
        if texto:
            page_4.insert_text((x, y), str(texto).upper(), fontsize=11, color=(0,0,0))


    doc.save(PDF_SAIDA, garbage=4, deflate=True)
    doc.close()
    messagebox.showinfo("PRONTO!", f"Contrato preenchido perfeitamente!\n{PDF_SAIDA}")
    os.startfile(PDF_SAIDA)

def gerar():
    dados = {
        "nome_completo": entry_nome.get().strip(),
        "nif": entry_nif.get().strip(),
        "rua_faturacao": entry_rua_faturacao.get().strip(),
        "cp4_faturacao": entry_cp4_faturacao.get("1.0", "end-1c").strip(),
        "cp3_faturacao": entry_cp3_faturacao.get("1.0", "end-1c").strip(),
        "localidade_faturacao": entry_localidade_faturacao.get().strip(),
        "rua_instalacao": entry_rua_instalacao.get().strip(),
        "cp4_instalacao": entry_cp4_instalacao.get().strip(),
        "cp3_instalacao": entry_cp3_instalacao.get().strip(),
        "localidade_instalacao": entry_localidade_instalacao.get().strip(),
        "contacto": entry_contacto.get().strip(),
        "email": entry_email.get().strip(),
        "plano_valor": entry_plano.get().strip(),
        "velocidade_internet": entry_velocidade.get().strip(),
        "telemovel1": entry_tel1.get().strip(),
        "telemovel2": entry_tel2.get().strip(),
        "telemovel3": entry_tel3.get().strip(),
        "telemovel4": entry_tel4.get().strip(),
        "net_movel1": entry_net1.get().strip(),
        "net_movel2": entry_net2.get().strip(),
        "gigas_min_telemovel1": entry_gigas1.get().strip(),
        "gigas_min_telemovel2": entry_gigas2.get().strip(),
        "gigas_min_telemovel3": entry_gigas3.get().strip(),
        "gigas_min_telemovel4": entry_gigas4.get().strip(),
        "gigas_net_movel1": entry_gigas_net1.get().strip(),
        "gigas_net_movel2": entry_gigas_net2.get().strip(),
        "fatura_eletronica": var_fatura_eletronica.get(),
        "fatura_tipo": combo_fatura.get(),
        "iban": entry_iban.get().strip(),
        "ze_sem_ze": var_ze_sem_ze.get(),
        "fixo": fixo.get().strip(),
        "nome_completo_movel": entry_pm_nome.get().strip(),
    }

    if not dados["nome_completo"] or not dados["nif"]:
        messagebox.showwarning("Atenção", "Nome e NIF obrigatórios!")
        return

    nome_vendedor, sfid = ler_dados()

    try:
        inserir_vendedor(nome_vendedor, sfid)  # garante que vendedor existe

        # ------------------- Verificar se existe NIF -------------------
        conn = ligar_db()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM RC WHERE nif = %s", (dados["nif"],))
        existe = cursor.fetchone()[0]
        if existe:
            # UPDATE
            sql_update = """
            UPDATE RC SET
                nome_completo=%s, tel_contacto=%s, morada=%s, cp7=%s,
                email=%s, pacote=%s, tel_fixo=%s, int_fixa=%s,
                tel_1=%s, tel_2=%s, tel_3=%s, tel_4=%s,
                tar_1=%s, tar_2=%s, tar_3=%s, tar_4=%s,
                FE=%s, NTCB=%s, IBAN=%s, banco=%s,
                SFID=%s, nome=%s, tipo_fatura=%s, ze_sem_ze=%s
            WHERE nif=%s
            """
            valores = (
                dados["nome_completo"],
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
                int(dados["ze_sem_ze"]),
                dados["nif"]
            )
            cursor.execute(sql_update, valores)
        else:
            # INSERT
            inserir_rc(dados, nome_vendedor, sfid)

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        messagebox.showerror("Erro BD", f"Erro ao gravar na base de dados:\n{e}")
        return

    preencher_pdf(dados)
    messagebox.showinfo("Sucesso", "Contrato atualizado/gerado com sucesso!")


# ======================= FUNÇÃO PRINCIPAL (INTERFACE) =======================
def abrir_interface(nome, ssfid):
    janela = tk.Tk()
    janela.title("Contrato Vodafone - POSIÇÕES FIXAS (Perfeito)")
    janela.geometry("950x800")

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

    notebook.add(aba_gerar, text="📝 Gerar Contrato")
    notebook.add(aba_lista, text="📂 Contratos Gerados")

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
    ttk.Label(header,
              text="CONTRATO VODAFONE - PREENCHIMENTO PERFEITO",
              style="Title.TLabel").pack(anchor="w", padx=10, pady=(10, 2))
    ttk.Label(header,
              text="Preenche os campos por ordem. Os obrigatórios têm asterisco (*). As secções avançadas abrem por setas.",
              style="Muted.TLabel").pack(anchor="w", padx=10, pady=(0, 8))

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

    # === CAMPOS ===
    global entry_nome, entry_nif
    ttk.Label(frame, text="DADOS DO CLIENTE", style="Section.TLabel").pack(anchor="w", pady=(0, 8))
    entry_nome = linha("NOME COMPLETO DO CLIENTE")
    entry_nif = linha("NIF DO CLIENTE")

    ttk.Separator(frame, orient="horizontal").pack(fill="x", pady=16)
    ttk.Label(frame, text="MORADA DE FATURAÇÃO", style="Section.TLabel").pack(anchor="w", pady=(0, 8))
    global entry_rua_faturacao
    entry_rua_faturacao = linha("Rua + Nº Porta + Andar/Fração")

    cp_row = tk.Frame(frame, bg=VDF_CARD); cp_row.pack(pady=6, anchor="w", fill="x")
    global entry_cp4_faturacao, entry_cp3_faturacao

    # Mantém Text como tens (não eliminamos código), mas dá “cara” de input
    entry_cp4_faturacao = tk.Text(cp_row, height=1, width=8, bd=1, relief="solid",
                                  highlightthickness=1, highlightbackground=VDF_BORDER, highlightcolor=VDF_RED,
                                  bg=VDF_WHITE, fg=VDF_TEXT, font=("Arial", 11))
    entry_cp4_faturacao.pack(side="left", padx=(0, 6))
    ttk.Label(cp_row, text=" - ", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
    entry_cp3_faturacao = tk.Text(cp_row, height=1, width=8, bd=1, relief="solid",
                                  highlightthickness=1, highlightbackground=VDF_BORDER, highlightcolor=VDF_RED,
                                  bg=VDF_WHITE, fg=VDF_TEXT, font=("Arial", 11))
    entry_cp3_faturacao.pack(side="left", padx=(6, 0))

    global entry_localidade_faturacao
    entry_localidade_faturacao = linha("Localidade")

    ttk.Separator(frame, orient="horizontal").pack(fill="x", pady=16)
    ttk.Label(frame, text="MORADA DE INSTALAÇÃO (App)", style="Section.TLabel").pack(anchor="w", pady=(0, 8))

    global entry_rua_instalacao, entry_cp4_instalacao, entry_cp3_instalacao, entry_localidade_instalacao
    entry_rua_instalacao = linha("Rua + Nº Porta + Andar/Fração")
    entry_cp4_instalacao = linha("Primeiros 4 números CP")
    entry_cp3_instalacao = linha("Últimos 3 números CP")
    entry_localidade_instalacao = linha("Localidade")

    ttk.Separator(frame, orient="horizontal").pack(fill="x", pady=16)
    ttk.Label(frame, text="CONTACTOS E SERVIÇO", style="Section.TLabel").pack(anchor="w", pady=(0, 8))

    global entry_contacto, entry_email, entry_plano, entry_velocidade
    entry_contacto = linha("CONTACTO DO CLIENTE")
    entry_email = linha("EMAIL")
    entry_plano = linha("PLANO E VALOR")
    entry_velocidade = linha("VELOCIDADE DA INTERNET FIXA")

    global entry_tel1, entry_tel2, entry_tel3, entry_tel4
    ttk.Separator(frame, orient="horizontal").pack(fill="x", pady=16)
    ttk.Label(frame, text="TELEMÓVEIS", style="Section.TLabel").pack(anchor="w", pady=(0, 8))
    entry_tel1 = linha("Telemóvel 1", True, 15)
    entry_tel2 = linha("Telemóvel 2", True, 15)
    entry_tel3 = linha("Telemóvel 3", True, 15)
    entry_tel4 = linha("Telemóvel 4", True, 15)

    global entry_net1, entry_net2
    ttk.Separator(frame, orient="horizontal").pack(fill="x", pady=16)
    ttk.Label(frame, text="NET MÓVEL", style="Section.TLabel").pack(anchor="w", pady=(0, 8))
    entry_net1 = linha("Net Móvel 1", False)
    entry_net2 = linha("Net Móvel 2", False)

    global entry_gigas1, entry_gigas2, entry_gigas3, entry_gigas4, entry_gigas_net1, entry_gigas_net2
    ttk.Separator(frame, orient="horizontal").pack(fill="x", pady=16)
    ttk.Label(frame, text="GIGAS E MINUTOS", style="Section.TLabel").pack(anchor="w", pady=(0, 8))
    entry_gigas1 = linha("Telemóvel 1", False)
    entry_gigas2 = linha("Telemóvel 2", False)
    entry_gigas3 = linha("Telemóvel 3", False)
    entry_gigas4 = linha("Telemóvel 4", False)
    entry_gigas_net1 = linha("Gigas Net Móvel 1", False)
    entry_gigas_net2 = linha("Gigas Net Móvel 2", False)

    global fixo
    ttk.Separator(frame, orient="horizontal").pack(fill="x", pady=16)
    ttk.Label(frame, text="TELEMOVEL FIXO", style="Section.TLabel").pack(anchor="w", pady=(0, 8))
    fixo = linha("Telemóvel fixo", False)

    global var_fatura_eletronica, var_ze_sem_ze, combo_fatura, entry_iban
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

    # ---------- FRAME DAS SEÇÕES EXPANSÍVEIS ----------
    frame_opcoes = ttk.Frame(main_frame, style="Card.TFrame")
    frame_opcoes.pack(fill="x", padx=18, pady=(6, 10))

    inner_opcoes = ttk.Frame(frame_opcoes, style="Card.TFrame")
    inner_opcoes.pack(fill="x", padx=18, pady=18)

    ttk.Label(inner_opcoes, text="OPÇÕES AVANÇADAS", style="Section.TLabel").pack(anchor="w", pady=(0, 8))
    ttk.Label(inner_opcoes, text="Abre apenas o que precisas: Portabilidade, Titularidade e Rescisão.",
              style="CardMuted.TLabel").pack(anchor="w", pady=(0, 12))

    # ---------- PORTABILIDADE MÓVEL (COM SETINHA) ----------
    pm_aberto = tk.BooleanVar(value=False)

    btn_pm = ttk.Button(inner_opcoes, text="▶ Portabilidade Móvel", style="Ghost.TButton")
    btn_pm.pack(anchor="w", pady=4)
    _bind_hover(btn_pm, "Ghost.TButton")

    frame_pm_extra = ttk.Frame(inner_opcoes, style="Card.TFrame")

    def toggle_pm():
        if pm_aberto.get():
            frame_pm_extra.pack_forget()
            btn_pm.config(text="▶ Portabilidade Móvel")
            pm_aberto.set(False)
        else:
            frame_pm_extra.pack(fill="x", padx=10, pady=10)
            btn_pm.config(text="▼ Portabilidade Móvel")
            pm_aberto.set(True)

    btn_pm.config(command=toggle_pm)

    # Campos================================================================
    # ----- Dados do titular da portabilidade -----
    ttk.Label(frame_pm_extra, text="DADOS DO TITULAR (PORTABILIDADE)", style="SubSection.TLabel").pack(anchor="w", pady=(0, 8))

    row_nome = tk.Frame(frame_pm_extra, bg=VDF_CARD)
    row_nome.pack(anchor="w", pady=4, fill="x")

    global entry_pm_nome
    ttk.Label(row_nome, text="Nome", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
    entry_pm_nome = ttk.Entry(row_nome, width=40)
    entry_pm_nome.pack(side="left", padx=10)

    row_nif = tk.Frame(frame_pm_extra, bg=VDF_CARD)
    row_nif.pack(anchor="w", pady=4, fill="x")

    ttk.Label(row_nif, text="NIF", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
    entry_pm_nif = ttk.Entry(row_nif, width=20)
    entry_pm_nif.pack(side="left", padx=10)

    def copiar_dados_fa():
        # Nome e NIF
        entry_pm_nome.delete(0, tk.END)
        entry_pm_nome.insert(0, entry_nome.get())

        entry_pm_nif.delete(0, tk.END)
        entry_pm_nif.insert(0, entry_nif.get())

        # Telemóveis (do FA → Portabilidade)
        telefones_fa = [
            entry_tel1.get(),
            entry_tel2.get(),
            entry_tel3.get(),
            entry_tel4.get()
        ]

        # Limpar linhas atuais
        for tel, cvp in linhas_telemoveis:
            tel.delete(0, tk.END)

        for i, numero in enumerate(telefones_fa):
            if numero.strip():
                # Criar mais linhas se necessário
                if i >= len(linhas_telemoveis):
                    adicionar_telemovel()
                linhas_telemoveis[i][0].delete(0, tk.END)
                linhas_telemoveis[i][0].insert(0, numero)

    btn_copiar_fa = ttk.Button(
        frame_pm_extra,
        text="📋 Copiar dados do FA",
        style="Ghost.TButton",
        command=copiar_dados_fa
    )
    btn_copiar_fa.pack(anchor="w", pady=10)
    _bind_hover(btn_copiar_fa, "Ghost.TButton")

    frame_cvp_meo = tk.Frame(frame_pm_extra, bg=VDF_CARD)
    ttk.Label(frame_cvp_meo, text="CVP (MEO – único)", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
    entry_cvp_meo = ttk.Entry(frame_cvp_meo, width=20)
    entry_cvp_meo.pack(side="left", padx=10)

    # ----- Operador atual -----
    row_operador = tk.Frame(frame_pm_extra, bg=VDF_CARD)
    row_operador.pack(anchor="w", pady=8, fill="x")

    ttk.Label(row_operador, text="Operador atual?", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")

    combo_operador = ttk.Combobox(
        row_operador,
        values=["MEO", "NOS", "NOWO", "UZO", "WOO", "DIGI", "Lycamobile", "Amigo"],
        width=15,
        state="readonly"
    )
    combo_operador.set("NOS")
    combo_operador.pack(side="left", padx=10)

    # ----- Frame dos telemóveis -----
    frame_telemoveis = tk.Frame(frame_pm_extra, bg=VDF_CARD)
    frame_telemoveis.pack(anchor="w", pady=6, fill="x")

    # Guardar linhas
    linhas_telemoveis = []

    MAX_TELEMOVEIS = 4

    # ----- CVP único (MEO) -----
    frame_cvp_meo = tk.Frame(frame_pm_extra, bg=VDF_CARD)
    ttk.Label(frame_cvp_meo, text="CVP (MEO – único)", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
    entry_cvp_meo = ttk.Entry(frame_cvp_meo, width=20)
    entry_cvp_meo.pack(side="left", padx=10)

    frame_cvp_meo.pack(anchor="w", pady=8)

    def atualizar_visibilidade_cvp():
        operador = combo_operador.get()

        if operador == "MEO":
            frame_cvp_meo.pack(anchor="w", pady=5)
            for _, cvp in linhas_telemoveis:
                cvp.pack_forget()
        else:
            frame_cvp_meo.pack_forget()
            for _, cvp in linhas_telemoveis:
                cvp.pack(side="left", padx=5)

    combo_operador.bind("<<ComboboxSelected>>", lambda e: atualizar_visibilidade_cvp())

    # ----- Função para adicionar telemóvel -----
    def adicionar_telemovel():
        if len(linhas_telemoveis) >= MAX_TELEMOVEIS:
            messagebox.showwarning("Limite", "Máximo de 4 telemóveis.")
            return

        linha = tk.Frame(frame_telemoveis, bg=VDF_CARD)
        linha.pack(anchor="w", pady=4)

        ttk.Label(linha, text=f"Telemóvel {len(linhas_telemoveis) + 1}", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")

        entry_tel = ttk.Entry(linha, width=18)
        entry_tel.pack(side="left", padx=5)

        ttk.Label(linha, text="CVP", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left", padx=(10, 0))
        entry_cvp = ttk.Entry(linha, width=20)

        linhas_telemoveis.append((entry_tel, entry_cvp))

        atualizar_visibilidade_cvp()

    # ----- Botão adicionar -----
    btn_add_tel = ttk.Button(
        frame_pm_extra,
        text="➕ Adicionar Telemóvel",
        style="Ghost.TButton",
        command=adicionar_telemovel
    )
    btn_add_tel.pack(anchor="w", pady=8)
    _bind_hover(btn_add_tel, "Ghost.TButton")

    # Criar o primeiro por defeito
    adicionar_telemovel()
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
            btn_pf.config(text="▼ Portabilidade Telemóvel Fixo")
            pf_aberto.set(True)

    btn_pf = ttk.Button(
        inner_conteudo,
        text="▶ Portabilidade Telemóvel Fixo",
        style="Ghost.TButton",
        command=toggle_portabilidade_fixa
    )
    btn_pf.pack(anchor="w", pady=6)
    _bind_hover(btn_pf, "Ghost.TButton")

    frame_pf_extra = ttk.Frame(inner_conteudo, style="Card.TFrame")
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

        entry_pf_fixo.delete(0, tk.END)
        entry_pf_fixo.insert(0, fixo.get())

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

    def toggle_titularidade():
        if tit_aberto.get():
            frame_tit_extra.pack_forget()
            btn_tit.config(text="▶ Alteração de Titularidade")
            tit_aberto.set(False)
        else:
            frame_tit_extra.pack(fill="x", pady=10)
            btn_tit.config(text="▼ Alteração de Titularidade")
            tit_aberto.set(True)

    btn_tit = ttk.Button(
        frame_titularidade,
        text="▶ Alteração de Titularidade",
        style="Ghost.TButton",
        command=toggle_titularidade
    )
    btn_tit.pack(anchor="w")
    _bind_hover(btn_tit, "Ghost.TButton")

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

    ttk.Label(
        frame_tit_extra,
        text="ALTERAÇÃO DE MORADA OU CONTACTO",
        style="SubSection.TLabel"
    ).pack(anchor="w", pady=(0, 6))

    entry_tit_morada = linha(frame_tit_extra, "Nova Morada", 50)

    row_cp = tk.Frame(frame_tit_extra, bg=VDF_CARD)
    row_cp.pack(anchor="w", pady=4)
    ttk.Label(row_cp, text="Código Postal", width=30, background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
    entry_cp4 = ttk.Entry(row_cp, width=8)
    entry_cp4.pack(side="left")
    ttk.Label(row_cp, text=" - ", background=VDF_CARD, foreground=VDF_TEXT).pack(side="left")
    entry_cp3 = ttk.Entry(row_cp, width=6)
    entry_cp3.pack(side="left", padx=5)

    entry_tit_contacto = linha(frame_tit_extra, "Novo Contacto")

    ttk.Separator(frame_tit_extra, orient="horizontal").pack(fill="x", pady=10)

    ttk.Label(
        frame_tit_extra,
        text="ALTERAÇÃO DE TITULARIDADE",
        style="SubSection.TLabel"
    ).pack(anchor="w", pady=(0, 6))

    entry_nif_antigo = linha(frame_tit_extra, "NIF Antigo Titular")
    entry_nif_novo = linha(frame_tit_extra, "NIF Novo Titular")

    ttk.Separator(frame_tit_extra, orient="horizontal").pack(fill="x", pady=10)

    ttk.Label(frame_tit_extra, text="ALTERAÇÃO DO Nº DE TELEMÓVEL", style="SubSection.TLabel").pack(anchor="w", pady=(0, 6))
    entry_tel_antigo = linha(frame_tit_extra, "Nº Telemóvel Antigo")
    entry_tel_novo = linha(frame_tit_extra, "Nº Telemóvel Novo")

    ttk.Separator(frame_tit_extra, orient="horizontal").pack(fill="x", pady=10)

    ttk.Label(frame_tit_extra, text="ALTERAÇÃO DO CARTÃO SIM", style="SubSection.TLabel").pack(anchor="w", pady=(0, 6))
    entry_sim_antigo = linha(frame_tit_extra, "Cartão SIM Antigo")
    entry_sim_novo = linha(frame_tit_extra, "Cartão SIM Novo")

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
            btn_rescisao.config(text="▼ Rescisão")
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

    combo_operadora_res = ttk.Combobox(
        frame_rescisao_extra,
        state="readonly",
        width=110,
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

    ttk.Separator(frame_rescisao_extra, orient="horizontal").pack(fill="x", pady=10)

    ttk.Label(
        frame_rescisao_extra,
        text="NÚMEROS DE TELEMÓVEL",
        style="SubSection.TLabel"
    ).pack(anchor="w", pady=(0, 6))

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
        # Nome
        entry_res_nome.delete(0, tk.END)
        entry_res_nome.insert(0, entry_nome.get())

        # Morada
        entry_res_morada.delete(0, tk.END)
        entry_res_morada.insert(0, entry_rua_instalacao.get())

        # Código Postal + Localidade
        entry_res_cp4.delete(0, tk.END)
        entry_res_cp4.insert(0, entry_cp4_instalacao.get())

        entry_res_cp3.delete(0, tk.END)
        entry_res_cp3.insert(0, entry_cp3_instalacao.get())

        entry_res_localidade.delete(0, tk.END)
        entry_res_localidade.insert(0, entry_localidade_instalacao.get())

        # Telefone fixo
        entry_res_fixo.delete(0, tk.END)
        entry_res_fixo.insert(0, fixo.get())

        # Telemóveis
        fa_tels = [entry_tel1.get(), entry_tel2.get(), entry_tel3.get(), entry_tel4.get()]

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
        text="GERAR CONTRATO 100% PERFEITO",
        style="Primary.TButton",
        command=gerar
    )
    btn_gerar.pack(fill="x", pady=(10, 6))

    ttk.Label(
        footer,
        text="Agora cada campo vai EXACTAMENTE para o sítio certo!",
        foreground="green",
        font=("Arial", 10, "bold"),
        background=VDF_BG
    ).pack(pady=(6, 0))

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

        for i, contrato in enumerate(contratos):
            # Card por contrato (branco com borda suave)
            outer = tk.Frame(aba_lista, bg=VDF_BG)
            outer.pack(fill="x", padx=14, pady=6)

            frame_contrato = tk.Frame(outer, bg=VDF_WHITE, bd=1, relief="solid", highlightthickness=0)
            frame_contrato.pack(fill="x")

            left = tk.Frame(frame_contrato, bg=VDF_WHITE)
            left.pack(side="left", fill="x", expand=True, padx=12, pady=10)

            title_txt = f"{contrato['nome_completo']} | {contrato['nif']}"
            tk.Label(left, text=title_txt, bg=VDF_WHITE, fg=VDF_TEXT, font=("Arial", 11, "bold")).pack(anchor="w")
            tk.Label(left, text=f"SFID: {contrato.get('SFID', '')}", bg=VDF_WHITE, fg=VDF_MUTED, font=("Arial", 9)).pack(anchor="w", pady=(2, 0))

            def editar_closure(c=contrato):
                # função para evitar 0 → vazio
                def valor(campo):
                    v = c.get(campo, "")
                    if v == 0 or v == "0" or v is None:
                        return ""
                    return str(v)

                entry_nome.delete(0, tk.END)
                entry_nome.insert(0, valor("nome_completo"))
                entry_nif.delete(0, tk.END)
                entry_nif.insert(0, valor("nif"))
                entry_rua_faturacao.delete(0, tk.END)
                entry_rua_faturacao.insert(0, valor("morada").split(",")[0] if "," in valor("morada") else valor("morada"))
                entry_localidade_faturacao.delete(0, tk.END)
                entry_localidade_faturacao.insert(0, valor("morada").split(",")[1] if "," in valor("morada") else "")
                entry_cp4_faturacao.delete("1.0", tk.END)
                entry_cp4_faturacao.insert("1.0", valor("cp7").split("-")[0] if "-" in valor("cp7") else valor("cp7"))
                entry_cp3_faturacao.delete("1.0", tk.END)
                entry_cp3_faturacao.insert("1.0", valor("cp7").split("-")[1] if "-" in valor("cp7") else "")
                entry_email.delete(0, tk.END)
                entry_email.insert(0, valor("email"))
                entry_plano.delete(0, tk.END)
                entry_plano.insert(0, valor("pacote"))
                entry_contacto.delete(0, tk.END)
                entry_contacto.insert(0, valor("tel_contacto"))
                entry_iban.delete(0, tk.END)
                entry_iban.insert(0, valor("IBAN"))
                entry_tel1.delete(0, tk.END); entry_tel1.insert(0, valor("tel_1"))
                entry_tel2.delete(0, tk.END); entry_tel2.insert(0, valor("tel_2"))
                entry_tel3.delete(0, tk.END); entry_tel3.insert(0, valor("tel_3"))
                entry_tel4.delete(0, tk.END); entry_tel4.insert(0, valor("tel_4"))
                entry_net1.delete(0, tk.END); entry_net1.insert(0, valor("int_fixa"))
                entry_net2.delete(0, tk.END); entry_net2.insert(0, valor("NTCB"))
                entry_gigas1.delete(0, tk.END); entry_gigas1.insert(0, valor("tar_1"))
                entry_gigas2.delete(0, tk.END); entry_gigas2.insert(0, valor("tar_2"))
                entry_gigas3.delete(0, tk.END); entry_gigas3.insert(0, valor("tar_3"))
                entry_gigas4.delete(0, tk.END); entry_gigas4.insert(0, valor("tar_4"))
                entry_gigas_net1.delete(0, tk.END); entry_gigas_net1.insert(0, valor("FE"))
                entry_gigas_net2.delete(0, tk.END); entry_gigas_net2.insert(0, valor("banco"))
                fixo.delete(0, tk.END); fixo.insert(0, valor("tel_fixo"))
                var_fatura_eletronica.set(bool(c.get("FE", 0)))
                var_ze_sem_ze.set(bool(c.get("ze_sem_ze", 0)))
                combo_fatura.set(valor("tipo_fatura"))

                notebook.select(aba_gerar)  # mudar para aba gerar

            # Botão editar com look Vodafone
            btn = tk.Button(frame_contrato, text="Editar", command=editar_closure,
                            bg=VDF_RED, fg=VDF_WHITE, bd=0, padx=14, pady=8,
                            activebackground=VDF_RED_DARK, activeforeground=VDF_WHITE,
                            font=("Arial", 10, "bold"), cursor="hand2")
            btn.pack(side="right", padx=12, pady=10)

    carregar_contratos()
    janela.mainloop()




def obter_uuid():
    """Obtem UUID do PC."""
    try:
        uuid = subprocess.check_output(
            'wmic csproduct get uuid', shell=True
        ).decode().split('\n')[1].strip()
        return uuid
    except:
        return None


# ======================= LOGIN =======================
def abrir_login():
    """Abre o login com integração do UUID e base de dados."""

    # Desbloquear escrita no ficheiro
    try:
        os.chmod(file_path, 0o666)
    except:
        pass

    # Tentar ler dados locais
    nome, ssfid = ler_dados()
    uuid_pc = obter_uuid()

    # Se UUID existe na base de dados → entrar direto
    conn = ligar_db()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM vendedores WHERE UUID = %s", (uuid_pc,))
    vendedor = cursor.fetchone()

    if vendedor:
        # UUID já existe → entrar direto
        cursor.close()
        conn.close()
        abrir_interface(vendedor["nome"], vendedor["SFID"])
        return

    # Caso não exista UUID → abrir login para introduzir nome e SFID
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

    tk.Label(form, text="NOME COMPLETO", font=("Helvetica", 10, "bold"),
             fg="white", bg=VODA_RED).pack()
    nome_entry = ttkb.Entry(form, font=("Arial", 14), bootstyle="light")
    nome_entry.pack(padx=40, ipadx=20, ipady=10)

    tk.Label(form, text="SSFID", font=("Helvetica", 10, "bold"),
             fg="white", bg=VODA_RED).pack(pady=(20, 5))
    ssfid_entry = ttkb.Entry(form, font=("Arial", 14), bootstyle="light")
    ssfid_entry.pack(padx=40, ipadx=20, ipady=10)

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
        cursor.execute("INSERT INTO vendedores (UUID, nome, SFID) VALUES (%s, %s, %s)",
                       (uuid_pc, n, s))
        conn.commit()
        cursor.close()
        conn.close()

        # Guardar dados localmente
        guardar_dados(n, s)

        login.destroy()
        abrir_interface(n, s)

    ttkb.Button(form, text="ACEDER À FERRAMENTA",
                command=registar, bootstyle="danger",
                width=32, padding=15).pack(pady=35)

    tk.Label(bg_frame, text="Ferramenta exclusiva para técnicos Vodafone\nPortugal © 2025",
             font=("Arial", 9), fg="#ff9999", bg=VODA_RED).place(relx=0.5, rely=0.94, anchor="center")

    login.mainloop()


# ======================= INICIAR APP =======================
if __name__ == "__main__":
    abrir_login()