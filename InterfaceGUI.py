import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import psycopg2
import pandas as pd
import matplotlib.pyplot as plt

DB_CONFIG = {
    'dbname': 'dbcontabil',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': '5432'
}


def criar_conexao():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Conexão com o banco de dados realizada com sucesso!")
        return conn
    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao conectar no banco de dados: {e}")
        exit()


def buscar_dados(tabela):
    conn = criar_conexao()
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT * FROM {tabela}")
        colunas = [desc[0] for desc in cursor.description]
        dados = cursor.fetchall()
        conn.close()
        return colunas, dados
    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao buscar dados: {e}")
        conn.close()
        return [], []


def exportar_csv(tabela):
    colunas, dados = buscar_dados(tabela)
    if colunas and dados:
        df = pd.DataFrame(dados, columns=colunas)
        caminho_arquivo = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
            initialfile=f"{tabela}_relatorio.csv"
        )
        if caminho_arquivo:
            df.to_csv(caminho_arquivo, index=False)
            messagebox.showinfo("Exportação", f"Relatório exportado com sucesso para {caminho_arquivo}")
    else:
        messagebox.showerror("Erro", "Não foi possível exportar os dados.")

def gerar_grafico(tabela):
 
    colunas, dados = buscar_dados(tabela)
    if colunas and dados:
        df = pd.DataFrame(dados, columns=colunas)

        if len(df.columns) < 2:
            messagebox.showerror("Erro", "Tabela não possui colunas suficientes para gerar gráficos.")
            return

        
        janela_grafico = tk.Toplevel(janela)
        janela_grafico.title("Gerar Gráfico")
        janela_grafico.geometry("400x300")

        tk.Label(janela_grafico, text="Selecione a coluna X (categorias):").pack(pady=5)
        coluna_x = ttk.Combobox(janela_grafico, values=colunas)
        coluna_x.pack(pady=5)

        tk.Label(janela_grafico, text="Selecione a coluna Y (valores numéricos):").pack(pady=5)
        coluna_y = ttk.Combobox(janela_grafico, values=colunas)
        coluna_y.pack(pady=5)

        def criar_grafico_colunas():
            x = coluna_x.get()
            y = coluna_y.get()

            if x and y:
                try:
                    
                    if not pd.api.types.is_numeric_dtype(df[y]):
                        raise ValueError("A coluna Y deve conter dados numéricos.")

                   
                    plt.figure(figsize=(10, 6))
                    plt.bar(df[x], df[y], color='skyblue')
                    plt.xlabel(x, fontsize=12)
                    plt.ylabel(y, fontsize=12)
                    plt.title(f"Gráfico de {y} por {x}", fontsize=14)
                    plt.xticks(rotation=45, ha='right')
                    plt.tight_layout()
                    plt.show()

                except Exception as e:
                    messagebox.showerror("Erro", f"Erro ao criar o gráfico: {e}")
            else:
                messagebox.showerror("Erro", "Selecione as colunas X e Y para o gráfico.")

        tk.Button(janela_grafico, text="Gerar Gráfico", command=criar_grafico_colunas).pack(pady=20)
    else:
        messagebox.showerror("Erro", "Tabela não contém dados suficientes para gerar gráficos.")


def exibir_dados(tabela):
    colunas, dados = buscar_dados(tabela)
    if not colunas or not dados:
        messagebox.showerror("Erro", "Tabela vazia ou não encontrada.")
        return

 
    for item in tree.get_children():
        tree.delete(item)

  
    tree["columns"] = colunas
    tree["show"] = "headings"
    for col in colunas:
        tree.heading(col, text=col)
        tree.column(col, anchor="center", width=150)

    
    for linha in dados:
        tree.insert("", "end", values=linha)


janela = tk.Tk()
janela.title("Sistema Contábil - Análise e Relatórios")
janela.geometry("1200x800")


frame_tabelas = tk.Frame(janela)
frame_tabelas.pack(pady=10)

tk.Label(frame_tabelas, text="Selecione a Tabela:").pack(side=tk.LEFT, padx=5)
tabela_selecionada = tk.StringVar()
tabela_menu = ttk.Combobox(frame_tabelas, textvariable=tabela_selecionada)
tabela_menu["values"] = ["categorias", "clientes", "filiais", "tipos_de_receita", "receitas_processadas"]
tabela_menu.pack(side=tk.LEFT, padx=5)


botao_exibir = tk.Button(frame_tabelas, text="Exibir Dados", command=lambda: exibir_dados(tabela_selecionada.get()))
botao_exibir.pack(side=tk.LEFT, padx=5)


botao_exportar = tk.Button(frame_tabelas, text="Exportar Relatório", command=lambda: exportar_csv(tabela_selecionada.get()))
botao_exportar.pack(side=tk.LEFT, padx=5)


botao_grafico = tk.Button(frame_tabelas, text="Gerar Gráfico", command=lambda: gerar_grafico(tabela_selecionada.get()))
botao_grafico.pack(side=tk.LEFT, padx=5)


frame_dados = tk.Frame(janela)
frame_dados.pack(pady=10, fill=tk.BOTH, expand=True)

tree = ttk.Treeview(frame_dados)
tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)


scrollbar = ttk.Scrollbar(frame_dados, orient=tk.VERTICAL, command=tree.yview)
scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
tree.configure(yscrollcommand=scrollbar.set)


janela.mainloop()
