import pandas as pd
import psycopg2
from datetime import datetime


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
        print(f"Erro ao conectar no banco de dados: {e}")
        exit()


def processar_arquivo(file_path):
    dados_processados = []
    linhas_invalidas = 0

    try:
        with open(file_path, 'r') as arquivo:
            for linha in arquivo:
                try:
                    data_evento = linha[0:8].strip() or None
                    codigo_filial = linha[8:10].strip() or None
                    tipo_receita = linha[10:13].strip() or None
                    valor_arrecadado = linha[13:20].strip()
                    codigo_categoria = linha[20:24].strip() or None
                    percentual_impostos = linha[24:27].strip() or None
                    codigo_moeda = linha[27:30].strip() or None
                    status_financeiro = linha[30:33].strip() or None
                    metodo_pagamento = linha[33:36].strip() or None
                    codigo_cliente = linha[36:43].strip() or None

                  
                    valor_arrecadado = int(valor_arrecadado) if valor_arrecadado.isdigit() else 0
                    if not all([data_evento, codigo_filial, tipo_receita, codigo_cliente]):
                        raise ValueError("Valores obrigatórios ausentes ou inválidos")

                    dados_processados.append({
                        "data_evento": data_evento,
                        "codigo_filial": codigo_filial,
                        "tipo_receita": tipo_receita,
                        "valor_arrecadado": valor_arrecadado,
                        "codigo_categoria": codigo_categoria,
                        "percentual_impostos": percentual_impostos,
                        "codigo_moeda": codigo_moeda,
                        "status_financeiro": status_financeiro,
                        "metodo_pagamento": metodo_pagamento,
                        "codigo_cliente": codigo_cliente
                    })
                except Exception as e:
                    print(f"Linha inválida ignorada: {linha.strip()} | Erro: {e}")
                    linhas_invalidas += 1

        print(f"Arquivo processado com sucesso! Linhas inválidas: {linhas_invalidas}")
        return pd.DataFrame(dados_processados)
    except Exception as e:
        print(f"Erro ao processar o arquivo: {e}")
        return pd.DataFrame()


def inserir_dados(df):
    conn = criar_conexao()
    cursor = conn.cursor()
    inseridos = 0
    erros = 0

    try:
        for _, row in df.iterrows():
            try:
                
                cursor.execute(
                    """
                    INSERT INTO receitas_processadas 
                    (data_evento, codigo_filial, tipo_receita, valor_arrecadado, 
                     codigo_categoria, percentual_impostos, codigo_moeda, 
                     status_financeiro, metodo_pagamento, codigo_cliente)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        row['data_evento'],
                        row['codigo_filial'],
                        row['tipo_receita'],
                        row['valor_arrecadado'],
                        row['codigo_categoria'],
                        row['percentual_impostos'],
                        row['codigo_moeda'],
                        row['status_financeiro'],
                        row['metodo_pagamento'],
                        row['codigo_cliente']
                    )
                )
                inseridos += 1
            except Exception as e:
                conn.rollback()  
                print(f"Erro ao inserir o registro: {e}")
                erros += 1

        conn.commit()
        print(f"Dados inseridos com sucesso! Registros inseridos: {inseridos}, Erros: {erros}")
    except Exception as e:
        print(f"Erro ao inserir os dados no banco: {e}")
    finally:
        cursor.close()
        conn.close()
        print("Conexão com o banco de dados encerrada.")


if __name__ == "__main__":
    caminho_arquivo = "PROFFIT-2024-04.txt"  
    df_dados = processar_arquivo(caminho_arquivo)

    if not df_dados.empty:
        inserir_dados(df_dados)
    else:
        print("Nenhum dado válido para inserir.")
