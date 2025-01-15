import psycopg2


DB_NAME = "dbcontabil"
DB_USER = "postgres"
DB_PASSWORD = "postgres"
DB_HOST = "localhost"
DB_PORT = "5432"


def criar_conexao():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        print("Conexão com o banco de dados realizada com sucesso!")
        return conn
    except Exception as e:
        print(f"Erro ao conectar no banco de dados: {e}")
        exit()


def criar_tabela():
    conn = criar_conexao()
    try:
        cursor = conn.cursor()
       
        create_table_query = """
        CREATE TABLE IF NOT EXISTS receitas_processadas (
            data_evento CHAR(8) NOT NULL,          -- Exemplo: YYYYMMDD
            codigo_filial CHAR(3) NOT NULL,        -- Exemplo: NN
            tipo_receita CHAR(3) NOT NULL,         -- Exemplo: TTT
            valor_arrecadado CHAR(10) NOT NULL,    -- Exemplo: VVVVVVVVVV
            codigo_categoria CHAR(4) NOT NULL,     -- Exemplo: CCCC
            percentual_impostos CHAR(3) NOT NULL,  -- Exemplo: PPP
            codigo_moeda CHAR(3) NOT NULL,         -- Exemplo: FFF
            status_financeiro CHAR(3) NOT NULL,    -- Exemplo: EEE
            metodo_pagamento CHAR(3) NOT NULL,     -- Exemplo: KKK
            codigo_cliente CHAR(7) NOT NULL        -- Exemplo: MMMMMMM
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        print("Tabela receitas_processadas criada com sucesso!")
    except Exception as e:
        print(f"Erro ao criar a tabela: {e}")
    finally:
        cursor.close()
        conn.close()
        print("Conexão com o banco de dados encerrada.")


if __name__ == "__main__":
    criar_tabela()
