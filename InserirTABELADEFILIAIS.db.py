import psycopg2
import pandas as pd


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


def criar_tabela_filiais():
    conn = criar_conexao()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS filiais (
            ID_Filial SERIAL PRIMARY KEY,
            Nome TEXT NOT NULL,
            Localizacao TEXT NOT NULL
        )
    ''')
    conn.commit()
    conn.close()
    print("Tabela 'Filiais' criada com sucesso!")


def inserir_dados_filiais(caminho_csv):
    try:
       
        df = pd.read_csv(caminho_csv)

       
        print("Colunas do CSV:", df.columns)

        
        conn = criar_conexao()
        cursor = conn.cursor()

       
        for _, row in df.iterrows():
            cursor.execute(
                "INSERT INTO filiais (ID_Filial, Nome, Localizacao) VALUES (%s, %s, %s)",
                (row['ID_Filial'], row['Nome'], row['Localizacao'])
            )

        conn.commit()
        conn.close()
        print("Dados inseridos com sucesso na tabela 'Filiais'!")
    except Exception as e:
        print(f"Erro ao inserir os dados no banco: {e}")


if __name__ == "__main__":
    caminho_csv = "Tabela_de_Filiais.csv"  
    criar_tabela_filiais()  
    inserir_dados_filiais(caminho_csv)  
