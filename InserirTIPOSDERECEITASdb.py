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


def criar_tabela_tipos_receita():
    conn = criar_conexao()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tipos_de_receita (
            ID_Tipo_Receita SERIAL PRIMARY KEY,
            Descricao TEXT NOT NULL
            
        )
    ''')
    conn.commit()
    conn.close()
    print("Tabela 'Tipos De Receita' criada com sucesso!")
    
    


def inserir_dados_tipos_receita(caminho_csv):
    try:
        
        df = pd.read_csv(caminho_csv)

        
        print("Colunas do CSV:", df.columns)

       
        conn = criar_conexao()
        cursor = conn.cursor()

        
        for _, row in df.iterrows():
            cursor.execute(
                "INSERT INTO tipos_de_receita (ID_Tipo_Receita, Descricao) VALUES (%s, %s)",
                (row['ID_Tipo_Receita'], row['Descricao'])
            )

        conn.commit()
        conn.close()
        print("Dados inseridos com sucesso na tabela 'Tipos De Receita'!")
    except Exception as e:
        print(f"Erro ao inserir os dados no banco: {e}")


if __name__ == "__main__":
    caminho_csv = "Tabela_de_Tipos_De_Receita.csv" 
    criar_tabela_tipos_receita()  
    inserir_dados_tipos_receita(caminho_csv)  
