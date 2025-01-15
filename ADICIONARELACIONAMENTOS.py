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


def relacionar_filiais_categorias():
    try:
        conn = criar_conexao()
        cursor = conn.cursor()
        cursor.execute('''
            ALTER TABLE filiais
            ADD COLUMN ID_Categoria INT,
            ADD CONSTRAINT fk_filiais_categorias
            FOREIGN KEY (ID_Categoria)
            REFERENCES categorias(ID_Categoria)
            ON DELETE CASCADE;
        ''')
        conn.commit()
        print("Relacionamento entre 'filiais' e 'categorias' adicionado com sucesso!")
    except Exception as e:
        print(f"Erro ao adicionar relacionamento entre 'filiais' e 'categorias': {e}")
    finally:
        conn.close()


def relacionar_clientes_tipos_de_receitas():
    try:
        conn = criar_conexao()
        cursor = conn.cursor()
        cursor.execute('''
            ALTER TABLE clientes
            ADD COLUMN ID_Tipo_Receita INT,
            ADD CONSTRAINT fk_clientes_tipos_de_receitas
            FOREIGN KEY (id_Tipo_Receita)
            REFERENCES tipos_de_receita(id_Tipo_Receita)
            ON DELETE CASCADE;
        ''')
        conn.commit()
        print("Relacionamento entre 'clientes' e 'tipos_de_receitas' adicionado com sucesso!")
    except Exception as e:
        print(f"Erro ao adicionar relacionamento entre 'clientes' e 'tipos_de_receitas': {e}")
    finally:
        conn.close()


def relacionar_tipos_de_receitas_categorias():
    try:
        conn = criar_conexao()
        cursor = conn.cursor()
        cursor.execute('''
            ALTER TABLE tipos_de_receita
            ADD COLUMN ID_Categoria INT,
            ADD CONSTRAINT fk_tipos_de_receitas_categorias
            FOREIGN KEY (ID_Categoria)
            REFERENCES categorias(ID_Categoria)
            ON DELETE CASCADE;
        ''')
        conn.commit()
        print("Relacionamento entre 'tipos_de_receitas' e 'categorias' adicionado com sucesso!")
    except Exception as e:
        print(f"Erro ao adicionar relacionamento entre 'tipos_de_receitas' e 'categorias': {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    relacionar_filiais_categorias()
    relacionar_clientes_tipos_de_receitas()
    relacionar_tipos_de_receitas_categorias()
