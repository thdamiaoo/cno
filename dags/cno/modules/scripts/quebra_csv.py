import pandas as pd
import os


def dividir_csv_em_chunks(
    input_file_path, output_dir, tamanho_chunk, encoding="ISO-8859-1", delimiter=";"
):
    """
    Divide um arquivo CSV grande em múltiplos arquivos menores usando chunks.

    :param input_file_path: Caminho para o arquivo CSV original.
    :param output_dir: Diretório onde os arquivos divididos serão salvos.
    :param tamanho_chunk: Número de linhas em cada chunk.
    :param encoding: Codificação do arquivo CSV.
    :param delimiter: Delimitador usado no arquivo CSV.
    """
    try:
        # Certifica-se de que o diretório de saída existe
        os.makedirs(output_dir, exist_ok=True)

        # Lê o CSV em pedaços e divide em arquivos menores
        chunk_iter = pd.read_csv(
            input_file_path,
            chunksize=tamanho_chunk,
            encoding=encoding,
            delimiter=delimiter,
        )

        # Inicializa o índice a partir de 20
        for i, chunk in enumerate(chunk_iter, start=30):
            parte_path = os.path.join(output_dir, f"empresas{i}.csv")
            chunk.to_csv(parte_path, index=False)
            print(f"Arquivo salvo: {parte_path}")

    except FileNotFoundError:
        print(f"Erro: O arquivo {input_file_path} não foi encontrado.")
    except PermissionError:
        print(
            f"Erro: Permissão negada para acessar {output_dir} \
            ou salvar arquivos."
        )
    except Exception as e:
        print(f"Erro inesperado: {e}")


if __name__ == "__main__":
    input_file = (
        "/home/thdamiao/projects/cno/dags/cno/modules/data/"
        + "input_files/cnpj/arquivos_processados/empresas/"
        + "bkp/empresas0.csv"
    )
    output_directory = (
        "/home/thdamiao/projects/cno/dags/cno/modules/scripts/files_chunk"
    )
    tamanho_do_chunk = 2000000  # Número de linhas por arquivo dividido
    dividir_csv_em_chunks(
        input_file, output_directory, tamanho_do_chunk, delimiter=";"
    )  # Altere o delimitador conforme necessário
