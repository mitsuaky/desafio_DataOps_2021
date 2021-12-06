from zipfile import ZipFile
from pathlib import Path


def main():
    # Pega todos os arquivos zip na pasta csv_brutos
    files = [x for x in Path('csv_brutos').glob('*.zip')]
    files.sort()

    print(f"Extraindo {len(files)} arquivos zip...")

    num = 0

    for file in files:
        with ZipFile(file, 'r') as zip:
            for zipped_file in zip.infolist():  # Lista de arquivos dentro do zip
                zipped_file.filename = f'csv_bruto_estabelecimentos_{num}.csv'
                zip.extract(zipped_file, file.parents[0])  # Extrai para ./csv_brutos/{arquivo}.csv
                num += 1
                print(f'Arquivo {file} extraído em {file.parents[0]}\{zipped_file.filename}.')

    print(f"Ao total {num} arquivos formam extraídos.")


if __name__ == "__main__":
    main()
