import boto3
import time
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed

# Inicializar o cliente Glacier
glacier_client = boto3.client('glacier')

def check_job_status(vault_name, job_id):
    """
    Verifica o status de um job até que ele seja concluído.
    """
    while True:
        job_status = glacier_client.describe_job(vaultName=vault_name, jobId=job_id)
        if job_status['Completed']:
            print("Job concluído.")
            return True
        else:
            print("Job ainda em progresso. Verificando novamente em 30 minutos...")
            time.sleep(1800)  # Espera 30 minutos antes de verificar novamente

def delete_archive(vault_name, archive_id):
    """
    Deleta um arquivo específico do Glacier.
    """
    try:
        glacier_client.delete_archive(vaultName=vault_name, archiveId=archive_id)
        print(f"Arquivo deletado: {archive_id}")
    except ClientError as e:
        print(f"Erro ao deletar {archive_id}: {e}")

def delete_glacier_vault(vault_name):
    try:
        # Inicia o job de inventário
        job_response = glacier_client.initiate_job(
            vaultName=vault_name,
            jobParameters={
                'Type': 'inventory-retrieval'
            }
        )

        job_id = job_response['jobId']
        print(f"Inventário solicitado para o cofre '{vault_name}', Job ID: {job_id}")

        # Verifica o status do job até que ele seja concluído
        print("Aguardando a conclusão do inventário...")
        if check_job_status(vault_name, job_id):
            # Obter o inventário
            inventory_output = glacier_client.get_job_output(
                vaultName=vault_name,
                jobId=job_id
            )

            # Ler o inventário recebido
            archive_list = inventory_output['body'].read().decode('utf-8')
            print("Inventário recebido. Deletando arquivos...")

            # Deletar arquivos no Glacier usando multithreading
            archive_list = eval(archive_list)  # Converter string para dicionário
            archive_ids = [archive['ArchiveId'] for archive in archive_list['ArchiveList']]

            # Usando ThreadPoolExecutor para deletar os arquivos de forma paralela
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(delete_archive, vault_name, archive_id) for archive_id in archive_ids]

                for future in as_completed(futures):
                    future.result()  # Espera cada tarefa ser concluída, lidando com exceções se necessário

            print(f"Todos os arquivos do cofre '{vault_name}' foram deletados.")
    except ClientError as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    delete_glacier_vault("NOME_DO_SEU_COFRE_A_SER_DELETADO")
