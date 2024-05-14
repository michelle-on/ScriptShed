import csv
import boto3
import io
import os

def get_latest_file(client, bucket_name, prefix):
    try:
        response = client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        files = response.get('Contents', [])
        return max(files, key=lambda x: x['LastModified']) if files else None
    except Exception as e:
        print(f"Erro ao obter o arquivo mais recente do ambiente'{prefix}': {e}")
        return None

def process_csv_content(csv_content):
    processed_rows = []
    if csv_content:
        try:
            data = io.StringIO(csv_content)
            reader = csv.DictReader(data, delimiter=';')
            
            for row in reader:
                compliance_value = 'false' if 'no' in row.values() else 'true'
                row['compliance'] = compliance_value
                processed_rows.append(row)
        except Exception as e:
            print(f"Erro ao processar o conteúdo do arquivo CSV: {e}")
    
    return processed_rows

def write_csv_file(data, output_s3_bucket, output_s3_key):
    try:
        csv_buffer = io.StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=['Name', 'InstanceID', 'Squad', 'Sre', 'Service', 'Capability', 'Domain', 'Service', 'Status'])
        writer.writeheader()

        for row in data:
            writer.writerow({key: row.get(key, '') for key in writer.fieldnames})
            
        session = boto3.Session(profile_name= os.environ["profile_name"])
        s3_client = session.client("s3", region_name=os.environ["region_name"])
        response = s3_client.put_object(
            Bucket=output_s3_bucket,
            Key=output_s3_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        print("Arquivo salvo com sucesso no S3.")
    except Exception as e:
        print(f"Erro ao salvar o arquivo no S3: {e}")

session = boto3.Session(profile_name=os.environ["profile_name"])
s3_client = session.client("s3", region_name=os.environ["region_name"])

bucket_name = os.environ["bucket_name"]
dev_prefix = '~dev/'
hom_prefix = '~hom/'

latest_dev_file = get_latest_file(s3_client, bucket_name, dev_prefix)
latest_hom_file = get_latest_file(s3_client, bucket_name, hom_prefix)

if latest_dev_file:
    try:
        dev_file_key = latest_dev_file['Key']
        dev_response = s3_client.get_object(Bucket=bucket_name, Key=dev_file_key)
        dev_csv_content = dev_response['Body'].read().decode('utf-8')
        processed_dev_data = process_csv_content(dev_csv_content)
        
        if processed_dev_data:
            dev_output_bucket = ''
            dev_output_key = '~/dev/' + latest_dev_file['Key'].split('/')[-1]
            write_csv_file(processed_dev_data, dev_output_bucket, dev_output_key)
    except Exception as e:
        print(f"Erro ao processar ou gravar o arquivo dev: {e}")

if latest_hom_file:
    try:
        hom_file_key = latest_hom_file['Key']
        hom_response = s3_client.get_object(Bucket=bucket_name, Key=hom_file_key)
        hom_csv_content = hom_response['Body'].read().decode('utf-8')
        processed_hom_data = process_csv_content(hom_csv_content)
        
        if processed_hom_data:
            hom_output_bucket = ''
            hom_output_key = '~/hom/' + latest_hom_file['Key'].split('/')[-1]
            write_csv_file(processed_hom_data, hom_output_bucket, hom_output_key)
    except Exception as e:
        print(f"Erro ao processar ou gravar o arquivo hom: {e}")
