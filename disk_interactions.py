# Здесь находятся функции для взаимодействия с Yandex Disk

import requests
from tqdm import tqdm
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import time
from pathlib import Path

OAUTH_TOKEN = '' # получен по https://yandex.ru/dev/disk-api/doc/ru/concepts/quickstart#quickstart__oauth
BASE_URL = 'https://cloud-api.yandex.net/v1/disk'
headers = {
    'Authorization': f'OAuth {OAUTH_TOKEN}'
}

def download_file(remote_folder_name:str, local_folder_name:str):
  ''' 
  Функция для загрузки одиночного файла из яндекс диска в локальную папку
  Аргументы:
    remote_folder_name -- абсолютный путь до файла в яндекс диске
    local_folder_name -- абсолютный путь, в который будет сохранён файл
  '''
  # чистим название файла от тире, чтобы позже не упала нормализация в cli
  local_folder_name = local_folder_name.replace('- ', '')
  # создаём локальную папку
  os.makedirs('/'.join(local_folder_name.split('/')[:-1]), exist_ok=True)

  params = {
              'path': remote_folder_name
          }

  download_response = requests.get(f'{BASE_URL}/resources/download',
                                    headers=headers,
                                    params=params,
                                    timeout=60)

  download_url = download_response.json().get('href')

  # скачиваем
  with requests.get(download_url, stream=True, timeout=60) as r:
      r.raise_for_status()
      with open(local_folder_name, 'wb') as f:
          for chunk in r.iter_content(chunk_size=8192*8):
              if chunk:
                  f.write(chunk)

def upload_file(local_file_name:str, remote_name:str, overwrite=True):
  ''' 
  Функция для выгрузки одиночного файла из локального хранилища в яндекс диск
  Аргументы:
    local_folder_name -- абсолютный путь до локального файла
    remote_name -- абсолютный путь в яндекс диске, по которому будет сохранён файл
    overwrite (True/False) -- будет ли перезаписан файл, находящийся по remote_name
  '''    

  params = {
            'path': remote_name,
            'overwrite': str(overwrite).lower()
        }

  upload_response = requests.get(f'{BASE_URL}/resources/upload/',
                                     headers=headers,
                                     params=params,
                                     timeout=30)

  if upload_response.status_code != 200:
        raise Exception(f"Failed to get upload URL: {upload_response.text}")

  upload_url = upload_response.json()['href']
  # выгружаем
  with open(local_file_name, 'rb') as file:
            put_response = requests.put(upload_url, data=file, timeout=100)



def delete_file(path:str, permanently=True):
      ''' 
  Функция для удаления одиночного файла из яндекс диска
  Аргументы:
    path -- абсолютный путь до файла на яндекс диске
    permanently (True/False) -- полное удаление или перемещение в корзину
  '''  
    url = "https://cloud-api.yandex.net/v1/disk/resources"
    params = {
        "path": path,
        "permanently": permanently 
    }

    response = requests.delete(url, headers=headers, params=params)

    if response.status_code in (202, 204, 404): # ok и файла нет -- считаем, что сами и удалили
        return True
    else:
        raise Exception(f"Delete failed: {response.text}")

def get_files(folder_name:str, limit:int=1000, folder_needed=False):
  '''
  Функция, которая принимает абсолютный путь до папки в яндекс диске и 
  возвращает список файлов или папок, находящихся в ней
  Аргументы:
    folder_name -- абсолютный путь до папки на яндекс диске
    limit -- условный предел прокручивания
    folder_needed -- возвращать список файлов или папок

  '''
  all_download_files = []
  offset = 0
  while True:
    params = {
              'path': folder_name,
              'offset': offset,
              "limit": limit,
          }

    response = requests.get(f'{BASE_URL}/resources/',
                                      headers=headers,
                                      params=params)

    if response.status_code != 200:
          raise Exception(f"Failed to get download URL: {response.text}")

    # получаем список файлов в папке
    download_files = response.json().get('_embedded', {}).get('items', [])

    if not download_files:
      break

    all_download_files.extend(download_files)
    offset += len(download_files)
    if len(all_download_files) >= limit:
      break

  if folder_needed: # возвращает папки
    folders = [item['name'] for item in all_download_files if item['type'] == 'dir']
    print(f'{len(folders)} folders in folder {folder_name}')

    return folders

  else: # возвразает файлы
    files = [item['name'] for item in all_download_files if item['type'] == 'file']

    print(f'{len(files)} files in folder {folder_name}')

    return files

def create_folder(folder_path:str):
'''
  Функция, которая создаёт папку в яндекс диске по абсолютному пути
  Аргументы:
    folder_path -- абсолютный путь до папки
  '''

  params = {'path': folder_path}
  response = requests.put(f'{BASE_URL}/resources',
                              headers=headers,
                              params=params)

def download_parallel_paths_new(download_tasks:list, max_workers:int=5):
'''
    Функция, скачивающая паралелльно несколько файлов из яндекс диска. 
    Возвращает список с файлами, которые не удалось скачать.
    Аргументы:
        download_tasks -- список списков/кортежей типа 
            (абсолютный путь до скачиваемого на яндекс диске, локальное название, название файла)
        max_workers -- максимальное число воркеров при скачивании 

'''
    successful, failed = 0, 0

    failed_list = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(download_file, remote, local): (remote, local, name)
            for remote, local, name in download_tasks
        }

        for future in tqdm(as_completed(futures), total=len(futures), desc="Downloading laughter"):
            remote, local, name = futures[future]
            try:
                future.result(timeout=120)
                successful += 1
            except Exception as e:
                failed += 1
                print(f"Failed: {remote} -> {local}: {e}")
                failed_list.append({
                    "name": name,
                    "remote": remote,
                    "local": local,
                    "error": str(e)
                })


    print(f"Downloaded: {successful}, failed: {failed}")
    return failed_list

def download_parallel_paths(folder_path:str, filenames:list, local_dir:str="/content/all_metas",
                            max_workers:int=2, retries:int=5):
    '''
    Legacy версия download_parallel_paths_new, сохранена, так как использовалась на части данных
    Аргументы:
        folder_path -- абсолютный путь до папки на яндекс диске
        filenames -- список строк-названий файлов
        local_dir -- абсолютный путь до локальной папке, куда будут скачаны файлы
        max_workers -- максимальное число воркеров при скачивании
        retries -- число повторных попыток при ошибке скачивания

    '''
    Path(local_dir).mkdir(parents=True, exist_ok=True)

    download_tasks = []

    for file_name in filenames:
        remote_path = f"{folder_path.rstrip('/')}/{file_name.lstrip('/')}"
        flat_name = file_name.replace("/", "_")
        local_path = str(Path(local_dir) / flat_name)

        download_tasks.append((remote_path, local_path, flat_name))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(download_file_with_retry, remote, local, retries): (remote, local, name)
            for remote, local, name in download_tasks
        }

        for future in tqdm(as_completed(futures), total=len(futures), desc="скачиваем записи", unit="file"):
            remote, local, name = futures[future]

            try:
                future.result(timeout=300)
            except Exception as e:
                print(f"Ошибка в файле {name}")
                print(f"Remote: {remote}")
                print(f"Local: {local}")
                print(f"Error: {e}")
                
def upload_parallel(upload_tasks:list, max_workers:int=4, delete_after=True):
'''
    Функция, загружающая паралелльно несколько файлов на яндекс диск. 
    Возвращает список с файлами, которые не удалось скачать.
    Аргументы:
        upload_tasks -- список списков/кортежей типа 
            (локальный абсолютный путь до файла, абсолютный путь на яндекс диске)
        max_workers -- максимальное число воркеров при скачивании 
        delete_after -- удалять ли локальный файл после выгрузки
'''
    failed = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(upload_file, local, remote): (local, remote)
            for local, remote in upload_tasks
        }

        for future in as_completed(futures):
            local, remote = futures[future]

            try:
                future.result()
                if delete_after and os.path.exists(local):
                    os.remove(local)
            except Exception as e:
                failed.append((local, remote, str(e)))
                print(f"Upload failed: {local} -> {remote}: {e}")

    return failed