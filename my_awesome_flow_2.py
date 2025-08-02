import requests
from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner

@task(retries=3, retry_delay_seconds=10)
def get_cat_fact():
    "Tugas independen: Mengambil satu fakta kucing dari API."
    logger = get_run_logger()
    response = requests.get("https://catfact.ninja/fact")
    response.raise_for_status()
    fact = response.json().get("fact", "No fact found")
    logger.info(f"Fetched Cat Fact: {fact}")
    return fact

@task(retries=3, retry_delay_seconds=10)
def get_dog_picture_url():
    "Tugas independen: Mengambil URL gambar anjing dari API."
    logger = get_run_logger()
    response = requests.get("https://dog.ceo/api/breeds/image/random")
    response.raise_for_status()
    url = response.json().get("message", "No image found")
    logger.info(f"Fetched Dog Picture URL: {url}")
    return url

@task
def report_results(cat_fact: list, dog_picture_url: str):
    "Tugas dependen: Melaporkan hasil dari tugas-tugas sebelumnya."
    logger = get_run_logger()
    logger.info("Membuat Laporan Hasil")
    logger.info(f"Cat Facts: {cat_fact}")
    logger.info(f"Dog Picture URL: {dog_picture_url}")
    for i, fact in enumerate(cat_fact):
        logger.info(f"Fact {i+1}: {fact}")
    logger.info("Laporan selesai.")
    
@flow(name="Cat and Dog Facts Flow", task_runner=ConcurrentTaskRunner())
def cat_and_dog_facts_flow(count: int = 3):
    """Alur utama yang menggabungkan tugas-tugas independen dan dependen.
    - Mengambil beberapa fakta kucing secara paralel.
    - Mengambil gambar anjing secara independen.
    """
    
    logger = get_run_logger()
    logger.info(f"Memulai alur utama dengan {count} iterasi.")
    
    # PARAREL STEP
    cat_facts_futures = [get_cat_fact.submit() for _ in range(count)]
    dog_picture_url_futures = get_dog_picture_url.submit()
    
    logger.info("Menunggu hasil tugas-tugas independen...")
    
    cat_facts_futures_results = [fact.result() for fact in cat_facts_futures]
    dog_picture_url_future_result = dog_picture_url_futures.result()
    
    report_results.submit(cat_fact=cat_facts_futures_results, 
                          dog_picture_url=dog_picture_url_future_result)
    
    logger.info("Alur utama selesai.")
    
if __name__ == "__main__":
    cat_and_dog_facts_flow(count=3)    