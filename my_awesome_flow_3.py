import requests
from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
from typing import List

@task(name="get_cat_fact", retries=3, retry_delay_seconds=10)
def fetch_cat_facts():
    """Fetch a list of cat facts from the API."""
    logger = get_run_logger()
    response = requests.get("https://catfact.ninja/fact")
    response.raise_for_status()
    fact = response.json().get("fact", "No fact found")
    logger.info(f"Fetched Cat Facts: {fact}")
    return fact

@task(name="get_dog_picture_url", retries=3, retry_delay_seconds=10)
def fetch_dog_picture_url():
    """Fetch a random dog picture URL from the API."""
    logger = get_run_logger()
    response = requests.get("https://dog.ceo/api/breeds/image/random")
    response.raise_for_status()
    url = response.json().get("message", "No image found")
    logger.info(f"Fetched Dog Picture URL: {url}")
    return url

@task(name="get_joke", retries=3, retry_delay_seconds=3)
def fetch_joke():
    """Fetch a random joke from the API."""
    logger = get_run_logger()
    response = requests.get("https://official-joke-api.appspot.com/random_joke")
    response.raise_for_status()
    joke = response.json()
    logger.info(f"Fetched Joke: {joke['setup']} - {joke['punchline']}")
    return joke

@task(name="report_results")
def report_results(unit_id: int, cat_facts: str, dog_picture_url: str, joke: dict):
    """Report the results of the independent tasks."""
    logger = get_run_logger()
    logger.info(f"Unit {unit_id} Results:")
    logger.info(f"Cat Facts: {cat_facts}")
    logger.info(f"Dog Picture URL: {dog_picture_url}")
    logger.info(f"Joke: {joke['setup']} - {joke['punchline']}")
    return {
        "unit_id": unit_id,
        "cat_facts": cat_facts,
        "dog_picture_url": dog_picture_url,
        "joke": joke
    }
    
@flow(name="SubFlow: Process Single Unit", task_runner=ConcurrentTaskRunner())
def process_single_unit(unit_id: int):
    """Subflow to process a single unit of data."""
    logger = get_run_logger()
    logger.info("Starting subflow to process a single unit.")
    cat_facts_future = fetch_cat_facts.submit()
    dog_picture_url_future = fetch_dog_picture_url.submit()
    joke_future = fetch_joke.submit()
    
    logger.info("Waiting for independent tasks to complete...") 
    
    final_result_future = report_results.submit(
        unit_id=unit_id,
        cat_facts=cat_facts_future,
        dog_picture_url=dog_picture_url_future,
        joke=joke_future
    )
    
    logger.info("Subflow completed.")
    return final_result_future

@task(name="subflow task wrapper")
def run_subflow(unit_id: int):
    """Wrapper to run the subflow."""
    return process_single_unit(unit_id)

@flow(name="Main Flow: Cat and Dog Facts", task_runner=ConcurrentTaskRunner())
def main_data_pipeline(units: List[int]):
    """Flow utama untuk memproses beberapa unit data."""
    logger = get_run_logger()
    logger.info(f"Memulai flow utama dengan {len(units)} unit...")
    
    subflow_futures = [run_subflow.submit(unit_id) for unit_id in units]
    
    logger.info("Pipeline selesai. Berikut hasil seluruh unit:")
    
    for res_future in subflow_futures:
        # hasil dari wrapper adalah future dari report_results
        report_future = res_future.result()
        # kita butuh .result() sekali lagi untuk hasil akhirnya
        result = report_future.result()
        logger.info(f"Unit {result['unit_id']} -> Fakta Kucing: {result['cat_facts']} \\\
                    -> Dog Picture URL: {result['dog_picture_url']} \\\
                    -> Joke: {result['joke']['setup']} - {result['joke']['punchline']}")
        
    return [r.result().result() for r in subflow_futures]