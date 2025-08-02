import time
import random
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
def report_results(results: dict):
    """Report the results of the fetched data."""
    logger = get_run_logger()
    logger.info("Reporting Results")
    logger.info(f"Unit ID: {results['unit_id']}")
    logger.info(f"Cat Facts: {results['cat_facts']}")
    logger.info(f"Dog Picture URL: {results['dog_picture_url']}")
    logger.info(f"Joke: {results['joke']['setup']} - {results['joke']['punchline']}")
    logger.info("Report completed.")
    
    return True if results['cat_facts'] and results['dog_picture_url'] and results['joke'] else False
    
@flow(name="SubFlow: Process Single Unit", task_runner=ConcurrentTaskRunner())
def process_single_unit(unit_id: int):
    """Subflow to process a single unit of data."""
    logger = get_run_logger()
    logger.info("Starting subflow to process a single unit.")
    cat_facts = fetch_cat_facts.submit()
    dog_picture_url = fetch_dog_picture_url.submit()
    joke = fetch_joke.submit()
    
    logger.info("Waiting for independent tasks to complete...")
    
    results = {
        "unit_id": unit_id,
        "cat_facts": cat_facts.result(),
        "dog_picture_url": dog_picture_url.result(),
        "joke": joke.result()
    }
    
    report = report_results(results)
    
    if report:
        logger.info(f"Subflow for unit {unit_id} completed successfully.")
    else:
        logger.error(f"Subflow for unit {unit_id} failed to complete successfully.")
    return results

@task(name="subflow task wrapper")
def run_subflow(unit_id: int):
    """Wrapper to run the subflow."""
    return process_single_unit(unit_id)

@flow(name="Main Flow: Cat and Dog Facts", task_runner=ConcurrentTaskRunner())
def main_data_pipeline(units: List[int]):
    """Main flow to process multiple units of data."""
    logger = get_run_logger()
    logger.info(f"Starting main flow with {len(units)} units.")
    
    subflow_results = [run_subflow.submit(unit_id) for unit_id in units]
    
    logger.info("Pipeline selesai. Berikut hasil seluruh unit:")
    
    for res in subflow_results:
        result = res.result()
        logger.info(f"Unit {result['unit_id']} - Cat Facts: {result['cat_facts']}, Dog Picture URL: {result['dog_picture_url']}, Joke: {result['joke']['setup']} - {result['joke']['punchline']}")
        
    return subflow_results

if __name__ == "__main__":
    units_to_process = list(range(5))
    main_data_pipeline(units=units_to_process)