import requests
from prefect import task, flow, get_run_logger

@task(retries=3, retry_delay_seconds=10)
def get_cat_fact():
    logger = get_run_logger()
    response = requests.get("https://catfact.ninja/fact")
    response.raise_for_status()
    fact = response.json().get("fact", "No fact found")
    logger.info(f"Fetched Cat Fact: {fact}")
    return fact

@flow(name="Cat Fact Flow")
def cat_fact_flow(count: int = 1):
    logger = get_run_logger()
    logger.info(f"Starting Cat Fact Flow with {count} iterations.")
    for _ in range(count):
        fact = get_cat_fact()
        logger.info(f"Cat Fact: {fact}")
    logger.info("Cat Fact Flow completed.")

if __name__ == "__main__":
    cat_fact_flow(count=5)