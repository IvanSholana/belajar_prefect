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

@flow(name="Get Cat Fact Subflow")
def get_cat_fact_flow():
    logger = get_run_logger()
    logger.info("Running sub-flow to get a cat fact.")
    fact = get_cat_fact()
    logger.info(f"Sub-flow received fact: {fact}")
    return fact

@flow(name="Cat Fact Flow")
def cat_fact_flow(count: int = 1):
    logger = get_run_logger()
    logger.info(f"Starting main flow with {count} iterations.")
    for i in range(count):
        logger.info(f"--- Iteration {i+1} ---")
        fact = get_cat_fact_flow()
        logger.info(f"Cat Fact from sub-flow: {fact}")
    logger.info("Main Cat Fact Flow completed.")

if __name__ == "__main__":
    cat_fact_flow()