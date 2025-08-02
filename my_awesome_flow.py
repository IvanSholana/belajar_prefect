import requests
from prefect import task, flow

@task(retries=3, retry_delay_seconds=10)
def get_cat_fact():
    response = requests.get("https://catfact.ninja/fact")
    response.raise_for_status()  # Raises an HTTPError for bad responses
    fact = response.json().get("fact", "No fact found")
    print(f"Fetched Cat Fact: {fact}")
    return fact

@flow(name="Cat Fact Flow")
def cat_fact_flow(count: int = 1):
    print(f"Starting Cat Fact Flow with {count} iterations.")
    for _ in range(count):
        fact = get_cat_fact()
        print(f"Cat Fact: {fact}")
    print("Cat Fact Flow completed.")
    
if __name__ == "__main__":
    cat_fact_flow(count=3)