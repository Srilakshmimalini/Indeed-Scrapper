from apify_client import ApifyClient
import pandas as pd

# Initialize the ApifyClient with your API token
client = ApifyClient("apify_api_CJZqs63Cc07G13Nw5JNF7chnEeEcex2wnyCG")

# Prepare the Actor input
run_input = {
    "position": "python developer",  # Change this to your preferred job title
    "country": "IN",  # Change to your preferred country
    "location": "Chennai",  # Change to your preferred location
    "maxItems": 50,  # Maximum number of jobs to scrape
    "parseCompanyDetails": True,
    "saveOnlyUniqueItems": True,
    "followApplyRedirects": False,
}

try:
    # Run the Actor and wait for it to finish
    run = client.actor("hMvNSpz3JnHgl5jkh").call(run_input=run_input)

    # Check if the Actor run was successful
    if run["status"] != "SUCCEEDED":
        raise Exception(f"Actor run failed with status: {run['status']}")

    # Initialize lists to hold the scraped data
    Positions = []
    company = []
    salaries = []
    locations = []
    date = []
    urls = []

    # Fetch and process Actor results from the run's dataset
    dataset_id = run["defaultDatasetId"]

    for item in client.dataset(dataset_id).iterate_items():
        # Extract data from the item
        Positions.append(item.get("Position","Data Analyst"))
        salaries.append(item.get("salary", "N/A"))
        locations.append(item.get("location", "N/A"))
        date.append(item.get("postedAt", "N/A"))
        urls.append(item.get("url", "N/A"))

    # Create a DataFrame from the scraped data
    df = pd.DataFrame({
        "Position": Positions,
        "Company": company,
        "Salary": salaries,
        "Location": locations,
        "PostedAt": date,
        "URL": urls
    })

    # Save the DataFrame to a CSV file
    df.to_csv("indeed_jobs.csv", index=False)

except Exception as e:
    print(f"An error occurred: {str(e)}")
