from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2025, 4, 26, 23, 0),
}

def fetch_user_data():
    import requests
    res = requests.get("https://jsonplaceholder.typicode.com/users")
    if res.status_code == 200:
        user = res.json()[0]  # Take the first user
        return user
    else:
        print(f"Failed to fetch data. Status code: {res.status_code}")
        return None

def format_user_data(user):
    if not user:
        return None
    
    # Flatten the address
    address = user.get("address", {})
    full_address = f"{address.get('street', '')}, {address.get('suite', '')}, {address.get('city', '')}, {address.get('zipcode', '')}"

    # Extract company name
    company_name = user.get("company", {}).get("name", "")

    # Build cleaned user dictionary
    cleaned_user = {
        "id": user.get("id"),
        "name": user.get("name"),
        "username": user.get("username"),
        "email": user.get("email"),
        "address": full_address,
        "phone": user.get("phone"),
        "website": user.get("website"),
        "company": company_name
    }
    
    return cleaned_user

def stream_data():
    import json
    user = fetch_user_data()
    cleaned_user = format_user_data(user)
    if cleaned_user:
        print(json.dumps(cleaned_user, indent=3))

with DAG('user_automation', 
         default_args=default_args,
         schedule="@daily",
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

# Optional testing
if __name__ == "__main__":
    stream_data()
