# Flights Data Engineering

## About

This repository is my first attempt at learning data engineering through creating a realistic data pipeline. This pipeline fetches data from **Aviationstack**'s flights API, and saves both the raw and transformed data to **Google Cloud Storage**. The execution and scheduling of the pipeline is done with **Google Cloud Run & Scheduler**. The project utilizes the free tier of the API, which is 100 requests per month.  

This repository can also serve as a template to create pipelines with other purposes, as the code can be refactored slightly to serve your needs.

## How to use this repository

1. In project root, create a Python virtual environment & activate it:  

    `python -m venv <YOUR_ENV_NAME>`  

    on Windows:  

    `./env/Scripts/activate`  

2. Create a `.env` file in project root for development & add a variable called `FLIGHTS_API_KEY=<YOUR_API_KEY_HERE>` (or you could rename it to something else and then refactor it in ingestion script)

3. Install dependencies from pyproject.toml:

    ```
    pip install .
    ```

4. Refactor code to your goal if needed

5. Make sure you have a Prefect Cloud account, Google Cloud Platform account, & Google Cloud SDK installed by now

6. Login to Prefect Cloud:

    ```
    prefect cloud login
    ```

7. Get Prefect API key and url (needed later):

    ```
    prefect profile inspect
    ```

8. Login to Google Cloud:

    ```
    gcloud auth login
    ```

9. Create a Google Cloud project:

    ```
    gcloud projects create <PROJECT_NAME>
    ```

10. Set active Google Cloud project:

    ```
    gcloud config set project <YOUR_PROJECT_ID>
    ```

11. Enable required services:

    ```
    gcloud services enable artifactregistry.googleapis.com run.googleapis.com cloudscheduler.googleapis.com
    ```

12. Create a Google Cloud Storage bucket:  

    ```
    gcloud storage buckets create gs://<YOUR_BUCKET_NAME> --location=me-central1 --uniform-bucket-level-access
    ```  

    Then in `.env` file, add a variable `GCS_BUCKET_NAME=<YOUR_BUCKET_NAME>`

13. Create a Google Artifact Registry repository:

    ```
    gcloud artifacts repository create flights-pipeline-repository --repository-format=docker --location=me-central1
    ```

14. Configure Docker to push to Artifact Registry:

    ```
    gcloud auth configure-docker me-central1-docker.pkg.dev
    ```

15. Create a Google Cloud Service Account:  
    ```
    gcloud iam service-accounts create flights-pipeline-sa --display-name="Flights Pipeline Service Account"
    ```

16. Configure service account IAM policies:  

    ```
    gcloud projects add-iam-policy-binding <PROJECT_ID> --member="serviceAccount:<SERVICE_ACCOUNT_EMAIL>"" --role="roles/storage.objectAdmin"
    ```
    then run once again for `--role="roles/run.invoker"`

17. Build a Docker image from Dockerfile:  
    ```
    docker build -t flights-pipeline .
    ```

18. Rename Docker image:  

    ```
    docker tag flights-pipeline me-central1-docker.pkg.dev/<PROJECT_ID>/<REPOSITORY_NAME>/flights-pipeline
    ```

19. Push to Google Artifact Registry:  

    ```
    docker push me-central1-docker.pkg.dev/<PROJECT_ID>/<REPOSITORY_NAME>/flights-pipeline
    ```

20. Create a Cloud Run job:

    ```
    gcloud run jobs create flights-pipeline-job --image me-central1-docker.pkg.dev/<PROJECT_ID>/<REPOSITORY_NAME>/flights-pipeline --region me-central1 --set-env-vars FLIGHTS_API_KEY=...,GCS_BUCKET_NAME=...,PREFECT_API_URL=...,PREFECT_API_KEY=...
    ```

21. Run manually:

    ```
    gcloud run jobs execute flights-pipeline-job --region=us-central1
    ```

22. Create Cron Cloud Scheduler Job:

    ```
    gcloud scheduler jobs create http flights-pipeline-schedule --schedule="0 12 * * *" --uri="https://me-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/<PROJECT_ID>/jobs/flights-pipeline-job:run" --http-method=POST --oauth-service-account-email="<SERVICE_ACCOUNT_EMAIL>" --location=us-central1
    ```

Please let me know if any issues are encountered in one of these steps.