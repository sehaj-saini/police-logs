# PAI102: Local Crime Trends Data Pipeline

This project turns the instructor's notebook into a runnable local and cloud pipeline. It keeps the notebook's main work: extract the Cambridge Police Log from Socrata or the supplied CSV, validate the records, transform the dates and remove duplicate IDs, load the result into PostgreSQL, and schedule runs with Prefect. A Streamlit dashboard reads only aggregate queries from PostgreSQL.

The supplied notebook says the visualization can use Metabase or another dashboard tool. This package uses Streamlit so the dashboard starts with the project and does not require a separate manual dashboard build.

## Architecture

~~~text
Cambridge Socrata API or local CSV
              |
              v
       Prefect ETL flow
       extract -> validate -> transform
              |
              v
          PostgreSQL
              |
              v
      Streamlit dashboard
       local: localhost:8501
       cloud: HTTPS domain via Caddy
~~~

The scheduled flow is set for midnight in America/New_York, which follows the source city's local time and adjusts for daylight saving time. You can change the schedule in .env.

## What the notebook code contributes

The project keeps the original notebook's dataset ID (3gki-wyrb), Socrata API extraction, required-field checks, ID-based deduplication, calendar field creation, PostgreSQL storage, and Prefect flow scheduling. The notebook in this package walks through those same modules.

The implementation also adjusts a few details from the supplied notebook:

- The attached CSV uses date strings such as 2025 Mar 06 01:07:00 AM, so the validator parses the real source format instead of assuming ISO 8601.
- The notebook's apt-get, service, and shell commands target a hosted Linux notebook. Docker Compose replaces those OS-specific installation steps on Windows and Linux.
- Credentials come from a local .env file, which is excluded from source control. The example password in the instructor notebook is not reused.
- The Postgres load replaces the table contents inside one database transaction. It keeps the ID primary key and avoids leaving a half-written snapshot after a failed run.
- The dashboard shows summary counts by date, type, hour, and weekday. It does not show IDs, street locations, or free-text descriptions.

## Evaluation criteria mapping

| Evaluation parameter | Project evidence for the lab presentation |
|---|---|
| Practice and deploy the complete pipeline during lab hours | Run the one-time flow, show Prefect task status, and open the dashboard with the loaded data. The repeatable commands are below. |
| Documentation of the data pipeline | This README documents architecture, setup, data handling, scheduling, cloud deployment, and the rubric. The included notebook is a guided lab walkthrough. |
| Dashboard accessible through any PC | The cloud profile serves the dashboard over HTTPS through the domain you configure. A browser on another computer can open that URL. |
| Pipeline implemented on cloud | Deploy the Docker Compose project to an Ubuntu VM. The same containers run the database, Prefect schedule, pipeline, dashboard, and HTTPS proxy. |
| Pipeline implemented on your system | The local steps below run the same pipeline and dashboard on Windows with Docker Desktop. |

## Windows local setup

### 1. Install the prerequisites

Install Docker Desktop for Windows and start it. Docker Desktop includes Docker Compose. You do not need to install PostgreSQL, Prefect, or Streamlit separately.

### 2. Place the supplied CSV in the project

Create a data folder in this project if it does not exist. Copy the downloaded file there and rename the copy to police_log.csv:

~~~powershell
New-Item -ItemType Directory -Force data
Copy-Item "$env:USERPROFILE\Downloads\Daily_Police_Log_20261106.csv" ".\data\police_log.csv"
~~~

The source CSV is deliberately excluded from Git uploads by .gitignore. You can use it for the lab run without placing it in a public repository.

### 3. Create local settings

From this project folder, copy .env.example to .env. Replace POSTGRES_PASSWORD with a long random value. For example, in PowerShell run:

~~~powershell
python -c "import secrets; print(secrets.token_urlsafe(32))"
~~~

Paste the generated value into .env. Leave DATA_SOURCE=csv for the first local run.

### 4. Start the services and load the CSV

~~~powershell
docker compose up -d --build
docker compose run --rm -e RUN_MODE=once pipeline
~~~

The first command starts PostgreSQL, the Prefect server, the daily scheduler, and the dashboard. The second command performs one immediate ETL run using the CSV. The regular schedule is registered by the pipeline service and then runs daily.

Open these pages on your PC:

- Dashboard: http://localhost:8501
- Prefect flow runs: http://localhost:4200

After the first successful load, set DATA_SOURCE=api in .env and recreate the pipeline service to use live Socrata data for scheduled runs:

~~~powershell
docker compose up -d --force-recreate pipeline
~~~

SOCRATA_APP_TOKEN is optional, but adding a token in .env can reduce API throttling. Do not put it in the notebook or source files.

### 5. Practice and present the pipeline

Show these points during lab hours:

1. The extraction source is the supplied CSV for the first run, then the Socrata dataset for daily refreshes.
2. Prefect records the extract, validation, transformation, and load tasks, including the quality counts.
3. The transformation removes duplicate incident IDs and adds year, month, day, hour, minute, second, month name, and weekday.
4. PostgreSQL stores the processed snapshot in cpd_incidents.
5. The dashboard filters by incident date and type and shows monthly, type, hour, and weekday summaries.
6. Open the dashboard from a second browser or computer after cloud deployment.

## Cloud deployment: AWS EC2 example

This deployment uses a small Ubuntu VM, Docker Compose, a DNS name, and Caddy for HTTPS. The dashboard is reachable from any PC with the URL. Cloud providers charge for VM compute and storage; check the selected region and instance plan before starting it.

### 1. Create the VM and firewall rules

In the AWS EC2 console, launch Ubuntu Server 24.04 LTS. A practical starting size for this all-in-one teaching stack is 2 vCPUs, 4 GB RAM, and at least 30 GB disk. Select or create a security group with these inbound rules:

| Port | Source | Use |
|---|---|---|
| 22 / SSH | Your current public IP only | Remote administration |
| 80 / HTTP | 0.0.0.0/0 | Caddy certificate setup and redirect |
| 443 / HTTPS | 0.0.0.0/0 | Dashboard access from any PC |

Do not open ports 5432, 5433, 4200, 8081, or 8501 to the internet. The Compose file binds database, Prefect, and Streamlit host ports to localhost. Only Caddy publishes public web ports.

Assign a stable public IP if the address must not change after an instance stop/start. Create a DNS A record for a domain or subdomain you control that points to that public IP. Caddy needs the DNS record to resolve to the VM before it can request a trusted HTTPS certificate.

### 2. Install Docker and upload the project

Connect to the Ubuntu VM from PowerShell. Replace the key name and IP with the values from your account:

~~~powershell
ssh -i "$env:USERPROFILE\Downloads\crime-trends-key.pem" ubuntu@EC2_PUBLIC_IP
~~~

Install Docker Engine and its Compose plugin by following Docker's official Ubuntu steps. Then upload the project ZIP from a second PowerShell window on your PC:

~~~powershell
scp -i "$env:USERPROFILE\Downloads\crime-trends-key.pem" "C:\path\to\PAI102_Crime_Trends_Pipeline.zip" ubuntu@EC2_PUBLIC_IP:~
~~~

Back in the SSH session, unpack it and enter the project folder. The exact folder name is PAI102_Crime_Trends_Pipeline:

~~~bash
unzip PAI102_Crime_Trends_Pipeline.zip
cd PAI102_Crime_Trends_Pipeline
cp .env.example .env
nano .env
~~~

In the cloud .env file, set DATA_SOURCE=api, choose a new long POSTGRES_PASSWORD, and replace DASHBOARD_DOMAIN with your DNS name. If the Socrata API requests a token, put it in SOCRATA_APP_TOKEN. Keep .env only on the VM; never upload it to GitHub or include it in the ZIP.

### 3. Start the cloud pipeline

~~~bash
docker compose --profile cloud up -d --build
docker compose run --rm -e RUN_MODE=once pipeline
docker compose ps
~~~

The immediate run imports the current API snapshot. The scheduler service then refreshes it daily. Open https://YOUR_DASHBOARD_DOMAIN from a browser on any PC. Dashboard data refreshes in the browser within about one minute after PostgreSQL changes.

To inspect logs on the VM:

~~~bash
docker compose logs --tail=100 pipeline
docker compose logs --tail=100 caddy
~~~

For a practice deployment, check AWS billing, stop the VM when you no longer need the live demo, and delete the VM and disk when finished if you do not want to keep the deployment. A stopped VM can still incur disk charges.

## Data handling and limits

- The supplied CSV has 12,845 rows, no duplicate IDs, and valid date strings. Five rows have a blank subtype and 104 have a blank description. Blank optional text is retained as empty text.
- Rows without a valid date, numeric incident ID, type, or location are counted and removed. The quality report records these counts.
- Dashboard queries do not select the description, ID, or location columns. Do not add those fields to a public chart. A cloud dashboard is visible to anyone who can open its URL.
- These are police log reports, not verified crime totals. The data has no population denominator or coordinates, so the dashboard does not claim per-capita rates or map locations.
- The cloud deployment is a single-server teaching/demo setup. For an operational production service, add managed database backups, monitoring, access controls, and a separate secret manager.

## Useful links

- [Cambridge Police Log dataset](https://data.cambridgema.gov/Public-Safety/Daily-Police-Log/3gki-wyrb)
- [Socrata API reference for this dataset](https://dev.socrata.com/foundry/data.cambridgema.gov/3gki-wyrb)
- [Docker Engine install for Ubuntu](https://docs.docker.com/engine/install/ubuntu/)
- [AWS EC2 security groups](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/creating-security-group.html)
- [Prefect schedules](https://docs.prefect.io/v3/concepts/schedules)
