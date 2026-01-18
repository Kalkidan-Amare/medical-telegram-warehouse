param(
    [int]$Limit = 200
)

Write-Host "Running Telegram scraping..."
python src/scraper.py --limit $Limit

Write-Host "Loading raw data into Postgres..."
python src/load_raw_to_postgres.py

Write-Host "Running dbt transformations..."
dbt run --project-dir medical_warehouse --profiles-dir medical_warehouse

dbt test --project-dir medical_warehouse --profiles-dir medical_warehouse

Write-Host "Running YOLO enrichment..."
python src/yolo_detect.py
python src/load_yolo_to_postgres.py
