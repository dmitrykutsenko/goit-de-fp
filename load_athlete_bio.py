import pandas as pd
from sqlalchemy import create_engine, text

# Шлях до csv
CSV_PATH = "data/athlete_bio.csv"

# Налаштування MySQL
MYSQL_USER = "root"
MYSQL_PASSWORD = "root"
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DB = "olympic_dataset"

engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
)

# Зчитування CSV
df = pd.read_csv(CSV_PATH)

# Залишаємо лише необхідні колонки
df = df[
    [
        "athlete_id",
        "sex",
        "height",
        "weight",
        "country_noc"
    ]
]

# Перетворення числових полів
df["height"] = pd.to_numeric(df["height"], errors="coerce")
df["weight"] = pd.to_numeric(df["weight"], errors="coerce")

# Запис у MySQL
df.to_sql(
    "athlete_bio",
    engine,
    if_exists="append",
    index=False,
    chunksize=1000
)

# Перевірка кількості записів
with engine.connect() as conn:
    result = conn.execute(
        text("SELECT COUNT(*) FROM athlete_bio")
    )
    count = result.scalar()

print(f"Завантажено записів у athlete_bio: {count}")
