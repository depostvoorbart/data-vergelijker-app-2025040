# Data Vergelijker App

Een Streamlit applicatie voor het vergelijken van datasets uit verschillende bronnen (CSV/Excel bestanden en Snowflake).

## Functionaliteiten

- Data inladen vanuit CSV/Excel bestanden
- Data inladen vanuit Snowflake
- Kolom mapping tussen verschillende datasets
- Gedetailleerde vergelijking van datasets
- Export van verschillen naar Excel/CSV

## Installatie

1. Clone de repository:
```bash
git clone [repository-url]
```

2. Installeer de benodigde packages:
```bash
pip install -r requirements.txt
```

## Gebruik

Start de applicatie met:
```bash
streamlit run data_vergelijker_app_v2.py
```

## Snowflake Configuratie

Voor Snowflake connecties, configureer de volgende environment variabelen:
- SNOWFLAKE_USER
- SNOWFLAKE_PASSWORD
- SNOWFLAKE_ACCOUNT
- SNOWFLAKE_WAREHOUSE
- SNOWFLAKE_DATABASE

Of voeg deze toe aan de Streamlit secrets bij deployment. 