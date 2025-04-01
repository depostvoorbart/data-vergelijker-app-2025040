import streamlit as st
import pandas as pd
import snowflake.connector
import logging
import io
import csv

# Logging setup
logging.basicConfig(filename="app_log.txt", level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

# Pagina configuratie voor volledig scherm
st.set_page_config(layout="wide")

st.title("Data Vergelijker")

# Twee kolommen maken voor de databronnen
col1, col2 = st.columns(2)

with col1:
    st.subheader("Databron A")
    bron_a = st.selectbox("Kies databron A", ["CSV", "Excel", "Snowflake"], key="bron_a")

with col2:
    st.subheader("Databron B")
    bron_b = st.selectbox("Kies databron B", ["CSV", "Excel", "Snowflake"], key="bron_b")

def load_from_snowflake(user, password, account, warehouse, database, schema, query):
    try:
        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema
        )
        cur = conn.cursor()
        cur.execute(query)
        df = cur.fetch_pandas_all()
        cur.close()
        conn.close()
        return df
    except Exception as e:
        logging.error(f"Snowflake connectie- of queryfout: {e}")
        raise

def load_input(bron, label):
    if bron == "CSV":
        file = st.file_uploader(f"Upload CSV-bestand voor {label}", type="csv", key=f"csv_uploader_{label}")
        if file:
            try:
                # Lees de eerste regel om de kolomnamen te krijgen
                content = file.read().decode('utf-8')
                file.seek(0)
                
                # Detecteer het scheidingsteken (comma of semicolon)
                first_line = content.split('\n')[0]
                separator = ';' if ';' in first_line else ','
                
                # Gebruik csv.reader om correct met quotes en scheidingstekens om te gaan
                csv_reader = csv.reader(io.StringIO(content), delimiter=separator)
                headers = next(csv_reader)  # Eerste rij zijn de kolomnamen
                
                # Verwijder quotes en witruimte uit kolomnamen
                headers = [h.strip().strip('"').strip("'") for h in headers]
                
                # CSV opnieuw inlezen met pandas, nu met de juiste kolomnamen
                file.seek(0)
                df = pd.read_csv(file,
                               sep=separator,
                               names=headers,
                               skiprows=1,
                               dtype=str,
                               quoting=csv.QUOTE_MINIMAL,
                               quotechar='"',
                               on_bad_lines='warn')
                
                # Toon kolommen in een nette tabel
                st.write(f"Beschikbare kolommen in {label}:")
                kolomnamen_df = pd.DataFrame({'Kolomnaam': headers})
                st.dataframe(kolomnamen_df, use_container_width=True)
                
                # Toon eerste 3 regels in een nette tabel met verbeterde styling
                st.write(f"Eerste 3 regels van {label}:")
                preview_df = df.head(3).copy()
                preview_df = preview_df.style.set_properties(**{'text-align': 'left'})
                preview_df = preview_df.set_table_styles([
                    {'selector': 'th', 'props': [('text-align', 'left')]},
                    {'selector': 'td', 'props': [('text-align', 'left')]}
                ])
                st.dataframe(preview_df, use_container_width=True)
                
                return df
            except Exception as e:
                st.error(f"Fout bij inlezen CSV: {e}")
                st.write("Tip: Controleer of:")
                st.write("1. Het bestand gebruikt komma's of puntkomma's als scheidingsteken")
                st.write("2. Alle regels hetzelfde aantal kolommen hebben")
                st.write("3. Er geen onverwachte regelbreuken in de data zitten")
                logging.error(f"Fout bij inlezen CSV ({label}): {e}")
    elif bron == "Excel":
        file = st.file_uploader(f"Upload Excel-bestand voor {label}", type=["xls", "xlsx"], key=f"excel_uploader_{label}")
        if file:
            try:
                df = pd.read_excel(file)
                return df
            except Exception as e:
                st.error(f"Fout bij inlezen Excel: {e}")
                logging.error(f"Fout bij inlezen Excel ({label}): {e}")
    elif bron == "Snowflake":
        with st.expander(f"Snowflake login voor {label}"):
            with st.form(f"snowflake_form_{label}"):
                user = st.text_input("Gebruikersnaam", key=f"user_{label}")
                password = st.text_input("Wachtwoord", type="password", key=f"pass_{label}")
                account = st.text_input("Account", key=f"acc_{label}")
                warehouse = st.text_input("Warehouse", key=f"wh_{label}")
                database = st.text_input("Database", key=f"db_{label}")
                schema = st.text_input("Schema", key=f"schema_{label}")
                query = st.text_area("SQL-query", key=f"query_{label}")

                submitted = st.form_submit_button("Laad data")
                if submitted:
                    try:
                        df = load_from_snowflake(user, password, account, warehouse, database, schema, query)
                        st.success("Data geladen")
                        return df
                    except Exception as e:
                        st.error(f"Fout bij ophalen data: {e}")
                        logging.error(f"Fout bij ophalen data ({label}): {e}")
    return None

# Laad de data voor beide bronnen
df_a = load_input(bron_a, "Bron A")
df_b = load_input(bron_b, "Bron B")

# Vergelijkingssectie
if df_a is not None and df_b is not None:
    st.markdown("---")
    st.header("Vergelijking")
    
    # Vind gemeenschappelijke kolommen
    gemeenschappelijke_kolommen = list(set(df_a.columns) & set(df_b.columns))
    
    if not gemeenschappelijke_kolommen:
        st.warning("Geen gemeenschappelijke kolommen gevonden tussen de twee bestanden.")
    else:
        # Toon kolommen die alleen in één van de bestanden voorkomen
        kolommen_alleen_a = set(df_a.columns) - set(df_b.columns)
        kolommen_alleen_b = set(df_b.columns) - set(df_a.columns)
        
        if kolommen_alleen_a:
            st.warning("Kolommen alleen aanwezig in Bron A:")
            st.write(", ".join(sorted(kolommen_alleen_a)))
        
        if kolommen_alleen_b:
            st.warning("Kolommen alleen aanwezig in Bron B:")
            st.write(", ".join(sorted(kolommen_alleen_b)))
        
        # Selectie van sleutelkolommen
        st.write("Selecteer één of meer kolommen die als sleutel gebruikt moeten worden voor de vergelijking:")
        sleutelkolommen = st.multiselect(
            "Sleutelkolommen",
            options=gemeenschappelijke_kolommen,
            help="Deze kolommen worden gebruikt om rijen tussen de twee bestanden te matchen"
        )
        
        if sleutelkolommen:
            if st.button("Vergelijk bestanden"):
                try:
                    # Voer de vergelijking uit
                    df_merge = df_a.merge(
                        df_b,
                        on=sleutelkolommen,
                        how='outer',
                        indicator=True,
                        suffixes=('_A', '_B')
                    )
                    
                    # Identificeer verschillen
                    verschillen = df_merge[df_merge['_merge'] != 'both']
                    
                    if verschillen.empty:
                        st.success("Geen verschillen gevonden!")
                    else:
                        # Toon samenvatting van de verschillen
                        st.warning("Verschillen gevonden:")
                        
                        # Tel het aantal rijen per type verschil
                        alleen_in_a = len(verschillen[verschillen['_merge'] == 'left_only'])
                        alleen_in_b = len(verschillen[verschillen['_merge'] == 'right_only'])
                        
                        if alleen_in_a > 0:
                            st.write(f"- {alleen_in_a} rijen alleen aanwezig in Bron A")
                        if alleen_in_b > 0:
                            st.write(f"- {alleen_in_b} rijen alleen aanwezig in Bron B")
                        
                        # Identificeer verschillen in kolomwaarden
                        vergelijk_kolommen = [col for col in gemeenschappelijke_kolommen if col not in sleutelkolommen]
                        
                        if vergelijk_kolommen:
                            waarde_verschillen = []
                            for col in vergelijk_kolommen:
                                col_a = f"{col}_A"
                                col_b = f"{col}_B"
                                if col_a in df_merge.columns and col_b in df_merge.columns:
                                    mask = (df_merge[col_a] != df_merge[col_b]) & (df_merge['_merge'] == 'both')
                                    if mask.any():
                                        verschillen_in_kolom = df_merge[mask]
                                        for _, rij in verschillen_in_kolom.iterrows():
                                            waarde_verschillen.append({
                                                'Sleutelwaarden': ', '.join(str(rij[key]) for key in sleutelkolommen),
                                                'Kolom': col,
                                                'Waarde in A': rij[col_a],
                                                'Waarde in B': rij[col_b]
                                            })
                            
                            if waarde_verschillen:
                                st.write(f"\nVerschillen in waarden voor overeenkomende rijen:")
                                waarde_verschillen_df = pd.DataFrame(waarde_verschillen)
                                st.dataframe(waarde_verschillen_df, use_container_width=True)
                        
                        # Download sectie
                        st.write("\nDownload de verschillen:")
                        download_format = st.radio(
                            "Kies bestandsformaat",
                            options=["Excel", "CSV"],
                            horizontal=True
                        )
                        
                        if download_format == "CSV":
                            csv = verschillen.to_csv(index=False).encode('utf-8')
                            st.download_button(
                                label="Download verschillen als CSV",
                                data=csv,
                                file_name="verschillen.csv",
                                mime='text/csv'
                            )
                        else:  # Excel
                            # Excel bestand in memory maken
                            output = io.BytesIO()
                            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                                verschillen.to_excel(writer, index=False, sheet_name='Verschillen')
                                if waarde_verschillen:
                                    waarde_verschillen_df.to_excel(writer, index=False, sheet_name='Waarde Verschillen')
                            excel_data = output.getvalue()
                            
                            st.download_button(
                                label="Download verschillen als Excel",
                                data=excel_data,
                                file_name="verschillen.xlsx",
                                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                            )
                except Exception as e:
                    st.error(f"Fout bij vergelijken: {e}")
                    logging.error(f"Fout bij vergelijken: {e}")
        else:
            st.info("Selecteer minimaal één sleutelkolom om de bestanden te kunnen vergelijken.")
