import streamlit as st
import pandas as pd
import snowflake.connector
import logging
import io
import csv
import plotly.graph_objects as go

# Pagina configuratie voor volledig scherm (moet als eerste Streamlit commando zijn)
st.set_page_config(layout="wide")

# Logging setup
logging.basicConfig(filename="app_log.txt", level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

# Voeg CSS toe voor vaste tabs en verwijder lege ruimte
st.markdown("""
    <style>
        /* Verwijder de lege ruimte bovenaan */
        .block-container {
            padding-top: 1rem !important;
        }
        
        /* Styling voor de header */
        .stApp header {
            background-color: transparent;
        }
        
        /* Styling voor de tabs */
        .stTabs {
            background-color: white;
            padding-top: 0;
            z-index: 999;
        }
        
        .stTabs [data-baseweb="tab-list"] {
            gap: 2rem;
            background-color: white;
            border-bottom: 1px solid #ddd;
            padding: 0.5rem 0;
        }
        
        /* Verberg de tweede titel */
        .element-container:has(h1:contains("Data Vergelijker")) + .element-container:has(h1:contains("Data Vergelijker")) {
            display: none;
        }
    </style>
""", unsafe_allow_html=True)

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
    if bron == "Bestand (CSV/Excel)":
        # Voeg een slider toe voor het aantal rijen
        max_rows = st.slider(
            f"Maximaal aantal rijen voor {label}",
            min_value=1000,
            max_value=1000000,
            value=100000,
            step=1000,
            help="Beperk het aantal rijen om de vergelijking sneller te maken. Kies een lagere waarde voor grote bestanden."
        )
        
        file = st.file_uploader(f"Upload bestand voor {label}", type=["csv", "xls", "xlsx"], key=f"file_uploader_{label}")
        if file:
            try:
                # Bepaal het bestandstype op basis van de extensie
                file_extension = file.name.split('.')[-1].lower()
                
                if file_extension == 'csv':
                    # Lees de eerste regel om te bepalen of er kolomnamen zijn
                    content = file.read().decode('utf-8')
                    file.seek(0)
                    
                    # Controleer of het bestand leeg is
                    if not content.strip():
                        st.error("Het bestand is leeg")
                        return None
                    
                    # Detecteer het scheidingsteken (comma of semicolon)
                    first_line = content.split('\n')[0]
                    if not first_line.strip():
                        st.error("Het bestand bevat geen data")
                        return None
                        
                    separator = ';' if ';' in first_line else ','
                    
                    # Gebruik csv.reader om correct met quotes en scheidingstekens om te gaan
                    csv_reader = csv.reader(io.StringIO(content), delimiter=separator)
                    headers = next(csv_reader)  # Eerste rij zijn de kolomnamen
                    
                    # Controleer of de headers numeriek zijn (geen echte kolomnamen)
                    try:
                        [int(h) for h in headers]
                        st.warning("Geen kolomnamen gevonden, gebruik numerieke kolomnamen")
                        # Maak betekenisvolle kolomnamen
                        headers = [f"Kolom_{i}" for i in range(len(headers))]
                    except ValueError:
                        # Er zijn echte kolomnamen
                        headers = [h.strip().strip('"').strip("'") for h in headers]
                    
                    # Controleer of er kolomnamen zijn
                    if not headers:
                        st.error("Geen kolomnamen gevonden in het bestand")
                        return None
                    
                    # Controleer of er dubbele kolomnamen zijn
                    if len(headers) != len(set(headers)):
                        st.warning("Let op: Er zijn dubbele kolomnamen gevonden")
                    
                    # CSV opnieuw inlezen met pandas, nu met de juiste kolomnamen
                    file.seek(0)
                    df = pd.read_csv(file,
                                   sep=separator,
                                   names=headers,
                                   skiprows=1,
                                   dtype=str,  # Alles als string inlezen
                                   na_values=['', 'nan', 'NaN', 'NULL', 'null'],
                                   keep_default_na=True,
                                   quoting=csv.QUOTE_MINIMAL,
                                   quotechar='"',
                                   on_bad_lines='warn',
                                   nrows=max_rows)  # Beperk het aantal rijen
                else:  # Excel bestand
                    # Excel inlezen met alle kolommen als string en geen categorische data
                    df = pd.read_excel(file, dtype=str, engine='openpyxl', nrows=max_rows)  # Beperk het aantal rijen
                
                # Controleer of er data is ingelezen
                if df.empty:
                    st.error("Geen data gevonden in het bestand")
                    return None
                
                # Vervang NaN waarden door lege string
                df = df.fillna('')
                
                # Verwijder witruimte uit kolomnamen
                df.columns = df.columns.str.strip()
                
                # Converteer alle waarden naar strings en verwijder categorische data
                for col in df.columns:
                    df[col] = df[col].apply(lambda x: str(x) if pd.notnull(x) else '')
                
                # Toon kolommen in een nette tabel
                st.write(f"Beschikbare kolommen in {label}:")
                kolomnamen_df = pd.DataFrame({'Kolomnaam': df.columns.tolist()})
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
                st.error(f"Fout bij inlezen bestand: {e}")
                st.write("Tip: Controleer of:")
                st.write("1. Het bestand niet leeg is")
                st.write("2. Het bestand kolomnamen bevat")
                if file_extension == 'csv':
                    st.write("3. Het bestand gebruikt komma's of puntkomma's als scheidingsteken")
                    st.write("4. Alle regels hetzelfde aantal kolommen hebben")
                    st.write("5. Er geen onverwachte regelbreuken in de data zitten")
                logging.error(f"Fout bij inlezen bestand ({label}): {e}")
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
                        # Converteer alle kolommen naar string type
                        df = df.astype(str)
                        df = df.fillna('')
                        st.success("Data geladen")
                        return df
                    except Exception as e:
                        st.error(f"Fout bij ophalen data: {e}")
                        logging.error(f"Fout bij ophalen data ({label}): {e}")
    return None

def vergelijk_data(df_a, df_b, key_columns):
    """
    Vergelijk twee DataFrames en retourneer een DataFrame met de verschillen.
    """
    # Zorg dat beide DataFrames dezelfde behandeling van lege waarden hebben
    for col in df_a.columns:
        df_a[col] = df_a[col].apply(lambda x: str(x) if pd.notnull(x) else '')
    for col in df_b.columns:
        df_b[col] = df_b[col].apply(lambda x: str(x) if pd.notnull(x) else '')
    
    # Voer de vergelijking uit
    df_merge = pd.merge(
        df_a,
        df_b,
        on=key_columns,
        how='outer',
        indicator=True,
        suffixes=('_A', '_B')
    )
    
    # Zorg ervoor dat alle kolommen string type blijven na de merge
    for col in df_merge.columns:
        df_merge[col] = df_merge[col].apply(lambda x: str(x) if pd.notnull(x) else '')
    
    # Identificeer verschillen
    verschillen = []
    
    # Rijen die alleen in A voorkomen
    alleen_in_a = df_merge[df_merge['_merge'] == 'left_only']
    for _, rij in alleen_in_a.iterrows():
        verschillen.append({
            'Verschil Type': 'Alleen in Bron A',
            'Rij': ', '.join(str(rij[key]) for key in key_columns),
            'Kolom': 'Alle kolommen',
            'Waarde in A': 'Aanwezig',
            'Waarde in B': 'Niet aanwezig'
        })
    
    # Rijen die alleen in B voorkomen
    alleen_in_b = df_merge[df_merge['_merge'] == 'right_only']
    for _, rij in alleen_in_b.iterrows():
        verschillen.append({
            'Verschil Type': 'Alleen in Bron B',
            'Rij': ', '.join(str(rij[key]) for key in key_columns),
            'Kolom': 'Alle kolommen',
            'Waarde in A': 'Niet aanwezig',
            'Waarde in B': 'Aanwezig'
        })
    
    # Verschillen in waarden voor overeenkomende rijen
    vergelijk_kolommen = [col for col in df_a.columns if col not in key_columns]
    for col in vergelijk_kolommen:
        col_a = f"{col}_A"
        col_b = f"{col}_B"
        if col_a in df_merge.columns and col_b in df_merge.columns:
            mask = (df_merge[col_a] != df_merge[col_b]) & (df_merge['_merge'] == 'both')
            if mask.any():
                verschillen_in_kolom = df_merge[mask]
                for _, rij in verschillen_in_kolom.iterrows():
                    verschillen.append({
                        'Verschil Type': 'Verschillende waarden',
                        'Rij': ', '.join(str(rij[key]) for key in key_columns),
                        'Kolom': col,
                        'Waarde in A': rij[col_a],
                        'Waarde in B': rij[col_b]
                    })
    
    return pd.DataFrame(verschillen)

# Titel en tabs
st.title("Data Vergelijker")
tab1, tab2, tab3 = st.tabs(["Data Inlezen", "Kolom Mapping", "Vergelijking"])

# Verwijder de dubbele header container en content container
with tab1:
    # Twee kolommen maken voor de databronnen
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Databron A")
        bron_a = st.selectbox("Kies databron A", ["Bestand (CSV/Excel)", "Snowflake"], key="bron_a")

    with col2:
        st.subheader("Databron B")
        bron_b = st.selectbox("Kies databron B", ["Bestand (CSV/Excel)", "Snowflake"], key="bron_b")

    # Laad de data voor beide bronnen
    df_a = load_input(bron_a, "Bron A")
    df_b = load_input(bron_b, "Bron B")

with tab2:
    if df_a is not None and df_b is not None:
        st.header("Kolom Mapping")
        
        # Vind gemeenschappelijke kolommen
        gemeenschappelijke_kolommen = list(set(df_a.columns) & set(df_b.columns))
        
        if not gemeenschappelijke_kolommen:
            st.warning("Geen gemeenschappelijke kolommen gevonden tussen de twee bestanden.")
            st.write("Je kunt kolommen handmatig aan elkaar koppelen:")
            
            # Toon beschikbare kolommen van beide bronnen
            col1, col2 = st.columns(2)
            with col1:
                st.write("Kolommen in Bron A:")
                kolommen_a = pd.DataFrame({'Kolomnaam': df_a.columns.tolist()})
                st.dataframe(kolommen_a, use_container_width=True)
            
            with col2:
                st.write("Kolommen in Bron B:")
                kolommen_b = pd.DataFrame({'Kolomnaam': df_b.columns.tolist()})
                st.dataframe(kolommen_b, use_container_width=True)
            
            # Toon kolommen die alleen in één van de bestanden voorkomen
            alleen_in_a = set(df_a.columns) - set(df_b.columns)
            alleen_in_b = set(df_b.columns) - set(df_a.columns)
            
            if alleen_in_a:
                st.write(f"Kolommen die alleen in Bron A voorkomen: {', '.join(alleen_in_a)}")
            if alleen_in_b:
                st.write(f"Kolommen die alleen in Bron B voorkomen: {', '.join(alleen_in_b)}")
            
            # Toon de kolom mapping interface
            st.subheader("Koppel kolommen met verschillende namen")
            st.write("Selecteer de overeenkomende kolommen uit beide bronnen:")
            
            # Maak een mapping dictionary
            mapping = {}
            
            # Toon de mapping interface in een grid
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("Bron A kolommen:")
                for i, col_a in enumerate(df_a.columns):
                    st.write(f"{i+1}. {col_a}")
            
            with col2:
                st.write("Bron B kolommen:")
                for i, col_b in enumerate(df_b.columns):
                    st.write(f"{i+1}. {col_b}")
            
            # Maak een mapping interface
            st.write("Koppel de kolommen:")
            for i, col_a in enumerate(df_a.columns):
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"{i+1}. {col_a}")
                with col2:
                    selected_col = st.selectbox(
                        f"Selecteer overeenkomende kolom voor {col_a}",
                        [""] + df_b.columns.tolist(),
                        key=f"mapping_{i}"
                    )
                    if selected_col:
                        mapping[col_a] = selected_col
            
            # Toon de gemaakte mapping
            if mapping:
                st.write("Gemaakte koppelingen:")
                for col_a, col_b in mapping.items():
                    st.write(f"{col_a} ↔ {col_b}")
        else:
            st.success("Gemeenschappelijke kolommen gevonden:")
            st.write(", ".join(gemeenschappelijke_kolommen))
            
            # Selectie van sleutelkolommen
            st.write("Selecteer één of meer kolommen die als sleutel gebruikt moeten worden voor de vergelijking:")
            sleutelkolommen = st.multiselect(
                "Sleutelkolommen",
                options=gemeenschappelijke_kolommen,
                help="Deze kolommen worden gebruikt om rijen tussen de twee bestanden te matchen"
            )

with tab3:
    if df_a is not None and df_b is not None:
        st.header("Vergelijking en Resultaten")
        
        if not gemeenschappelijke_kolommen:
            if mapping:
                if st.button("Vergelijk met gekoppelde kolommen"):
                    # Voeg een voortgangsindicator toe
                    with st.spinner("Vergelijking wordt uitgevoerd..."):
                        try:
                            # Hernoem kolommen in df_b volgens de mapping
                            df_b_mapped = df_b.rename(columns={v: k for k, v in mapping.items()})
                            
                            # Voer de vergelijking uit met de gemapte kolommen
                            verschillen = vergelijk_data(df_a, df_b_mapped, list(mapping.keys()))
                            
                            if verschillen.empty:
                                st.success("Geen verschillen gevonden!")
                            else:
                                st.warning(f"Er zijn {len(verschillen)} verschillen gevonden")
                                
                                # Toon een overzicht van de verschillen
                                st.subheader("Overzicht van verschillen")
                                
                                # Tel het aantal unieke rijen per type verschil
                                verschil_types = verschillen['Verschil Type'].value_counts()
                                
                                # Maak een staafdiagram van de verschillen
                                fig = go.Figure(data=[
                                    go.Bar(
                                        x=verschil_types.index,
                                        y=verschil_types.values,
                                        text=verschil_types.values,
                                        textposition='auto',
                                        marker_color='#FF4B4B'
                                    )
                                ])
                                
                                fig.update_layout(
                                    title="Verdeling van verschillen",
                                    xaxis_title="Type verschil",
                                    yaxis_title="Aantal",
                                    showlegend=False,
                                    height=400
                                )
                                
                                st.plotly_chart(fig, use_container_width=True)
                                
                                # Toon de verschillen in een tabel
                                st.subheader("Gedetailleerde verschillen")
                                st.dataframe(verschillen, use_container_width=True)
                                
                                # Download opties
                                st.subheader("Download verschillen")
                                download_format = st.radio(
                                    "Kies download formaat",
                                    ["Excel", "CSV"],
                                    horizontal=True
                                )
                                
                                if download_format == "Excel":
                                    # Maak een Excel bestand met meerdere sheets
                                    output = io.BytesIO()
                                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                                        # Sheet 1: Alle verschillen
                                        verschillen.to_excel(writer, sheet_name='Alle verschillen', index=False)
                                        
                                        # Sheet 2: Samenvatting per type verschil
                                        verschil_samenvatting = verschillen.groupby('Verschil Type').agg({
                                            'Rij': 'count',
                                            'Kolom': lambda x: ', '.join(sorted(set(x)))
                                        }).reset_index()
                                        verschil_samenvatting.columns = ['Type Verschil', 'Aantal', 'Betrokken Kolommen']
                                        verschil_samenvatting.to_excel(writer, sheet_name='Samenvatting', index=False)
                                        
                                        # Sheet 3: Unieke kolommen per bron
                                        unieke_kolommen = pd.DataFrame({
                                            'Bron A': sorted(set(df_a.columns) - set(df_b.columns)),
                                            'Bron B': sorted(set(df_b.columns) - set(df_a.columns))
                                        })
                                        unieke_kolommen.to_excel(writer, sheet_name='Unieke kolommen', index=False)
                                    
                                    output.seek(0)
                                    st.download_button(
                                        label="Download verschillen als Excel",
                                        data=output,
                                        file_name="verschillen.xlsx",
                                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                    )
                                else:
                                    # Download als CSV
                                    csv_data = verschillen.to_csv(index=False)
                                    st.download_button(
                                        label="Download verschillen als CSV",
                                        data=csv_data,
                                        file_name="verschillen.csv",
                                        mime="text/csv"
                                    )
                        except Exception as e:
                            st.error(f"Er is een fout opgetreden tijdens de vergelijking: {str(e)}")
                            st.error("Controleer of de geselecteerde kolommen correct zijn en of de data het juiste formaat heeft.")
            else:
                st.info("Koppel eerst kolommen aan elkaar in het 'Kolom Mapping' tabblad.")
        else:
            if sleutelkolommen:
                if st.button("Vergelijk bestanden"):
                    # Voeg een voortgangsindicator toe
                    with st.spinner("Vergelijking wordt uitgevoerd..."):
                        try:
                            # Voer de vergelijking uit
                            verschillen = vergelijk_data(df_a, df_b, sleutelkolommen)
                            
                            if verschillen.empty:
                                st.success("Geen verschillen gevonden!")
                            else:
                                st.warning(f"Er zijn {len(verschillen)} verschillen gevonden")
                                
                                # Toon een overzicht van de verschillen
                                st.subheader("Overzicht van verschillen")
                                
                                # Tel het aantal unieke rijen per type verschil
                                verschil_types = verschillen['Verschil Type'].value_counts()
                                
                                # Maak een staafdiagram van de verschillen
                                fig = go.Figure(data=[
                                    go.Bar(
                                        x=verschil_types.index,
                                        y=verschil_types.values,
                                        text=verschil_types.values,
                                        textposition='auto',
                                        marker_color='#FF4B4B'
                                    )
                                ])
                                
                                fig.update_layout(
                                    title="Verdeling van verschillen",
                                    xaxis_title="Type verschil",
                                    yaxis_title="Aantal",
                                    showlegend=False,
                                    height=400
                                )
                                
                                st.plotly_chart(fig, use_container_width=True)
                                
                                # Toon de verschillen in een tabel
                                st.subheader("Gedetailleerde verschillen")
                                st.dataframe(verschillen, use_container_width=True)
                                
                                # Download opties
                                st.subheader("Download verschillen")
                                download_format = st.radio(
                                    "Kies download formaat",
                                    ["Excel", "CSV"],
                                    horizontal=True
                                )
                                
                                if download_format == "Excel":
                                    # Maak een Excel bestand met meerdere sheets
                                    output = io.BytesIO()
                                    with pd.ExcelWriter(output, engine='openpyxl') as writer:
                                        # Sheet 1: Alle verschillen
                                        verschillen.to_excel(writer, sheet_name='Alle verschillen', index=False)
                                        
                                        # Sheet 2: Samenvatting per type verschil
                                        verschil_samenvatting = verschillen.groupby('Verschil Type').agg({
                                            'Rij': 'count',
                                            'Kolom': lambda x: ', '.join(sorted(set(x)))
                                        }).reset_index()
                                        verschil_samenvatting.columns = ['Type Verschil', 'Aantal', 'Betrokken Kolommen']
                                        verschil_samenvatting.to_excel(writer, sheet_name='Samenvatting', index=False)
                                        
                                        # Sheet 3: Unieke kolommen per bron
                                        unieke_kolommen = pd.DataFrame({
                                            'Bron A': sorted(set(df_a.columns) - set(df_b.columns)),
                                            'Bron B': sorted(set(df_b.columns) - set(df_a.columns))
                                        })
                                        unieke_kolommen.to_excel(writer, sheet_name='Unieke kolommen', index=False)
                                    
                                    output.seek(0)
                                    st.download_button(
                                        label="Download verschillen als Excel",
                                        data=output,
                                        file_name="verschillen.xlsx",
                                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                                    )
                                else:
                                    # Download als CSV
                                    csv_data = verschillen.to_csv(index=False)
                                    st.download_button(
                                        label="Download verschillen als CSV",
                                        data=csv_data,
                                        file_name="verschillen.csv",
                                        mime="text/csv"
                                    )
                        except Exception as e:
                            st.error(f"Er is een fout opgetreden tijdens de vergelijking: {str(e)}")
                            st.error("Controleer of de geselecteerde kolommen correct zijn en of de data het juiste formaat heeft.")
            else:
                st.info("Selecteer eerst sleutelkolommen in het 'Kolom Mapping' tabblad.")
    else:
        st.info("Laad eerst data in het 'Data Inlezen' tabblad.")
