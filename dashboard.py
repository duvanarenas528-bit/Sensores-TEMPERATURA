import streamlit as st
import pandas as pd
import glob
import time

st.set_page_config(page_title="Sensores", layout="wide")

st.title("📊 Dashboard de Sensores en Tiempo Real")

placeholder = st.empty()

while True:
    try:
        files = glob.glob("output_sensores/*.json")

        if len(files) == 0:
            with placeholder.container():
                st.warning("Esperando datos desde Spark...")
        else:
            df_list = []

            for file in files:
                try:
                    df_temp = pd.read_json(file, lines=True)
                    df_list.append(df_temp)
                except:
                    pass

            if len(df_list) > 0:
                df = pd.concat(df_list, ignore_index=True)

                with placeholder.container():

                  
                    
                    st.subheader("📋 Datos en tiempo real")
                    st.dataframe(df.tail(20))

                   
                    col1, col2 = st.columns(2)

                    temp_df = df[df["tipo"] == "temperatura"]
                    pres_df = df[df["tipo"] == "presion"]

                    if not temp_df.empty:
                        col1.metric("🌡️ Temp Máxima", round(temp_df["valor"].max(), 2))

                    if not pres_df.empty:
                        col2.metric("🧭 Presión Promedio", round(pres_df["valor"].mean(), 2))

                    if not temp_df.empty:
                        st.subheader("🌡️ Temperatura")
                        st.line_chart(temp_df["valor"])

                    if not pres_df.empty:
                        st.subheader("🧭 Presión")
                        st.line_chart(pres_df["valor"])

                   
                    st.subheader("🚨 Alertas")

                    df["alerta"] = None

                    df.loc[(df["tipo"] == "temperatura") & (df["valor"] > 80), "alerta"] = "🔴 TEMP_ALTA"
                    df.loc[(df["tipo"] == "temperatura") & (df["valor"] < 20), "alerta"] = "🔵 TEMP_BAJA"
                    df.loc[(df["tipo"] == "presion") & ((df["valor"] < 950) | (df["valor"] > 1050)), "alerta"] = "⚠️ PRESION_ANOMALA"

                    alertas = df[df["alerta"].notnull()]

                    if not alertas.empty:
                        st.dataframe(alertas[["sensor_id", "tipo", "valor", "alerta"]].tail(10))
                    else:
                        st.success("Sin alertas")

    except Exception as e:
        st.error(f"Error: {e}")

    time.sleep(2)