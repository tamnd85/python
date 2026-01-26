def detectar_alertas(df_pred):
    """
    Recibe el dataframe de predicciones (df_pred)
    y devuelve una lista de mensajes de alerta.
    """
    alertas = []

    # 1) Descenso brusco de temperatura ≥ 2°C
    for i in range(1, len(df_pred)):
        t_hoy = df_pred.loc[i, "pred_hibrida"]
        t_ayer = df_pred.loc[i-1, "pred_hibrida"]

        if t_hoy <= t_ayer - 2:
            fecha = df_pred.loc[i, "time"].date()
            alertas.append(f"🔻 Descenso brusco de temperatura el {fecha}: {t_hoy:.1f}°C")

    # 2) Riesgo de heladas (< 3°C)
    heladas = df_pred[df_pred["temperature_2m_min"] < 3]
    for _, row in heladas.iterrows():
        fecha = row["time"].date()
        alertas.append(f"❄️ Riesgo de heladas el {fecha}: {row['pred_hibrida']:.1f}°C")

    # 3) Probabilidad de lluvia (> 50%)
    if "precipitation_probability" in df_pred.columns:
        lluvia = df_pred[df_pred["precipitation_probability"] > 50]
        for _, row in lluvia.iterrows():
            fecha = row["time"].date()
            alertas.append(
                f"🌧️ Alta probabilidad de lluvia el {fecha}: {row['precipitation_probability']}%"
            )

    # 4) Cielo cubierto (> 80%)
    if "cloud_cover" in df_pred.columns:
        nublado = df_pred[df_pred["cloud_cover"] > 80]
        for _, row in nublado.iterrows():
            fecha = row["time"].date()
            alertas.append(
                f"☁️ Cielo muy cubierto el {fecha}: {row['cloud_cover']}%"
            )

    return alertas
