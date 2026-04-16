import pandas as pd
import json
import time
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'trm_stream'
CSV_FILE = 'trm_historico.csv'
SLEEP_TIME_SECONDS = 0.5

def limpiar_valor(valor_str):
    if pd.isna(valor_str):
        return 0.0
    s = str(valor_str).replace('$', '').replace(' ', '').strip()
    if '.' in s and ',' in s:
        s = s.replace('.', '').replace(',', '.')
    elif ',' in s:
        s = s.replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return 0.0

def run_producer():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
    )

    try:
        try:
            df = pd.read_csv(CSV_FILE, encoding='utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(CSV_FILE, encoding='latin-1')

        df.columns = [col.strip().upper() for col in df.columns]
        df.rename(columns={
            'VIGENCIADESDE': 'Fecha',
            'VIGENCIAHASTA': 'Fecha_Hasta',
            'VALOR': 'Valor_TRM',
            'UNIDAD': 'Unidad'
        }, inplace=True)

        df['Valor_TRM'] = df['Valor_TRM'].apply(limpiar_valor)
        df['Fecha'] = pd.to_datetime(df['Fecha'], dayfirst=True, errors='coerce')
        df.dropna(subset=['Fecha', 'Valor_TRM'], inplace=True)
        df.sort_values(by='Fecha', ascending=True, inplace=True)
        df.reset_index(drop=True, inplace=True)

        print("=" * 60)
        print("   RESUMEN TRM - DATOS CORREGIDOS")
        print("=" * 60)
        print(f"  Primer registro (1994): ${df.iloc[0]['Valor_TRM']:,.2f}")
        print(f"  Último registro (2026): ${df.iloc[-1]['Valor_TRM']:,.2f}")
        print("=" * 60)

        valor_anterior = None
        for idx, row in df.iterrows():
            variacion = 0.0
            if valor_anterior is not None and valor_anterior != 0:
                variacion = round(((row['Valor_TRM'] - valor_anterior) / valor_anterior) * 100, 4)

            if variacion > 1.0: tendencia = 'DEVALUACION_FUERTE'
            elif variacion > 0: tendencia = 'DEVALUACION_LEVE'
            elif variacion < -1.0: tendencia = 'REVALUACION_FUERTE'
            elif variacion < 0: tendencia = 'REVALUACION_LEVE'
            else: tendencia = 'ESTABLE'

            if row['Valor_TRM'] >= 4500: alerta = 'ALERTA_TRM_MUY_ALTA'
            elif row['Valor_TRM'] <= 1000: alerta = 'ALERTA_HISTORICA_BAJA'
            else: alerta = 'NORMAL'

            evento = {
                'Fecha': row['Fecha'].strftime('%Y-%m-%d'),
                'Valor_TRM': round(float(row['Valor_TRM']), 2),
                'Unidad': str(row['Unidad']),
                'Variacion_Pct': variacion,
                'Tendencia': tendencia,
                'Alerta': alerta,
                'Timestamp_Envio': time.time()
            }

            producer.send(KAFKA_TOPIC, value=evento)
            alerta_txt = f" | {alerta}" if alerta != 'NORMAL' else ''
            print(f"Enviado | {evento['Fecha']} | TRM: ${evento['Valor_TRM']:>10,.2f} | Var: {variacion:>+7.2f}% | {tendencia}{alerta_txt}")

            valor_anterior = row['Valor_TRM']
            time.sleep(SLEEP_TIME_SECONDS)

    except KeyboardInterrupt:
        pass
    finally:
        producer.close()

if __name__ == "__main__":
    run_producer()
