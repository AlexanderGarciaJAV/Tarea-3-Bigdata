import pandas as pd
import json
import time
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'trm_stream'
CSV_FILE = 'trm_historico.csv'
SLEEP_TIME_SECONDS = 0.5

def limpiar_valor(valor_str):
    return float(str(valor_str).replace('$', '').replace('.', '').replace(',', '.').strip())

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

        total = len(df)
        maximo = df['Valor_TRM'].max()
        minimo = df['Valor_TRM'].min()
        promedio = df['Valor_TRM'].mean()
        fecha_max = df.loc[df['Valor_TRM'].idxmax(), 'Fecha'].strftime('%Y-%m-%d')
        fecha_min = df.loc[df['Valor_TRM'].idxmin(), 'Fecha'].strftime('%Y-%m-%d')
        fecha_inicio = df['Fecha'].min().strftime('%Y-%m-%d')
        fecha_fin = df['Fecha'].max().strftime('%Y-%m-%d')

        print("=" * 60)
        print("   RESUMEN DEL DATASET - TRM HISTORICO")
        print("=" * 60)
        print(f"  Total de registros   : {total:,}")
        print(f"  Rango de fechas      : {fecha_inicio} → {fecha_fin}")
        print(f"  TRM Maxima           : ${maximo:,.2f} COP  ({fecha_max})")
        print(f"  TRM Minima           : ${minimo:,.2f} COP  ({fecha_min})")
        print(f"  TRM Promedio         : ${promedio:,.2f} COP")
        print(f"  TRM Actual (ultimo)  : ${df['Valor_TRM'].iloc[-1]:,.2f} COP  ({fecha_fin})")
        print("=" * 60)

        decadas = df.copy()
        decadas['Decada'] = (df['Fecha'].dt.year // 10) * 10
        resumen_decada = decadas.groupby('Decada')['Valor_TRM'].agg(['mean', 'min', 'max']).reset_index()
        print("\n  PROMEDIO TRM POR DECADA:")
        print(f"  {'Decada':<10} {'Promedio':>12} {'Minimo':>12} {'Maximo':>12}")
        print("  " + "-" * 50)
        for _, row in resumen_decada.iterrows():
            print(f"  {int(row['Decada'])}s{'':<7} ${row['mean']:>10,.2f} ${row['min']:>10,.2f} ${row['max']:>10,.2f}")
        print("=" * 60)
        print(f"\nIniciando envio al topic '{KAFKA_TOPIC}'...\n")

        valor_anterior = None
        for idx, row in df.iterrows():
            variacion = 0.0
            if valor_anterior is not None:
                variacion = round(((row['Valor_TRM'] - valor_anterior) / valor_anterior) * 100, 4)

            if variacion > 2.0:
                tendencia = 'DEVALUACION_FUERTE'
            elif variacion > 0:
                tendencia = 'DEVALUACION_LEVE'
            elif variacion < -2.0:
                tendencia = 'REVALUACION_FUERTE'
            elif variacion < 0:
                tendencia = 'REVALUACION_LEVE'
            else:
                tendencia = 'ESTABLE'

            if row['Valor_TRM'] >= 5000:
                alerta = 'ALERTA_TRM_ALTA'
            elif row['Valor_TRM'] <= 1800:
                alerta = 'ALERTA_TRM_BAJA'
            else:
                alerta = 'NORMAL'

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
            producer.flush()

            alerta_txt = f" | ⚠ {alerta}" if alerta != 'NORMAL' else ''
            print(f"Enviado | Fecha: {evento['Fecha']} | TRM: ${evento['Valor_TRM']:>10,.2f} | {variacion:>+6.2f}% | {tendencia}{alerta_txt}")

            valor_anterior = row['Valor_TRM']
            time.sleep(SLEEP_TIME_SECONDS)

    except KeyboardInterrupt:
        print("\nProduccion detenida por el usuario.")
    except Exception as e:
        print(f"ERROR: {e}")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    run_producer()