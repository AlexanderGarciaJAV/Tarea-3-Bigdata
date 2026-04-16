# README.md

... (existing content)

## 2. Parsing y Validación de Datos

- Se ha añadido la columna **Fecha_Moneda** para incluir la fecha de la moneda que se está validando en el proceso.

## 4. Agregación en Ventanas de Tiempo

- Ahora, se incluye **Fecha_Moneda** en el `groupBy` para realizar la agregación correctamente en función de esta nueva columna.

## Ejemplo de Salida del Consumidor

La salida mostrará la columna **Fecha_Moneda** con ejemplos de fechas como:
- 2026-04-15
- 2026-04-14
- 2026-04-13

Esto refleja que el esquema ahora incluye **Fecha_Moneda** como una columna de agrupación en el paso de agregación.

... (existing content)