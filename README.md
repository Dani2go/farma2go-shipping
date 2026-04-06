# Farma2go — Shipping P&L App

## Instalación local (tu ordenador)

```bash
pip install flask pandas openpyxl xlrd gunicorn
cd farma2go_app
python app.py
# Abrir: http://localhost:5000
```

---

## Railway — datos persistentes para el equipo

### Configurar el volumen (una sola vez)

1. Entra en tu proyecto en **railway.app**
2. Clic en el servicio (el cuadrado con el nombre de tu app)
3. Pestaña **Volumes** → **Add Volume**
4. En *Mount Path* escribe: `/storage`
5. Clic en **Add**

6. Pestaña **Variables** → **Add Variable**
   - Name: `STORAGE_PATH`  /  Value: `/storage`
7. Clic en **Add**

Listo. Los datos ya no se borran nunca.

### Compartir con el equipo
Comparte la URL de Railway. No necesitan cuenta.

---

## Uso mensual

1. Sube facturas de transportistas (barra lateral)
2. Sube export Odoo ventas
3. Sube Excel Google Ads (opcional)
4. Clic en **Calcular P&L**

Los datos se acumulan mes a mes. Botón **Limpiar todo** para empezar de cero.
