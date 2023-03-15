import pandas as pd 
import pyautogui

#PASO 1: Reporte generado de facturas radicadas y tabla estadístico de facturación se  cruza , la llave es número de factura , 
#se lleva  el número de radicado, numero radicado entidad, fecha  de radicado entidad y  valor radicado.

read_estadistico_facturas = pd.read_excel("ESTADISTICO DE FACTURACION.xlsx") 
read_facturas_radicadas = pd.read_excel("FACTURAS RADICADAS.xlsx", parse_dates=['FECHA_RADICADO_ENTIDAD']) 
df_factura_radicadas = read_facturas_radicadas[["FACTURA","RADICADO","RADICADO_ENTIDAD","FECHA_RADICADO_ENTIDAD","VALOR_RADICADO"]]
df_factura_radicadas.rename(columns={"FACTURA": "NUMERO_FACTURA"}, inplace=True)


df_cruce1 = pd.merge(read_estadistico_facturas,df_factura_radicadas, on='NUMERO_FACTURA',how ='left')

#PASO 2:Se realiza buscar v entre el informe generado de relación pagare con factura y la tabla de estadístico de factura, 
# la llave es el número de la factura, se Cruza la  tabla de lt  y  el estadístico  para  llevar el número del Lt 
# y el valor si la factura genero un lt, si no dejar espacio en blanco

read_relacion_pagare_facturas = pd.read_excel("RELACION PAGARE CON FACTURA.xlsx")
df_lt = read_relacion_pagare_facturas[["FACTURA","PAGARE","VALOR_PAGARE"]]
df_lt.rename(columns={"FACTURA": "NUMERO_FACTURA"}, inplace=True)

df_cruce2 = pd.merge(df_cruce1,df_lt, on='NUMERO_FACTURA',how ='left')


#PASO 3: SEGUIMIENTO DE FACTURAS df.fillna(method='ffill') EN LA COLUMNA "FacturaNumero"
#FILTRAR "NotaTipo" por credito y debito
#EN LAS CREDITO MULTIPLICAR POR -1 "NotaValor"
#HACER TABLA DINAMICA CON LAS COLUMNAS SELECCIONADAS 

read_seguimiento_facturas = pd.read_excel("SEGUIMIENTO DE FACTURAS.xlsx")
df_seguimiento_facturas = read_seguimiento_facturas[["FacturaNumero","FacturaValor","FacturaSaldo","TotalObjetado","FacturaEstCar","NotaTipo","NotaValor","PagoValor","TrasladoValor", "TerceroNombre"]]
df_seguimiento_facturas["FacturaNumero"].fillna(method='ffill', inplace=True)
df_seguimiento_facturas.loc[df_seguimiento_facturas["NotaTipo"] == "Credito", "NotaValor"] = df_seguimiento_facturas["NotaValor"] * -1

td_seg_facturas = df_seguimiento_facturas.groupby("FacturaNumero",as_index= False).agg({
"FacturaSaldo" : "sum",
"TotalObjetado" : "sum",
"NotaValor" : "sum",
"PagoValor" : "sum",
"TrasladoValor" : "sum"
})
df_debcred = read_seguimiento_facturas[["FacturaNumero","TerceroNombre"]]
td_seg_facturas = pd.merge(td_seg_facturas,df_debcred, on='FacturaNumero',how ='left')
td_seg_facturas.rename(columns={"FacturaNumero": "NUMERO_FACTURA"}, inplace=True)

df_cruce3 = pd.merge(df_cruce2,td_seg_facturas, on='NUMERO_FACTURA',how ='left')



#PASO 4: CON EL ARCHIVO INFORME DE TRASLADOS FILTRAR "Tercero.TipoDocumento" POR NIT = ENTIDADES, DEMAS PACIENTES
#HACER TABLA DINAMICA CON "DetalleTraslado.CxCDestino.Factura", "DetalleTraslado.ValorNetoTraslado"
#CRUZAR CON  DF_CRUCE, PARA LOS PACIENTES COMPARAR PRIMERO CON NUMERO DE FACTURA Y LUEGO CON "PAGARE"(LT) Y DEJAR EN OTRA COLUMNA DIFERENTE  

read_informe_traslados = pd.read_excel("INFORME DE TRASLADOS.xlsx")
df_informe_traslados = read_informe_traslados[["DetalleTraslado.CxCDestino.Factura","Tercero.TipoDocumento", "DetalleTraslado.ValorNetoTraslado"]]
df_informe_traslados.rename(columns={"DetalleTraslado.CxCDestino.Factura": "NUMERO_FACTURA"}, inplace=True)
df_entidades = df_informe_traslados[df_informe_traslados["Tercero.TipoDocumento"] == "Nit"]
df_pacientes = df_informe_traslados[df_informe_traslados["Tercero.TipoDocumento"] != "Nit"]

td_entidades = df_entidades.groupby("NUMERO_FACTURA",as_index= False).agg({
"DetalleTraslado.ValorNetoTraslado" : "sum",
}) 
td_entidades.rename(columns={"DetalleTraslado.ValorNetoTraslado": "TRASLADO ENTIDAD"}, inplace=True)

td_pacientes = df_pacientes.groupby("NUMERO_FACTURA",as_index= False).agg({
"DetalleTraslado.ValorNetoTraslado" : "sum",
})

td_pacientes_lt = td_pacientes[td_pacientes["NUMERO_FACTURA"].str.contains("LT")]
td_pacientes_lt.rename(columns={"NUMERO_FACTURA": "PAGARE"}, inplace=True)
td_pacientes_lt.rename(columns={"DetalleTraslado.ValorNetoTraslado": "TRASLADO LT"}, inplace=True)
td_pacientes_no_lt = td_pacientes[td_pacientes["NUMERO_FACTURA"].str.contains("UNA")]
td_pacientes_no_lt.rename(columns={"DetalleTraslado.ValorNetoTraslado": "TRASLADO PACIENTE"}, inplace=True) 

df_cruce4 = pd.merge(df_cruce3,td_entidades, on='NUMERO_FACTURA',how ='left')
df_cruce5 = pd.merge(df_cruce4,td_pacientes_no_lt, on='NUMERO_FACTURA',how ='left')
df_cruce6 = pd.merge(df_cruce5,td_pacientes_lt, on='PAGARE',how ='left')
# df_cruce6.to_excel("cruce1.xlsx", encoding="utf-8-sig", index= False)

#PASO 5: Por último se cruza con la informe cartera por edades de este informe nos llevamos al estadístico los siguientes campos

read_informe_cartera = pd.read_excel("INFORME CARTERA.xlsx")
df_informe_cartera = read_informe_cartera[['Consecutivo','EstadoCartera','TotalSaldoFacturaFechaCorte','ValorObjetado','DiasAtraso']] 
df_informe_cartera.rename(columns={"Consecutivo": "NUMERO_FACTURA"}, inplace=True) 
cruce_final = pd.merge(df_cruce6,df_informe_cartera, on='NUMERO_FACTURA',how ='left')
cruce_final.to_excel("cruce final.xlsx", encoding="utf-8-sig", index= False)

pyautogui.alert("Reporte generado")