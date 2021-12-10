#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#predictor de kml


# In[1]:


import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import datetime
from sqlalchemy.pool import NullPool
from sqlalchemy.sql import text

engine=create_engine('mysql+pymysql://admin:sergioescobarbuc@proyectobuc2.cpl0rcr8wloc.us-east-1.rds.amazonaws.com/DW',echo=False)
DS=create_engine('mysql+pymysql://admin:sergioescobarbuc@proyectobuc2.cpl0rcr8wloc.us-east-1.rds.amazonaws.com/DataScience',echo=False)


# In[2]:


query = text("SELECT PATENTE,MARCA,MODELO,KML,FECHA_PEDIDO FROM DW.FACT_CABECERA_VENTA_TALLER;")
TALLER= pd.read_sql(query, engine)
TALLER['FECHA_PEDIDO'] = pd.to_datetime(TALLER['FECHA_PEDIDO'], format='%d/%m/%Y')


# In[3]:


query = text("SELECT * FROM ext_patente_kml;")
df= pd.read_sql(query, DS)
df['FECHA_PEDIDO'] = pd.to_datetime(df['FECHA'], format='%d/%m/%Y')
df = df.drop("FECHA",axis=1)


# In[4]:


TALLER1=TALLER.drop_duplicates(subset="PATENTE")


# In[5]:


df2 = df.merge(TALLER1[["PATENTE","MARCA","MODELO"]],how='left',on='PATENTE')


# In[6]:


df3 = df2[["PATENTE","MARCA","MODELO","FECHA_PEDIDO","KML"]]


# In[7]:


df3 = df3[["PATENTE","MARCA","MODELO","KML","FECHA_PEDIDO"]]


# In[8]:


df_final=df3.append(TALLER)


# In[9]:


df_final = df_final[~df_final["PATENTE"].isna()]

df_final=df_final.sort_values(["PATENTE","FECHA_PEDIDO"])


# In[10]:


df_final["KML"]=df_final["KML"].fillna(0)


# In[11]:


df_final["KML"] = df_final["KML"].astype(int)


# In[12]:


df_final=df_final.drop_duplicates(subset=["PATENTE","FECHA_PEDIDO"])
df_final=df_final.drop_duplicates(subset=["PATENTE","KML"])


# In[17]:


df_final.to_sql('Registros_KML_Servicio',con=DS,if_exists='replace',index=False)


# In[18]:


#ultima visita por mantencion

query = text("SELECT DISTINCT PATENTE,KML, FECHA_PEDIDO FROM DW.FACT_CABECERA_VENTA_TALLER where MOTIVO_PEDIDO='Mantención' AND PATENTE IS NOT NULL;")
ULTIMA_VISITA = pd.read_sql(query, engine)

ULTIMA_VISITA['fecha_format'] = pd.to_datetime(ULTIMA_VISITA['FECHA_PEDIDO'], format='%d/%m/%Y')

ULTIMA_VISITA = ULTIMA_VISITA.groupby("PATENTE").max()


# In[19]:


#TABLA BASE

base = df_final.drop_duplicates(subset="PATENTE")
base = base.drop(["KML","FECHA_PEDIDO"],axis=1)


# In[20]:


base_2 = base.merge(ULTIMA_VISITA,how='left',on='PATENTE')
base_2 = base_2.rename(columns={"KML":"KML_ULT_MANT","fecha_format":"FECHA_ULT_MANT"}).drop("FECHA_PEDIDO",axis=1)


# In[21]:


#antiguedad (no la tengo) para quienes vienen antes
df_inicial = df[df["KML"]<100]

df_inicial = df_inicial.drop_duplicates("PATENTE").rename(columns={"FECHA_PEDIDO":"INSCRIPCION"}).drop("KML",axis=1)


# In[22]:


from datetime import timedelta

df_inicial_1 = df[df["KML"]>1000].groupby("PATENTE").min().reset_index().drop("KML",axis=1)
df_inicial_1["INSCRIPCION"] = df_inicial_1["FECHA_PEDIDO"] 
df_inicial_1["INSCRIPCION"] = df_inicial_1["FECHA_PEDIDO"] - timedelta(days=720)
df_inicial_1 = df_inicial_1.drop("FECHA_PEDIDO",axis=1)


# In[23]:


query = text("SELECT * FROM df_matric")
df_matric = pd.read_sql(query, DS)


# In[28]:


df_matric=df_matric.groupby("PATENTE").min().reset_index()


# In[29]:


df_matric["INSCRIPCION"]=0
for i in range(len(df_matric)):
    df_matric["INSCRIPCION"].iloc[i] = datetime.datetime(df_matric["ANIO_FABRICACION"].iloc[i], 12, 31)

df_matric['INSCRIPCION'] = pd.to_datetime(df_matric['INSCRIPCION'], format='%d-%m-%Y')


# In[30]:


df_matric_1 = df_matric[["PATENTE","INSCRIPCION"]]


# In[31]:


#union de posibles fechas de inscripcion
#df_matric_1
#df_inicial_1
#df_inicial

matricula_estimada = df_matric_1.append(df_inicial_1).append(df_inicial)
matricula_estimada = matricula_estimada.groupby("PATENTE").max().reset_index()


# In[32]:


base_3 = base_2.merge(matricula_estimada,how='left',on="PATENTE")


# In[33]:


# estimacion de kml
df_final_1 = df_final.merge(matricula_estimada,how='left',on="PATENTE")
df_final_1["Antiguedad"]=0
for i in range(len(df_final_1)):
    df_final_1["Antiguedad"].iloc[i] = abs((df_final_1["FECHA_PEDIDO"].iloc[i]-df_final_1["INSCRIPCION"].iloc[i]).days)


# In[34]:


df_final_1["Antiguedad_meses"] = df_final_1["Antiguedad"]/30
contador = df_final_1.groupby("PATENTE").count().reset_index()[["PATENTE","MARCA"]]
contador = contador.rename(columns={"MARCA":"REP"})
df_final_2 = df_final_1.merge(contador,how='left',on='PATENTE')

df_final_3 = df_final_2[df_final_2["REP"]>2]


# In[35]:


matricula_estimada["hoy"] = datetime.datetime.today().date()

matricula_estimada['hoy'] = pd.to_datetime(matricula_estimada['hoy'], format='%Y-%m-%d')

matricula_estimada["Antiguedad"]=0
for i in range(len(matricula_estimada)):
    matricula_estimada["Antiguedad"].iloc[i] = abs((matricula_estimada["hoy"].iloc[i]-matricula_estimada["INSCRIPCION"].iloc[i]).days)


# In[36]:


matricula_estimada["Antiguedad_1"] = matricula_estimada["Antiguedad"]/30


# In[37]:


import statsmodels.api as sm
resultados_modelo = pd.DataFrame()

for i in df_final_3["PATENTE"].unique():
    try:
        filtro = df_final_3[df_final_3["PATENTE"] == i]
        X = filtro["Antiguedad_meses"]
        y = filtro["KML"]

        model = sm.OLS(y, X).fit()
        mat = matricula_estimada[matricula_estimada["PATENTE"] == i ]

        resultado = model.predict(mat["Antiguedad_1"])
        resultados_modelo=resultados_modelo.append(pd.DataFrame({"PATENTE":i,"KML":resultado}))
    except:
        pass
    


# In[39]:


#pego kilometraje estimado
resultados_modelo=resultados_modelo.rename(columns={"KML":"KML_ESTIMADO"})
base_4=base_3.merge(resultados_modelo,how='left',on='PATENTE')


# In[45]:


#agrego kilometraje promedio

df_final_3["KML_MENSUAL"] = df_final_3["KML"]/df_final_3["Antiguedad_meses"]


df_final_3["MOD"] = df_final_3["MODELO"].str[:2]

df_final_3 = df_final_3[df_final_3["Antiguedad"]!=0]

df_final_4=df_final_3.groupby(["MOD","MARCA"]).mean().reset_index()[["MOD","MARCA","KML_MENSUAL"]]


# In[46]:


#proyecto kilometraje promedio
base_4["MOD"] = base_4["MODELO"].str[:2]


base_5=base_4.merge(df_final_4,how='left',on=["MARCA","MOD"])

base_6 = base_5.merge(matricula_estimada[["PATENTE","Antiguedad_1"]],how='left',on='PATENTE')

n=base_6["KML_MENSUAL"].quantile(q=0.60)

base_6["KML_MENSUAL"] = np.where(base_6["KML_MENSUAL"]>n,n,base_6["KML_MENSUAL"])

base_6=base_6[~base_6["Antiguedad_1"].isna()]

base_6["KML_ESTIMADO_FINAL"] =  np.where(base_6["KML_ESTIMADO"].isna(),base_6["KML_MENSUAL"]*base_6["Antiguedad_1"],base_6["KML_ESTIMADO"])


# In[48]:


#busco diferencia entre ultima mantencion (fecha) para saber si debe venir
base_6["FECHA_PROX_VISITA"] = base_6["FECHA_ULT_MANT"] + timedelta(days=350)


# In[49]:


#busco por kml si debe venir
base_6["tot_kml"] = (base_6["KML_ESTIMADO_FINAL"]/10000).astype(str)


# In[50]:


a=base_6["tot_kml"].str.split(".",expand=True).rename(columns={1:"nivel_llegada"})
a["nivel_llegada"] = a["nivel_llegada"].str[:2]
base_7 = pd.concat([base_6,a["nivel_llegada"]],axis=1)

base_8 = base_7[~base_7["FECHA_PROX_VISITA"].isna()]

mes = datetime.datetime.now().month

anio = datetime.datetime.now().year

base_8 = base_8[(base_8["FECHA_PROX_VISITA"].dt.month == mes ) & (base_8["FECHA_PROX_VISITA"].dt.year == anio)]

base_9 = base_7[base_7["nivel_llegada"].isin(["97","98","99"])]


# In[51]:


base_8["caso"]='Por tiempo'
base_9 = base_9[~base_9["PATENTE"].isin(base_8["PATENTE"])]


# In[52]:


base_9["caso"]='Por Kilometraje'


# In[53]:


base_10 = base_9.append(base_8)


# In[54]:


#busco ultimo dueño
query = text("SELECT * FROM transferencias_patente;")
transferencias= pd.read_sql(query, DS)


# In[55]:


ult_transferencias =transferencias.groupby("PATENTE").max().reset_index()[["PATENTE","ULTIMA_TRANSFERENCIA"]]
ult_transferencias = ult_transferencias[~ult_transferencias["ULTIMA_TRANSFERENCIA"].isna()]
ult_transferencias["aux"]=1


# In[56]:


transferencias_1 = transferencias.merge(ult_transferencias[["PATENTE","ULTIMA_TRANSFERENCIA","aux"]],how='left',on=['PATENTE',"ULTIMA_TRANSFERENCIA"])


# In[57]:


transferencias_1 = transferencias_1[transferencias_1["aux"]==1]


# In[58]:


base_11=base_10.merge(transferencias_1[["PATENTE","RUT"]],how='left',on='PATENTE')

noconsiderar = base_11[base_11["RUT"].isna()]


# In[59]:


transferencias_inicic = transferencias[transferencias["PATENTE"].isin(noconsiderar["PATENTE"].unique())]


# In[61]:


transferencias_inicic=transferencias_inicic.drop_duplicates(subset="PATENTE")


# In[62]:


base_12 =base_11.merge(transferencias_inicic[["PATENTE","RUT"]],how='left',on='PATENTE')


# In[63]:


base_12["rut_final"] =np.where(base_12["RUT_x"].isna(),base_12["RUT_y"],base_12["RUT_x"])


# In[64]:


base_13 = base_12[["rut_final","PATENTE",'MARCA', 'MODELO', 'KML_ULT_MANT', 'FECHA_ULT_MANT','INSCRIPCION', 'KML_ESTIMADO',
                  'KML_MENSUAL', 'Antiguedad_1','FECHA_PROX_VISITA', 'caso']]


# In[65]:


base_13 = base_13.rename(columns={"rut_final":"RUT"})


# In[66]:


base_13.to_sql("ml_visita_mantencion",con=DS,if_exists='replace',index=False)


# In[ ]:




