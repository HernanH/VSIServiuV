import pyfirmata
import time
import threading 
from pymongo import MongoClient
import pymongo


def guardarenlocal(coleccion,datos):
    list(datos)[0]['_id']=28
    coleccion.update({"_id":28},datos[0])

def ConsultaRele(colecRelee):
    return list(colecRelee.find())

def rescatarDatos(arreglo,numrele,identificadorrele):
    if arreglo[0][numrele]['dias'][time.strftime("%a")]==True:
        dia=time.strftime("%a")
        horaencendido=arreglo[0][numrele]['horario']['encendido']
        horaapagado=arreglo[0][numrele]['horario']['apagado']
        identificadorrele[numrele]['hencendido']=horaencendido
        identificadorrele[numrele]['hapagado']=horaapagado
        identificadorrele[numrele]['dias']=dia

def EntregarNuevaId(UltimaInsercionMDB):
    return UltimaInsercionMDB['_id']+1
    
def ObtenerUltimaInsercion(MDBColeccion):
    if (len(list(MDBColeccion.find()))==0):
        MDBColeccion.insert({'_id':0})
    return list(MDBColeccion.find().sort('_id',1))[-1]

def saltarcero(ultimoID,coleccion,idpuerta,ultimalectura,x,repeticiones,ejemplo):
    ultimoID['IDultimo2']=ultimoID['IDultimo2']+1
    if (ultimalectura==0.0 or ultimalectura<=0.0015):
        if(idpuerta=='p1'):
            x['p1']= x['p1'] +1
            if (ultimalectura<=0.0015 and x['p1']>=2 and repeticiones['p1']==False):
                ejemplo["_id"]= ultimoID['IDultimo2']
                ejemplo["Fidsensor"]=idpuerta
                ejemplo["medicion"]=False
                ejemplo["tipomedicion"]=True
                ejemplo["fecha"]=time.strftime("%A/%m/%y")
                ejemplo["hora2"]=time.strftime("%I :%M %P")
                coleccion.insert(ejemplo)
                print idpuerta+ ": cerrado"
                repeticiones['p1']=True
        elif(idpuerta=='p2'):
            x['p2']= x['p2'] +1
            if (ultimalectura<=0.0015 and x['p2']>=2 and repeticiones['p2']==False):
                ejemplo["_id"]= ultimoID['IDultimo2']
                ejemplo["Fidsensor"]=idpuerta
                ejemplo["medicion"]=False
                ejemplo["tipomedicion"]=True
                ejemplo["fecha"]=time.strftime("%d/%m/%y")
                ejemplo["hora2"]=time.strftime("%I :%M %P")
                coleccion.insert(ejemplo)
                print idpuerta+ ": cerrado"
                repeticiones['p2']=True
    elif (ultimalectura>0.0015):
        if(idpuerta=='p1' and repeticiones['p1']==True):
            ejemplo["_id"]= ultimoID['IDultimo2']
            ejemplo["Fidsensor"]=idpuerta
            ejemplo["medicion"]=True
            ejemplo["tipomedicion"]=True
            ejemplo["fecha"]=time.strftime("%d/%m/%y")
            ejemplo["hora2"]=time.strftime("%I :%M %P")
            coleccion.insert(ejemplo)
            print idpuerta+": abierto"
            x['p1']=0
            repeticiones['p1']=False
        elif(idpuerta=='p2' and repeticiones['p2']==True):
            ejemplo["_id"]= ultimoID['IDultimo2']
            ejemplo["Fidsensor"]=idpuerta
            ejemplo["medicion"]=True
            ejemplo["tipomedicion"]=True
            ejemplo["fecha"]=time.strftime("%d/%m/%y")
            ejemplo["hora2"]=time.strftime("%I :%M %P")
            coleccion.insert(ejemplo)
            print idpuerta+": abierto"
            x['p2']=0
            repeticiones['p2']=False

def registroHistorico(coleccion,tabla,contadorHistorico,sensores):
    if ((int(time.strftime("%M"))%5)==0 and time.strftime("%S")=='00'):
        for i in sensores:
            if (i!="pir1" and sensores[i][1]==0):
                a=[]
                contadorHistorico["historicoid"]=contadorHistorico["historicoid"]+1
                ejemplo['_id']=contadorHistorico["historicoid"]
                ejemplo['Fidsensor']=i
                for x in range(10):
                    a.append(sensores[i][0].read())
                a=sum(a)/len(a)
                ejemplo['medicion']=a
                ejemplo['tipomedicion']=True
                ejemplo['fecha']=time.strftime("%d/%m/%y")
                ejemplo['hora2']=time.strftime("%I :%M %P")
                coleccion.insert(ejemplo)
                sensores[i][1]=1
    for i in sensores:
        sensores[i][1]=0
    time.sleep(0.5)



client= MongoClient('mongodb://serviu12:serviu12@ds145389.mlab.com:45389/pruebaaa')
db=client['pruebaaa']

local= MongoClient('localhost',27017)
dbb=local['relee']

colecRelelocal=dbb.relee
colecRelee=db.relee
colecPuerta=db.puerta
colecHistorico=db.historicop

while True:
    try:
        contadorID={'IDultimo2':EntregarNuevaId(ObtenerUltimaInsercion(colecPuerta))+1}
        contador4={"historicoid":EntregarNuevaId(ObtenerUltimaInsercion(colecHistorico))+1}
        break
    except pymongo.errors.AutoReconnect:
        print "error de conexion "
        time.sleep(2)
        
board=pyfirmata.Arduino('/dev/ttyACM1')
iter=pyfirmata.util.Iterator(board)
iter.start()

relee1=board.get_pin("d:11:p")
relee2=board.get_pin("d:10:p")

board.analog[3].mode=pyfirmata.INPUT
board.analog[3].enable_reporting()
puerta1=board.get_pin('a:3:i')

board.analog[5].mode=pyfirmata.INPUT
board.analog[5].enable_reporting()
puerta2=board.get_pin('a:5:i')

a=[]
b=[]

listado={"p1":[puerta1,0],"p2":[puerta2,0]}

reles={"rele1":relee1,"rele2":relee2}
estadorele={'rele1':True, 'rele2':True, 'rele3':True}
identificadorrele= {'rele1':{'hencendido':0,'hapagado':0,'dias':0},'rele2':{'hencendido':0,'hapagado':0,'dias':0}}

repeticiones={'p1':False,'p2':False}
contador=0
cont={'p1':0.0,'p2':0.0}

ejemplo={"_id":0,"Fidsensor": 0,"medicion": 0,"tipomedicion": 0, "fecha": 0, "hora2": 0}

while True:
    try:
        largorele= len(ConsultaRele(colecRelee)[0])-1
        break
    except pymongo.errors.AutoReconnect:
        print "error de conexion "
        time.sleep(2)


guardarenlocal(colecRelelocal,ConsultaRele(colecRelee))

while True:
    while True:
        try:
            registroHistorico(colecHistorico,board,contador4,listado)
            break
        except pymongo.errors.AutoReconnect or pymongo.errors.ServerSelectionTimeOutError or pymongo.errors.DuplicateKeyError:
            print "error de conexion"
            time.sleep(1)
    if time.strftime("%M")=='42' or time.strftime("%M")=='00':
        guardarenlocal(colecRelelocal,ConsultaRele(colecRelee))
    for i in range(largorele):
               rele="rele"+str(i+1)
               while True:
                   try:
                       rescatarDatos(colecRelelocal.find(),rele,identificadorrele)
                       break
                   except pymongo.errors.AutoReconnect or pymongo.errors.ServerSelectionTimeOutError or pymongo.errors.DuplicateKeyError:
                       print "error de conexion"
                       time.sleep(1)
               if identificadorrele[rele]['hencendido']==time.strftime("%H:%M") and estadorele[rele]==True:
                   print str(rele)+'=encendido'
                   reles[rele].write(float(1.0))
                   estadorele[rele]=False
               if identificadorrele[rele]['hapagado']==time.strftime("%H:%M")and estadorele[rele]==False:
                   print str(rele)+'=apagado'
                   reles[rele].write(float(0.0))
                   estadorele[rele]=True

    if (puerta1.read()==None or puerta2.read()==None):
        pass
        
    elif(puerta1.read()!=None and puerta2.read()!=None) :
        for i in range(10):
            a.append(puerta1.read())
            time.sleep(0.05)
            b.append(puerta2.read())
        while True:
                   try:
                       saltarcero(contadorID,colecPuerta,"p1",(sum(a)/len(a)),cont,repeticiones,ejemplo)
                       saltarcero(contadorID,colecPuerta,"p2",(sum(b)/len(b)),cont,repeticiones,ejemplo)
                       break
                   except pymongo.errors.AutoReconnect or pymongo.errors.ServerSelectionTimeOutError or pymongo.errors.DuplicateKeyError:
                       print "error de conexion"
                       time.sleep(1)
        a=[]
        b=[]
    time.sleep(2)

