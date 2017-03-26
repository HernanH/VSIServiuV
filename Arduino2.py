import pyfirmata
import time
import threading 
from pymongo import MongoClient
import pymongo

tLock=threading.Lock()


def EntregarNuevaId(UltimaInsercionMDB):
    return UltimaInsercionMDB['_id']+1
    
def ObtenerUltimaInsercion(MDBColeccion):
    if (len(list(MDBColeccion.find()))==0):
        MDBColeccion.insert({'_id':0})
    return list(MDBColeccion.find().sort('_id',1))[-1]


def registro(sensor,maximo,mensaje,fidsensor,ejemplo,coleccion,tabla,contador,cont,contmq,contagua,colecHistorico,contadorHistorico,sensores,listaconsum):
    while True:
        contador=contador+1
        if (type(maximo) is float and fidsensor=='funduino1'):
            tLock.acquire()
            if (sensor.read()>maximo and contagua==1):
                contador=contador+1
                ejemplo['_id']=contador
                ejemplo['Fidsensor']=fidsensor
                ejemplo['medicion']=sensor.read()
                ejemplo['tipomedicion']=True
                ejemplo['fecha']=time.strftime("%d/%m/%y")
                ejemplo['hora2']=time.strftime("%I :%M %P")
                while True:
                    try:
                        coleccion.insert(ejemplo)
                        break
                    except pymongo.errors.AutoReconnect or pymongo.errors.ServerSelectionTimeOutError:
                        print "error de conexion" 
                        time.sleep(1)
                print 'fuga de agua'
                contagua=0

            elif (sensor.read()<=maximo and contagua==0):
                contador=contador+1
                ejemplo['_id']=contador
                ejemplo['Fidsensor']=fidsensor
                ejemplo['medicion']=sensor.read()
                ejemplo['tipomedicion']=True
                ejemplo['fecha']=time.strftime("%d/%m/%y")
                ejemplo['hora2']=time.strftime("%I :%M %P")
                while True:
                    try:
                        coleccion.insert(ejemplo)
                        break
                    except pymongo.errors.AutoReconnect or pymongo.errors.ServerSelectionTimeOutError:
                        print "error de conexion" 
                        time.sleep(1)
                print 'libre de agua'
                contagua=1
            tLock.release()
            time.sleep(1)
        if (type(maximo) is float and fidsensor=='mq1'):
            tLock.acquire()
            if (sensor.read()>maximo and contmq==1):
                contador=contador+1
                ejemplo['_id']=contador
                ejemplo['Fidsensor']=fidsensor
                ejemplo['medicion']=sensor.read()
                ejemplo['tipomedicion']=True
                ejemplo['fecha']=time.strftime("%d/%m/%y")
                ejemplo['hora2']=time.strftime("%I :%M %P")
                while True:
                    try:
                        coleccion.insert(ejemplo)
                        break
                    except pymongo.errors.AutoReconnect or pymongo.errors.ServerSelectionTimeOutError:
                        print "error de conexion" 
                        time.sleep(1)
                print 'fuga de gas'
                contmq=0

            elif (sensor.read()<=maximo and contmq==0):
                contador=contador+1
                ejemplo['_id']=contador
                ejemplo['Fidsensor']=fidsensor
                ejemplo['medicion']=sensor.read()
                ejemplo['tipomedicion']=True
                ejemplo['fecha']=time.strftime("%d/%m/%y")
                ejemplo['hora2']=time.strftime("%I :%M %P")
                while True:
                    try:
                        coleccion.insert(ejemplo)
                        break
                    except pymongo.errors.AutoReconnect or pymongo.errors.ServerSelectionTimeOutError:
                        print "error de conexion" 
                        time.sleep(1)
                print 'libre de gas'
                contmq=1
            tLock.release()
            time.sleep(1)

        if (type(maximo) is float and fidsensor=='corriente1'):
            if sensor.read() == None:
                pass
            else:
                contador=contador+1
                valor = sensor.read()
                if valor<=0.0127:
                    valor_real=0.0
                    listaconsum.append(valor_real)
                else:
                    valor=valor-0.0098
                    valor=sensor.read()*1000
                    valor_real=(valor*(0.05))*220
                    listaconsum.append(valor_real)
                tLock.acquire()
                while True:
                    try:
                        registroHistorico(sensor,fidsensor,ejemplo,tabla,colecHistorico,contadorHistorico,sensores,coleccion,listaconsum,contador)
                        break
                    except pymongo.errors.AutoReconnect or pymongo.errors.ServerSelectionTimeOutError:
                        print "error de conexion" 
                        time.sleep(1)
                tLock.release()
            
        elif type(maximo) is bool:
            tLock.acquire()
            ejemplo['_id']=contador
            ejemplo['Fidsensor']=fidsensor
            ejemplo['medicion']=tabla.digital[sensor].read()
            ejemplo['tipomedicion']=True
            ejemplo['fecha']=time.strftime("%d/%m/%y")
            ejemplo['hora2']=time.strftime("%I :%M %P")
            if(tabla.digital[sensor].read()==True and cont==1):
                while True:
                    try:
                        coleccion.insert(ejemplo)
                        break
                    except pymongo.errors.AutoReconnect or pymongo.errors.ServerSelectionTimeOutError:
                        print "error de conexion" 
                        time.sleep(1)
                cont = 0
                print 'movimiento'
            elif (tabla.digital[sensor].read()==False and cont==0):
                while True:
                    try:
                        coleccion.insert(ejemplo)
                        break
                    except pymongo.errors.AutoReconnect or pymongo.errors.ServerSelectionTimeOutError:
                        print "error de conexion" 
                        time.sleep(1)
                cont = 1
                print'sin movimiento'
            tLock.release()
            time.sleep(0.1)
        time.sleep(0.1)
        
        
def registroHistorico(sensor,fidsensor,ejemplo,tabla,colecHistorico,contadorHistorico,sensores,colecc,listaco,contadorg):
    if ((int(time.strftime("%M"))%5)==0 and time.strftime("%S")=='00'):
        for i in sensores:
            if (i!="pir1" and i!="corriente1" and sensores[i][1]==0):
                contadorHistorico["historicoid"]=contadorHistorico["historicoid"]+1
                ejemplo['_id']=contadorHistorico["historicoid"]
                ejemplo['Fidsensor']=i
                ejemplo['medicion']=sensores[i][0].read()
                ejemplo['tipomedicion']=True
                ejemplo['fecha']=time.strftime("%d/%m/%y")
                ejemplo['hora2']=time.strftime("%I :%M %P")
                while True:
                    try:
                        colecHistorico.insert(ejemplo)
                        break
                    except pymongo.errors.AutoReconnect or pymongo.errors.ServerSelectionTimeOutError:
                        print "error de conexion" 
                        time.sleep(1)
                sensores[i][1]=1
            elif (i=="pir1" and i!="corriente1" and sensores[i][1]==0):
                contadorHistorico["historicoid"]=contadorHistorico["historicoid"]+1
                ejemplo['_id']=contadorHistorico["historicoid"]
                ejemplo['Fidsensor']=i
                ejemplo['medicion']=tabla.digital[sensores[i][0]].read()
                ejemplo['tipomedicion']=True
                ejemplo['fecha']=time.strftime("%d/%m/%y")
                ejemplo['hora2']=time.strftime("%I :%M %P")
                while True:
                    try:
                        colecHistorico.insert(ejemplo)
                        break
                    except pymongo.errors.AutoReconnect or pymongo.errors.ServerSelectionTimeOutError:
                        print "error de conexion" 
                        time.sleep(1)
                sensores[i][1]=1
            elif (i!="pir1" and i=="corriente1" and sensores[i][1]==0):
                contadorHistorico["historicoid"]=contadorHistorico["historicoid"]+1
                ejemplo['_id']=contadorHistorico["historicoid"]
                ejemplo['Fidsensor']=i
                ejemplo['medicion']=sum(listaco)/len(listaco)
                ejemplo['tipomedicion']=True
                ejemplo['fecha']=time.strftime("%d/%m/%y")
                ejemplo['hora2']=time.strftime("%I :%M %P")
                while True:
                    try:
                        colecHistorico.insert(ejemplo)
                        break
                    except pymongo.errors.AutoReconnect or pymongo.errors.ServerSelectionTimeOutError:
                        print "error de conexion" 
                        time.sleep(1)
                ejemplo['_id']=contadorg
                while True:
                    try:
                        colecc.insert(ejemplo)
                        break
                    except pymongo.errors.AutoReconnect or pymongo.errors.ServerSelectionTimeOutError:
                        print "error de conexion" 
                        time.sleep(1)
                sensores[i][1]=1
                listaco=[]
    for i in sensores:
        sensores[i][1]=0
    time.sleep(1)
            
            
        
    
        

def main():
    client= MongoClient('mongodb://serviu12:serviu12@ds145389.mlab.com:45389/pruebaaa')
    db=client['pruebaaa']

    cont=0
    contmq=0
    contagua=0
    promconsumo=[]
 
    colecGas = db.Gas
    colecAgua = db.Agua
    colecMov=db.Mov
    colecCorriente=db.gastoenergetico
    colecHistorico=db.historico
    board=pyfirmata.Arduino('/dev/ttyACM0')
    iter=pyfirmata.util.Iterator(board)
    iter.start()
    while True:
        try:
            contador1=EntregarNuevaId(ObtenerUltimaInsercion(colecGas))+1
            contador2=EntregarNuevaId(ObtenerUltimaInsercion(colecAgua))+1
            contador3=EntregarNuevaId(ObtenerUltimaInsercion(colecMov))+1
            contador4={"historicoid":EntregarNuevaId(ObtenerUltimaInsercion(colecHistorico))+1}
            contador5=EntregarNuevaId(ObtenerUltimaInsercion(colecCorriente))+1
            break
        except pymongo.errors.AutoReconnect or pymongo.errors.ServerSelectionTimeOutError:
            print "error de conexion"
            time.sleep(1)
            
    board.analog[4].mode=pyfirmata.INPUT
    board.analog[4].enable_reporting()
    mq7 = board.get_pin('a:4:i')

    board.analog[5].mode=pyfirmata.INPUT
    board.analog[5].enable_reporting()
    funduino = board.get_pin('a:5:i')

    
    board.analog[0].mode=pyfirmata.INPUT
    board.analog[0].enable_reporting()
    corriente1 = board.get_pin('a:0:i')


    board.digital[6].mode=pyfirmata.INPUT
    board.digital[6].enable_reporting()

    listado={"mq1":[mq7,0],"funduino1":[funduino,0],"pir1":[6,0],"corriente1":[corriente1,0]}


    ejemplo={"_id":0,"Fidsensor": 0,"medicion": 0,"tipomedicion": 0, "fecha": 0, "hora2": 0}
    
    s1=threading.Thread(target=registro,args=(mq7,0.5,'fuga gas','mq1',ejemplo,colecGas,board,contador1,cont,contmq,contagua,colecHistorico,contador4,listado,promconsumo))
    s2=threading.Thread(target=registro,args=(funduino,0.4,'agua','funduino1',ejemplo,colecAgua,board,contador2,cont,contmq,contagua,colecHistorico,contador4,listado,promconsumo))
    s3=threading.Thread(target=registro,args=(6,True,'presencia','pir1',ejemplo,colecMov,board,contador3,cont,contmq,contagua,colecHistorico,contador4,listado,promconsumo))
    s4=threading.Thread(target=registro,args=(corriente1,0.0,'control','corriente1',ejemplo,colecCorriente,board,contador5,cont,contmq,contagua,colecHistorico,contador4,listado,promconsumo))
    s1.start()
    s2.start()
    s3.start()
    s4.start()

if __name__=='__main__':
    main()
