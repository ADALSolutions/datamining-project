import re
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import numpy as np
import matplotlib.cm as cm

indexFile="output.txt"
nomeFile="img.png"
f=open(indexFile)
s=f.readlines()
xList=list()
yList=list()
diz={}
for i in range(0,len(s),1):
    split=re.split(" ",s[i])
    diz.setdefault(split[2],(list(),list()))
    x=int(float(split[0]))
    y=int(float(split[1]))
    diz[split[2]][0].append(x)
    diz[split[2]][1].append(y)
    xList.append(x)
    yList.append(y)
f.close()


fontP = FontProperties()
fontP.set_size('small')
keys=list(diz.keys())
colors=['red', 'black', 'yellow','green','purple','aqua','darkgreen','lightsalmon','orange','darkgray',
        'blue','gold','skyblue','lawngreen','sienna','darkorchid','y','springgreen','orangered','sandybrown']
#colors=iter(cm.rainbow(np.linspace(0, 1, len(keys))))

print(keys)
for i in range(0,len(keys),1) :
    (listax,listay)=diz[keys[i]]
    plt.scatter(listax,listay,label=keys[i],color=colors[i])#=next(colors)

#IMPOSTO limiti assi
xmax=max(xList)
ymax=max(yList)
supy=ymax*1.1
infy=-ymax*0.1
supx=xmax*1.1
infx=-xmax*0.1
plt.axis([infx,supx,infy,supy])
plt.xlabel("x")
plt.ylabel("y")
plt.title("Plot")
plt.legend(bbox_to_anchor=(1.1, 0.6))
plt.show()
plt.savefig(nomeFile)
    
