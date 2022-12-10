import argparse
import os
import csv
import numpy as np
from numpy import sqrt
import pandas as pd
import matplotlib.pyplot as plt
from sklearn import metrics
from sklearn.metrics import roc_curve
from sklearn.metrics import roc_auc_score
from sklearn.metrics import precision_recall_curve
from sklearn.svm import SVC
import seaborn as sns
import pathlib
from pathlib import Path


result_ai = r'c:/Users/YixuanLi-VerdooldKah/Desktop/val_mic_d5.2_valid_age.csv'

## classification of L0, L1-melanoma and L1, add column 'L0_actual','L0_pred','L1mela_act','L1mela_pred','L1_actual','L1_pred' in df
def binary_classification(actual,pred_prob):    
    #claculate ROC curves, Youden's J statistics
    fpr,tpr,thresholds = roc_curve(actual ,pred_prob)
    #print(len(tpr),len(fpr),len(thresholds))
    list_95 = np.where(tpr>=0.95)
    #print(list_95)
    ix = list_95[0][0]
    #print(ix)
    best_threshold = thresholds[ix]
    print('Threshold of tpr %0.4f' % tpr[ix], '=%0.4f,' % (best_threshold),'specificity = %0.4f' % (1-fpr[ix]))
    # random contrast
    r_probs = [0 for _ in range(len(df))] # random prediction as contrast
    r_fpr,r_tpr,_ = roc_curve(actual,r_probs)
    r_auc=roc_auc_score(actual,r_probs)
    # plot roc curve
    L_auc = roc_auc_score(actual ,pred_prob)
    L_fpr, L_tpr, _ = roc_curve(actual ,pred_prob)
    plt.plot(r_fpr, r_tpr,  label='random prediction AUC =%0.3f' % r_auc)
    plt.plot(L_fpr, L_tpr,  label= str(actual.name)+' AUC =%0.3f' % L_auc)
    plt.axhline(tpr[ix],linestyle='--',color='red',label ='0.95 sensitivity') # x = 0.95
    plt.xlabel('1 - Specificity')
    plt.ylabel('Sensitivity')
    plt.legend()
#    plt.show()
    name_col = (actual.name).split('_')
    plt.title(name_col[0]+' ROC curve')
    plt.savefig('./'+ name_col[0] + '_ROC.png')
    # output predicted class to df
    name_col_pred = name_col[0] + '_pred'    
    df[name_col_pred] = pred_prob.map(lambda x:0 if x<best_threshold else 1)

def L0_mela_L1_classi(RESULT_AI):
    global df
    df= pd.read_csv(result_ai, encoding='UTF-8', index_col=False, delimiter=',')
    split_name = df['name'].str.split("_", n=2, expand = True)
    # get actual class of each records
    df['L0_act']=split_name[0]
    df['L1_act']=split_name[1]
    temp=split_name[2].str.split('/',n=1,expand=True)
    df['L2_act']=temp[0]
    # change string value of the above columns to the corresponding class label,L0 benign:0, malignant:1,L1 benign:0, iec:1, melanoma:2, nmsc:3 
    df['L0_actual'] = df['L0_act'].map(lambda x:0 if x=="benign" else 1)
    df['L1mela_act']=df['L1_act'].map(lambda x:1 if x=="melanoma" else 0)
    df['L1_actual'] = df['L1_act'].map(lambda x:0 if x=="benign" else(1 if x=="iec" else(2 if x=="melanoma" else 3)))
    #L0 classification
    binary_classification(df['L0_actual'],df['malignant'])
    # melanoma classification
    binary_classification(df['L1mela_act'],df['malignant:melanoma'])
    # get predicted L1 class in column L1_pred
    max_ind_L1=[]
    for i in range(0,len(df)):
        max_ind_L1.append(np.argmax(df.iloc[i,3:7]))
    df['L1_pred'] = max_ind_L1
    # replace all melanoma positive in column'L1mela_pred' to 2 in df['L1_pred]
    df['L1_pred']=np.where(df['L1mela_pred']==1, 2 , df['L1_pred'])

## get TP,FP,TN,FN and all parameters of performance
def perf_measure(y_actual, y_pred):
    TP = 0
    FP = 0
    TN = 0
    FN = 0
    
    for i in range(len(y_pred)): 
        if y_actual[i]==y_pred[i]==1:
           TP += 1
        if y_pred[i]==1 and y_actual[i]!=y_pred[i]:
           FP += 1
        if y_actual[i]==y_pred[i]==0:
           TN += 1
        if y_pred[i]==0 and y_actual[i]!=y_pred[i]:
           FN += 1
    TPR = TP/(TP+FN)
    TNR = TN/(TN+FP)
    PPV = TP /(TP+FP)
    NPV = TN/(TN+FN)
    FPR = FP/(FP+TN)
    FNR = FN/(FN+TP)
    PT =np.sqrt(FPR)/(np.sqrt(TPR)+np.sqrt(FPR))
    print('TP =', TP,',FP =',FP,',TN =', TN,',FN =', FN,',TPR = %0.3f '% TPR,',TNR =%0.3f' % TNR, 'PPV =%0.3f' % PPV,'NPV =%0.3f' % NPV, 'PT =%0.3f' % PT)

## plot confusion matrix and output heatmap graphs
def plot_cm():    
    # L0 confusion matrix
    cm_L0 = metrics.confusion_matrix(df['L0_actual'], df['L0_pred'])
    print(cm_L0)
    ## check value of TP,FP,TN,FN
    perf_measure(df['L0_actual'], df['L0_pred'])
    cm_display = metrics.ConfusionMatrixDisplay(confusion_matrix = cm_L0, display_labels = ['benign', 'malignant'])
    #sklearn.metrics.roc_curve((df['L0_actual'], df['L0_pred'])
    cm_display.plot()
#    plt.show()
    plt.savefig('./L0_cm.png')
    # L1 melanoma confusion matrix
    cm_L1mela=metrics.confusion_matrix(df['L1mela_act'], df['L1mela_pred'])
    print(cm_L1mela)
    perf_measure(df['L1mela_act'], df['L1mela_pred'])
    cm_display = metrics.ConfusionMatrixDisplay(confusion_matrix = cm_L1mela, display_labels = ['Non-melanoma', 'melanoma'])
    cm_display.plot()
#    plt.show()
    plt.savefig('./L1mela_cm.png')
    cm_L1 = metrics.confusion_matrix(df['L1_actual'], df['L1_pred'])
    cm_L1_df = pd.DataFrame(cm_L1,index = ['benign','iec','melanoma','nmsc'],columns = ['benign','iec','melanoma','nmsc'])
    #Plotting the confusion matrix
    plt.figure(figsize=(10,10))
    sns.heatmap(cm_L1_df, annot=True)
    plt.title('Confusion Matrix L1-lassification')
    plt.ylabel('Actal Values')
    plt.xlabel('Predicted Values')
#    plt.show()
    plt.savefig('./L1_cm.png')


def main(arg):
    if arg.test:
        L0_mela_L1_classi(result_ai)
        plot_cm()
        col_name = ['name','L0_actual','L0_pred','L1mela_act','L1mela_pred','L1_actual','L1_pred']
        df.to_csv(r'c:/Users/YixuanLi-VerdooldKah/Desktop/YL_classanalysis.csv',columns=col_name)
    elif arg.directory:
        RESULT_AI = str(arg.directory)
        L0_mela_L1_classi(RESULT_AI)
        plot_cm()
        col_name = ['name','L0_actual','L0_pred','L1mela_act','L1mela_pred','L1_actual','L1_pred']
        df.to_csv('./L0L1classification.csv',columns=col_name)
    else:
        print("argument required")
    
if __name__=="__main__":
    parser = argparse.ArgumentParser(description="process classification result to get L0,L1-melanoma and L1 classes")
    parser.add_argument('--test',default=False,action="store_true",help="testing mode")
    parser.add_argument('--directory',type = pathlib.Path,help ="python xxx.py --directory /path/filename.csv",required = False)
    main(parser.parse_args())
