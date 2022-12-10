#!/bin/bash
#split files for comparison"

# split file into photoname, L0 label, L1 label, L2 label, IS_TRAIN AND DATA_TYPE,status and date
awk -F, -v OFS="," 'NR==1 {print("photo,L0,L1,L2,status,date,is_train,data_type"); next} {split($1,a,"/");split($2,b,":"); print(a[3],b[1],b[2],b[3],"","",$3,$4)}' bash_old.csv > temp.csv
awk -F, -v OFS="," 'NR==1 {print("photo,L0,L1,L2,status,date,is_train,data_type"); next} {split($1,c,"/");split($2,d,":"); print(c[3],d[1],d[2],d[3],"","",$3,$4)}' bash_new.csv > temp1.csv
echo "data splitted, temporary files generated, temp.csv/old & temp1.csv/new"
# create result.csv
head -1 temp.csv > result.csv
echo "empty result.csv created"
# set variable date
mydate=$(date +%F)

# SHOW DELETED RECORDS
awk -F, 'NR==FNR{a[$1]=$1; next} !($1 in a)' temp1.csv temp.csv | awk -v d="$mydate" -F, 'BEGIN{OFS=","}{$5="delete";$6=d; print}' >> result.csv
echo "deleted jpg files appended in result.csv"
# SHOW ADDED RECORDS INCLUDING LABEL-CHANGED IMAGES
head -1 temp.csv > add.csv | comm -13 <(sort -u  temp.csv) <(sort -u temp1.csv) >> add.csv
echo "new records stored in add.csv"
# SHOW NEW IMAGE FILES
awk -F, 'NR==FNR{a[$1]=$1; next} !($1 in a)' temp.csv add.csv | awk -v d=$(date +%F) -F, 'BEGIN{OFS=","}{$5="new";$6=d; print}' >> result.csv
echo "New jpg files appended in result.csv"
## All cases of label changes -- L0, L0&L1, L0&L1&L2,L1,L1$L2,L2,L0&L2
# L0 CHANGES 
awk -F, '{k=$1FS$3FS$4} NR==FNR{a[k]=$2;next}(k in a) && $2!= a[k]' temp.csv add.csv | awk -v d="$mydate" -F, 'BEGIN{OFS=","}{$5="L0change";$6=d; print}' >> result.csv
echo "if any, records of L0 label change appended in result.csv"
# L0L1 CHANGE
awk -F, '{k=$1FS$4} NR==FNR{a[k]=$2;b[k]=$3;next}(k in a) && $2!=a[k] && $3!=b[k]' temp.csv add.csv | awk -v d="$mydate" -F, 'BEGIN{OFS=","}{$5="L0L1change";$6=d; print}' >> result.csv
echo "if any, records of L0L1 label change appended in result.csv"
# L0L1L2 change
awk -F, '{k=$1} NR==FNR{a[k]=$2;b[k]=$3;c[k]=$4; next}(k in a) && $2!=a[k] && $3!=b[k] && $4!=c[k]' temp.csv add.csv | awk -v d="$mydate" -F, 'BEGIN{OFS=","}{$5="L0L1L2change";$6=d; print}' >> result.csv
echo "if any, records of L0L1L2 label change appended in result.csv"
# L1 CHANGE
awk -F, '{k=$1FS$2FS$4} NR==FNR{a[k]=$3;next}(k in a) && $3!= a[k]' temp.csv add.csv | awk -v d="$mydate" -F, 'BEGIN{OFS=","}{$5="L1change";$6=d; print}' >> result.csv
echo "if any, records of L1 label change appended in result.csv"
# L1L2 change
awk -F, '{k=$1FS$2} NR==FNR{a[k]=$3;b[k]=$4;next}(k in a) && $3!= a[k] && $4!= b[k]' temp.csv add.csv | awk -v d="$mydate" -F, 'BEGIN{OFS=","}{$5="L1L2change";$6=d; print}' >> result.csv
echo "if any, records of L1L2 label change appended in result.csv"
# L0L2 change
awk -F, '{k=$1FS$3} NR==FNR{a[k]=$2;b[k]=$4;next} (k in a) && $2!=a[k] && $4!=b[k]' temp.csv add.csv | awk -v d="$mydate" -F, 'BEGIN{OFS=","}{$5="L0L2change";$6=d; print}' >> result.csv
echo "if any, records of L0L2 label change appended in result.csv"
# L2 change only
awk -F, '{k=$1FS$2FS$3} NR==FNR{a[k]=$4;next}(k in a) && $4!=a[k]' temp.csv add.csv | awk -v d="$mydate" -F, 'BEGIN{OFS=","}{$5="L2change";$6=d; print}' >> result.csv
echo "if any, records of L2 label change appended in result.csv"
