#!/bin/bash

for i in 1 2 3 3.5 3.7 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21;
do
    echo "Test $i begin"
    java -jar ../adb.jar Test$i Output$i
    echo "Test $i done"
done